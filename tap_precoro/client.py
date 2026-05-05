"""REST client handling, including PrecoroStream base class and stream mixins."""

import requests
import json
import hmac
import hashlib
from pathlib import Path
from typing import Any, Dict, Optional, Union, List, Iterable
import time
from memoization import cached
import pendulum
from singer_sdk.streams import RESTStream
from singer_sdk.authenticators import APIKeyAuthenticator
from singer_sdk.exceptions import RetriableAPIError, FatalAPIError
from pendulum import parse
import backoff


class PrecoroStream(RESTStream):
    """Precoro stream class."""

    page = 1

    @property
    def url_base(self) -> str:
        url = self.config.get("base_url", "https://api.precoro.com")
        if not url.startswith("https://"):
            url = f"https://{url}"
        return url

    records_jsonpath = "$.data[*]"
    next_page_token_jsonpath = "$.next_page"  # Or override `get_next_page_token`.

    @property
    def authenticator(self) -> APIKeyAuthenticator:
        """Return a new authenticator object."""
        return APIKeyAuthenticator.create_for_stream(
            self,
            key="X-AUTH-TOKEN",
            value=self.config.get("auth_token"),
            location="header",
        )

    @property
    def http_headers(self) -> dict:
        """Return the http headers needed."""
        headers = {}
        if "user_agent" in self.config:
            headers["User-Agent"] = self.config.get("user_agent")
        headers["email"] = self.config.get("email")

        return headers

    def get_next_page_token(
        self, response: requests.Response, previous_token: Optional[Any]
    ) -> Optional[Any]:
        """Return a token for identifying next page or None if no more pages."""
        resp = response.json()
        pagination = resp.get("meta", {}).get("pagination", {})

        has_next_page = pagination.get("has_next_page")
        if isinstance(has_next_page, bool):
            if has_next_page:
                self.page += 1
            else:
                self.page = None
            return self.page

        # Backward compatibility for older payloads.
        current_page = pagination.get("current_page")
        total_pages = pagination.get("total_pages")
        if current_page is not None and total_pages is not None:
            if current_page == total_pages:
                self.page = None
            else:
                self.page += 1
            return self.page

        return None
    
    def get_starting_time(self, context):
        start_date = self.config.get("start_date")
        if start_date:
            start_date = parse(self.config.get("start_date"))
        rep_key = self.get_starting_timestamp(context)
        return rep_key or start_date

    def get_url_params(
        self, context: Optional[dict], next_page_token: Optional[Any]
    ) -> Dict[str, Any]:
        """Return a dictionary of values to be used in URL parameterization."""
        params: dict = {}
        if next_page_token:
            params["page"] = next_page_token

        start_date = self.get_starting_time(context)
        if self.replication_key and start_date:
            params["modifiedSince"] = start_date.strftime('%Y-%m-%dT%H:%M:%S') 
        return params

    def request_decorator(self, func):
        decorator = backoff.on_exception(
            self.backoff_wait_generator,
            (
                RetriableAPIError,
                requests.exceptions.RequestException,
            ),
            max_tries=self.backoff_max_tries,
            on_backoff=self.backoff_handler,
        )(func)
        return decorator

    def _request(
        self, prepared_request: requests.PreparedRequest, context: Optional[dict]
    ) -> requests.Response:
        # Precoro has a rate limit of 1 request per second
        time.sleep(1)
        return super()._request(prepared_request, context)

    def validate_response(self, response: requests.Response) -> None:
        if response.status_code == 429 and 'RateLimit-Type' in response.text:
            seconds_to_wait = 1
            try:
                retry_info_json = response.json()
                rate_limit_type = retry_info_json.get("RateLimit-Type")
                if rate_limit_type == "Daily limiter":
                    self.logger.error("Daily rate limit hit. Exiting.")
                    raise FatalAPIError("Daily rate limit hit. Exiting.")

                datetime_str = retry_info_json.get('RateLimit-Retry-After')
                retry_after = pendulum.from_format(datetime_str, 'YYYY-MM-DD HH:mm:ss z')
                seconds_to_wait = (retry_after - pendulum.now()).total_seconds()
                seconds_to_wait = max(seconds_to_wait, 1)
            except Exception as e:
                self.logger.warning(f"Could not parse retry info: {response.text}")
                self.logger.warning(f"Error parsing retry info: {e}")
            self.logger.info(f"Waiting {seconds_to_wait} seconds before retrying")
            time.sleep(seconds_to_wait)
            raise RetriableAPIError("Rate limit exceeded", response=response)
        super().validate_response(response)


class AccountSetupMixin:
    """Mixin to support Account Setup mode, where one Precoro supplier maps to multiple LegalEntities in external systems."""
    
    def _get_account_setup_headers(self, account_setup: dict, payload: dict = None) -> dict:
        """Generate Authorization signature and headers for AccountSetup microservice."""
        secret = str(account_setup.get("secret"))
        company_id = str(account_setup.get("companyId", ""))

        # Signature must use compact JSON for non-GET and empty payload for GET.
        payload_json = json.dumps(payload, separators=(",", ":")) if payload is not None else ""

        string_to_sign = f"{payload_json}.{company_id}"
        signature = hmac.new(bytes(secret, 'UTF-8'), string_to_sign.encode(), hashlib.sha256).hexdigest()

        headers = {
            "X-PRECORO-AUTH": signature,
            "X-COMPANY-ID": company_id
        }
        return headers

    def post_process(self, row: dict, context: Optional[dict] = None) -> Optional[dict]:
        # Process through other inheritances first
        if hasattr(super(), "post_process"):
            row = super().post_process(row, context) or row
            
        if not row:
            return row
            
        account_setup = self.config.get("AccountSetup", {})
        if account_setup.get("enabled") and row.get("externalId"):
            external_id = row["externalId"]
            base_url = account_setup.get("url").rstrip("/")
            url = f"{base_url}/api/hotglue/account_setup"
            
            try:
                self.logger.info(f"Fetching Account Setup data for externalId {external_id} from {url}")
                headers = self._get_account_setup_headers(account_setup)
                response = requests.get(url, params={"externalId": external_id}, headers=headers, timeout=10)
                response.raise_for_status()
                data = response.json()
                
                records = data.get("records")
                if data.get("isSuccess") and records:
                    row["accountSetupData"] = records
            except Exception as e:
                self.logger.warning(f"Failed to fetch account setup data for externalId {external_id}: {e}")
                
        return row


class ExternalIdTwoPassMixin:
    """Mixin for streams that fetch incremental records first, then records without externalId; yields deduplicated results."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        if self.config.get("fetch_unexported", False):
            start_date = self.config.get("start_date")
            if not start_date or start_date == "2000-01-01T00:00:00Z":
                raise Exception("Please set start_date param if you want to use fetch_unexported")

    def request_records(self, context: Optional[dict]) -> Iterable[dict]:
        seen_ids = set()
        for record in super().request_records(context):
            seen_ids.add(record["id"])
            yield record
        self.logger.info(f"{self.name.capitalize()} from incremental sync: {len(seen_ids)}")

        if not self.config.get("fetch_unexported", False):
            return

        self._fetch_no_external_only = True
        self.page = 1
        pass2_count = 0
        try:
            for record in super().request_records(context):
                if record["id"] in seen_ids:
                    continue
                pass2_count += 1
                yield record
        finally:
            self._fetch_no_external_only = False
        self.logger.info(f"{self.name.capitalize()} without externalId: {pass2_count}")
