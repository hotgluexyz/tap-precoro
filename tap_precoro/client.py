"""REST client handling, including PrecoroStream base class."""

import requests
import json
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
        if "meta" in resp:
            if (
                resp["meta"]["pagination"]["current_page"]
                == resp["meta"]["pagination"]["total_pages"]
            ):
                self.page = None
            else:
                self.page += 1
        else:
            return None

        return self.page
    
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
