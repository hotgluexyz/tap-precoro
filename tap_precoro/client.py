"""REST client handling, including PrecoroStream base class."""

import requests
from pathlib import Path
from typing import Any, Dict, Optional, Union, List, Iterable

from memoization import cached

from singer_sdk.streams import RESTStream
from singer_sdk.authenticators import APIKeyAuthenticator
from pendulum import parse


class PrecoroStream(RESTStream):
    """Precoro stream class."""

    page = 1
    url_base = "https://api.precoro.com"

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
