"""Precoro tap class."""

from typing import List

from singer_sdk import Tap, Stream
from singer_sdk import typing as th  # JSON schema typing helpers

from tap_precoro.streams import (
    TaxesStream,
    InvoicesStream,
    InvoiceDetailsStream,
    SuppliersStream,
    ItemsStream,
    ExpensesStream,
    ExpensesDetailsStream,
    CreditNotesStream,
    CreditNoteDetailsStream
)

STREAM_TYPES = [
    TaxesStream,
    InvoicesStream,
    InvoiceDetailsStream,
    SuppliersStream,
    ItemsStream,
    ExpensesStream,
    ExpensesDetailsStream,
    CreditNotesStream,
    CreditNoteDetailsStream
]


class TapPrecoro(Tap):
    """Precoro tap class."""

    name = "tap-precoro"

    config_jsonschema = th.PropertiesList(
        th.Property(
            "auth_token",
            th.StringType,
            required=True,
        ),
        th.Property(
            "email",
            th.StringType,
            required=True,
        ),
        th.Property(
            "AccountSetup",
            th.ObjectType(
                th.Property("enabled", th.BooleanType),
                th.Property(
                    "legalEntity",
                    th.CustomType({"type": "object", "additionalProperties": {"type": "string"}}),
                ),
                th.Property("companyId", th.IntegerType),
                th.Property("secret", th.StringType),
                th.Property("url", th.StringType),
                th.Property("integrationType", th.StringType),
            ),
            description="Configuration for account setup mapping",
        ),
        th.Property(
            "fetch_supplier_details",
            th.BooleanType,
            default=False,
            description="If true, enrich suppliers with per-record /suppliers/{id} requests.",
        ),
    ).to_dict()

    def discover_streams(self) -> List[Stream]:
        """Return a list of discovered streams."""
        return [stream_class(tap=self) for stream_class in STREAM_TYPES]


if __name__ == "__main__":
    TapPrecoro.cli()
