"""Stream type classes for tap-precoro."""

from pathlib import Path
from typing import Any, Dict, Optional, Union, List, Iterable

from singer_sdk import typing as th  # JSON Schema typing helpers

from tap_precoro.client import PrecoroStream
from singer_sdk.helpers.jsonpath import extract_jsonpath
import requests


class TaxesStream(PrecoroStream):
    """Define custom stream."""

    name = "taxes"
    path = "/taxes"
    primary_keys = ["id"]
    replication_key = None

    schema = th.PropertiesList(
        th.Property("id", th.NumberType),
        th.Property("name", th.StringType),
        th.Property("percent", th.NumberType),
        th.Property("value", th.NumberType),
        th.Property("qboId", th.CustomType({"type": ["number", "string"]})),
        th.Property("taxAmount", th.NumberType),
        th.Property("externalId", th.CustomType({"type": ["number", "string"]})),
        th.Property("isWithholdingTax", th.BooleanType),
        th.Property("xeroId", th.CustomType({"type": ["number", "string"]})),
    ).to_dict()


class InvoicesStream(PrecoroStream):
    """Define custom stream."""

    name = "invoices"
    path = "/invoices"
    primary_keys = ["id"]
    replication_key = "updateDate"
    schema = th.PropertiesList(
        th.Property("id", th.NumberType),
        th.Property("idn", th.StringType),
        th.Property("status", th.NumberType),
        th.Property("updateDate", th.DateTimeType),
        th.Property("statusString", th.StringType),
    ).to_dict()

    def get_child_context(self, record: dict, context: Optional[dict]) -> dict:
        """Return a context dictionary for child streams."""
        return {
            "invoice_id": record["idn"],
        }

    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        data = extract_jsonpath(self.records_jsonpath, input=response.json())
        for row in data:
            # Only download approved invoices
            if row["status"] == 5:
                yield row


class InvoiceDetailsStream(PrecoroStream):
    """Define custom stream."""

    name = "invoices_details"
    path = "/invoices/{invoice_id}"
    primary_keys = ["id"]
    records_jsonpath = "$[*]"
    replication_key = None
    parent_stream_type = InvoicesStream
    schema = th.PropertiesList(
        th.Property("id", th.NumberType),
        th.Property("idn", th.StringType),
        th.Property("status", th.NumberType),
        th.Property("customName", th.StringType),
        th.Property("updateDate", th.DateTimeType),
        th.Property("createDate", th.DateTimeType),
        th.Property("requiredDate", th.DateTimeType),
        th.Property("issueDate", th.DateTimeType),
        th.Property("updateExchangeRateDate", th.DateTimeType),
        th.Property("sumPaid", th.StringType),
        th.Property("sumPaidInCompanyCurrency", th.CustomType({"type": ["number", "string"]})),
        th.Property("sum", th.CustomType({"type": ["number", "string"]})),
        th.Property("netSum", th.CustomType({"type": ["number", "string"]})),
        th.Property(
            "sumInCompanyCurrency", th.CustomType({"type": ["number", "string"]})
        ),
        th.Property(
            "netSumInCompanyCurrency", th.CustomType({"type": ["number", "string"]})
        ),
        th.Property("withholdingTaxSum", th.CustomType({"type": ["number", "string"]})),
        th.Property("currency", th.StringType),
        th.Property("precisionData", th.CustomType({"type": ["object", "string"]})),
        th.Property("note", th.StringType),
        th.Property("exchangeRate", th.CustomType({"type": ["object", "array"]})),
        th.Property("fromSupplier", th.BooleanType),
        th.Property("statusString", th.StringType),
        th.Property("logicType", th.CustomType({"type": ["number", "string"]})),
        th.Property("invoiceNumber", th.StringType),
        th.Property("deliveryNote", th.StringType),
        th.Property("toleranceRateSum", th.StringType),
        th.Property("toleranceRatePercent", th.StringType),
        th.Property("purchaseOrder", th.CustomType({"type": ["array", "object"]})),
        th.Property("prepaymentPercent", th.CustomType({"type": ["number", "string"]})),
        th.Property(
            "postpaymentPercent", th.CustomType({"type": ["number", "string"]})
        ),
        th.Property("creditPeriodDays", th.NumberType),
        th.Property("approvalStep", th.CustomType({"type": ["object", "string"]})),
        th.Property("paymentTerm", th.CustomType({"type": ["object", "string"]})),
        th.Property("company", th.CustomType({"type": ["object", "string"]})),
        th.Property("qboId", th.CustomType({"type": ["number", "string"]})),
        th.Property("externalId", th.CustomType({"type": ["number", "string"]})),
        th.Property("xeroId", th.CustomType({"type": ["number", "string"]})),
        th.Property("budgetedSum", th.CustomType({"type": ["number", "string"]})),
        th.Property("usedTaxPercentInBudget", th.StringType),
        th.Property(
            "allDocumentCustomFieldOptionsIds",
            th.CustomType({"type": ["number", "string"]}),
        ),
        th.Property("isRequiredTaxesForItems", th.BooleanType),
        th.Property("approvingWay", th.CustomType({"type": ["object", "array", "string"]})),
        th.Property("location", th.CustomType({"type": ["object", "string"]})),
        th.Property("supplier", th.CustomType({"type": ["object", "string"]})),
        th.Property("budget", th.CustomType({"type": ["array", "object"]})),
        th.Property("budgetLine", th.CustomType({"type": ["array", "object"]})),
        th.Property("legalEntity", th.CustomType({"type": ["array", "object"]})),
        th.Property("creator", th.CustomType({"type": ["object", "string"]})),
        th.Property("secondInCharge", th.CustomType({"type": ["array", "object"]})),
        th.Property("lastApprover", th.CustomType({"type": ["object", "array"]})),
        # th.Property("lastEditor", th.CustomType({"type": ["array", "string"]})),
        th.Property("approvalSteps", th.CustomType({"type": ["object", "array"]})),
        th.Property("items", th.CustomType({"type": ["object", "array"]})),
        th.Property("taxes", th.CustomType({"type": ["object", "array"]})),
        th.Property("comments", th.CustomType({"type": ["object", "array"]})),
        th.Property("payments", th.CustomType({"type": ["object", "array"]})),
        th.Property("followers", th.CustomType({"type": ["object", "array"]})),
        th.Property(
            "dataDocumentCustomFields", th.CustomType({"type": ["object", "array"]})
        ),
        th.Property("attachments", th.CustomType({"type": ["object", "array"]})),
        th.Property("allocatedInvoice", th.CustomType({"type": ["array", "array"]})),
        th.Property("contracts", th.CustomType({"type": ["object", "array"]})),
        th.Property("isBudgetOverLimit", th.BooleanType),
    ).to_dict()


class SuppliersStream(PrecoroStream):
    """Define custom stream."""

    name = "suppliers"
    path = "/suppliers"
    primary_keys = ["id"]
    # This stream supports replication but there is no updated key
    replication_key = None
    schema = th.PropertiesList(
        th.Property("id", th.NumberType),
        th.Property("name", th.StringType),
        th.Property("uniqueCode", th.StringType),
        th.Property("legalAddress", th.StringType),
        th.Property("phone", th.StringType),
        th.Property("city", th.StringType),
        th.Property("state", th.StringType),
        th.Property("postalCode", th.StringType),
        th.Property("postalCode", th.StringType),
        th.Property("createDate", th.DateTimeType),
    ).to_dict()


class ItemsStream(PrecoroStream):
    """Define custom stream."""

    name = "items"
    path = "/items"
    primary_keys = ["id"]
    replication_key = "updateDate"
    schema = th.PropertiesList(
        th.Property("id", th.NumberType),
        th.Property("name", th.StringType),
        th.Property("sku", th.StringType),
        th.Property("typeString", th.StringType),
        th.Property("description", th.StringType),
        th.Property("disabledBySupplier", th.BooleanType),
        th.Property("hiddenInCatalog", th.BooleanType),
        th.Property("freeOfCharge", th.BooleanType),
        th.Property("mainInSimilar", th.BooleanType),
        th.Property("category", th.CustomType({"type": ["object", "array"]})),
        th.Property("supplier", th.CustomType({"type": ["object", "array"]})),
        th.Property("similar", th.CustomType({"type": ["object", "array"]})),
        th.Property("marketProduct", th.CustomType({"type": ["object", "array"]})),
        th.Property(
            "dataProductCustomFields", th.CustomType({"type": ["object", "array"]})
        ),
        th.Property("bundleItems", th.CustomType({"type": ["object", "array"]})),
        th.Property("groupItems", th.CustomType({"type": ["object", "array"]})),
        th.Property("typeString", th.StringType),
        th.Property("externalId", th.StringType),
        th.Property("xeroId", th.StringType),
        th.Property("createDate", th.DateTimeType),
        th.Property("updateDate", th.DateTimeType),
    ).to_dict()
