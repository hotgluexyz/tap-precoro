"""Stream type classes for tap-precoro."""

import time
from datetime import datetime
from typing import Optional, Iterable

import requests
from singer_sdk import typing as th  # JSON Schema typing helpers
from pendulum import parse

from tap_precoro.client import PrecoroStream, ExternalIdTwoPassMixin, AccountSetupMixin


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


class TransactionsStream(PrecoroStream):

    schema = th.PropertiesList(
        th.Property("id", th.NumberType),
        th.Property("idn", th.StringType),
        th.Property("status", th.NumberType),
        th.Property("updateDate", th.DateTimeType),
        th.Property("statusString", th.StringType),
    ).to_dict()

    def get_statuses_config(self) -> Optional[str]:
        return self.config.get("statuses")


    def get_approval_date(self) -> Optional[datetime]:
        approval_date = self.config.get("approval_date")
        if not approval_date:
            return None
        try:
            return parse(approval_date)
        except (ValueError, TypeError):
            self.logger.warning(f"Invalid approval date format: {approval_date!r}")
            return None
    
    def get_url_params(self, context, next_page_token):
        params = super().get_url_params(context, next_page_token)

        # status mapp
        invoice_status = {
            "open": 0,
            "pending": 1,
            "approved": 2,
            "denied": 3,
            "partly_paid": 4,
            "paid": 5,
            "awaiting_confirmation": 6,
            "on_revise": 7,
            "canceled": 8,
            "pending_receipt": 9,
            "approval_review": 10,
            "closed": 11
        }
        statuses = self.get_statuses_config()

        # Fetch only approved invoices by default
        params["status[]"] = 2
        # Fetch all invoices if flag all_invoices
        if self.config.get("all_invoices"):
            self.logger.info("Flag all_invoices on, fetching all status invoices.")
            del params["status[]"]
        # Fetch invoices with statuses in config statuses flag
        elif statuses:
            statuses = statuses.split(",")
            statuses = [status.strip() for status in statuses]
            self.logger.info(
                f"Status flag found in config file fetching {self.name} with status in {statuses}."
            )
            statuses = [invoice_status.get(status.lower()) for status in statuses if status in invoice_status]
            params["status[]"] = statuses

        # Add approval date filtering
        approval_date = self.get_approval_date()
        if approval_date:
            params["approvalLeftDate"] = approval_date.strftime('%Y-%m-%dT%H:%M:%S')

        return params


class InvoicesStream(ExternalIdTwoPassMixin, TransactionsStream):
    """Define custom stream."""

    name = "invoices"
    path = "/invoices"
    primary_keys = ["id"]
    replication_key = "updateDate"
    export_conditions = None

    def get_url_params(self, context, next_page_token):
        params = super().get_url_params(context, next_page_token)
        # Second pass: fetch records without externalId (sent_to_external=0)
        if getattr(self, "_fetch_no_external_only", False):
            start_date = self.config.get("start_date")
            params["modifiedSince"] = start_date
            params["sent_to_external"] = 0
        return params

    def get_child_context(self, record: dict, context: Optional[dict]) -> dict:
        """Return a context dictionary for child streams."""
        return {
            "invoice_id": record["idn"],
        }
    
    def post_process(self, row, context):
        row = super().post_process(row, context)
        
        # Filter by perantIdn: only keep invoices where there are no parentIdn
        parent_idn = row.get("parentIdn")
        if parent_idn:
            self.logger.info(
                f"Invoice with id {row['id']} skipped because parenIdn: '{parent_idn}'"
            )
            return None
        
        if self.export_conditions is None:
            export_conditions = []

            # Check for new format first: "export_condition" (array)
            new_format_conditions = self.config.get("export_condition", [])
            if new_format_conditions:
                export_conditions = new_format_conditions
                
            # Check for old format: "exportOptions.export_condition" (single object) 
            old_format_condition = self.config.get("exportOptions", {}).get("export_condition")
            if old_format_condition and not export_conditions:
                export_conditions = [old_format_condition]
            
            if export_conditions:
                self.logger.info("Export conditions found in config file, filtering invoices...")
                self.export_conditions = []
                for condition in export_conditions:
                    try:
                        self.export_conditions.append({
                            "id": int(condition.get("id")),
                            "value": str(condition.get("value"))
                        })
                    except Exception as e:
                        raise Exception(f"Error while processing export condition: {e}")
            else:
                self.export_conditions = []
        
        if self.export_conditions:
            record_dcf = row.get("dataDocumentCustomFields", {}).get("data", [])
            for condition in self.export_conditions:
                record_dcf_ec = [
                    dcf
                    for dcf in record_dcf
                    if dcf.get("documentCustomField", {}).get("id") == condition["id"]
                ]
                if not record_dcf_ec or record_dcf_ec[0].get("value") != condition["value"]:
                    self.logger.debug(
                        f"Invoice with id {row['id']} skipped because it didn't match export condition with id {condition['id']}"
                    )
                    return None
        
        return row


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
        th.Property("approvalDate", th.DateTimeType),
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
        th.Property("allDocumentCustomFieldOptionsIds", th.StringType),
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
        th.Property("allocatedInvoice", th.CustomType({"type": ["object", "array"]})),
        th.Property("contracts", th.CustomType({"type": ["object", "array"]})),
        th.Property("isBudgetOverLimit", th.BooleanType),
    ).to_dict()


class SuppliersStream(AccountSetupMixin, ExternalIdTwoPassMixin, PrecoroStream):
    """Define custom stream."""

    name = "suppliers"
    path = "/suppliers"
    primary_keys = ["id"]
    replication_key = "updateDate"
    schema = th.PropertiesList(
        th.Property("id", th.NumberType),
        th.Property("uniqueCode", th.StringType),
        th.Property("name", th.StringType),
        th.Property("createDate", th.DateTimeType),
        th.Property("updateDate", th.DateTimeType),
        th.Property("approvalDate", th.DateTimeType),
        th.Property("legalAddress", th.StringType),
        th.Property("currency", th.StringType),
        th.Property("autoSendPOSupplier", th.BooleanType),
        th.Property("deliveryPeriod", th.NumberType),
        th.Property("minimumSum", th.NumberType),
        th.Property("businessRegistrationNumber", th.StringType),
        th.Property("accountHolderName", th.StringType),
        th.Property("bankName", th.StringType),
        th.Property("accountNumber", th.StringType),
        th.Property("bankAddress", th.StringType),
        th.Property("swiftCode", th.StringType),
        th.Property("permanentAccountNumber", th.StringType),
        th.Property("internationalBankAccountNumber", th.StringType),
        th.Property("americanBankersAssociationNumber", th.StringType),
        th.Property("indianFinancialSystemCode", th.StringType),
        th.Property("sortCode", th.StringType),
        th.Property("taxPayer", th.BooleanType),
        th.Property("currencies", th.ArrayType(th.StringType)),
        th.Property("taxPayerType", th.NumberType),
        th.Property("taxPayerLabel", th.StringType),
        th.Property("taxPayerNumber", th.StringType),
        th.Property("phone", th.StringType),
        th.Property("city", th.StringType),
        th.Property("country", th.StringType),
        th.Property("state", th.StringType),
        th.Property("postalCode", th.StringType),
        th.Property("note", th.StringType),
        th.Property("conditions", th.StringType),
        th.Property("enableToleranceRate", th.BooleanType),
        th.Property("toleranceRatePercent", th.NumberType),
        th.Property("enable", th.BooleanType),
        th.Property("isMarketUpdatable", th.BooleanType),
        th.Property("qboId", th.StringType),
        th.Property("netSuiteId", th.CustomType({"type": ["number", "string"]})),
        th.Property("integrationStatus", th.CustomType({"type": ["object", "string", "array", "number"]})),
        th.Property("externalId", th.CustomType({"type": ["number", "string"]})),
        th.Property("accountSetupData", th.ArrayType(
            th.ObjectType(
                th.Property("companyId", th.IntegerType),
                th.Property("integrationId", th.StringType),
                th.Property("integrationType", th.StringType),
                th.Property("legalEntityId", th.IntegerType),
                th.Property("mapField", th.StringType),
                th.Property("name", th.StringType),
                th.Property("precoroId", th.IntegerType),
            )
        )),
        th.Property("xeroId", th.StringType),
        th.Property("marketSupplier", th.ObjectType(
            th.Property("id", th.IntegerType),    
            th.Property("minimumSum", th.CustomType({"type": ["number", "string"]})),
        )),
        th.Property("enableMarketSupplier", th.BooleanType),
        th.Property("creditBalanceSums", th.CustomType({"type": ["object", "array"]})),
        th.Property("afaxysSupplierId", th.StringType),
        th.Property("status", th.IntegerType),
        th.Property("creator", th.ObjectType(
            th.Property("id", th.IntegerType),    
        )),
        th.Property("enterInvoiceAsOneLine", th.BooleanType),
        th.Property("paymentTerms", th.ObjectType(
            th.Property("data", th.ArrayType(
                th.ObjectType(
                    th.Property("id", th.NumberType),
                    th.Property("name", th.StringType),
                    th.Property("prepaymentPercent", th.NumberType),
                    th.Property("postpaymentPercent", th.NumberType),
                    th.Property("creditPeriodDays", th.NumberType),
                    th.Property("termsDueDateAfterInvoicing", th.NumberType),
                    th.Property("paymentType", th.NumberType),
                    th.Property("enable", th.BooleanType),
                    th.Property("externalId", th.CustomType({"type": ["number", "string"]})),
                )
            )),
        )),
        th.Property("approvalSteps", th.ObjectType(
           th.Property("data", th.ArrayType(th.CustomType({"type": ["object", "array"]}))), 
        )),
        th.Property("approvingWay", th.CustomType({"type": ["object", "array", "string"]})),
        th.Property("contacts", th.ObjectType(
           th.Property("data", th.ArrayType(th.CustomType({"type": ["object", "array"]}))), 
        )),
        th.Property("marketContacts", th.ObjectType(
           th.Property("data", th.ArrayType(th.CustomType({"type": ["object", "array", "string"]}))), 
        )),
        th.Property("supplierRegistration", th.CustomType({"type": ["object", "array", "string"]})),
        th.Property("supplierRegistrations", th.CustomType({"type": ["object", "array"]})),
        th.Property("attachments", th.CustomType({"type": ["object", "array"]})),
        th.Property("marketAttachments", th.CustomType({"type": ["object", "array"]})),
        th.Property("approvalInfo", th.ObjectType(
            th.Property("canApprove", th.BooleanType),
            th.Property("canReject", th.BooleanType),
            th.Property("canEditSupplierByInRevision", th.BooleanType),
        )),
        th.Property("dataSupplierCustomFields", th.CustomType({"type": ["object", "string"]})),
        th.Property("propsVisibility", th.CustomType({"type": ["object", "array"]})),
        th.Property("supplierTaxDefaultOptions", th.CustomType({"type": ["object", "array"]})),
        th.Property("supplierICFDefaultOptions", th.CustomType({"type": ["object", "array"]})),
        th.Property("supplierDCFDefaultOptions", th.CustomType({"type": ["object", "array"]})),
        th.Property("supplierCustomFieldOptionValues", th.CustomType({"type": ["object", "array"]})),
        th.Property("supplierToleranceLimitsSetup", th.CustomType({"type": ["object", "array"]})),
        th.Property("showApprovingWay", th.CustomType({"type": ["object", "array", "string"]})),
        th.Property("showApprovalReviewers", th.CustomType({"type": ["object", "array", "string"]})),
        th.Property("substitute", th.CustomType({"type": ["object", "array", "string"]})),
        th.Property("supplierLegalEntities", th.CustomType({"type": ["object", "array"]})),
        th.Property("voters", th.CustomType({"type": ["object", "array"]})),
        th.Property("isEnabledApproval", th.BooleanType),
        th.Property("accessToDocuments", th.CustomType({"type": ["object", "array"]})),
        th.Property("isPunchoutSupplier", th.BooleanType),
        th.Property("isAllAccessLegalEntity", th.BooleanType),
    ).to_dict()

    def _fetch_supplier_details(self, supplier_id) -> dict:
        headers = dict(self.http_headers)
        headers["X-AUTH-TOKEN"] = self.config.get("auth_token")

        time.sleep(1)

        started_at = time.perf_counter()

        response = self.requests_session.get(
            f"{self.url_base}/suppliers/{supplier_id}",
            headers=headers,
        )
        self.validate_response(response)
        self.logger.info(
            "Supplier details request endpoint=/suppliers/%s status=%s duration=%.3fs",
            supplier_id,
            response.status_code,
            time.perf_counter() - started_at,
        )
        payload = response.json()
        return payload if isinstance(payload, dict) else {}

    def request_records(self, context: Optional[dict]) -> Iterable[dict]:
        for supplier in super().request_records(context):
            supplier_id = supplier.get("id")

            try:
                supplier_details = self._fetch_supplier_details(supplier_id)
            except (requests.RequestException, ValueError) as exc:
                self.logger.warning(
                    "Failed to fetch supplier details for id %s: %s",
                    supplier_id,
                    exc,
                )
                yield supplier
                continue

            if not supplier_details:
                yield supplier
                continue

            supplier_details.setdefault("id", supplier_id)
            supplier_details.setdefault("updateDate", supplier.get("updateDate"))
            yield supplier_details

    def get_url_params(self, context, next_page_token):
        params = super().get_url_params(context, next_page_token)
        supplier_status = self.config.get("supplier_status")

        if supplier_status:
            # status map
            sup_status_map = {"approved": 2, "pending": 1, "rejected": 3}

            # Fetch invoices with statuses in config statuses flag
            statuses = supplier_status.split(",")
            statuses = [status.strip() for status in statuses]
            self.logger.info(
                f"Status flag found in config file fetching suppliers with status in {statuses}."
            )
            statuses = [
                sup_status_map.get(status.lower())
                for status in statuses
                if status in sup_status_map
            ]
            params["status[]"] = statuses

        # Second pass: suppliers without externalId (externalIntegrated=0, enable=1)
        if getattr(self, "_fetch_no_external_only", False):
            start_date = self.config.get("start_date")
            params["modifiedSince"] = start_date
            params["externalIntegrated"] = 0
            params["enable"] = 1

        return params


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
        th.Property("price", th.NumberType),
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
        th.Property("type", th.IntegerType),
        th.Property("externalId", th.StringType),
        th.Property("xeroId", th.StringType),
        th.Property("createDate", th.DateTimeType),
        th.Property("updateDate", th.DateTimeType),
    ).to_dict()


class ExpensesStream(TransactionsStream):
    """Define custom stream."""

    name = "expenses"
    path = "/expenses"
    primary_keys = ["id"]
    replication_key = "updateDate"

    def get_child_context(self, record: dict, context: Optional[dict]) -> dict:
        """Return a context dictionary for child streams."""
        return {
            "expense_idn": record["idn"],
        }


class ExpensesDetailsStream(PrecoroStream):
    """Define custom stream."""

    name = "expenses_details"
    path = "/expenses/{expense_idn}"
    primary_keys = ["id"]
    records_jsonpath = "$[*]"
    parent_stream_type = ExpensesStream
    schema = th.PropertiesList(
        th.Property("id", th.IntegerType),
        th.Property("idn", th.StringType),
        th.Property("customName", th.StringType),
        th.Property("updateDate", th.DateTimeType),
        th.Property("createDate", th.DateTimeType),
        th.Property("requiredDate", th.DateTimeType),
        th.Property("issueDate", th.DateTimeType),
        th.Property("approvalDate", th.DateTimeType),
        th.Property("sumPaid", th.NumberType),
        th.Property("sumPaidInCompanyCurrency", th.NumberType),
        th.Property("sum", th.NumberType),
        th.Property("netSum", th.NumberType),
        th.Property(
            "sumInCompanyCurrency", th.NumberType),
        th.Property(
            "netSumInCompanyCurrency", th.NumberType),
        th.Property("withholdingTaxSum", th.NumberType),
        th.Property("currency", th.StringType),
        th.Property("precisionData", th.CustomType({"type": ["object", "string"]})),
        th.Property("note", th.StringType),
        th.Property("exchangeRate", th.CustomType({"type": ["object", "array"]})),
        th.Property("status", th.IntegerType),
        th.Property("expenseNumber", th.StringType),
        th.Property("budgetedSum", th.NumberType),
        th.Property("usedTaxPercentInBudget", th.StringType),
        th.Property("allDocumentCustomFieldOptionsIds", th.StringType),
        th.Property("qboId", th.StringType),
        th.Property("approvingWay", th.CustomType({"type": ["object", "array", "string"]})),
        th.Property("location", th.CustomType({"type": ["object", "string"]})),
        th.Property("budget", th.CustomType({"type": ["array", "object"]})),
        th.Property("creator", th.CustomType({"type": ["object", "string"]})),
        th.Property("lastEditor", th.CustomType({"type": ["object", "string", "array"]})),
        th.Property("legalEntity", th.CustomType({"type": ["array", "object"]})),
        th.Property("approvalSteps", th.CustomType({"type": ["object", "array"]})),
        th.Property("items", th.CustomType({"type": ["object", "array"]})),
        th.Property("taxes", th.CustomType({"type": ["object", "array"]})),
        th.Property("comments", th.CustomType({"type": ["object", "array"]})),
        th.Property("expensePayments", th.CustomType({"type": ["object", "array"]})),
        th.Property("followers", th.CustomType({"type": ["object", "array"]})),
        th.Property(
            "dataDocumentCustomFields", th.CustomType({"type": ["object", "array"]})
        ),
        th.Property("attachments", th.CustomType({"type": ["object", "array"]})),
        th.Property("isBudgetOverLimit", th.BooleanType),
    ).to_dict()


class CreditNotesStream(ExternalIdTwoPassMixin, TransactionsStream):
    """Credit notes stream. Same as invoices but with logicType[]=1 and no parentIdn filtering."""

    name = "credit_notes"
    path = "/invoices"
    primary_keys = ["id"]
    replication_key = "updateDate"
    export_conditions = None

    def get_statuses_config(self) -> Optional[str]:
        return self.config.get("credit_note_statuses")

    def get_url_params(self, context, next_page_token):
        params = super().get_url_params(context, next_page_token)
        
        # Param to fetch only creditNote type from invoices endpoints
        params["logicType[]"] = [1,5]
        
        # Second pass: fetch records without externalId (sent_to_external=0)
        if getattr(self, "_fetch_no_external_only", False):
            start_date = self.config.get("start_date")
            params["modifiedSince"] = start_date
            params["sent_to_external"] = 0
        return params

    def get_child_context(self, record: dict, context: Optional[dict]) -> dict:
        """Return a context dictionary for child streams."""
        return {
            "credit_note_id": record["idn"],
        }

    def post_process(self, row, context):
        row = super().post_process(row, context)

        if self.export_conditions is None:
            export_conditions = []

            # Check for new format first: "export_condition" (array)
            new_format_conditions = self.config.get("export_condition", [])
            if new_format_conditions:
                export_conditions = new_format_conditions

            # Check for old format: "exportOptions.export_condition" (single object)
            old_format_condition = self.config.get("exportOptions", {}).get("export_condition")
            if old_format_condition and not export_conditions:
                export_conditions = [old_format_condition]

            if export_conditions:
                self.logger.info("Export conditions found in config file, filtering credit notes...")
                self.export_conditions = []
                for condition in export_conditions:
                    try:
                        self.export_conditions.append({
                            "id": int(condition.get("id")),
                            "value": str(condition.get("value"))
                        })
                    except Exception as e:
                        raise Exception(f"Error while processing export condition: {e}")
            else:
                self.export_conditions = []

        if self.export_conditions:
            record_dcf = row.get("dataDocumentCustomFields", {}).get("data", [])
            for condition in self.export_conditions:
                record_dcf_ec = [
                    dcf
                    for dcf in record_dcf
                    if dcf.get("documentCustomField", {}).get("id") == condition["id"]
                ]
                if not record_dcf_ec or record_dcf_ec[0].get("value") != condition["value"]:
                    self.logger.debug(
                        f"Credit note with id {row['id']} skipped because it didn't match export condition with id {condition['id']}"
                    )
                    return None

        return row


class CreditNoteDetailsStream(PrecoroStream):
    """Credit note details stream."""

    name = "credit_notes_details"
    path = "/invoices/{credit_note_id}"
    primary_keys = ["id"]
    records_jsonpath = "$[*]"
    replication_key = None
    parent_stream_type = CreditNotesStream
    schema = th.PropertiesList(
        th.Property("id", th.NumberType),
        th.Property("idn", th.StringType),
        th.Property("status", th.NumberType),
        th.Property("approvalDate", th.DateTimeType),
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
        th.Property("allDocumentCustomFieldOptionsIds", th.StringType),
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
        th.Property("allocatedInvoice", th.CustomType({"type": ["object", "array"]})),
        th.Property("contracts", th.CustomType({"type": ["object", "array"]})),
        th.Property("isBudgetOverLimit", th.BooleanType),
    ).to_dict()
