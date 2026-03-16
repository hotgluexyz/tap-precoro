# tap-precoro Configuration

Reference for all configuration options supported by the Precoro Singer tap.

---

## Authentication

#### `auth_token` (string, required)
API authentication token for the Precoro API.
- **Example**: `"xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"`

#### `email` (string, required)
Email address associated with the Precoro account. Sent in request headers.
- **Example**: `"your-email@example.com"`

---

## API Configuration

#### `base_url` (string, optional)
Base URL for the Precoro API. If the value does not start with `https://`, it is prefixed automatically.
- **Default**: `"https://api.precoro.com"`
- **Example**: `"https://api.precoro.com"`

#### `user_agent` (string, optional)
Custom User-Agent string sent in HTTP requests. If omitted, the header is not set.
- **Example**: `"tap-precoro/0.0.1"`

#### `start_date` (string, optional)
Start date or datetime for incremental sync. Records modified on or after this time are synced. Used for `modifiedSince` query parameter.
- **Example**: `"2024-01-01"` or `"2024-01-01T00:00:00"`

---

## Invoices Stream

#### `approval_date` (string, optional)
Filter invoices by approval date. Only invoices approved on or after this date are fetched. Accepts date or datetime strings.
- **Example**: `"2024-01-01T00:00:00"`

#### `statuses` (string, optional)
Comma-separated list of invoice statuses to sync. When set, overrides the default (approved only). Ignored if `all_invoices` is `true`.
Valid values: `open`, `pending`, `approved`, `denied`, `partly_paid`, `paid`, `awaiting_confirmation`, `on_revise`, `canceled`, `pending_receipt`, `approval_review`, `closed`.
- **Example**: `"approved,paid"`

#### `all_invoices` (boolean, optional)
If `true`, fetches invoices in all statuses. If `false` or omitted, only approved invoices are fetched (unless `statuses` is set).
- **Default**: `false`
- **Example**: `true`

## Credit Notes Stream

#### `credit_note_statuses` (string, optional)
Comma-separated list of credit note statuses to sync. When set, it is applied only to `credit_notes` stream.
Valid values: `open`, `pending`, `approved`, `denied`, `partly_paid`, `paid`, `awaiting_confirmation`, `on_revise`, `canceled`, `pending_receipt`, `approval_review`, `closed`.
- **Example**: `"approved,paid"`

#### `export_condition` (array, optional)
List of document custom field conditions. Only invoices matching all conditions are synced. Each item must have `id` (custom field id) and `value` (string).
- **Example**: `[{"id": 12345, "value": "some_value"}]`

#### `exportOptions` (object, optional)
Legacy format for a single export condition. Use `export_condition` (array) for multiple conditions.
- **Property**: `export_condition` (object with `id` and `value`)
- **Example**: `{"export_condition": {"id": 12345, "value": "some_value"}}`

---

## Suppliers Stream

#### `supplier_status` (string, optional)
Comma-separated list of supplier statuses to sync. Valid values: `approved`, `pending`, `rejected`.
- **Example**: `"approved"`

---

## Minimal config.json (required options only)

```json
{
  "auth_token": "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",
  "email": "your-email@example.com"
}
```

---

## Complete config.json example

```json
{
  "auth_token": "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",
  "email": "your-email@example.com",
  "base_url": "https://api.precoro.com",
  "user_agent": "tap-precoro/0.0.1",
  "start_date": "2024-01-01T00:00:00",
  "approval_date": "2024-01-01T00:00:00",
  "statuses": "approved,paid",
  "credit_note_statuses": "approved",
  "all_invoices": false,
  "supplier_status": "approved",
  "export_condition": [
    {
      "id": 12345,
      "value": "some_value"
    }
  ]
}
```
