# tap-precoro

`tap-precoro` is a Singer tap for Precoro.

Built with the [Meltano Tap SDK](https://sdk.meltano.com) for Singer Taps.

## Installation

- [ ] `Developer TODO:` Update the below as needed to correctly describe the install procedure. For instance, if you do not have a PyPi repo, or if you want users to directly install from your git repo, you can modify this step as appropriate.

```bash
pipx install tap-precoro
```

## Configuration

### Accepted Config Options

A full list of supported settings, types, defaults, and examples is in [templates/README.md](templates/README.md). A sample [templates/config.json](templates/config.json) is provided with placeholder values for all options.

A full list of supported settings and capabilities for this
tap is available by running:

```bash
tap-precoro --about
```

### Account Setup Integration (Multi-Entity Mapping)

`tap-precoro` supports an optional data enrichment feature to map a single entity in Precoro to multiple entities (Legal Entities, Subsidiaries, Companies, etc.) in external accounting systems (like NetSuite, QuickBooks, Xero). This feature is particularly useful when working with suppliers or other records that have an `externalId` but need to be distributed across multiple external accounts.

**How it works:**
When this feature is enabled, the tap automatically intercepts each processed record (using a custom `AccountSetupMixin`). If the record contains an `externalId`, the tap makes an HTTP GET request to your Account Setup microservice endpoint:
`GET <url>/api/hotglue/account_setup?externalId=<externalId>`

If the microservice returns a success response, the tap injects an `accountSetupData` array field into the outgoing record before passing it to the target. This array contains configuration for how the record should be mapped externally (e.g., `companyId`, `legalEntityId`, `integrationType`, etc.).

**Configuration:**
To enable and configure this feature, add the `AccountSetup` object to your tap configuration dictionary (`config.json`):

```json
{
  ...
  "AccountSetup": {
    "enabled": true,
    "url": "http://your-microservice-url:8080"
  }
}
```

- `AccountSetup.enabled` (boolean): Set to `true` to turn on account setup enrichment. Defaults to `false`.
- `AccountSetup.url` (string): The base URL of your target Account Setup microservice.

### Configure using environment variables

This Singer tap will automatically import any environment variables within the working directory's
`.env` if the `--config=ENV` is provided, such that config values will be considered if a matching
environment variable is set either in the terminal context or in the `.env` file.

### Source Authentication and Authorization

- [ ] `Developer TODO:` If your tap requires special access on the source system, or any special authentication requirements, provide those here.

## Usage

You can easily run `tap-precoro` by itself or in a pipeline using [Meltano](https://meltano.com/).

### Executing the Tap Directly

```bash
tap-precoro --version
tap-precoro --help
tap-precoro --config CONFIG --discover > ./catalog.json
```

## Developer Resources

- [ ] `Developer TODO:` As a first step, scan the entire project for the text "`TODO:`" and complete any recommended steps, deleting the "TODO" references once completed.

### Initialize your Development Environment

```bash
pipx install poetry
poetry install
```

### Create and Run Tests

Create tests within the `tap_precoro/tests` subfolder and
  then run:

```bash
poetry run pytest
```

You can also test the `tap-precoro` CLI interface directly using `poetry run`:

```bash
poetry run tap-precoro --help
```

### Testing with [Meltano](https://www.meltano.com)

_**Note:** This tap will work in any Singer environment and does not require Meltano.
Examples here are for convenience and to streamline end-to-end orchestration scenarios._

Your project comes with a custom `meltano.yml` project file already created. Open the `meltano.yml` and follow any _"TODO"_ items listed in
the file.

Next, install Meltano (if you haven't already) and any needed plugins:

```bash
# Install meltano
pipx install meltano
# Initialize meltano within this directory
cd tap-precoro
meltano install
```

Now you can test and orchestrate using Meltano:

```bash
# Test invocation:
meltano invoke tap-precoro --version
# OR run a test `elt` pipeline:
meltano elt tap-precoro target-jsonl
```

### SDK Dev Guide

See the [dev guide](https://sdk.meltano.com/en/latest/dev_guide.html) for more instructions on how to use the SDK to 
develop your own taps and targets.
