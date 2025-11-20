# Variables
TAP_NAME := tap-precoro
CONFIG_FILE := .secrets/config.json
DISCOVER_OUTPUT := catalog.json
SELECTED_CATALOG := catalog-selected.json
DATA_OUTPUT := data.singer

# Default target
all: discover select run transfer

# Discover target
discover:
	$(TAP_NAME) --config $(CONFIG_FILE) --discover > $(DISCOVER_OUTPUT)

# Select target
select: $(DISCOVER_OUTPUT)
	singer-discover --input $(DISCOVER_OUTPUT) --output $(SELECTED_CATALOG)

# Run target
run: $(SELECTED_CATALOG)
	$(TAP_NAME) --config $(CONFIG_FILE) --catalog $(SELECTED_CATALOG) > $(DATA_OUTPUT)

# Clean target
clean:
	rm -f $(DISCOVER_OUTPUT) $(SELECTED_CATALOG) $(DATA_OUTPUT)

transfer:
	cat $(DATA_OUTPUT) | target-csv