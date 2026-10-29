SHELL := /bin/sh

GO ?= go
MYGO_VERSION ?= v0.7.3
VERSION ?=
OUTPUT_DIR ?= build
PLATFORMS ?= darwin/universal,windows/amd64,windows/arm64,linux/amd64,linux/arm64

.DEFAULT_GOAL := help
.PHONY: help prepare test build package clean

help:
	@printf 'Kafka Keeper packaging targets:\n'
	@printf '  make package  Run tests and package all platforms\n'
	@printf '  make build    Package without running tests\n'
	@printf '  make package VERSION=1.2.3  Package with the specified app version\n'
	@printf '  make test     Run the test suite\n'
	@printf '  make clean    Remove %s\n' '$(OUTPUT_DIR)'
	@printf '\nPlatforms: %s\n' '$(PLATFORMS)'

test:
	GOTOOLCHAIN=auto CGO_ENABLED=0 $(GO) test ./...

prepare:
	@if ! command -v makensis >/dev/null 2>&1; then \
		case "$$(uname -s)" in \
			Darwin) \
				if ! command -v brew >/dev/null 2>&1; then \
					echo 'error: NSIS is required for Windows installers; install Homebrew or makensis' >&2; \
					exit 1; \
				fi; \
				echo 'Installing NSIS for Windows installers...'; \
				brew install makensis ;; \
			MINGW*|MSYS*|CYGWIN*) \
				: ;; \
			*) \
				echo 'error: NSIS (makensis) is required for Windows installers' >&2; \
				exit 1 ;; \
		esac; \
	fi

build: prepare
	@set -eu; \
	version_backup=; \
	version_tmp=; \
	cleanup() { \
		if [ -n "$$version_tmp" ]; then rm -f "$$version_tmp"; fi; \
		if [ -n "$$version_backup" ]; then mv "$$version_backup" mygo.json; fi; \
	}; \
	trap cleanup EXIT HUP INT TERM; \
	if [ -n "$(VERSION)" ]; then \
		case "$(VERSION)" in \
			*[!0-9A-Za-z.+-]*) \
				echo 'error: VERSION may contain only letters, digits, ".", "+", and "-"' >&2; \
				exit 1 ;; \
			esac; \
		version_backup=$$(mktemp "$(CURDIR)/.mygo.json.backup.XXXXXX"); \
		version_tmp=$$(mktemp "$(CURDIR)/.mygo.json.tmp.XXXXXX"); \
		cp mygo.json "$$version_backup"; \
		sed 's/"version"[[:space:]]*:[[:space:]]*"[^"]*"/"version": "$(VERSION)"/' mygo.json > "$$version_tmp"; \
		mv "$$version_tmp" mygo.json; \
		version_tmp=; \
	fi; \
	GOTOOLCHAIN=auto CGO_ENABLED=0 $(GO) run github.com/egoist/mygo/cmd/mygo@$(MYGO_VERSION) build \
		-platform '$(PLATFORMS)' \
		-o '$(OUTPUT_DIR)' \
		.

package: test
	@rm -rf '$(OUTPUT_DIR)'
	@$(MAKE) --no-print-directory build

clean:
	rm -rf '$(OUTPUT_DIR)'
