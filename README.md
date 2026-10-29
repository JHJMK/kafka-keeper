# Kafka Keeper

English | [简体中文](README.zh-CN.md)

Kafka Keeper is a native desktop Kafka client written entirely in Go. It uses the [MyGo](https://github.com/egoist/mygo) native UI framework and [franz-go](https://github.com/twmb/franz-go), with no HTML, JavaScript, or WebView.

## Features

- Manage multiple Kafka connections with TLS, SASL/PLAIN, SCRAM-SHA-256, and SCRAM-SHA-512 authentication.
- Select a custom CA certificate through the native file picker. The certificate is copied into Kafka Keeper's application data directory.
- Reach Kafka clusters that are only available inside a VPC through an SSH jump host.
- Confirm and pin the SSH host key on first connection; later connections verify the saved fingerprint.
- View brokers, topics, partitions, consumer groups, members, offsets, and lag from a cluster overview.
- Enable configurable automatic refresh, in seconds, on the Overview, Topics, and Consumer Groups pages.
- Inspect topic partition metadata, including leaders, replicas, start/end offsets, and message counts.
- View topic configuration entries.
- Browse messages without joining a consumer group, using offset, timestamp, or time-range queries.
- Browse a single partition or all partitions, with an optional message limit.
- Search messages by key or header using time and offset ranges, maximum scan count, and maximum result count.
- Use optional Key substring matching and exact header-key/header-value matching. Message values are not searched.
- Produce messages with an editable, prefix-filtered topic combobox, key, value, headers, compression, and timestamp.
- Choose automatic partitioning with the sticky partitioner or specify a partition manually.
- Use none, gzip, snappy, lz4, or zstd compression when producing messages.
- Filter consumer groups to show only groups with active members.
- View consumer group state, members, assignments, committed offsets, and lag.
- Start an explicit consumer session to join a consumer group, with earliest/latest reset behavior, optional automatic commits, manual commits, pause, resume, and stop.
- Work in a light blue and white native desktop interface with rounded panels and native date/time controls.

## Requirements

- Go 1.27 or newer
- macOS, Windows, or Linux

## Development

Run the application in development mode:

```bash
GOTOOLCHAIN=auto go run github.com/egoist/mygo/cmd/mygo@v0.7.3 dev .
```

Run tests and static checks:

```bash
make test
GOTOOLCHAIN=auto go test -race ./...
GOTOOLCHAIN=auto go vet ./...
```

## Build and Package

Package all supported platforms:

```bash
make package
```

Override the application version for one build:

```bash
make package VERSION=1.2.3
```

`VERSION` temporarily updates `mygo.json` and restores the original file when packaging finishes. Without an override, the version in `mygo.json` is used.

Build a specific platform or a combination of platforms:

```bash
make package PLATFORMS=darwin/universal
make package PLATFORMS=darwin/universal,linux/amd64
```

Build without running tests:

```bash
make build
```

The default platform set is macOS Universal (Apple Silicon and Intel), Windows amd64/arm64, and Linux amd64/arm64. Artifacts are written to `build/` and include macOS `.app`/`.dmg` packages, Windows executables/installers, and Linux executables, `.deb` packages, and `.tar.gz` archives. Windows installers require NSIS. Official macOS releases require code signing and notarization.

## Local Data and Security

Application data is stored under:

```text
~/kafka-keeper/
```

The main files are:

```text
connections.json
credentials.json
ca-certificates/
```

Kafka and SSH passwords are stored in `credentials.json`, which is written atomically with `0600` permissions. Kafka Keeper does not depend on the operating system keychain. The credentials file is not encrypted; its security depends on local file permissions and the security of the user account.

TLS verifies server certificates by default. When a custom CA is selected, Kafka Keeper copies it into the managed `ca-certificates/` directory so the connection does not depend on the original file path.

Do not commit, share, or expose the application data directory. It may contain cluster addresses and credentials.

SSH proxy support currently accepts password authentication only. Private-key authentication, SSH Agent, and multi-hop proxies are not supported. When an SSH proxy is enabled, Bootstrap Servers must use addresses reachable from the SSH host's network.

## Usage

1. Add a Kafka connection in the left sidebar and verify connectivity.
2. Open **Topics** to inspect partitions and configuration, browse messages, or search message keys and headers.
3. Open **Consumer Groups** to inspect group members and lag, or start an explicit consumer session to poll and commit offsets.
4. Open **Send Message** to choose or filter a topic, enter the message, and send it.

Only one active consumer session is allowed per Kafka connection at a time, preventing competing offset commits and consumer-state changes.
