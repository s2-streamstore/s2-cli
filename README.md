<div align="center">
  <p>
    <!-- Light mode logo -->
    <a href="https://s2.dev#gh-light-mode-only">
      <img src="./assets/s2-black.png" height="60">
    </a>
    <!-- Dark mode logo -->
    <a href="https://s2.dev#gh-dark-mode-only">
      <img src="./assets/s2-white.png" height="60">
    </a>
  </p>

  <h1>S2 CLI</h1>

  <p>
    <!-- Crates.io -->
    <a href="https://crates.io/crates/streamstore-cli"><img src="https://img.shields.io/crates/v/streamstore-cli.svg" /></a>
    <!-- Github Actions (CI) -->
    <a href="https://github.com/s2-streamstore/s2-cli/actions?query=branch%3Amain++"><img src="https://github.com/s2-streamstore/s2-cli/actions/workflows/ci.yml/badge.svg" /></a>
    <!-- Discord (chat) -->
    <a href="https://discord.gg/vTCs7kMkAf"><img src="https://img.shields.io/discord/1209937852528599092?logo=discord" /></a>
    <!-- LICENSE -->
    <a href="./LICENSE"><img src="https://img.shields.io/github/license/s2-streamstore/s2-cli" /></a>
  </p>
</div>

Command Line Tool to interact with the
[S2 API](https://buf.build/streamstore/s2/docs/main:s2.v1alpha).

## Getting started

1. [Install](#installation) the S2 CLI using your preferred method.

1. Generate an authentication token by logging onto the web console at
   [s2.dev](https://s2.dev/dashboard) and set the token in CLI config:
   ```bash
   s2 config set --auth-token <YOUR AUTH TOKEN>
   ```

1. You're ready to run S2 commands!
   ```bash
   s2 list-basins
   ```

Head over to [S2 Docs](https://s2.dev/docs/quickstart) for a quick dive into
using the CLI.

## Commands and reference

You can add the `--help` flag to any command for CLI reference. Run `s2 --help`
to view all the supported commands and options.

> [!TIP]
> The `--help` command displays a verbose help message whereas the `-h` displays
> the same message in brief.

## Installation

### Using Homebrew

This method works on macOS and Linux distributions with
[Homebrew](https://brew.sh) installed.

```bash
brew install s2-streamstore/s2/s2
```

### Using Cargo

This method works on any system with [Rust](https://www.rust-lang.org/)
and [Cargo](https://doc.rust-lang.org/cargo/) installed.

```bash
cargo install streamstore-cli
```

### From Release Binaries

Check out the [S2 CLI Releases](https://github.com/s2-streamstore/s2-cli/releases)
for prebuilt binaries for many different architectures and operating systems.

Linux and macOS users can download the release binary using:

```bash
curl -fsSL s2.dev/install.sh | bash
```

To install a specific version, you can set the `VERSION` environment variable.

```bash
export VERSION=0.5.2
curl -fsSL s2.dev/install.sh | bash
```

## Feedback

We use [Github Issues](https://github.com/s2-streamstore/s2-cli/issues) to
track feature requests and issues with the SDK. If you wish to provide feedback,
report a bug or request a feature, feel free to open a Github issue.

### Contributing

Developers are welcome to submit Pull Requests on the repository. If there is
no tracking issue for the bug or feature request corresponding to the PR, we
encourage you to open one for discussion before submitting the PR.

## Reach out to us

Join our [Discord](https://discord.gg/vTCs7kMkAf) server. We would love to hear
from you.

You can also email us at [hi@s2.dev](mailto:hi@s2.dev).

## License

This project is licensed under the [Apache-2.0 License](./LICENSE).
