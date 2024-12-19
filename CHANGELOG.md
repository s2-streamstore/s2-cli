# Changelog

All notable changes to this project will be documented in this file.

## [0.6.3] - 2024-12-19

### Documentation

- Update README API link ([#89](https://github.com/s2-streamstore/s2-cli/issues/89))

### Miscellaneous Tasks

- Upgrade SDK to `0.5.0` ([#90](https://github.com/s2-streamstore/s2-cli/issues/90))

## [0.6.2] - 2024-12-18

### Bug Fixes

- Update output for reconfigure basin and create basin results ([#86](https://github.com/s2-streamstore/s2-cli/issues/86))

### Miscellaneous Tasks

- Add `README.md` ([#83](https://github.com/s2-streamstore/s2-cli/issues/83))

## [0.6.1] - 2024-12-17

### Miscellaneous Tasks

- Update cargo binary name to `s2` ([#84](https://github.com/s2-streamstore/s2-cli/issues/84))
- *(release)* Upgrade SDK to 0.4.0 ([#85](https://github.com/s2-streamstore/s2-cli/issues/85))
- *(release)* Upgrade SDK to 0.4.1 ([#87](https://github.com/s2-streamstore/s2-cli/issues/87))

## [0.6.0] - 2024-12-14

### Features

- Support `s2://` URIs ([#74](https://github.com/s2-streamstore/s2-cli/issues/74))
- Better display for ping stats ([#81](https://github.com/s2-streamstore/s2-cli/issues/81))

### Bug Fixes

- Disable noisy description in help ([#79](https://github.com/s2-streamstore/s2-cli/issues/79))

### Miscellaneous Tasks

- Remove unnecessary dependencies from `Cargo.toml` ([#80](https://github.com/s2-streamstore/s2-cli/issues/80))

## [0.5.2] - 2024-12-13

### Miscellaneous Tasks

- Rename binary to s2 when releasing ([#76](https://github.com/s2-streamstore/s2-cli/issues/76))

## [0.5.1] - 2024-12-13

### Features

- Homebrew sync ([#71](https://github.com/s2-streamstore/s2-cli/issues/71))

## [0.5.0] - 2024-12-11

### Bug Fixes

- Use a different `std::thread::Thread` for `Stdin` IO ([#69](https://github.com/s2-streamstore/s2-cli/issues/69))

### Miscellaneous Tasks

- Release to crates.io ([#68](https://github.com/s2-streamstore/s2-cli/issues/68))

## [0.4.0] - 2024-12-11

### Features

- Allow append concurrency control on `fence` and `trim` too ([#60](https://github.com/s2-streamstore/s2-cli/issues/60))
- Ping ([#48](https://github.com/s2-streamstore/s2-cli/issues/48)) ([#63](https://github.com/s2-streamstore/s2-cli/issues/63))

### Bug Fixes

- Usage example

### Documentation

- Clarify fencing token is in hex

### Miscellaneous Tasks

- Mandatory read `start_seq_num` ([#58](https://github.com/s2-streamstore/s2-cli/issues/58))
- Make all short args explicit ([#29](https://github.com/s2-streamstore/s2-cli/issues/29)) ([#59](https://github.com/s2-streamstore/s2-cli/issues/59))
- Upgrade deps ([#64](https://github.com/s2-streamstore/s2-cli/issues/64))
- Update cargo.toml ([#65](https://github.com/s2-streamstore/s2-cli/issues/65))
- Rename to streamstore-cli ([#66](https://github.com/s2-streamstore/s2-cli/issues/66))
- Description - Cargo.toml
- Update README.md

## [0.3.0] - 2024-12-05

### Features

- Return reconfigured stream ([#53](https://github.com/s2-streamstore/s2-cli/issues/53))
- Stderr `CommandRecord` when reading ([#45](https://github.com/s2-streamstore/s2-cli/issues/45)) ([#55](https://github.com/s2-streamstore/s2-cli/issues/55))
- Sign and notarize apple binaries ([#54](https://github.com/s2-streamstore/s2-cli/issues/54))
- Flatten commands ([#52](https://github.com/s2-streamstore/s2-cli/issues/52)) ([#56](https://github.com/s2-streamstore/s2-cli/issues/56))

## [0.2.0] - 2024-12-05

### Features

- Load endpoints `from_env()` ([#16](https://github.com/s2-streamstore/s2-cli/issues/16))
- Display throughput for read session ([#25](https://github.com/s2-streamstore/s2-cli/issues/25))
- Exercise limits for read session ([#27](https://github.com/s2-streamstore/s2-cli/issues/27))
- Better error reporting ([#30](https://github.com/s2-streamstore/s2-cli/issues/30))
- Appends with `fencing_token` and `match_seq_num` ([#38](https://github.com/s2-streamstore/s2-cli/issues/38))
- Stream `fence` and `trim` commands ([#46](https://github.com/s2-streamstore/s2-cli/issues/46))

### Bug Fixes

- Config env var precedence
- Flush BufWriter ([#22](https://github.com/s2-streamstore/s2-cli/issues/22))
- Handle common signals for streams ([#32](https://github.com/s2-streamstore/s2-cli/issues/32))
- Optional `start_seq_num` in `StreamService/ReadSession` ([#42](https://github.com/s2-streamstore/s2-cli/issues/42))
- Catch `ctrl-c` signal on windows ([#50](https://github.com/s2-streamstore/s2-cli/issues/50))

### Documentation

- Consistency
- Nits ([#19](https://github.com/s2-streamstore/s2-cli/issues/19))

### Miscellaneous Tasks

- Rm `S2ConfigError::PathError` ([#17](https://github.com/s2-streamstore/s2-cli/issues/17))
- Only attempt to load config from file if it exists ([#18](https://github.com/s2-streamstore/s2-cli/issues/18))
- Rename binary to s2 ([#21](https://github.com/s2-streamstore/s2-cli/issues/21))
- Set user-agent to s2-cli ([#23](https://github.com/s2-streamstore/s2-cli/issues/23)) ([#24](https://github.com/s2-streamstore/s2-cli/issues/24))
- Create LICENSE
- Update Cargo.toml with license
- Update SDK ([#26](https://github.com/s2-streamstore/s2-cli/issues/26))
- Sdk update ([#31](https://github.com/s2-streamstore/s2-cli/issues/31))
- Update CLI to latest sdk ([#37](https://github.com/s2-streamstore/s2-cli/issues/37))
- Upgrade SDK ([#41](https://github.com/s2-streamstore/s2-cli/issues/41))
- Upgrade sdk version ([#43](https://github.com/s2-streamstore/s2-cli/issues/43))
- Update SDK ([#47](https://github.com/s2-streamstore/s2-cli/issues/47))

## [0.1.0] - 2024-11-05

### Features

- Implement `AccountService` ([#1](https://github.com/s2-streamstore/s2-cli/issues/1))
- Implement `BasinService` ([#2](https://github.com/s2-streamstore/s2-cli/issues/2))
- Implement `StreamService` ([#3](https://github.com/s2-streamstore/s2-cli/issues/3))

### Bug Fixes

- Try to fix release CI ([#9](https://github.com/s2-streamstore/s2-cli/issues/9))
- Release CI ([#10](https://github.com/s2-streamstore/s2-cli/issues/10))
- Release CI ([#11](https://github.com/s2-streamstore/s2-cli/issues/11))
- Automatically add release notes ([#12](https://github.com/s2-streamstore/s2-cli/issues/12))
- Changelog ([#13](https://github.com/s2-streamstore/s2-cli/issues/13))
- Release CI ([#14](https://github.com/s2-streamstore/s2-cli/issues/14))

### Miscellaneous Tasks

- Reflect renamed repo
- Upgrade deps
- Clippy, whitespace
- Add CI action ([#6](https://github.com/s2-streamstore/s2-cli/issues/6))
- CODEOWNERS ([#7](https://github.com/s2-streamstore/s2-cli/issues/7))
- Add release CI action ([#8](https://github.com/s2-streamstore/s2-cli/issues/8))
- *(release)* Release 0.1.0 ([#15](https://github.com/s2-streamstore/s2-cli/issues/15))

<!-- generated by git-cliff -->
