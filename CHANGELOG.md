# Changelog

All notable changes to this project will be documented in this file.

## [0.21.1] - 2025-07-28

### Bug Fixes

- Make csoa/csor flags ([#167](https://github.com/s2-streamstore/s2-cli/issues/167))

## [0.21.0] - 2025-07-28

### Features

- Delete-on-empty ([#163](https://github.com/s2-streamstore/s2-cli/issues/163))

### Bug Fixes

- Specifying stream config args does not work ([#165](https://github.com/s2-streamstore/s2-cli/issues/165))

## [0.20.0] - 2025-07-22

### Features

- Clamp ([#161](https://github.com/s2-streamstore/s2-cli/issues/161))

### Release

- 0.19.2 ([#159](https://github.com/s2-streamstore/s2-cli/issues/159))

## [0.19.2] - 2025-07-15

### Bug Fixes

- Reconfigure-* ([#158](https://github.com/s2-streamstore/s2-cli/issues/158))

## [0.19.1] - 2025-07-04

### Features

- Add env var flag to disable tls ([#156](https://github.com/s2-streamstore/s2-cli/issues/156))

## [0.19.0] - 2025-06-13

### Bug Fixes

- Error message for missing access token

## [0.18.0] - 2025-06-13

### Miscellaneous Tasks

- Update ubuntu version in release

## [0.17.0] - 2025-06-06

### Features

- Compress by default ([#153](https://github.com/s2-streamstore/s2-cli/issues/153))
- Add `until` timestamp support + metrics ops ([#154](https://github.com/s2-streamstore/s2-cli/issues/154))

## [0.16.0] - 2025-05-25

### Features

- Add linger opt for append ([#148](https://github.com/s2-streamstore/s2-cli/issues/148))
- Fencing token as string rather than base64-encoded bytes ([#150](https://github.com/s2-streamstore/s2-cli/issues/150))

### Miscellaneous Tasks

- Default `read` to tailing rather than reading from head of stream ([#149](https://github.com/s2-streamstore/s2-cli/issues/149))
- Updated `--format` names ([#151](https://github.com/s2-streamstore/s2-cli/issues/151))

## [0.15.0] - 2025-05-10

### Miscellaneous Tasks

- Bump SDK version ([#146](https://github.com/s2-streamstore/s2-cli/issues/146))

## [0.14.0] - 2025-05-08

### Features

- Support timestamping configs ([#143](https://github.com/s2-streamstore/s2-cli/issues/143))

## [0.13.2] - 2025-05-02

### Miscellaneous Tasks

- `CHANGELOG` update

## [0.13.1] - 2025-05-02

### Miscellaneous Tasks

- `Cargo.lock` update

## [0.13.0] - 2025-05-02

### Features

- `tail` command ([#140](https://github.com/s2-streamstore/s2-cli/issues/140))

### Miscellaneous Tasks

- Reorder fields for json format

## [0.12.0] - 2025-04-30

### Features

- Support reading from timestamp or tail-offset ([#137](https://github.com/s2-streamstore/s2-cli/issues/137))

### Bug Fixes

- Ping ([#138](https://github.com/s2-streamstore/s2-cli/issues/138))
- `create_stream_on_read` for reconfigure basin ([#136](https://github.com/s2-streamstore/s2-cli/issues/136))

## [0.11.0] - 2025-04-15

### Features

- Access token methods ([#133](https://github.com/s2-streamstore/s2-cli/issues/133))

### Miscellaneous Tasks

- Release 0.11.0
- Typed errors ([#135](https://github.com/s2-streamstore/s2-cli/issues/135))

## [0.10.0] - 2025-03-14

### Bug Fixes

- `--create-stream-on-append` to accept explicit bool ([#131](https://github.com/s2-streamstore/s2-cli/issues/131))

## [0.9.0] - 2025-03-12

### Features

- Auto-paginate for stream and basin list ([#128](https://github.com/s2-streamstore/s2-cli/issues/128))

### Bug Fixes

- Ls to return fully qualified s2 uri ([#126](https://github.com/s2-streamstore/s2-cli/issues/126))

### Miscellaneous Tasks

- Remove unused deps + bump sdk version ([#125](https://github.com/s2-streamstore/s2-cli/issues/125))
- *(release)* Upgrade SDK ([#129](https://github.com/s2-streamstore/s2-cli/issues/129))

## [0.8.4] - 2025-02-05

### Bug Fixes

- Improve output messages for command record appends ([#119](https://github.com/s2-streamstore/s2-cli/issues/119))
- Metered bytes log ([#121](https://github.com/s2-streamstore/s2-cli/issues/121))

### Miscellaneous Tasks

- Improve read cli command docs ([#117](https://github.com/s2-streamstore/s2-cli/issues/117))
- Add uri args struct ([#120](https://github.com/s2-streamstore/s2-cli/issues/120))

## [0.8.3] - 2025-01-22

### Miscellaneous Tasks

- Reflect the update to make list limit optional instead of a default of 0 ([#114](https://github.com/s2-streamstore/s2-cli/issues/114))
- Minor upgrades

## [0.8.2] - 2025-01-21

### Miscellaneous Tasks

- Update SDK to `0.8.0` [#113](https://github.com/s2-streamstore/s2-cli/issues/113))

## [0.8.1] - 2025-01-16

### Miscellaneous Tasks

- Update SDK to `0.7.0` ([#111](https://github.com/s2-streamstore/s2-cli/issues/111))

## [0.8.0] - 2025-01-13

### Features

- Update fencing token to accept base64 instead of base16 ([#106](https://github.com/s2-streamstore/s2-cli/issues/106))
- Support different formats for append ([#105](https://github.com/s2-streamstore/s2-cli/issues/105))

### Miscellaneous Tasks

- Update clap CLI name ([#104](https://github.com/s2-streamstore/s2-cli/issues/104))
- Update deps ([#108](https://github.com/s2-streamstore/s2-cli/issues/108))

## [0.7.0] - 2024-12-26

### Features

- Only accept URIs in basin+stream args ([#100](https://github.com/s2-streamstore/s2-cli/issues/100))
- `s2 ls` command to list basins or streams ([#102](https://github.com/s2-streamstore/s2-cli/issues/102))

### Miscellaneous Tasks

- Inline path consts for consistency

## [0.6.4] - 2024-12-23

### Bug Fixes

- Error/help messages ([#95](https://github.com/s2-streamstore/s2-cli/issues/95))

### Documentation

- Update README S2 doc link ([#92](https://github.com/s2-streamstore/s2-cli/issues/92))

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
