# Release

Guide the user through the release process for `s2-cli`.

## Pre-requisites

`cargo install git-cliff`

## Flow

1. Prompt the user for the new version

2. Update version `Cargo.toml` and `cargo generate-lockfile`

3. Generate changelog with `git cliff --unreleased --tag ${NEW_VERSION} --prepend CHANGELOG.md`

4. Get approval on diffs

5. Commit and push

6. Tag the release*:
  - `git tag -a ${NEW_VERSION} -m "Release ${NEW_VERSION}"`
  - `git push origin tag ${NEW_VERSION}`
