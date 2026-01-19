use assert_cmd::Command;
use predicates::prelude::*;

fn s2() -> Command {
    Command::new(assert_cmd::cargo::cargo_bin!("s2"))
}

#[test]
fn invalid_uri_scheme() {
    s2().args(["get-stream-config", "foo://invalid/stream"])
        .assert()
        .failure()
        .stderr(predicate::str::contains("s2://"));
}

#[test]
fn missing_stream_in_uri() {
    s2().args(["get-stream-config", "s2://basin-only"])
        .assert()
        .failure();
}

#[test]
fn invalid_basin_name() {
    s2().args(["create-basin", "-invalid-name"])
        .assert()
        .failure();
}

#[test]
fn missing_access_token() {
    let mut cmd = s2();
    cmd.env_remove("S2_ACCESS_TOKEN");
    cmd.args(["list-basins"])
        .assert()
        .failure()
        .stderr(predicate::str::contains("access token"));
}

#[test]
fn unknown_subcommand() {
    s2().args(["unknown-command"])
        .assert()
        .failure()
        .stderr(predicate::str::contains("unrecognized subcommand"));
}
