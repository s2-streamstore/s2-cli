use std::time::Duration;

use reqwest::Client;
use serde::Deserialize;

#[derive(Deserialize)]
struct Release {
    tag_name: String,
    draft: bool,
    prerelease: bool,
}

pub async fn check_for_updates() -> Option<String> {
    let client = Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .ok()?;

    let url = format!("https://api.github.com/repos/s2-streamstore/s2-cli/releases/latest");

    let response = client
        .get(&url)
        .header("User-Agent", "s2-cli")
        .send()
        .await
        .ok()?;

    let release: Release = response.json().await.ok()?;

    if !release.draft && !release.prerelease {
        let latest = release.tag_name.trim_start_matches('v');
        let current = env!("CARGO_PKG_VERSION");
        let current = current.trim_start_matches('v');

        if latest > current {
            return Some(release.tag_name);
        }
    }
    None
}
