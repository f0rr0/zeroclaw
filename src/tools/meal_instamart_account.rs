use super::traits::{Tool, ToolResult};
use crate::auth::oauth_common::{generate_pkce_state, parse_query_params, url_decode, url_encode};
use crate::auth::profiles::{AuthProfile, AuthProfilesStore, TokenSet};
use crate::config::Config;
use crate::meal::store::MealStore;
use anyhow::{Context, Result};
use async_trait::async_trait;
use chrono::{Duration as ChronoDuration, Utc};
use serde::{Deserialize, Serialize};
use std::fmt::Write;
use std::sync::Arc;

const SWIGGY_CLIENT_ID: &str = "swiggy-mcp";
const SWIGGY_AUTHORIZE_URL: &str = "https://mcp.swiggy.com/auth/authorize";
const SWIGGY_TOKEN_URL: &str = "https://mcp.swiggy.com/auth/token";
const SWIGGY_ACCOUNT_PROVIDER: &str = "swiggy-instamart-mcp";
const SWIGGY_PENDING_PROVIDER: &str = "swiggy-instamart-mcp-pending";
const DEFAULT_REDIRECT_URI: &str = "http://127.0.0.1:17654/auth/callback";
const SESSION_TTL_SECS: i64 = 15 * 60;

const META_HOUSEHOLD_ID: &str = "household_id";
const META_USER_ID: &str = "user_id";

#[derive(Debug, Deserialize)]
#[serde(default)]
struct InstamartAuthArgs {
    action: String,
    household_id: String,
    user_id: String,
    scope: String,
    payload: String,
}

impl Default for InstamartAuthArgs {
    fn default() -> Self {
        Self {
            action: "status".to_string(),
            household_id: String::new(),
            user_id: String::new(),
            scope: "household".to_string(),
            payload: String::new(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PendingSession {
    code_verifier: String,
    state: String,
    redirect_uri: String,
    expires_at: i64,
    created_at: i64,
}

#[derive(Debug, Deserialize)]
struct SwiggyTokenResponse {
    access_token: String,
    #[serde(default)]
    refresh_token: Option<String>,
    #[serde(default)]
    token_type: Option<String>,
    #[serde(default)]
    expires_in: Option<i64>,
    #[serde(default)]
    scope: Option<String>,
}

#[derive(Debug)]
struct PendingMatch {
    profile_id: String,
    user_id: String,
    pending: PendingSession,
}

pub struct MealInstamartAccountTool {
    store: MealStore,
    profiles: AuthProfilesStore,
    http: reqwest::Client,
}

impl MealInstamartAccountTool {
    pub fn new(config: Arc<Config>) -> Self {
        let state_dir = crate::auth::state_dir_from_config(config.as_ref());
        Self {
            store: MealStore::new(config.clone()),
            profiles: AuthProfilesStore::new(&state_dir, config.secrets.encrypt),
            http: crate::config::build_runtime_proxy_client_with_timeouts(
                "tool.meal_instamart_account",
                45,
                10,
            ),
        }
    }

    fn account_profile_name(household_id: &str, user_id: &str) -> String {
        format!(
            "h={}&u={}",
            url_encode(household_id.trim()),
            url_encode(user_id.trim())
        )
    }

    fn pending_profile_name(household_id: &str, user_id: &str) -> String {
        format!(
            "h={}&u={}",
            url_encode(household_id.trim()),
            url_encode(user_id.trim()),
        )
    }

    fn build_authorize_url(code_challenge: &str, state: &str, redirect_uri: &str) -> String {
        let scope = "mcp:tools mcp:resources";
        format!(
            "{SWIGGY_AUTHORIZE_URL}?response_type=code&client_id={}&redirect_uri={}&scope={}&code_challenge={}&code_challenge_method=S256&state={}",
            url_encode(SWIGGY_CLIENT_ID),
            url_encode(redirect_uri),
            url_encode(scope),
            url_encode(code_challenge),
            url_encode(state)
        )
    }

    fn parse_oauth_payload(payload: &str) -> Result<(String, Option<String>)> {
        fn parse_code_state_from_query(query: &str) -> Option<(String, Option<String>)> {
            let params = parse_query_params(query);
            let code = params.get("code").cloned()?;
            let state = params.get("state").cloned();
            Some((code, state))
        }

        fn parse_code_state_from_urlish(input: &str) -> Option<(String, Option<String>)> {
            if let Some(query_start) = input.find('?') {
                let query = &input[query_start + 1..];
                return parse_code_state_from_query(query);
            }
            if input.contains('&') && input.contains('=') {
                return parse_code_state_from_query(input);
            }
            None
        }

        let trimmed = payload.trim();
        if trimmed.is_empty() {
            anyhow::bail!(
                "Missing OAuth callback payload. Paste the full callback URL or authorization code."
            );
        }

        if let Some(query_start) = trimmed.find('?') {
            let query = &trimmed[query_start + 1..];
            let params = parse_query_params(query);
            if let Some(err) = params.get("error") {
                let detail = params
                    .get("error_description")
                    .cloned()
                    .unwrap_or_else(|| "Authorization was denied".to_string());
                anyhow::bail!("OAuth authorization failed ({err}): {detail}");
            }
            if let Some(parsed) = parse_code_state_from_query(query) {
                return Ok(parsed);
            }

            // Swiggy may return an intermediate URL like:
            // /auth?success=true&redirect_uri=<encoded callback URL>
            if let Some(redirect_uri) = params.get("redirect_uri") {
                if let Some(parsed) = parse_code_state_from_urlish(redirect_uri) {
                    return Ok(parsed);
                }
                let decoded_once = url_decode(redirect_uri);
                if let Some(parsed) = parse_code_state_from_urlish(&decoded_once) {
                    return Ok(parsed);
                }
            }

            anyhow::bail!("Callback URL is missing `code`");
        }

        if trimmed.contains('&') && trimmed.contains('=') {
            let params = parse_query_params(trimmed);
            if let Some(code) = params.get("code").cloned() {
                let state = params.get("state").cloned();
                return Ok((code, state));
            }
        }

        Ok((trimmed.to_string(), None))
    }

    fn resolve_user(&self, _household_id: &str, provided_user: &str) -> Option<String> {
        let user = provided_user.trim();
        if !user.is_empty() {
            return Some(user.to_string());
        }
        None
    }

    async fn start_connect(&self, household_id: &str, user_id: &str) -> Result<String> {
        let cleared = self
            .clear_pending_sessions_for_user(household_id, user_id)
            .await?;
        let pkce = generate_pkce_state();
        let code_challenge = pkce.code_challenge.clone();
        let now = Utc::now().timestamp();
        let pending = PendingSession {
            code_verifier: pkce.code_verifier,
            state: pkce.state,
            redirect_uri: DEFAULT_REDIRECT_URI.to_string(),
            expires_at: now + SESSION_TTL_SECS,
            created_at: now,
        };

        let mut profile = AuthProfile::new_token(
            SWIGGY_PENDING_PROVIDER,
            &Self::pending_profile_name(household_id, user_id),
            serde_json::to_string(&pending)?,
        );
        profile
            .metadata
            .insert(META_HOUSEHOLD_ID.to_string(), household_id.to_string());
        profile
            .metadata
            .insert(META_USER_ID.to_string(), user_id.to_string());

        self.profiles
            .upsert_profile(profile, false)
            .await
            .context("Failed to persist Instamart auth session")?;

        let auth_url =
            Self::build_authorize_url(&code_challenge, &pending.state, &pending.redirect_uri);

        let mut output = String::new();
        let _ = writeln!(output, "Instamart connect session created for `{user_id}`.");
        if cleared > 0 {
            let _ = writeln!(
                output,
                "Replaced {cleared} older pending connect session(s) for this user."
            );
        }
        let _ = writeln!(output, "Session expires in 15 minutes.");
        let _ = writeln!(output, "\n1. Open this URL in a browser and authorize:");
        let _ = writeln!(output, "{auth_url}");
        let _ = writeln!(
            output,
            "\n2. Copy the final callback URL (or just the code) and complete with:"
        );
        let _ = write!(output, "`/instamart complete <callback-url-or-code>`");
        Ok(output)
    }

    async fn clear_pending_sessions_for_user(
        &self,
        household_id: &str,
        user_id: &str,
    ) -> Result<usize> {
        let data = self.profiles.load().await?;
        let mut removed = 0_usize;
        for (id, profile) in data.profiles {
            if profile.provider != SWIGGY_PENDING_PROVIDER {
                continue;
            }
            if profile.metadata.get(META_HOUSEHOLD_ID).map(String::as_str) != Some(household_id) {
                continue;
            }
            if profile.metadata.get(META_USER_ID).map(String::as_str) != Some(user_id) {
                continue;
            }
            if self.profiles.remove_profile(&id).await? {
                removed += 1;
            }
        }
        Ok(removed)
    }

    async fn find_pending_match(
        &self,
        household_id: &str,
        user_id: &str,
        state_hint: Option<&str>,
    ) -> Result<PendingMatch> {
        let data = self.profiles.load().await?;
        let mut matches = Vec::new();

        for (id, profile) in &data.profiles {
            if profile.provider != SWIGGY_PENDING_PROVIDER {
                continue;
            }
            if profile.metadata.get(META_HOUSEHOLD_ID).map(String::as_str) != Some(household_id) {
                continue;
            }
            if profile.metadata.get(META_USER_ID).map(String::as_str) != Some(user_id) {
                continue;
            }

            let Some(token_blob) = profile.token.as_deref() else {
                continue;
            };
            let pending: PendingSession = match serde_json::from_str(token_blob) {
                Ok(value) => value,
                Err(_) => continue,
            };

            if let Some(state) = state_hint {
                if pending.state != state {
                    continue;
                }
            }

            matches.push(PendingMatch {
                profile_id: id.clone(),
                user_id: user_id.to_string(),
                pending,
            });
        }

        matches.sort_by_key(|entry| entry.pending.created_at);

        let matched = matches.into_iter().last().ok_or_else(|| {
            anyhow::anyhow!("No pending Instamart connect session found for this user.")
        })?;

        if matched.pending.expires_at <= Utc::now().timestamp() {
            let _ = self.profiles.remove_profile(&matched.profile_id).await;
            anyhow::bail!("The Instamart connect session expired. Run `/instamart connect` again.");
        }

        Ok(matched)
    }

    async fn exchange_code(
        &self,
        code: &str,
        code_verifier: &str,
        redirect_uri: &str,
    ) -> Result<TokenSet> {
        let form = [
            ("grant_type", "authorization_code"),
            ("client_id", SWIGGY_CLIENT_ID),
            ("code", code),
            ("code_verifier", code_verifier),
            ("redirect_uri", redirect_uri),
        ];

        let response = self
            .http
            .post(SWIGGY_TOKEN_URL)
            .form(&form)
            .send()
            .await
            .context("Failed to call Swiggy token endpoint")?;

        let status = response.status();
        if !status.is_success() {
            let body = response
                .text()
                .await
                .unwrap_or_else(|_| "<failed to read body>".to_string());
            anyhow::bail!("Swiggy token exchange failed ({status}): {body}");
        }

        let token: SwiggyTokenResponse = response
            .json()
            .await
            .context("Invalid token response from Swiggy")?;

        let expires_at = token
            .expires_in
            .map(|seconds| Utc::now() + ChronoDuration::seconds(seconds.clamp(0, 31_536_000)));

        Ok(TokenSet {
            access_token: token.access_token,
            refresh_token: token.refresh_token,
            id_token: None,
            expires_at,
            token_type: token.token_type,
            scope: token.scope,
        })
    }

    async fn complete_connect(
        &self,
        household_id: &str,
        user_id: &str,
        payload: &str,
    ) -> Result<String> {
        let (code, state_hint) = Self::parse_oauth_payload(payload)?;
        let matched = self
            .find_pending_match(household_id, user_id, state_hint.as_deref())
            .await?;

        let token_set = self
            .exchange_code(
                code.as_str(),
                matched.pending.code_verifier.as_str(),
                matched.pending.redirect_uri.as_str(),
            )
            .await?;

        let mut account_profile = AuthProfile::new_oauth(
            SWIGGY_ACCOUNT_PROVIDER,
            &Self::account_profile_name(household_id, user_id),
            token_set.clone(),
        );
        account_profile
            .metadata
            .insert(META_HOUSEHOLD_ID.to_string(), household_id.to_string());
        account_profile
            .metadata
            .insert(META_USER_ID.to_string(), user_id.to_string());

        self.profiles
            .upsert_profile(account_profile, false)
            .await
            .context("Failed to persist Instamart account tokens")?;

        let _ = self.profiles.remove_profile(&matched.profile_id).await;

        let mut output = String::new();
        let _ = write!(output, "Instamart connected for `{}`.", matched.user_id);
        if let Some(expiry) = token_set.expires_at {
            let _ = write!(
                output,
                " Access token expires around `{}` UTC.",
                expiry.format("%Y-%m-%d %H:%M:%S")
            );
        }
        Ok(output)
    }

    async fn disconnect(&self, household_id: &str, user_id: &str) -> Result<String> {
        let account_profile_id = format!(
            "{}:{}",
            SWIGGY_ACCOUNT_PROVIDER,
            Self::account_profile_name(household_id, user_id)
        );
        let removed_account = self.profiles.remove_profile(&account_profile_id).await?;

        let data = self.profiles.load().await?;
        for (id, profile) in data.profiles {
            if profile.provider != SWIGGY_PENDING_PROVIDER {
                continue;
            }
            if profile.metadata.get(META_HOUSEHOLD_ID).map(String::as_str) != Some(household_id) {
                continue;
            }
            if profile.metadata.get(META_USER_ID).map(String::as_str) != Some(user_id) {
                continue;
            }
            let _ = self.profiles.remove_profile(&id).await;
        }

        if removed_account {
            Ok(format!(
                "Instamart account disconnected for `{user_id}`. (Server-side revocation is not currently available.)"
            ))
        } else {
            Ok(format!(
                "No connected Instamart account found for `{user_id}`."
            ))
        }
    }

    async fn status(
        &self,
        household_id: &str,
        user_id: Option<&str>,
        scope: &str,
    ) -> Result<String> {
        let data = self.profiles.load().await?;
        let now = Utc::now();

        let mut lines = Vec::new();
        for profile in data.profiles.values() {
            if profile.provider != SWIGGY_ACCOUNT_PROVIDER {
                continue;
            }
            if profile.metadata.get(META_HOUSEHOLD_ID).map(String::as_str) != Some(household_id) {
                continue;
            }
            let Some(profile_user_id) = profile.metadata.get(META_USER_ID).cloned() else {
                continue;
            };

            if scope.eq_ignore_ascii_case("self") {
                if let Some(requested_user) = user_id {
                    if requested_user != profile_user_id {
                        continue;
                    }
                }
            }

            let expiry_text = profile
                .token_set
                .as_ref()
                .and_then(|token| token.expires_at)
                .map(|value| value.format("%Y-%m-%d %H:%M:%S").to_string())
                .unwrap_or_else(|| "unknown".to_string());
            let freshness = profile
                .token_set
                .as_ref()
                .and_then(|token| token.expires_at)
                .map(|value| if value > now { "valid" } else { "expired" })
                .unwrap_or("unknown");

            lines.push(format!(
                "- `{profile_user_id}`: connected, token {freshness}, expires `{expiry_text}` UTC"
            ));
        }

        if scope.eq_ignore_ascii_case("self") {
            if let Some(requested_user) = user_id {
                if lines.is_empty() {
                    return Ok(format!(
                        "No connected Instamart account for `{requested_user}`."
                    ));
                }
                return Ok(format!(
                    "Instamart status for `{requested_user}`:\n{}",
                    lines.join("\n")
                ));
            }
        }

        if lines.is_empty() {
            Ok("No Instamart accounts are connected for this household.".to_string())
        } else {
            Ok(format!(
                "Connected Instamart accounts in this household:\n{}",
                lines.join("\n")
            ))
        }
    }
}

#[async_trait]
impl Tool for MealInstamartAccountTool {
    fn name(&self) -> &str {
        "meal_instamart_account"
    }

    fn description(&self) -> &str {
        "Manage Swiggy Instamart MCP account linking per household user (connect, complete OAuth, status, disconnect)."
    }

    fn parameters_schema(&self) -> serde_json::Value {
        serde_json::json!({
            "type": "object",
            "properties": {
                "action": {
                    "type": "string",
                    "enum": ["connect", "complete", "status", "disconnect"]
                },
                "household_id": { "type": "string" },
                "user_id": { "type": "string" },
                "scope": {
                    "type": "string",
                    "enum": ["self", "household"]
                },
                "payload": {
                    "type": "string",
                    "description": "For action=complete: OAuth callback URL/query/code from user"
                }
            }
        })
    }

    async fn execute(&self, args: serde_json::Value) -> Result<ToolResult> {
        if !self.store.enabled() {
            return Ok(ToolResult {
                success: false,
                output: String::new(),
                error: Some(
                    "Meal agent is disabled. Enable `[meal_agent].enabled = true` in config."
                        .to_string(),
                ),
            });
        }

        let mut parsed: InstamartAuthArgs = match serde_json::from_value(args) {
            Ok(value) => value,
            Err(error) => {
                return Ok(ToolResult {
                    success: false,
                    output: String::new(),
                    error: Some(format!(
                        "Invalid arguments for meal_instamart_account: {error}"
                    )),
                });
            }
        };

        if parsed.household_id.trim().is_empty() {
            parsed.household_id = self.store.household_id().to_string();
        }

        let action = parsed.action.trim().to_ascii_lowercase();
        let household_id = parsed.household_id.trim();

        let response = match action.as_str() {
            "connect" => match self.resolve_user(household_id, parsed.user_id.as_str()) {
                Some(user_id) => self.start_connect(household_id, user_id.as_str()).await,
                None => Err(anyhow::anyhow!(
                    "Could not infer user identity. Provide `user_id`."
                )),
            },
            "complete" => match self.resolve_user(household_id, parsed.user_id.as_str()) {
                Some(user_id) => {
                    self.complete_connect(household_id, user_id.as_str(), parsed.payload.as_str())
                        .await
                }
                None => Err(anyhow::anyhow!(
                    "Could not infer user identity. Provide `user_id`."
                )),
            },
            "status" => {
                let scope = if parsed.scope.trim().is_empty() {
                    "household"
                } else {
                    parsed.scope.trim()
                };
                let user = if scope.eq_ignore_ascii_case("self") {
                    self.resolve_user(household_id, parsed.user_id.as_str())
                } else {
                    None
                };
                self.status(household_id, user.as_deref(), scope).await
            }
            "disconnect" => match self.resolve_user(household_id, parsed.user_id.as_str()) {
                Some(user_id) => self.disconnect(household_id, user_id.as_str()).await,
                None => Err(anyhow::anyhow!(
                    "Could not infer user identity. Provide `user_id`."
                )),
            },
            _ => Err(anyhow::anyhow!(
                "Unknown action `{action}`. Use connect|complete|status|disconnect."
            )),
        };

        match response {
            Ok(output) => Ok(ToolResult {
                success: true,
                output,
                error: None,
            }),
            Err(error) => Ok(ToolResult {
                success: false,
                output: String::new(),
                error: Some(error.to_string()),
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use tempfile::TempDir;

    async fn test_config(tmp: &TempDir) -> Arc<Config> {
        let mut config = Config {
            workspace_dir: tmp.path().join("workspace"),
            config_path: tmp.path().join("config.toml"),
            ..Config::default()
        };
        tokio::fs::create_dir_all(&config.workspace_dir)
            .await
            .expect("workspace dir should be created");
        config.meal_agent.enabled = true;
        config.meal_agent.household_id = "household_test".to_string();
        config.meal_agent.db_path = tmp
            .path()
            .join("meal-agent-test.db")
            .to_string_lossy()
            .to_string();
        Arc::new(config)
    }

    #[tokio::test]
    async fn connect_generates_session_link() {
        let tmp = TempDir::new().expect("tempdir");
        let cfg = test_config(&tmp).await;
        let tool = MealInstamartAccountTool::new(cfg);

        let result = tool
            .execute(json!({
                "action": "connect",
                "user_id": "alice"
            }))
            .await
            .expect("tool call should return ToolResult");

        assert!(result.success, "{:?}", result.error);
        assert!(result
            .output
            .contains("/instamart complete <callback-url-or-code>"));
        assert!(result
            .output
            .contains("https://mcp.swiggy.com/auth/authorize"));
    }

    #[tokio::test]
    async fn status_defaults_to_household_scope() {
        let tmp = TempDir::new().expect("tempdir");
        let cfg = test_config(&tmp).await;
        let tool = MealInstamartAccountTool::new(cfg);

        let result = tool
            .execute(json!({
                "action": "status"
            }))
            .await
            .expect("tool call should return ToolResult");

        assert!(result.success, "{:?}", result.error);
        assert!(result.output.contains("No Instamart accounts"));
    }

    #[tokio::test]
    async fn disconnect_without_account_is_graceful() {
        let tmp = TempDir::new().expect("tempdir");
        let cfg = test_config(&tmp).await;
        let tool = MealInstamartAccountTool::new(cfg);

        let result = tool
            .execute(json!({
                "action": "disconnect",
                "user_id": "alice"
            }))
            .await
            .expect("tool call should return ToolResult");

        assert!(result.success, "{:?}", result.error);
        assert!(result.output.contains("No connected Instamart account"));
    }

    #[tokio::test]
    async fn connect_replaces_older_pending_sessions_for_same_user() {
        let tmp = TempDir::new().expect("tempdir");
        let cfg = test_config(&tmp).await;
        let tool = MealInstamartAccountTool::new(cfg);

        let first = tool
            .execute(json!({
                "action": "connect",
                "user_id": "alice"
            }))
            .await
            .expect("first connect should return ToolResult");
        assert!(first.success, "{:?}", first.error);

        let second = tool
            .execute(json!({
                "action": "connect",
                "user_id": "alice"
            }))
            .await
            .expect("second connect should return ToolResult");
        assert!(second.success, "{:?}", second.error);
        assert!(second
            .output
            .contains("Replaced 1 older pending connect session"));

        let data = tool.profiles.load().await.expect("profiles should load");
        let pending_count = data
            .profiles
            .values()
            .filter(|profile| {
                profile.provider == SWIGGY_PENDING_PROVIDER
                    && profile.metadata.get(META_HOUSEHOLD_ID).map(String::as_str)
                        == Some("household_test")
                    && profile.metadata.get(META_USER_ID).map(String::as_str) == Some("alice")
            })
            .count();
        assert_eq!(pending_count, 1);
    }

    #[tokio::test]
    async fn connect_without_user_id_is_rejected() {
        let tmp = TempDir::new().expect("tempdir");
        let cfg = test_config(&tmp).await;
        let tool = MealInstamartAccountTool::new(cfg);

        let result = tool
            .execute(json!({
                "action": "connect"
            }))
            .await
            .expect("tool call should return ToolResult");

        assert!(!result.success);
        assert!(result
            .error
            .unwrap_or_default()
            .contains("Could not infer user identity"));
    }

    #[test]
    fn parse_oauth_payload_accepts_callback_url() {
        let payload = "http://127.0.0.1:17654/auth/callback?code=abc123&state=s1";
        let (code, state) =
            MealInstamartAccountTool::parse_oauth_payload(payload).expect("payload should parse");
        assert_eq!(code, "abc123");
        assert_eq!(state.as_deref(), Some("s1"));
    }

    #[test]
    fn parse_oauth_payload_accepts_swiggy_redirect_wrapper_url() {
        let payload = "https://mcp.swiggy.com/auth?success=true&redirect_uri=http%3A%2F%2F127.0.0.1%3A17654%2Fauth%2Fcallback%3Fcode%3Dabc123%26state%3Ds1&state=s1";
        let (code, state) =
            MealInstamartAccountTool::parse_oauth_payload(payload).expect("payload should parse");
        assert_eq!(code, "abc123");
        assert_eq!(state.as_deref(), Some("s1"));
    }
}
