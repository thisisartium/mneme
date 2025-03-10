use crate::error::Error;
use eventstore::ClientSettings;
use std::fmt;

#[derive(Clone)]
pub struct ConnectionSettings {
    host: String,
    port: u16,
    tls: bool,
    username: String,
    password: SecureString,
}

impl fmt::Debug for ConnectionSettings {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ConnectionSettings")
            .field("host", &self.host)
            .field("port", &self.port)
            .field("tls", &self.tls)
            .field("username", &self.username)
            .field("password", &"<redacted>")
            .finish()
    }
}

impl ConnectionSettings {
    /// Creates a new ConnectionSettings builder.
    pub fn builder() -> ConnectionSettingsBuilder {
        ConnectionSettingsBuilder::default()
    }

    pub fn from_env() -> Result<Self, Error> {
        let host = env_safe::var_opt("KURRENT_HOST").unwrap_or_else(|| "localhost".to_string());
        let port = env_safe::var_opt("KURRENT_PORT")
            .and_then(|p| p.parse().ok())
            .unwrap_or(2113);
        let tls = env_safe::var_opt("KURRENT_TLS")
            .and_then(|t| t.parse().ok())
            .unwrap_or(false);
        let username = env_safe::var_opt("KURRENT_USERNAME").unwrap_or_else(|| "admin".to_string());

        let password = env_safe::var("KURRENT_PASSWORD").map_err(|_| Error::InvalidConfig {
            message: "KURRENT_PASSWORD environment variable is required".to_string(),
            parameter: Some("password".to_string()),
        })?;

        Ok(Self {
            host,
            port,
            tls,
            username,
            password: SecureString::new(password),
        })
    }

    pub(crate) fn to_connection_string(&self) -> String {
        format!(
            "esdb://{}:{}@{}:{}?tls={}",
            self.username,
            self.password.as_str(),
            self.host,
            self.port,
            self.tls
        )
    }

    pub(crate) fn to_client_settings(&self) -> Result<ClientSettings, Error> {
        let conn_string = self.to_connection_string();
        conn_string.parse().map_err(Error::EventStoreSettings)
    }
}

#[derive(Default)]
pub struct ConnectionSettingsBuilder {
    host: Option<String>,
    port: Option<u16>,
    tls: Option<bool>,
    username: Option<String>,
    password: Option<SecureString>,
}

impl ConnectionSettingsBuilder {
    pub fn host(mut self, host: impl Into<String>) -> Self {
        self.host = Some(host.into());
        self
    }

    pub fn port(mut self, port: u16) -> Self {
        self.port = Some(port);
        self
    }

    pub fn tls(mut self, enable: bool) -> Self {
        self.tls = Some(enable);
        self
    }

    pub fn username(mut self, username: impl Into<String>) -> Self {
        self.username = Some(username.into());
        self
    }

    pub fn password(mut self, password: impl Into<String>) -> Self {
        self.password = Some(SecureString::new(password.into()));
        self
    }

    pub fn build(self) -> Result<ConnectionSettings, Error> {
        Ok(ConnectionSettings {
            host: self.host.unwrap_or_else(|| "localhost".to_string()),
            port: self.port.unwrap_or(2113),
            tls: self.tls.unwrap_or(false),
            username: self.username.unwrap_or_else(|| "admin".to_string()),
            password: self.password.ok_or_else(|| Error::InvalidConfig {
                message: "password is required".to_string(),
                parameter: Some("password".to_string()),
            })?,
        })
    }
}

struct SecureString {
    inner: String,
    should_zero: bool,
}

impl SecureString {
    fn new(s: String) -> Self {
        Self {
            inner: s,
            should_zero: true,
        }
    }

    fn as_str(&self) -> &str {
        &self.inner
    }
}

impl Clone for SecureString {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            should_zero: false, // Don't zero cloned strings - original will handle it
        }
    }
}

impl fmt::Debug for SecureString {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "<redacted>")
    }
}

impl Drop for SecureString {
    fn drop(&mut self) {
        if self.should_zero {
            // Only zero if this is the original string
            let mut vec = self.inner.as_bytes().to_vec();
            vec.fill(0);
        }
    }
}

mod env_safe {
    //! Safe wrappers around unsafe environment variable operations.
    //! These are deliberately limited to just what we need for settings.
    use std::env;

    pub fn var(key: &str) -> Result<String, env::VarError> {
        env::var(key)
    }

    pub fn var_opt(key: &str) -> Option<String> {
        var(key).ok()
    }

    /// Helper function to set environment variables for testing.
    ///
    /// # Safety
    ///
    /// This function is unsafe because modifying environment variables can affect
    /// other parts of the program and child processes.
    #[cfg(test)]
    pub(crate) unsafe fn set_var_for_test(key: &str, value: &str) {
        unsafe { env::set_var(key, value) }
    }

    /// Helper function to remove environment variables for testing.
    ///
    /// # Safety
    ///
    /// This function is unsafe because modifying environment variables can affect
    /// other parts of the program and child processes.
    #[cfg(test)]
    pub(crate) unsafe fn remove_var_for_test(key: &str) {
        unsafe { env::remove_var(key) }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct TestEnv {
        vars: Vec<(String, String)>,
    }

    impl TestEnv {
        fn new() -> Self {
            Self { vars: Vec::new() }
        }

        fn with(mut self, key: &str, value: &str) -> Self {
            self.vars.push((key.to_string(), value.to_string()));
            self
        }

        fn run<T, F: FnOnce() -> T>(&self, f: F) -> T {
            let mut old_values = Vec::new();
            for (key, _) in &self.vars {
                old_values.push((key.clone(), env_safe::var_opt(key)));
            }

            for (key, value) in &self.vars {
                // SAFETY: This is only used in tests and the values are restored
                unsafe { env_safe::set_var_for_test(key, value) };
            }

            let result = f();

            for (key, value) in old_values {
                match value {
                    Some(v) => unsafe { env_safe::set_var_for_test(&key, &v) },
                    None => unsafe { env_safe::remove_var_for_test(&key) },
                }
            }

            result
        }
    }
    #[test]
    fn builds_connection_settings() {
        let settings = ConnectionSettings::builder()
            .host("example.com")
            .port(1234)
            .tls(true)
            .username("user")
            .password("pass")
            .build()
            .unwrap();

        assert_eq!(settings.host, "example.com");
        assert_eq!(settings.port, 1234);
        assert!(settings.tls);
        assert_eq!(settings.username, "user");
        assert_eq!(settings.password.as_str(), "pass");
    }

    #[test]
    fn uses_defaults() {
        let settings = ConnectionSettings::builder()
            .password("pass")
            .build()
            .unwrap();

        assert_eq!(settings.host, "localhost");
        assert_eq!(settings.port, 2113);
        assert!(!settings.tls);
        assert_eq!(settings.username, "admin");
        assert_eq!(settings.password.as_str(), "pass");
    }

    #[test]
    fn requires_password() {
        let result = ConnectionSettings::builder().build();
        assert!(matches!(
            result,
            Err(Error::InvalidConfig {
                message,
                parameter: Some(param),
                ..
            }) if message == "password is required" && param == "password"
        ));
    }

    #[test]
    fn debug_output_hides_password() {
        let settings = ConnectionSettings::builder()
            .password("supersecret")
            .build()
            .unwrap();

        let debug_str = format!("{:?}", settings);
        assert!(!debug_str.contains("supersecret"));
        assert!(debug_str.contains("<redacted>"));
    }

    #[test]
    fn generates_connection_string() {
        let settings = ConnectionSettings::builder()
            .host("example.com")
            .port(1234)
            .tls(true)
            .username("user")
            .password("pass")
            .build()
            .unwrap();

        assert_eq!(
            settings.to_connection_string(),
            "esdb://user:pass@example.com:1234?tls=true"
        );
    }

    #[test]
    fn loads_from_env() {
        let test_env = TestEnv::new()
            .with("KURRENT_HOST", "test.com")
            .with("KURRENT_PORT", "5555")
            .with("KURRENT_TLS", "true")
            .with("KURRENT_USERNAME", "tester")
            .with("KURRENT_PASSWORD", "secret");

        let settings = test_env.run(|| ConnectionSettings::from_env().unwrap());
        assert_eq!(settings.host, "test.com");
        assert_eq!(settings.port, 5555);
        assert!(settings.tls);
        assert_eq!(settings.username, "tester");
        assert_eq!(settings.password.as_str(), "secret");

        let test_env = TestEnv::new().with("KURRENT_PASSWORD", "secret");

        let settings = test_env.run(|| ConnectionSettings::from_env().unwrap());
        assert_eq!(settings.host, "localhost");
        assert_eq!(settings.port, 2113);
        assert!(!settings.tls);
        assert_eq!(settings.username, "admin");
        assert_eq!(settings.password.as_str(), "secret");

        let test_env = TestEnv::new();
        let result = test_env.run(ConnectionSettings::from_env);
        assert!(matches!(
            result,
            Err(Error::InvalidConfig {
                message,
                parameter: Some(param),
                ..
            }) if message == "KURRENT_PASSWORD environment variable is required" && param == "password"
        ));
    }
}
