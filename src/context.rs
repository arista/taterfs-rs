use serde::Deserialize;

#[derive(Debug, Deserialize, Clone)]
pub struct AppConfig {
    pub data_dir: String,
}

#[derive(Clone)]
pub struct Context {
    pub cfg: AppConfig,
}

impl Context {
    pub fn load() -> anyhow::Result<Self> {
        use figment::{providers::{Env, Format, Toml}, Figment};
        let cfg: AppConfig = Figment::new()
            .merge(Toml::file("taterfs.toml"))
            .merge(Env::prefixed("TATERFS_").split("_"))
            .extract()?;
        Ok(Self { cfg })
    }
}
