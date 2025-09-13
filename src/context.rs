use serde::Deserialize;

#[derive(Debug, Deserialize, Clone)]
pub struct AppConfig {
    //    pub data_dir: String,
}

#[derive(Clone)]
pub struct Context {
    #[allow(dead_code)]
    pub cfg: AppConfig,
}

impl Context {
    pub fn load() -> anyhow::Result<Self> {
        use figment::{
            Figment,
            providers::{Env, Format, Toml},
        };
        let cfg: AppConfig = Figment::new()
            // FIXME - do actual config file loading
            .merge(Toml::file("taterfs.toml"))
            .merge(Env::prefixed("TATERFS_").split("_"))
            .extract()?;
        Ok(Self { cfg })
    }
}
