pub mod file_store;
pub mod repo;
pub mod samples;
pub mod util;
pub mod upload;

use anyhow::anyhow;

use crate::samples::sample1::sample1;
use crate::samples::sample2::sample2;
use crate::samples::sample3::sample3;
use crate::samples::sample4::sample4;
use crate::samples::sample5::sample5;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    run_sample(1).await?;
    Ok(())
}

async fn run_sample(sample_num: i32) -> anyhow::Result<()> {
    match sample_num {
        1 => Ok(sample1().await?),
        2 => Ok(sample2().await?),
        3 => Ok(sample3().await?),
        4 => Ok(sample4().await?),
        5 => Ok(sample5().await?),
        _ => Err(anyhow!("unknown")),
    }
}
