pub mod file_store;
pub mod repo;
pub mod samples;
pub mod util;
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::anyhow;

use crate::samples::sample1::sample1;
use crate::samples::sample2::sample2;
use crate::samples::sample3::sample3;
use crate::samples::sample4::sample4;
use crate::samples::sample5::sample5;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    run_sample(6).await?;
    Ok(())
}

async fn run_sample(sample_num: i32) -> anyhow::Result<()> {
    match sample_num {
        1 => Ok(sample1().await?),
        2 => Ok(sample2().await?),
        3 => Ok(sample3().await?),
        4 => Ok(sample4().await?),
        5 => Ok(sample5().await?),
        6 => {
            let num = inverted_timestamp_ms();
            let num_str = format!("{:020}", num);
            let num_split_str = format!("{}/{}/{}/{}", &num_str[0..8], &num_str[8..10], &num_str[10..12], &num_str[12..20]);
            println!("number: {} - {}", num_str, num_split_str);
            Ok(())
        },
        _ => Err(anyhow!("unknown")),
    }
}

fn inverted_timestamp_ms() -> u64 {
    let now_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64;
    u64::MAX - now_ms
}
