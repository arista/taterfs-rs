pub mod file_store;
pub mod repo;
pub mod samples;

//use crate::samples::sample1::sample1;
//use crate::samples::sample2::sample2;
use crate::samples::sample3::sample3;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    //    sample1().await?;
    //    sample2().await?;
    sample3().await?;
    Ok(())
}
