pub mod file_store;
pub mod repo;
pub mod samples;
pub mod util;

//use crate::samples::sample1::sample1;
//use crate::samples::sample2::sample2;
//use crate::samples::sample3::sample3;
use crate::samples::sample4::sample4;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    //    sample1().await?;
    //    sample2().await?;
    //    sample3().await?;
    sample4().await?;
    Ok(())
}
