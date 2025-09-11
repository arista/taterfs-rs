use std::path::PathBuf;

use crate::file_source::{FileSourceService, FsFileSourceService};

pub async fn sample2() -> anyhow::Result<()> {
    let fs = FsFileSourceService {};
    let mut iter = fs
        .get_file_chunks(PathBuf::from("/data/taterfs/tatercore/docs/log"))
        .await?;
    let mut total = 0;
    while let Some(handle) = iter.next().await? {
        println!("{:<20} + {:10}", handle.offset(), handle.size(),);
        total += handle.size();
    }
    println!("total: {}", total);
    Ok(())
}
