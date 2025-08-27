pub mod file_store;
pub mod fs_file_store;

use crate::file_store::{DirEntry, DirectoryLister, FileStoreService};
use crate::fs_file_store::FsFileStoreService;
use std::future::Future;
use std::io;
use std::path::Path;
use std::pin::Pin;

#[tokio::main]
async fn main() -> io::Result<()> {
    let fs = FsFileStoreService::default();
    let lister = fs.list_directory(Path::new(".")).await?;

    fn descend(
        mut l: Box<dyn DirectoryLister>,
        indent: usize,
    ) -> Pin<Box<dyn Future<Output = io::Result<()>> + Send>> {
        Box::pin(async move {
            while let Some(entry) = l.next().await? {
                match entry {
                    DirEntry::File(f) => {
                        println!(
                            "{:indent$}FILE  {:<30} size={:<8} exec={}",
                            "",
                            f.rel_path.display(),
                            f.size,
                            f.executable,
                            indent = indent
                        );
                    }
                    DirEntry::Directory(d) => {
                        println!(
                            "{:indent$}DIR   {}",
                            "",
                            d.rel_path.display(),
                            indent = indent
                        );
                        // Recursive call is fine now because we're boxing each level.
                        descend(d.lister, indent + 2).await?;
                    }
                }
            }
            Ok(())
        })
    }

    descend(lister, 0).await
}
