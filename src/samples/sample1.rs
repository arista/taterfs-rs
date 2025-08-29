use crate::file_store::file_store::{DirEntry, DirectoryLister, FileStoreService};
use crate::file_store::fs_file_store::FsFileStoreService;
use std::future::Future;
use std::path::Path;
use std::pin::Pin;

pub async fn sample1() -> anyhow::Result<()> {
    let fs = FsFileStoreService::default();
    let lister = fs.list_directory(Path::new(".")).await?;

    fn descend(
        mut l: Box<dyn DirectoryLister>,
        indent: usize,
    ) -> Pin<Box<dyn Future<Output = anyhow::Result<()>> + Send>> {
        Box::pin(async move {
            while let Some(entry) = l.next().await? {
                match entry {
                    DirEntry::File(f) => {
                        println!(
                            "{:indent$}FILE  {:<30} size={:<8} exec={}",
                            "",
                            f.rel_path().display(),
                            f.size(),
                            f.executable(),
                            indent = indent
                        );
                    }
                    DirEntry::Directory(d) => {
                        println!(
                            "{:indent$}DIR   {}",
                            "",
                            d.rel_path().display(),
                            indent = indent
                        );
                        // Recursive call is fine now because we're boxing each level.
                        descend(d.lister(), indent + 2).await?;
                    }
                }
            }
            Ok(())
        })
    }

    descend(lister, 0).await
}
