use crate::file_source::{self, FileSourceService};
use crate::{context::Context, prelude::*};
use std::path::Path;

pub async fn handle(_ctx: &Context, args: &crate::cli::test::ListDirectoryArgs) -> Result<()> {
    let s = file_source::FsFileSourceService::new();
    let mut dirstack: Vec<Box<dyn file_source::DirectoryLister>> = Vec::new();
    dirstack.push(s.list_directory(Path::new(&args.path)).await?);
    println!("{}/", args.path);
    let mut indent: usize = 2;

    while let Some(d) = dirstack.last_mut() {
        match d.next().await? {
            Some(direntry) => match direntry {
                file_source::DirEntry::Directory(dde) => {
                    println!(
                        "{:indent$}DIR   {}/",
                        "",
                        dde.rel_path.display(),
                        indent = indent
                    );
                    dirstack.push(dde.lister);
                    indent += 2;
                }
                file_source::DirEntry::File(fe) => {
                    println!(
                        "{:indent$}FILE  {} ({:?} executable: {:?})",
                        "",
                        fe.rel_path.display(),
                        fe.size,
                        fe.executable,
                        indent = indent,
                    );
                    let mut chunks = s.get_file_chunks(fe.abs_path).await?;
                    let mut pos: usize = 0;
                    while let Some(c) = chunks.next().await? {
                        println!(
                            "{:indent$}  CHUNK {:10?} + {:?}",
                            "",
                            pos,
                            c.size(),
                            indent = indent,
                        );
                        pos += c.size();
                    }
                }
            },
            None => {
                dirstack.pop();
                indent = indent.checked_sub(2).expect("Assertion failed: indent < 2");
            }
        }
    }

    Ok(())
}
