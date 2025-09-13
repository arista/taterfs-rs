use crate::{context::Context, prelude::*};

pub async fn handle(_ctx: &Context, args: &crate::cli::test::ListDirectoryArgs) -> Result<()> {
    print!("running list_directory, path: {:?}", args.path);
    Ok(())
}
