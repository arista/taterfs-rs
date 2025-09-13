use crate::{context::Context, prelude::*};

pub async fn handle(ctx: &Context, args: &crate::cli::test::ListDirectoryArgs) -> Result<()> {
    print!("data_dir: {:?}", ctx.cfg.data_dir);
    print!("running list_directory, path: {:?}", args.path);
    Ok(())
}
