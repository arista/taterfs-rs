use taterfs_rs::cli;

#[tokio::main]
async fn main() {
    if let Err(e) = cli::main().await {
        eprintln!("Error: {}", e);
        std::process::exit(1);
    }
}
