use std::path::PathBuf;
use clap::Parser;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
pub struct Args {
    /// how many transactions to generate
    #[arg(short, long)]
    pub count: Option<u32>,
    /// Where to write the output to. Defaults to stdout
    #[arg(short, long)]
    pub output: Option<PathBuf>,
    /// path to the sqlite db to open. Creates if it doesn't exist
    pub db_path: PathBuf,
}
