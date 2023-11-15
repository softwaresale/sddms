use std::path::PathBuf;
use clap::Parser;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
pub struct Args {
    /// path to the sqlite db to open. Creates if it doesn't exist
    pub db_path: PathBuf,
}
