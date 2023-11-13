use std::path::PathBuf;
use clap::Parser;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
pub struct Args {

    /// the port to host on
    #[arg(short, long)]
    pub port: u16,

    /// A file to execute on the database file at runtime
    #[arg(short, long)]
    pub init_file: Option<PathBuf>,

    /// path to the sqlite db to open. Creates if it doesn't exist
    pub db_path: PathBuf,
    /// the address of the central controller, <ip_addr>:<port>
    pub cc_addr: String,
}
