use std::path::PathBuf;
use clap::Parser;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
pub struct Args {
    #[arg(short, long, default_value = "false")]
    pub rollback_on_deadlock: bool,
    /// if set, read sql statements from the given path and execute them one by one
    #[arg(short, long)]
    pub input: Option<PathBuf>,
    /// The host string of the site controller to connect to, <ip_addr>:<port>
    pub connect_host: String
}
