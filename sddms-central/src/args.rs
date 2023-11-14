use clap::Parser;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
pub struct Args {
    /// the port to host on
    #[arg(short, long)]
    pub port: u16,
}
