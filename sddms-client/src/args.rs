use clap::Parser;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
pub struct Args {
    /// The host string of the site controller to connect to, <ip_addr>:<port>
    pub connect_host: String
}
