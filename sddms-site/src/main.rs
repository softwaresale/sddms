mod args;
mod site_server;
mod sqlite_row_serializer;
mod central_client;
mod client_connection_map;

use std::error::Error;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use clap::Parser;
use log::{info, LevelFilter};
use tonic::transport::Server;
use sddms_services::site_controller::site_manager_service_server::SiteManagerServiceServer;
use sddms_shared::error::SddmsError;
use crate::args::Args;
use crate::central_client::CentralClient;
use crate::site_server::SddmsSiteManagerService;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {

    env_logger::builder()
        .filter_level(LevelFilter::Info)
        .parse_default_env()
        .init();

    let args = Args::parse();

    info!("Starting up site...");

    let db = sqlite::Connection::open(&args.db_path)
        .map_err(|err| SddmsError::site("Failed to connect to db").with_cause(err))?;

    info!("Connected to database");

    if args.init_file.is_some() {
        todo!("Need to implement initial file mode")
    }

    // establish connection with central server
    let mut client = CentralClient::new(&args.cc_addr).await?;
    let site_id = client.register_self("0.0.0.0", args.port).await?;

    // setup server
    let service = SddmsSiteManagerService::new(&args.db_path, client, site_id);
    let server = SiteManagerServiceServer::new(service);

    // start up the local site controller service
    let serve_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(0,0,0,0)), args.port);
    Server::builder()
        .add_service(server)
        .serve(serve_addr)
        .await
        .map_err(|err| SddmsError::site("Error while starting server").with_cause(err))?;

    info!("Done");
    Ok(())
}
