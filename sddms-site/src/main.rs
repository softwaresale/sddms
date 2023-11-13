mod args;
mod site_server;
mod sqlite_row_serializer;

use std::error::Error;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use clap::Parser;
use log::{info, LevelFilter};
use tonic::transport::Server;
use sddms_services::site_controller::site_manager_service_server::SiteManagerServiceServer;
use sddms_shared::error::SddmsError;
use crate::args::Args;
use crate::site_server::SddmsSiteManagerService;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {

    env_logger::builder()
        .filter_level(LevelFilter::Info)
        .init();

    let args = Args::parse();

    info!("Starting up site...");

    let db = sqlite::Connection::open_thread_safe(&args.db_path)
        .map_err(|err| SddmsError::site("Failed to connect to db").with_cause(err))?;

    info!("Connected to database");

    if args.init_file.is_some() {
        todo!("Need to implement initial file mode")
    }

    let service = SddmsSiteManagerService::new(&args.db_path, db);
    let server = SiteManagerServiceServer::new(service);

    // start up the local site controller service
    let serve_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(0,0,0,0)), args.port);
    Server::builder()
        .add_service(server)
        .serve(serve_addr)
        .await
        .map_err(|err| SddmsError::site("Error while starting server").with_cause(err))?;

    // connect to the central controller

    info!("Done");
    Ok(())
}
