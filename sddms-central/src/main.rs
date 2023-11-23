mod args;
mod central_service;
mod lock_table;
mod connection_pool;
mod transaction_id;
mod live_transaction_set;
mod site_client;

use std::error::Error;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use clap::Parser;
use log::{info, LevelFilter};
use tonic::transport::Server;
use sddms_services::central_controller::concurrency_controller_service_server::ConcurrencyControllerServiceServer;
use sddms_shared::error::SddmsError;
use crate::args::Args;
use crate::central_service::CentralService;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {

    env_logger::builder()
        .filter_level(LevelFilter::Info)
        .parse_default_env()
        .init();

    let args = Args::parse();

    info!("Setting up central controller on 0.0.0.0:{}...", args.port);
    let service = CentralService::new();
    let server = ConcurrencyControllerServiceServer::new(service);
    info!("Server is initialized");

    let serve_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(0,0,0,0)), args.port);
    Server::builder()
        .add_service(server)
        .serve(serve_addr)
        .await
        .map_err(|err| SddmsError::site("Error while starting server").with_cause(err))?;

    info!("Done");
    Ok(())
}
