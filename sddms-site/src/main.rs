mod args;
mod site_server;
mod sqlite_row_serializer;
mod central_client;
mod client_connection;
mod transaction_history;
mod history_logger;

use std::error::Error;
use std::fs::File;
use std::io::{BufReader, Read};
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::Path;
use clap::Parser;
use log::{info, LevelFilter};
use rusqlite::Connection;
use tonic::transport::Server;
use sddms_services::site_controller::site_manager_service_server::SiteManagerServiceServer;
use sddms_shared::error::SddmsError;
use crate::args::Args;
use crate::central_client::CentralClient;
use crate::history_logger::{FileHistoryLogger, HistoryLogger, NopHistoryLogger};
use crate::site_server::SddmsSiteManagerService;

fn configure_database(db_path: &Path, init_path: &Path) -> Result<Connection, SddmsError> {

    let db = rusqlite::Connection::open(db_path)
        .map_err(|err| SddmsError::site("Failed to connect to db").with_cause(err))?;

    let file = File::open(init_path)
        .map_err(|err| SddmsError::general("Failed to open SQL init file").with_cause(err))?;
    let mut contents: String = String::new();
    BufReader::new(file)
        .read_to_string(&mut contents)
        .map_err(|err| SddmsError::general("Failed to read SQL contents").with_cause(err))?;

    db.execute(&contents, ())
        .map_err(|err| SddmsError::client("SQL error while initializing DB").with_cause(err))?;

    Ok(db)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {

    env_logger::builder()
        .filter_level(LevelFilter::Info)
        .parse_default_env()
        .init();

    let args = Args::parse();

    info!("Starting up site...");
    {
        if args.init_file.is_some() {
            configure_database(&args.db_path, args.init_file.unwrap().as_path())?;
            info!("Database configured")
        }
    }

    let history_logger: Box<dyn HistoryLogger> = if let Some(history_path) = &args.history_file {
        FileHistoryLogger::open(history_path)
            .map(|file_logger| {
                let file: Box<dyn HistoryLogger> = Box::new(file_logger);
                file
            })
    } else {
        let nop: Box<dyn HistoryLogger> = Box::new(NopHistoryLogger);
        Ok(nop)
    }?;

    // establish connection with central server
    let client = CentralClient::new(&args.cc_addr).await?;
    let site_id = client.register_self("0.0.0.0", args.port).await?;

    info!("Site registered with concurrency controller");

    // setup server
    let service = SddmsSiteManagerService::new(&args.db_path, client, site_id, history_logger);
    let server = SiteManagerServiceServer::new(service);

    info!("Site configured");

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
