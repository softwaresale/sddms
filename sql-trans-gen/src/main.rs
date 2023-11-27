use std::error::Error;
use std::fs::File;
use std::io::Read;
use std::path::Path;
use clap::Parser;
use log::LevelFilter;
use rusqlite::{Connection, OpenFlags};
use sddms_shared::error::SddmsError;
use crate::args::Args;
use crate::config::{Config};
use crate::db_schema::DatabaseSchema;
use crate::query_gen::QueryGenerator;
use crate::value_generator::ValueGeneratorMap;

mod args;
mod value_generator;
mod db_schema;
mod config;
mod query_gen;

fn parse_config(path: &Path) -> Result<Config, SddmsError> {
    let mut file = File::open(path)
        .map_err(|err| SddmsError::general("Couldn't open config file").with_cause(err))?;

    let mut buffer = String::new();
    file.read_to_string(&mut buffer)
        .map_err(|err| SddmsError::general("Couldn't read config file").with_cause(err))?;

    toml::from_str(&buffer)
        .map_err(|err| SddmsError::general("Configuration error").with_cause(err))
}

fn main() -> Result<(), Box<dyn Error>> {

    // configure logging
    env_logger::builder()
        .filter_level(LevelFilter::Info)
        .parse_default_env()
        .init();

    // parse arguments
    let args = Args::parse();

    let connection = Connection::open_with_flags(args.db_path, OpenFlags::empty() | OpenFlags::SQLITE_OPEN_READ_ONLY)?;

    // schema
    let db_schema = {
        let mut schema = DatabaseSchema::new(&connection);
        schema.add_insert_restricted("students");
        schema
    };
    println!("{:#?}", db_schema);

    let query_gen = QueryGenerator::new(db_schema, ValueGeneratorMap::default());

    let txns = query_gen.gen_transactions(5);
    for txn in txns {
        println!("{txn}")
    }

    Ok(())
}
