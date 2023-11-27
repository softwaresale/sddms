use std::error::Error;
use std::fmt::Write;
use std::fs::File;
use std::io;
use std::io::BufWriter;
use clap::Parser;
use log::LevelFilter;
use rusqlite::{Connection, OpenFlags};
use crate::args::Args;
use crate::db_schema::DatabaseSchema;
use crate::query_gen::QueryGenerator;
use crate::value_generator::ValueGeneratorMap;

mod args;
mod value_generator;
mod db_schema;
mod config;
mod query_gen;

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

    let query_gen = QueryGenerator::new(db_schema, ValueGeneratorMap::default());

    let transactions = query_gen.gen_transactions(args.count.unwrap_or(10) as usize);
    let mut txn_buffer = String::new();
    for txn in transactions {
        txn_buffer.write_fmt(format_args!("{}\n", txn))
            .expect("Txn format should not fail");
    }

    let stmts = txn_buffer.split(";")
        .map(|stmt| stmt.trim())
        .filter(|stmt| !stmt.is_empty())
        .collect::<Vec<_>>();

    let mut output: Box<dyn io::Write> = if let Some(output_path) = args.output {
        let output = File::create(output_path)?;
        let buf_writer = BufWriter::new(output);
        Box::new(buf_writer)
    } else {
        Box::new(std::io::stdout())
    };

    for stmt in stmts {
        writeln!(output, "{stmt};")?;
    }

    Ok(())
}
