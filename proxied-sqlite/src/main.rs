mod args;
mod reader;
mod sqlite_row_serializer;
mod results;

use std::error::Error;
use std::time::Duration;
use clap::Parser;
use log::{info, LevelFilter};
use rusqlite::backup::Backup;
use rusqlite::Connection;
use rustyline::DefaultEditor;
use tabled::{col, Table};
use crate::args::Args;
use crate::reader::{Command, MetaCommand, read_next_command};
use crate::results::ResultsInfo;
use crate::sqlite_row_serializer::serialize_row;

fn main() -> Result<(), Box<dyn Error>> {

    env_logger::builder()
        .filter_level(LevelFilter::Info)
        .parse_default_env()
        .init();

    let args = Args::parse();

    let db_connection = {
        let disk_connection = Connection::open(&args.db_path)?;
        info!("Opened disk connection");
        let mut connection = Connection::open_in_memory()?;
        info!("Created in memory database");
        {
            info!("Starting backup...");
            let backup = Backup::new(&disk_connection, &mut connection)?;
            backup.run_to_completion(5, Duration::from_millis(500), None)?;
            info!("Backup complete")
        }
        connection
    };

    let mut reader = DefaultEditor::new()?;

    loop {
        let next_commmand = read_next_command(&mut reader)?;
        match next_commmand {
            Command::Meta(cmd) => {
                match cmd {
                    MetaCommand::Quit => break,
                }
            }
            Command::Lines(stmts) => {
                for stmt in stmts {
                    let mut stmt = db_connection.prepare(&stmt)?;
                    let columns = stmt.column_names().iter()
                        .map(|name| String::from(*name))
                        .collect::<Vec<_>>();

                    if stmt.readonly() {
                        let row_lines = stmt
                            .query_map([],|row| Ok(serialize_row(row, &columns)))
                            ?.filter_map(|item| item.ok())
                            .collect::<Vec<_>>();

                        let table: Table = ResultsInfo {
                            columns,
                            results: row_lines
                        }.into();

                        println!("{}", table);
                    } else {
                        let modified = stmt.execute([])?;
                        println!("Affected {} rows", modified)
                    }
                }
            }
        }
    }

    info!("Session ended. Writing back to database");
    let mut disk_connection = Connection::open(args.db_path)?;
    info!("Opened disk connection. Starting to copy in-memory database to disk...");
    let backup = Backup::new(&db_connection, &mut disk_connection)?;
    backup.run_to_completion(5, Duration::from_millis(500), None)?;
    info!("Done");
    Ok(())
}
