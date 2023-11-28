use std::error::Error;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::{Path};
use clap::Parser;
use log::{error, info, LevelFilter, warn};
use rustyline::{DefaultEditor};
use tabled::Table;
use sddms_shared::error::SddmsError;
use sddms_shared::sql_metadata::{parse_transaction_stmt, TransactionStmt};
use crate::args::Args;
use crate::query_results::QueryResults;
use crate::reader::{Command, MetaCommand, read_next_command};
use crate::site_client::SddmsSiteClient;
use crate::transaction_state::TransactionState;

mod args;
mod reader;
mod site_client;
mod query_results;
mod transaction_state;

async fn invoke_query(client: &mut SddmsSiteClient, transaction_state: &TransactionState, query: &str) -> Result<bool, SddmsError> {
    let trans_id = transaction_state.transaction_id().ok();

    let results = client.invoke_query(trans_id, query).await?;

    match results {
        QueryResults::AffectedRows(row_count) => println!("Affected {} rows", row_count),
        QueryResults::Results(results) => {
            let table: Table = results.into();
            println!("{}", table);
        }
        QueryResults::DeadLock(deadlock_err) => {
            error!("{}", deadlock_err);
            return Ok(true);
        }
    };

    Ok(false)
}

async fn handle_lines(next_statements: &[String], args: &Args, client: &mut SddmsSiteClient, transaction_state: &mut TransactionState) -> Result<(), Box<dyn Error>> {
    for stmt in next_statements {
        let parse_attempt = parse_transaction_stmt(stmt);
        let Ok(transaction_stmt_opt) = parse_attempt else {
            eprintln!("{}", parse_attempt.unwrap_err());
            continue;
        };

        let invoke_stmt_result = if let Some(transaction_stmt) = transaction_stmt_opt {
            match transaction_stmt {
                TransactionStmt::Begin => {
                    client.begin_transaction().await
                        .and_then(|id| transaction_state.push(id))
                }
                finalize_cmd => {
                    let transaction_id = transaction_state.transaction_id()?;
                    client.finalize_transaction(transaction_id, finalize_cmd).await?;
                    transaction_state.clear();
                    Ok(())
                }
            }
        } else {
            let dead_locked = invoke_query(client, &transaction_state, stmt).await?;
            if dead_locked && args.rollback_on_deadlock {
                warn!("Automatically rolling back transaction");
                let transaction_id = transaction_state.transaction_id()?;
                client.finalize_transaction(transaction_id, TransactionStmt::Rollback).await?;
                transaction_state.clear();
            }
            Ok(())
        };

        if invoke_stmt_result.is_err() {
            let err = invoke_stmt_result.unwrap_err();
            eprintln!("{err}");
        }
    }

    Ok(())
}

async fn interactive_mode(client_id: u32, args: &Args, mut client: SddmsSiteClient, mut transaction_state: TransactionState) -> Result<(), Box<dyn Error>> {
    let mut line_reader = DefaultEditor::new()?;

    loop {
        let next_lines = read_next_command(&mut line_reader);
        if next_lines.is_err() {
            let err = next_lines.unwrap_err();
            let err = SddmsError::client("Error while reading line")
                .with_cause(err);
            eprint!("{}", err);
            break;
        }

        let next_statements = next_lines.unwrap();

        match next_statements {
            Command::Meta(meta_command) => {
                match meta_command {
                    MetaCommand::Quit => break,
                    MetaCommand::PrintTransactionInfo => {
                        if transaction_state.has_transaction() {
                            println!("client_id={}", client_id);
                            println!("transaction_id={}", transaction_state.transaction_id().unwrap());
                        } else {
                            println!("No transaction in progress");
                        }
                    }
                }
            }
            Command::Lines(next_statements) => {
                handle_lines(&next_statements, args, &mut client, &mut transaction_state).await?
            }
        }
    }

    Ok(())
}

async fn input_file_mode(input_file_path: &Path, args: &Args, mut client: SddmsSiteClient, mut transaction_state: TransactionState) -> Result<(), Box<dyn Error>> {
    let input_file = File::open(input_file_path)?;
    let input_file_reader = BufReader::new(input_file);
    let all_lines = input_file_reader.lines()
        .filter_map(|line| line.ok())
        .collect::<Vec<_>>();

    handle_lines(&all_lines, &args, &mut client, &mut transaction_state).await
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    env_logger::builder()
        .filter_level(LevelFilter::Info)
        .init();

    let args = {
        let mut args = Args::parse();

        if args.input.is_some() {
            info!("Input file is provided, so auto rollback on deadlock is enabled");
            args.rollback_on_deadlock = true;
        }

        if args.rollback_on_deadlock {
            warn!("Rollback on deadlock is on!")
        }

        args
    };

    info!("Connecting to {}", args.connect_host);

    // configure connection to site controller
    let mut client = SddmsSiteClient::connect(&args.connect_host).await?;
    info!("Connected to site client at {}", args.connect_host);
    let client_id = client.register_self().await?;
    client.set_client_id(client_id);
    info!("Client successfully registered at site with id {}", client_id);

    let transaction_state = TransactionState::new();

    if let Some(input_file_path) = &args.input {
        input_file_mode(input_file_path, &args, client, transaction_state).await?;
    } else {
        interactive_mode(client_id, &args, client, transaction_state).await?;
    }

    info!("Done!");
    Ok(())
}
