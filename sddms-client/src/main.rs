use clap::Parser;
use log::{info, LevelFilter};
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

async fn invoke_query(client: &mut SddmsSiteClient, transaction_state: &TransactionState, query: &str) -> Result<(), SddmsError> {
    let trans_id = transaction_state.transaction_id().ok();

    let results = client.invoke_query(trans_id, query).await?;

    match results {
        QueryResults::AffectedRows(row_count) => println!("Affected {} rows", row_count),
        QueryResults::Results(results) => {
            let table: Table = results.into();
            println!("{}", table);
        }
    };

    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::builder()
        .filter_level(LevelFilter::Info)
        .init();
    let args = Args::parse();
    info!("Connecting to {}", args.connect_host);

    // configure connection to site controller
    let mut client = SddmsSiteClient::connect(&args.connect_host).await?;
    info!("Connected to site client at {}", args.connect_host);
    let client_id = client.register_self().await?;
    client.set_client_id(client_id);
    info!("Client successfully registered at site with id {}", client_id);

    let mut line_reader = DefaultEditor::new()?;

    let mut transaction_state = TransactionState::new();

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
                    MetaCommand::Quit => break
                }
            }
            Command::Lines(next_statements) => {
                for stmt in &next_statements {
                    let invoke_stmt_result = if let Some(transaction_stmt) = parse_transaction_stmt(stmt)? {
                        match transaction_stmt {
                            TransactionStmt::Begin => {
                                client.begin_transaction().await
                                    .and_then(|id| transaction_state.push(id))
                            }
                            finalize_cmd => {
                                let transaction_id = transaction_state.transaction_id()?;
                                client.finalize_transaction(transaction_id, finalize_cmd).await
                            }
                        }
                    } else {
                        invoke_query(&mut client, &transaction_state, stmt).await
                    };

                    if invoke_stmt_result.is_err() {
                        let err = invoke_stmt_result.unwrap_err();
                        eprintln!("{err}");
                    }
                }
            }
        }
    }

    info!("Done!");
    Ok(())
}
