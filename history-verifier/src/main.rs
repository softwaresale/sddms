use std::error::Error;
use std::fs::File;
use std::io::BufReader;
use std::process::ExitCode;
use clap::Parser;
use log::{debug, error, info, LevelFilter};
use crate::args::Args;
use crate::history_file_parser::ActionParser;
use crate::history_file_parser::action::Action;
use crate::organize::AssociatedActionMap;
use crate::verify::verify_action_history;

mod history_file_parser;
mod args;
mod organize;
mod verify;
mod transaction_id;
mod serial_view;

fn main() -> Result<ExitCode, Box<dyn Error>> {

    env_logger::builder()
        .filter_level(LevelFilter::Info)
        .init();

    let args = Args::parse();

    if args.history_file_paths.is_empty() {
        info!("No files provided, so nothing to do!");
        return Ok(ExitCode::SUCCESS)
    }

    let file_count = args.history_file_paths.len();
    let mut actions: Vec<Action> = Vec::new();
    for history_file_path in args.history_file_paths {
        info!("Parsing file {}", history_file_path.display());

        let action_file = File::open(history_file_path)?;
        let buf_reader = BufReader::new(action_file);
        let mut parser: ActionParser<BufReader<File>> = ActionParser::new(buf_reader);

        while let Some(next) = parser.parse_next() {
            debug!("Parsed action {:?}", next);
            actions.push(next);
        }
    }

    info!("Parsed {} items from {} files", actions.len(), file_count);

    info!("Sorting actions chronologically...");
    actions.sort_by(|left, right| left.instant.cmp(&right.instant));

    info!("Associating actions...");
    let associated_actions = AssociatedActionMap::new()
        .build(actions);
    info!("Associated actions!");

    info!("Verifying chronological actions...");
    match verify_action_history(&associated_actions) {
        Ok(_) => {
            info!("History is conflict free!");
            Ok(ExitCode::SUCCESS)
        }
        Err(conflict_error) => {
            let error_count = conflict_error.len();
            for err in conflict_error {
                println!("{}\n", err);
            }
            error!("There was/were {} conflicts", error_count);
            Ok(ExitCode::FAILURE)
        }
    }
}
