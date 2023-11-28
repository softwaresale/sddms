use rustyline::history::History;
use regex::{RegexSet};
use rustyline::{Editor, Helper};
use sddms_shared::error::{SddmsError, SddmsResult};

#[derive(Debug, Clone)]
pub enum MetaCommand {
    Quit,
    PrintTransactionInfo,
    CancelLine,
}

impl MetaCommand {
    fn looks_like_meta_command(line: &str) -> bool {
        line.starts_with("\\")
    }
}

impl TryFrom<&str> for MetaCommand {
    type Error = SddmsError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        let meta_command = RegexSet::new([
            r#"\\q(uit)?"#,
            r#"\\txn"#,
            r#"\\c(ancel)?"#,
        ]).unwrap();

        let commands = vec![
            MetaCommand::Quit,
            MetaCommand::PrintTransactionInfo,
            MetaCommand::CancelLine,
        ];

        let result = meta_command.matches(value).iter()
            .min().ok_or(SddmsError::client("Meta command not recognized"))?;

        Ok(commands.get(result).cloned().unwrap())
    }
}

#[derive(Debug)]
pub enum Command {
    Meta(MetaCommand),
    Lines(Vec<String>)
}

pub fn read_next_command<HelperT: Helper, HistoryT: History>(reader: &mut Editor<HelperT, HistoryT>) -> SddmsResult<Command> {

    let mut lines = Vec::new();
    let mut multiline = false;
    loop {
        // read the line given line
        let line = if !multiline {
            reader.readline(">> ")
        } else {
            reader.readline(" > ")
        };

        if line.is_err() {
            let err = SddmsError::client("Error while reading input lines")
                .with_cause(line.unwrap_err());
            return Err(err);
        }

        let line = line
            .map(|result| String::from(result.trim()))
            .unwrap();

        // check for meta command
        if MetaCommand::looks_like_meta_command(&line) {
            let meta = MetaCommand::try_from(line.as_str())?;
            return Ok(Command::Meta(meta));
        }

        let ends_with_semi = line.ends_with(';');

        // if the line ends with a semicolon, then we're done
        lines.push(line);
        if ends_with_semi {
            break;
        } else {
            // we need to keep readline lines
            multiline = true;
        }
    }

    Ok(Command::Lines(split_statements(lines)))
}

pub fn split_statements(lines: Vec<String>) -> Vec<String> {
    let buffer = lines.join("\n");
    buffer.split(";").into_iter()
        .map(|slice| slice.trim())
        .filter(|slice| !slice.is_empty())
        .map(|slice| format!("{};", slice))
        .collect()
}

#[cfg(test)]
mod tests {
    use crate::reader::split_statements;

    #[test]
    fn split_statements__works() {
        let lines = vec![
            String::from("hello world; how"),
            String::from("are you doing; I'm doing really"),
            String::from("well;")
        ];

        let actual = split_statements(lines);
        assert_eq!(actual.len(), 3);
        assert_eq!(actual[0], "hello world;");
        assert_eq!(actual[1], "how\nare you doing;");
        assert_eq!(actual[2], "I'm doing really\nwell;");
    }
}
