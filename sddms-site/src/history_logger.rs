use std::collections::HashSet;
use std::fs::{File};
use std::io::{BufWriter, Write};
use std::path::Path;
use sddms_shared::error::SddmsError;
use sddms_shared::sql_metadata::parse_statements;

pub trait HistoryLogger: Send {
    fn log(&mut self, client_id: u32, site_id: u32, trans_id: u32, cmd: &str) -> Result<(), SddmsError>;
    fn log_replication(&mut self, originating_site: u32, cmds: &[String]) -> Result<(), SddmsError>;

    fn log_query(&mut self, client_id: u32, site_id: u32, trans_id: u32, write_set: &[String], read_set: &[String]) -> Result<(), SddmsError> {
        let read_set_string = if !read_set.is_empty() {
            format!("Read({:?})", read_set)
        } else {
            String::default()
        };

        let write_set_string = if !write_set.is_empty() {
            format!("Write({:?})", write_set)
        } else {
            String::default()
        };

        let joiner = if !(write_set.is_empty() || read_set.is_empty()) {
            ","
        } else {
            ""
        };

        let total = format!("{}{}{}", read_set_string, joiner, write_set_string);
        self.log(client_id, site_id, trans_id, &total)
    }
}

pub struct FileHistoryLogger
{
    output: BufWriter<File>
}

impl FileHistoryLogger {
    pub fn open(path: &Path) -> Result<Self, SddmsError> {
        let output = File::options()
            .create(true)
            .append(false)
            .write(true)
            .open(path)
            .map_err(|err| SddmsError::general("Failed to open history file").with_cause(err))?;

        Ok(Self {
            output: BufWriter::new(output)
        })
    }
}

impl HistoryLogger for FileHistoryLogger {
    fn log(&mut self, client_id: u32, site_id: u32, trans_id: u32, cmd: &str) -> Result<(), SddmsError> {
        self.output.write_fmt(format_args!("site={}, client={}, txn={}: {}\n", site_id, client_id, trans_id, cmd))
            .map_err(|err| SddmsError::general("Failed to log history").with_cause(err))?;
        self.output.flush()
            .map_err(|err| SddmsError::general("Failed to flush history").with_cause(err))
    }

    fn log_replication(&mut self, originating_site: u32, cmds: &[String]) -> Result<(), SddmsError> {

        let mut write_tables = Vec::new();
        for cmd in cmds {
            let Ok(stmt_metadatas) = parse_statements(cmd) else {
                return Err(SddmsError::site("Failed to parse replication statement"));
            };
            let unique_write_tables = stmt_metadatas.into_iter()
                .flat_map(|metadata| metadata.take_write_tables())
                .collect::<HashSet<_>>();

            write_tables.extend(unique_write_tables.into_iter());
        }

        let write_info = format!("Write({:?})", write_tables);

        self.output.write_fmt(format_args!("replication: orig_site={}: {}\n", originating_site, write_info))
            .map_err(|err| SddmsError::general("Failed to log history").with_cause(err))?;
        self.output.flush()
            .map_err(|err| SddmsError::general("Failed to flush history").with_cause(err))
    }
}

pub struct NopHistoryLogger;

impl HistoryLogger for NopHistoryLogger {
    fn log(&mut self, _client_id: u32, _site_id: u32, _trans_id: u32, _cmd: &str) -> Result<(), SddmsError> {
        Ok(())
    }

    fn log_replication(&mut self, _originating_site: u32, _cmds: &[String]) -> Result<(), SddmsError> {
        Ok(())
    }
}
