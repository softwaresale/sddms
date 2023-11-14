use serde_json::{Map, Value};
use tabled::builder::Builder;
use tabled::Table;

#[derive(Debug)]
pub struct ResultsInfo {
    pub columns: Vec<String>,
    pub results: Vec<Map<String, Value>>,
}

#[derive(Debug)]
pub enum QueryResults {
    AffectedRows(u32),
    Results(ResultsInfo)
}

impl Into<Table> for ResultsInfo {
    fn into(self) -> Table {

        let columns = self.columns;
        let mut rows: Vec<Vec<String>> = Vec::new();
        for record in self.results {
            let mut row: Vec<String> = Vec::new();
            for column_name in &columns {
                let column_value = record.get(column_name).unwrap();
                row.push(column_value.to_string())
            }
            rows.push(row);
        }

        let mut builder = Builder::new();
        builder.set_header(columns);
        for row in rows {
            builder.push_record(row);
        }

        builder.build()
    }
}
