use std::collections::HashSet;
use sqlparser::ast::{Query, SetExpr, Statement};
use sqlparser::dialect::SQLiteDialect;
use sqlparser::parser::{Parser, ParserError};
use crate::error::SddmsError;

pub struct SqlMetadata {
    /// true if this statement modifies the database
    modifiable: bool,
    /// true if the QUERY will return data
    has_results: bool,
    /// The tables touches by this sql metadata
    tables: HashSet<String>,
}

impl SqlMetadata {
    pub fn modifiable(&self) -> bool {
        self.modifiable
    }
    pub fn tables(&self) -> &HashSet<String> {
        &self.tables
    }

    pub fn has_results(&self) -> bool {
        self.has_results
    }
}

fn extract_tables_from_query(query: Box<Query>) -> Vec<String> {
    let query_body = query.body;
    match *query_body {
        SetExpr::Select(select) => {
            select.from.into_iter()
                .map(|table| table.relation.to_string())
                .collect::<Vec<_>>()
        }
        SetExpr::Query(_query) => {
            vec![]
        }
        SetExpr::SetOperation { .. } => { vec![] }
        SetExpr::Values(_) => { vec![] }
        SetExpr::Insert(_) => { vec![] }
        SetExpr::Update(_) => { vec![] }
        SetExpr::Table(table) => { vec![table.table_name.unwrap_or_default()] }
    }
}

impl From<Statement> for SqlMetadata {
    fn from(value: Statement) -> Self {
        match value {
            Statement::Insert { table_name, .. } => {
                SqlMetadata {
                    modifiable: true,
                    tables: HashSet::from([table_name.to_string()]),
                    has_results: false
                }
            }
            Statement::Update { table, .. } => {
                SqlMetadata {
                    modifiable: true,
                    tables: HashSet::from([table.relation.to_string()]),
                    has_results: false,
                }
            }
            Statement::Delete { tables, .. } => {
                SqlMetadata {
                    modifiable: true,
                    tables: HashSet::from_iter(tables.into_iter().map(|item| item.to_string())),
                    has_results: false,
                }
            }
            Statement::Query(query) => {
                let tables = extract_tables_from_query(query);
                SqlMetadata {
                    modifiable: false,
                    tables: HashSet::from_iter(tables.into_iter()),
                    has_results: true,
                }
            }

            // TODO lock the table that's created too
            Statement::CreateTable { query , .. } => {
                let read_tables = if let Some(query) = query {
                    extract_tables_from_query(query)
                } else {
                    vec![]
                };

                SqlMetadata {
                    modifiable: false,
                    tables: HashSet::from_iter(read_tables.into_iter()),
                    has_results: false
                }
            }

            _other_stmt => {
                panic!("Unsupported SQL instruction type")
            }
        }
    }
}

pub fn parse_statements(sql: &str) -> Result<Vec<SqlMetadata>, ParserError> {
    let dialect = SQLiteDialect {};
    let statements = Parser::parse_sql(&dialect, sql)?;
    let metadata = statements.into_iter()
        .map(|item| SqlMetadata::from(item))
        .collect::<Vec<_>>();

    Ok(metadata)
}

#[derive(Debug)]
pub enum TransactionStmt {
    Begin,
    Commit,
    Rollback,
}

pub fn parse_transaction_stmt(sql: &str) -> Result<Option<TransactionStmt>, SddmsError> {
    let dialect = SQLiteDialect {};
    let mut statements = Parser::parse_sql(&dialect, sql)
        .map_err(|err| SddmsError::client("Failed to parse sql").with_cause(err))?;

    if statements.len() != 1 {
        panic!("Too many statements. Expected 1 but got {}", statements.len())
    }
    
    let statement = statements.swap_remove(0);
    
    let transaction_kind = match statement {
        Statement::StartTransaction { .. } => Some(TransactionStmt::Begin),
        Statement::Commit { .. } => Some(TransactionStmt::Commit),
        Statement::Rollback { .. } => Some(TransactionStmt::Rollback),
        _ => None
    };
    
    Ok(transaction_kind)
}

#[cfg(test)]
mod tests {
    use crate::sql_metadata::parse_statements;

    #[test]
    fn parses_select() {
        let sql = "SELECT * FROM students;";
        let metadata_result = parse_statements(sql);
        assert!(metadata_result.is_ok());
        let metadata_list = metadata_result.unwrap();
        assert!(metadata_list.len() >= 1);
        let metadata = metadata_list.get(0).unwrap();
        assert_eq!(metadata.modifiable, false);
        assert!(metadata.tables.contains("students"));
    }
}
