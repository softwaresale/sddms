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

    pub fn take_tables(self) -> HashSet<String> { self.tables }
}

fn extract_tables_from_query(query: Box<Query>) -> Vec<String> {
    let query_body = query.body;
    match *query_body {
        SetExpr::Select(select) => {
            select.from.into_iter()
                .flat_map(|table| {
                    let relation_table = table.relation.to_string();
                    let mut join_tables = table.joins.iter()
                        .map(|join_tab| join_tab.relation.to_string())
                        .collect::<Vec<_>>();

                    join_tables.insert(0, relation_table);
                    join_tables
                })
                .collect::<Vec<_>>()
        }
        SetExpr::Query(query) => {
            // TODO recursive might be bad...
            extract_tables_from_query(query)
        }
        SetExpr::SetOperation { .. } => { vec![] }
        SetExpr::Values(_) => { vec![] }
        SetExpr::Insert(_) => { vec![] }
        SetExpr::Update(_) => { vec![] }
        SetExpr::Table(table) => {
            table.table_name.map(|name| vec![name])
                .unwrap_or_default()
        }
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

enum TransactionStatementMode {
    Open,
    Close,
    Normal,
}

fn classify_transaction_stmt(sql: &str) -> Result<TransactionStatementMode, SddmsError> {
    let trans_stmt = parse_transaction_stmt(sql)?;
    if trans_stmt.is_none() {
        return Ok(TransactionStatementMode::Normal);
    }

    Ok(match trans_stmt.unwrap() {
        TransactionStmt::Begin => TransactionStatementMode::Open,
        _ => TransactionStatementMode::Close
    })
}

pub fn split_stmts_into_transactions(stmts: Vec<String>) -> Result<Vec<Vec<String>>, SddmsError> {
    let mut transactions: Vec<Vec<String>> = Vec::new();
    let mut has_transaction = false;
    for stmt in stmts {
        match classify_transaction_stmt(&stmt)? {
            TransactionStatementMode::Open => {
                let new_transaction = vec![stmt];
                transactions.push(new_transaction);
                has_transaction = true;
            }
            TransactionStatementMode::Normal => {
                if has_transaction {
                    transactions.last_mut().unwrap().push(stmt)
                } else {
                    transactions.push(vec![stmt])
                }
            }
            TransactionStatementMode::Close => {
                transactions.last_mut().unwrap().push(stmt);
                has_transaction = false;
            }
        }
    }

    Ok(transactions)
}

#[cfg(test)]
mod tests {
    use crate::sql_metadata::{parse_statements, split_stmts_into_transactions};

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

    #[test]
    fn split_stmts_into_transactions__works() {
        let stmts = vec!["BEGIN", "SELECT * FROM STUDENTS", "COMMIT", "SELECT * FROM STUDENTS", "BEGIN", "SELECT * FROM STUDENTS", "COMMIT"].iter()
            .map(|str_ref| str_ref.to_string())
            .collect::<Vec<_>>();
        let transactions = split_stmts_into_transactions(stmts).unwrap();
        assert_eq!(transactions.len(), 3);
        assert_eq!(transactions.get(0).unwrap(), &vec!["BEGIN", "SELECT * FROM STUDENTS", "COMMIT"].iter()
            .map(|str_ref| str_ref.to_string())
            .collect::<Vec<_>>());
        assert_eq!(transactions.get(1).unwrap(), &vec!["SELECT * FROM STUDENTS"].iter()
            .map(|str_ref| str_ref.to_string())
            .collect::<Vec<_>>());
        assert_eq!(transactions.get(2).unwrap(), &vec!["BEGIN", "SELECT * FROM STUDENTS", "COMMIT"].iter()
            .map(|str_ref| str_ref.to_string())
            .collect::<Vec<_>>());
    }
}
