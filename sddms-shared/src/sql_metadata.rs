use std::collections::{HashMap, HashSet};
use sqlparser::ast::{Query, SetExpr, Statement, With};
use sqlparser::dialect::SQLiteDialect;
use sqlparser::parser::{Parser, ParserError};
use crate::error::SddmsError;

#[derive(Debug, Default)]
pub struct SqlMetadata {
    /// true if this statement modifies the database
    modifiable: bool,
    /// true if the QUERY will return data
    has_results: bool,
    /// The tables that are written by this statement
    write_tables: HashSet<String>,
    /// the tables that are read from
    read_tables: HashSet<String>,
}

impl SqlMetadata {
    pub fn modifiable(&self) -> bool {
        self.modifiable
    }
    pub fn write_tables(&self) -> &HashSet<String> {
        &self.write_tables
    }
    pub fn read_tables(&self) -> &HashSet<String> { &self.read_tables }

    pub fn has_results(&self) -> bool {
        self.has_results
    }

    pub fn take_write_tables(self) -> HashSet<String> { self.write_tables }

    fn merge_override_flags(mut self, mut other: SqlMetadata, modifiable: bool, has_results: bool) -> Self {
        other.read_tables.drain()
            .for_each(|read_table| {
                self.read_tables.insert(read_table);
            });

        other.write_tables.drain()
            .for_each(|write_table| {
                self.write_tables.insert(write_table);
            });

        Self {
            modifiable,
            has_results,
            read_tables: self.read_tables,
            write_tables: self.write_tables
        }
    }

    fn merge(self, other: SqlMetadata) -> Self {
        let modifiable = self.modifiable || other.modifiable;
        let has_results = self.has_results || other.has_results;
        self.merge_override_flags(other, modifiable, has_results)
    }

    fn remove_aliases<'items_lifetime, KeySetT: Iterator<Item=&'items_lifetime String>>(&mut self, aliases: KeySetT) {
        for alias in aliases {
            self.read_tables.remove(alias);
            self.write_tables.remove(alias);
        }
    }

    fn consolidate_tables(mut self) -> Self {
        let read_and_write = self.read_tables.intersection(&self.write_tables)
            .cloned()
            .collect::<HashSet<_>>();

        // any table that is both read and write should be just write
        for tab in read_and_write {
            self.read_tables.remove(&tab);
        }

        self
    }
}

fn extract_ctes_from_with(with: With) -> HashMap<String, SqlMetadata> {
    let mut cte_aliases: HashMap<String, SqlMetadata> = HashMap::new();
    for cte in with.cte_tables {
        let metadata = extract_metadata_from_query(cte.query);
        let alias_name = cte.alias.name.value.to_string();
        cte_aliases.insert(alias_name, metadata);
    }

    cte_aliases
}

fn extract_metadata_from_query(query: Box<Query>) -> SqlMetadata {

    let with_cte_aliases = if let Some(with) = query.with {
        extract_ctes_from_with(with)
    } else {
        HashMap::new()
    };

    let query_body = query.body;
    let mut body_metadata = match *query_body {
        SetExpr::Select(select) => {
            let read_tables = select.from.into_iter()
                .flat_map(|table| {
                    let relation_table = table.relation.to_string();
                    let mut join_tables = table.joins.iter()
                        .map(|join_tab| join_tab.relation.to_string())
                        .collect::<Vec<_>>();

                    join_tables.insert(0, relation_table);
                    join_tables
                })
                // remove any
                .filter(|read_tables| !with_cte_aliases.contains_key(read_tables))
                .collect::<Vec<_>>();

            SqlMetadata {
                modifiable: false,
                has_results: true,
                write_tables: Default::default(),
                read_tables: read_tables.into_iter().collect::<HashSet<_>>(),
            }
        }
        SetExpr::Query(query) => {
            // TODO recursive might be bad...
            extract_metadata_from_query(query)
        }
        SetExpr::SetOperation { .. } => { SqlMetadata::default() }
        SetExpr::Values(_) => {  SqlMetadata::default()  }
        SetExpr::Insert(insert_stmt) => {
            SqlMetadata::from(insert_stmt)
        }
        SetExpr::Update(update) => { SqlMetadata::from(update) }
        SetExpr::Table(_table) => {
            todo!()
        }
    };

    // remove any aliases from the body
    body_metadata.remove_aliases(with_cte_aliases.keys());

    // we only want body info for if things are modified and/or have results
    let modifiable = body_metadata.modifiable();
    let has_results = body_metadata.has_results();

    // merge all the data
    for with_metadata in with_cte_aliases.into_values() {
        body_metadata = body_metadata.merge_override_flags(with_metadata, modifiable, has_results);
    }

    // consolidate any tables in both read and write mode
    body_metadata.consolidate_tables()
}

impl From<Statement> for SqlMetadata {
    fn from(value: Statement) -> Self {
        let metadata = match value {
            Statement::Insert { table_name, source, .. } => {

                // read any metadata from source query
                let source_metadata = if let Some(source_query) = source {
                    extract_metadata_from_query(source_query)
                } else {
                    SqlMetadata::default()
                };

                // make metadata for the insert part
                let insert_metadata = SqlMetadata {
                    modifiable: true,
                    write_tables: HashSet::from([table_name.to_string()]),
                    read_tables: HashSet::default(),
                    has_results: false
                };

                // merge the two
                insert_metadata.merge_override_flags(source_metadata, true, false)
            }
            Statement::Update { table, .. } => {
                SqlMetadata {
                    modifiable: true,
                    write_tables: HashSet::from([table.relation.to_string()]),
                    read_tables: HashSet::default(),
                    has_results: false,
                }
            }
            Statement::Delete { tables, .. } => {
                SqlMetadata {
                    modifiable: true,
                    write_tables: HashSet::from_iter(tables.into_iter().map(|item| item.to_string())),
                    read_tables: HashSet::new(),
                    has_results: false,
                }
            }
            Statement::Query(query) => {
                extract_metadata_from_query(query)
            }

            // TODO lock the table that's created too
            Statement::CreateTable { query , .. } => {
                if let Some(query) = query {
                    extract_metadata_from_query(query)
                } else {
                    SqlMetadata::default()
                }
            }

            _other_stmt => {
                panic!("Unsupported SQL instruction type")
            }
        };

        metadata
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
    use std::collections::HashSet;
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
        assert!(metadata.read_tables.contains("students"));
    }

    #[test]
    fn parses_with_stmt_correctly() {
        let sql = "WITH teacher_id_set AS (SELECT id as teacher_id FROM professors ORDER BY RANDOM() LIMIT 1),
VALUES_CTE(class_name,enroll_count) AS (VALUES ('P3is',79),('hriWO9kPBr',81),('Iia',47)) INSERT INTO classes (class_name,enroll_count,teacher_id) SELECT class_name, enroll_count, teacher_id FROM VALUES_CTE,teacher_id_set;";
        let metadata = parse_statements(sql).unwrap();
        let metadata = metadata.get(0).unwrap();
        assert_eq!(metadata.has_results, false);
        assert_eq!(metadata.modifiable, true);
        assert_eq!(metadata.write_tables(), &HashSet::from(["classes".to_string()]));
        assert_eq!(metadata.read_tables(), &HashSet::from(["professors".to_string()]));
    }

    #[test]
    fn parses_insert_correctly() {
        let sql = "INSERT INTO students (column1, column2) VALUES ('value1', 'value2'),('value2', 'value3');";
        let metadata = parse_statements(sql).unwrap();
        let metadata = metadata.get(0).unwrap();
        assert_eq!(metadata.has_results, false);
        assert_eq!(metadata.modifiable, true);
        assert_eq!(metadata.write_tables(), &HashSet::from(["students".to_string()]));
        assert!(metadata.read_tables().is_empty());
    }

    #[test]
    fn parses_update_correctly() {
        let sql = "UPDATE students SET column1=column1 + 1 WHERE students=1;";
        let metadata = parse_statements(sql).unwrap();
        let metadata = metadata.get(0).unwrap();
        assert_eq!(metadata.has_results, false);
        assert_eq!(metadata.modifiable, true);
        assert_eq!(metadata.write_tables(), &HashSet::from(["students".to_string()]));
        assert!(metadata.read_tables().is_empty());
    }

    #[test]
    fn split_stmts_into_transactions_works() {
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
