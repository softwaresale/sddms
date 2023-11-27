use std::collections::HashMap;
use std::fmt::{Display, Formatter};
use crate::query_gen::random_query_stmt::RandomQueryStmt;
use sql_query_builder as sqlb;
use sql_query_builder::Transaction;
use rusqlite::types::Value;

pub struct RandomQuerySpec {
    /// The table we are operating on
    pub(super) table_name: String,
    /// the statement we're going to make
    pub(super)stmt: RandomQueryStmt
}

pub struct RandomTransactionSpec {
    pub(super) single: bool,
    pub(super) stmts: Vec<RandomQuerySpec>,
}

pub enum SqlQuery {
    Select(sqlb::Select),
    Insert(sqlb::Insert),
    Update(sqlb::Update),
}

impl Display for SqlQuery {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SqlQuery::Select(select) => write!(f, "{}", select),
            SqlQuery::Insert(insert) => write!(f, "{}", insert),
            SqlQuery::Update(update) => write!(f, "{}", update)
        }
    }
}

fn stringify_value(value: Value) -> String {
    match value {
        Value::Null => "null".to_string(),
        Value::Integer(iv) => iv.to_string(),
        Value::Real(real) => real.to_string(),
        Value::Text(string) => format!("'{}'", string),
        Value::Blob(blob) => String::from_utf8(blob).unwrap(),
    }
}

fn stringify_update_set(field: String, value: Value) -> String {
    let value_str = stringify_value(value);

    format!("{} = {}", field, value_str)
}

fn stringify_insert_into(table_name: &str, columns: &[String]) -> String {
    let columns_str = columns.join(",");
    format!("{} ({})", table_name, columns_str)
}

fn stringify_record_value(columns: &[String], mut record: HashMap<String, Value>) -> String {
    let mut value_array: Vec<Value> = Vec::with_capacity(record.len());
    for column in columns {
        let value = record.remove(column).unwrap();
        value_array.push(value);
    }

    let inner = value_array.into_iter()
        .map(|value| stringify_value(value))
        .collect::<Vec<_>>()
        .join(",");

    format!("({inner})")
}

impl From<RandomQuerySpec> for SqlQuery {
    fn from(value: RandomQuerySpec) -> Self {
        let name = value.table_name;
        let stmt = value.stmt;

        match stmt {
            RandomQueryStmt::Select { columns } => {
                let mut select_builder = sqlb::Select::new();
                for column in &columns {
                    select_builder = select_builder.select(column);
                }

                select_builder = select_builder.from(&name);
                SqlQuery::Select(select_builder)
            }
            RandomQueryStmt::Update { updates } => {
                let sets = updates.into_iter()
                    .map(|(field, value)| stringify_update_set(field, value))
                    .collect::<Vec<_>>();

                let mut update = sqlb::Update::new();
                for set in &sets {
                    update = update.set(set);
                }
                update = update.update(&name);

                SqlQuery::Update(update)
            }
            RandomQueryStmt::Insert { columns, values } => {
                let mut insert = sqlb::Insert::new();
                insert = insert.insert_into(&stringify_insert_into(&name, &columns));
                for value in values {
                    insert = insert.values(&stringify_record_value(&columns, value));
                }

                SqlQuery::Insert(insert)
            }
        }
    }
}

pub enum GeneratedTransaction {
    Single(String),
    Transaction(Transaction)
}

impl From<RandomTransactionSpec> for GeneratedTransaction {
    fn from(mut value: RandomTransactionSpec) -> Self {
        if value.single {
            let single = value.stmts.remove(0);
            let query = SqlQuery::from(single);
            Self::Single(query.to_string())
        } else {
            let mut transaction = Transaction::new();
            transaction = transaction.start_transaction("");
            for stmt in value.stmts {
                let sql = SqlQuery::from(stmt);
                transaction = match sql {
                    SqlQuery::Select(select) => transaction.select(select),
                    SqlQuery::Insert(insert) => transaction.insert(insert),
                    SqlQuery::Update(update) => transaction.update(update)
                };
            }
            transaction = transaction.commit("");

            Self::Transaction(transaction)
        }
    }
}

impl Display for GeneratedTransaction {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            GeneratedTransaction::Single(single) => f.write_str(single),
            GeneratedTransaction::Transaction(txn) => write!(f, "{}", txn)
        }
    }
}
