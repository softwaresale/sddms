use std::collections::{HashMap, HashSet};
use std::fmt::{Display, Formatter};
use crate::query_gen::random_query_stmt::RandomQueryStmt;
use sql_query_builder as sqlb;
use sql_query_builder::Transaction;
use rusqlite::types::Value;
use crate::db_schema::field_info::ForeignKey;

pub struct RandomQuerySpec {
    /// The table we are operating on
    pub(super) table_name: String,
    /// the statement we're going to make
    pub(super) stmt: RandomQueryStmt
}

impl RandomQuerySpec {
    pub fn is_empty(&self) -> bool {
        match &self.stmt {
            RandomQueryStmt::Select { columns } => columns.is_empty(),
            RandomQueryStmt::Update { updates, .. } => updates.is_empty(),
            RandomQueryStmt::Insert { values, columns, foreign_keys } => columns.is_empty() || values.is_empty() || foreign_keys.is_empty()
        }
    }
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

fn create_foreign_keys_with_clauses(record_count: usize, foreign_keys: &HashMap<String, ForeignKey>) -> HashMap<String, (String, String)> {
    let mut columns: HashMap<String, (String, String)> = HashMap::with_capacity(foreign_keys.len());
    for (column_name, foreign_key) in foreign_keys {
        let foreign_field = foreign_key.field();
        let foreign_table = foreign_key.table();

        let set_name = format!("{}_set", column_name);
        let query = format!("{} AS (SELECT {} as {} FROM {} ORDER BY RANDOM() LIMIT {})", set_name, foreign_field, column_name, foreign_table, 1 /* was record_count */);
        columns.insert(column_name.clone(), (set_name, query));
    }

    columns
}

fn create_values_with_clauses(column_order: &[String], foreign_key_columns: &HashSet<String>, records: Vec<HashMap<String, Value>>) -> (String, String) {

    let records_column_order = column_order.into_iter()
        .filter(|name| !foreign_key_columns.contains(*name))
        .cloned()
        .collect::<Vec<_>>();

    let column_order_str = &records_column_order
        .join(",");

    let records_string = records.into_iter()
        .map(|record| stringify_record_value(&records_column_order, record))
        .collect::<Vec<_>>()
        .join(",");

    let clause = format!("VALUES_CTE({}) AS (VALUES {})", column_order_str, records_string);
    ("VALUES_CTE".to_string(), clause)
}

fn create_with_cause(clauses: Vec<&String>) -> String {
    let clauses_string = clauses.into_iter()
        .cloned()
        .collect::<Vec<_>>()
        .join(",\n");
    format!("WITH {}\n", clauses_string)
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
            RandomQueryStmt::Update { updates, predicate } => {
                let sets = updates.into_iter()
                    .map(|(field, value)| stringify_update_set(field, value))
                    .collect::<Vec<_>>();

                let mut update = sqlb::Update::new();
                for set in &sets {
                    update = update.set(set);
                }
                update = update.update(&name);
                update = update.where_clause(&predicate);

                SqlQuery::Update(update)
            }
            RandomQueryStmt::Insert { columns, values, foreign_keys } => {

                let foreign_key_columns = foreign_keys.keys().cloned().collect::<HashSet<_>>();
                let foreign_keys_clause_map = create_foreign_keys_with_clauses(values.len(), &foreign_keys);
                let (values_clause_ref, values_clause) = create_values_with_clauses(&columns,  &foreign_key_columns, values);

                let mut foreign_key_clauses = foreign_keys_clause_map
                    .values()
                    .map(|(_, clause)| clause)
                    .collect::<Vec<_>>();

                foreign_key_clauses.push(&values_clause);

                let with_clause = create_with_cause(foreign_key_clauses);

                let mut value_select = sqlb::Select::new();
                for column in &columns {
                    value_select = value_select.select(column);
                }

                let column_to_set_map = foreign_keys_clause_map.into_iter()
                    .map(|(_, (handle_name, _))| (handle_name))
                    .collect::<Vec<_>>()
                    .join(",");

                let value_select_from = format!("{},{}", values_clause_ref, column_to_set_map);
                value_select = value_select.from(&value_select_from);

                let mut insert = sqlb::Insert::new();
                insert = insert.raw(&with_clause);
                insert = insert.insert_into(&stringify_insert_into(&name, &columns));
                insert = insert.select(value_select);

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
