pub mod random_query_stmt;
mod query_specs;

use std::collections::{HashMap};
use std::ops::Range;
use rand::{Rng, thread_rng};
use rand::distributions::{Bernoulli, Distribution};
use rand::seq::{IteratorRandom};
use rusqlite::types::{Value};
use crate::db_schema::{DatabaseSchema, TableInfo};
use crate::db_schema::field_info::FieldInfo;
use crate::query_gen::query_specs::{GeneratedTransaction, RandomQuerySpec, RandomTransactionSpec};
use crate::query_gen::random_query_stmt::{RandomQueryStmt, RandomQueryStmtKind, RandomQueryStmtKindGen};
use crate::value_generator::{TableRecordGenerator, ValueGeneratorMap};

fn sample_columns_pred<'table, RngT: Rng, PredT: Fn(&FieldInfo) -> bool>(rng: &mut RngT, table_spec: &'table TableInfo, pred: PredT) -> HashMap<&'table String, &'table FieldInfo> {
    let col_count = rng.gen_range(1..=table_spec.fields().len());

    let fields = table_spec.fields()
        .iter()
        .filter(|(_, field_info)| !field_info.primary_key() && pred(field_info))
        .choose_multiple(rng, col_count);

    let mut sampled_columns: HashMap<&'table String, &'table FieldInfo> = HashMap::new();
    for (column, field_info) in fields {

        sampled_columns.insert(column, field_info);
    }

    sampled_columns
}

fn sample_columns<'table, RngT: Rng>(rng: &mut RngT, table_spec: &'table TableInfo) -> HashMap<&'table String, &'table FieldInfo> {
    sample_columns_pred(rng, table_spec, |_| true)
}

pub struct QueryGenerator {
    db_schema: DatabaseSchema,
    table_gens: HashMap<String, TableRecordGenerator>,
}

impl QueryGenerator {
    pub fn new(db_schema: DatabaseSchema, value_gen: ValueGeneratorMap) -> Self {
        let mut table_gens: HashMap<String, TableRecordGenerator> = HashMap::new();

        for (table_name, table_info) in db_schema.tables() {
            let table_gen = TableRecordGenerator::new(table_info, &value_gen);
            table_gens.insert(table_name.to_string(), table_gen);
        }

        Self {
            db_schema,
            table_gens,
        }
    }

    fn gen_random_records_from_columns(&self, columns: &[String], table_gen: &TableRecordGenerator, count_range: Range<usize>) -> Vec<HashMap<String, Value>> {
        let mut rng = thread_rng();
        let record_count = rng.gen_range(count_range);
        let mut records: Vec<HashMap<String, Value>> = Vec::with_capacity(record_count);
        for _ in 0..record_count {
            let record = table_gen.generate_record(columns).unwrap();
            records.push(record);
        }

        records
    }

    fn generate_query_spec(&self) -> RandomQuerySpec {
        let mut rng = thread_rng();

        let kind_gen = RandomQueryStmtKindGen;
        let operation_kind = kind_gen.sample(&mut rng);

        // randomly choose a table
        let (table_name, table_spec) = self.db_schema.choose_table(&mut rng, Some(operation_kind.clone()));
        let table_gen = self.table_gens.get(table_name).unwrap();

        // randomly choose an operation type
        let stmt = match operation_kind {
            RandomQueryStmtKind::Select => {

                let columns = sample_columns(&mut rng, table_spec)
                    .keys()
                    .cloned()
                    .cloned()
                    .collect::<Vec<_>>();

                RandomQueryStmt::Select { columns }
            }
            RandomQueryStmtKind::Update => {
                let values = sample_columns_pred(&mut rng, table_spec, |field_info| !(field_info.generated() || field_info.auto_inc() || field_info.foreign_key().is_some())).into_iter()
                    .filter(|(_, info)| !info.primary_key())
                    .map(|(name, _)| {
                        let random_value = table_gen.generate_for_column(name).unwrap();
                        (name.clone(), random_value)
                    })
                    .collect::<HashMap<_, _>>();

                // make the predicate
                let primary_key_field_name  = table_spec.fields().iter()
                    .filter_map(|(field, field_info)| if field_info.primary_key() {
                        Some(field.clone())
                    } else { None })
                    .next()
                    .unwrap();

                let predicate = format!("{} IN (SELECT {} FROM {} ORDER BY RANDOM() LIMIT 1)", primary_key_field_name, primary_key_field_name, table_name);

                RandomQueryStmt::Update { updates: values, predicate }
            }
            RandomQueryStmtKind::Insert => {
                let columns = table_spec.fields()
                    .iter()
                    .filter(|(_, info)| !info.primary_key())
                    .map(|(key, _)| key)
                    .cloned()
                    .collect::<Vec<_>>();

                let records = self.gen_random_records_from_columns(&columns, table_gen, 1..6);

                RandomQueryStmt::Insert { columns, values: records }
            }
        };

        RandomQuerySpec {
            table_name: table_name.clone(),
            stmt
        }
    }

    fn gen_transaction(&self) -> RandomTransactionSpec {
        let mut rng = thread_rng();
        let is_multi = rng.sample(Bernoulli::new(0.65f64).unwrap());
        let stmt_count = if is_multi {
            rng.gen_range(1..5)
        } else {
            1
        };

        let mut stmts: Vec<RandomQuerySpec> = Vec::with_capacity(stmt_count);

        while stmts.len() < stmt_count {
            let random_query = self.generate_query_spec();
            if !random_query.is_empty() {
                stmts.push(random_query);
            }
        }

        RandomTransactionSpec {
            single: !is_multi,
            stmts
        }
    }

    pub fn gen_transactions(&self, count: usize) -> Vec<GeneratedTransaction> {
        let mut txns: Vec<GeneratedTransaction> = Vec::with_capacity(count);
        for _ in 0..count {
            let transaction_spec = self.gen_transaction();
            let gen_txn = GeneratedTransaction::from(transaction_spec);
            txns.push(gen_txn);
        }

        txns
    }
}
