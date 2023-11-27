mod random_query_stmt;
mod query_specs;

use std::collections::{HashMap};
use std::ops::Range;
use rand::{Rng, thread_rng};
use rand::distributions::{Bernoulli, Distribution};
use rand::seq::{IteratorRandom};
use rusqlite::types::{Type, Value};
use crate::db_schema::{DatabaseSchema, TableInfo};
use crate::query_gen::query_specs::{GeneratedTransaction, RandomQuerySpec, RandomTransactionSpec};
use crate::query_gen::random_query_stmt::{RandomQueryStmt, RandomQueryStmtKind, RandomQueryStmtKindGen};
use crate::value_generator::ValueGeneratorMap;

fn sample_columns<'table, RngT: Rng>(rng: &mut RngT, table_spec: &'table TableInfo) -> HashMap<&'table String, &'table Type> {
    let col_count = rng.gen_range(1..=table_spec.fields().len());

    let columns = table_spec.fields().keys()
        .filter(|field_name| *field_name != table_spec.primary_key())
        .choose_multiple(rng, col_count);

    let mut sampled_columns: HashMap<&'table String, &'table Type> = HashMap::new();
    for column in columns {
        let (field_name, field_type) = table_spec.fields().get_key_value(column).unwrap();
        sampled_columns.insert(field_name, field_type);
    }

    sampled_columns
}

pub struct QueryGenerator {
    db_schema: DatabaseSchema,
    val_gen: ValueGeneratorMap,
}

impl QueryGenerator {
    pub fn new(db_schema: DatabaseSchema, value_gen: ValueGeneratorMap) -> Self {
        Self {
            db_schema,
            val_gen: value_gen
        }
    }

    fn gen_random_record_from_columns(&self, columns: &[String], table_info: &TableInfo) -> HashMap<String, Value> {
        let mut record: HashMap<String, Value> = HashMap::new();

        for column in columns {
            let col_type = table_info.fields().get(column).unwrap();
            let rand_value = self.val_gen.generate(col_type);
            record.insert(column.clone(), rand_value);
        }

        record
    }

    fn gen_random_records_from_columns(&self, columns: &[String], table_spec: &TableInfo, count_range: Range<usize>) -> Vec<HashMap<String, Value>> {
        let mut rng = thread_rng();
        let record_count = rng.gen_range(count_range);
        let mut records: Vec<HashMap<String, Value>> = Vec::with_capacity(record_count);
        for _ in 0..record_count {
            let record = self.gen_random_record_from_columns(&columns, table_spec);
            records.push(record);
        }

        records
    }

    fn generate_query_spec(&self) -> RandomQuerySpec {
        let mut rng = thread_rng();

        // randomly choose a table
        let (table_name, table_spec) = self.db_schema.tables().iter().choose(&mut rng).unwrap();

        let kind_gen = RandomQueryStmtKindGen;

        // randomly choose an operation type
        let stmt = match kind_gen.sample(&mut rng) {
            RandomQueryStmtKind::Select => {

                let columns = sample_columns(&mut rng, table_spec)
                    .keys()
                    .cloned()
                    .cloned()
                    .collect::<Vec<_>>();

                RandomQueryStmt::Select { columns }
            }
            RandomQueryStmtKind::Update => {
                let values = sample_columns(&mut rng, table_spec).into_iter()
                    .filter(|(key, _)| *key != table_spec.primary_key())
                    .map(|(name, field_type)| {
                        let random_value = self.val_gen.generate(field_type);
                        (name.clone(), random_value)
                    })
                    .collect::<HashMap<_,_>>();

                RandomQueryStmt::Update { updates: values }
            }
            RandomQueryStmtKind::Insert => {
                let columns = table_spec.fields()
                    .keys()
                    .filter(|key| *key != table_spec.primary_key())
                    .cloned()
                    .collect::<Vec<_>>();

                let records = self.gen_random_records_from_columns(&columns, table_spec, 0..6);

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
        for _ in 0..stmt_count {
            stmts.push(self.generate_query_spec());
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
