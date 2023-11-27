mod check_parser;
pub mod field_info;

use std::collections::{HashMap, HashSet};
use rand::Rng;
use rand::seq::IteratorRandom;
use rusqlite::Connection;
use rusqlite::types::Type;
use sqlparser::ast::{DataType, Statement, TableConstraint};
use sqlparser::dialect::SQLiteDialect;
use sqlparser::parser::Parser;
use sddms_shared::error::SddmsError;
use crate::db_schema::field_info::{FieldInfo, ForeignKey};
use crate::query_gen::random_query_stmt::RandomQueryStmtKind;

struct TableMetadata {
    tp: String,
    name: String,
    table_name: String,
    sql: String,
}

impl TableMetadata {

    fn map_data_type_to_sqlite_type(data_type: DataType) -> Result<Type, SddmsError> {
        match data_type {
            DataType::Character(_) |
            DataType::Char(_) |
            DataType::CharacterVarying(_) |
            DataType::CharVarying(_) |
            DataType::Varchar(_) |
            DataType::Nvarchar(_) |
            DataType::Uuid |
            DataType::Text |
            DataType::String(_) |
            DataType::Bytea => Ok(Type::Text),

            DataType::Binary(_) |
            DataType::Varbinary(_) |
            DataType::Blob(_) |
            DataType::Bytes(_) => Ok(Type::Blob),

            DataType::Decimal(_) |
            DataType::BigDecimal(_) |
            DataType::Dec(_) |
            DataType::Float(_) |
            DataType::Float4 |
            DataType::Float64 |
            DataType::Real |
            DataType::Float8 |
            DataType::Double |
            DataType::DoublePrecision => Ok(Type::Real),

            DataType::TinyInt(_) |
            DataType::UnsignedTinyInt(_) |
            DataType::Int2(_) |
            DataType::UnsignedInt2(_) |
            DataType::SmallInt(_) |
            DataType::UnsignedSmallInt(_) |
            DataType::MediumInt(_) |
            DataType::UnsignedMediumInt(_) |
            DataType::Int(_) |
            DataType::Int4(_) |
            DataType::Int64 |
            DataType::Integer(_) |
            DataType::UnsignedInt(_) |
            DataType::UnsignedInt4(_) |
            DataType::UnsignedInteger(_) |
            DataType::BigInt(_) |
            DataType::UnsignedBigInt(_) |
            DataType::Int8(_) |
            DataType::UnsignedInt8(_) |
            DataType::Bool |
            DataType::Boolean => Ok(Type::Integer),

            _ => Err(SddmsError::general("Unsupported datatype"))
        }
    }
}

#[derive(Debug)]
pub struct TableInfo {
    /// the name of the table
    name: String,
    /// fields
    fields: HashMap<String, FieldInfo>,
}

impl TableInfo {
    pub fn name(&self) -> &str {
        &self.name
    }
    pub fn fields(&self) -> &HashMap<String, FieldInfo> {
        &self.fields
    }
    pub fn fields_mut(&mut self) -> &mut HashMap<String, FieldInfo> { &mut self.fields }
}

impl TryFrom<TableMetadata> for TableInfo {
    type Error = SddmsError;

    fn try_from(value: TableMetadata) -> Result<Self, Self::Error> {
        let dialect = SQLiteDialect {};
        let create_table_statement = Parser::new(&dialect)
            .try_with_sql(&value.sql)
            .map_err(|err| SddmsError::general(format!("Error while parsing table spec for table {}", value.table_name)).with_cause(err))?
            .parse_statement()
            .map_err(|err| SddmsError::general(format!("Error while parsing table spec statement for table {}", value.table_name)).with_cause(err))?;

        let Statement::CreateTable { columns, constraints, .. } = create_table_statement else {
            panic!("Is not a CREATE_TABLE statement")
        };

        let mut column_specs: HashMap<String, FieldInfo> = HashMap::new();
        for column in columns {
            let column_name = (&column).name.value.clone();
            let field_info = FieldInfo::from(column);
            column_specs.insert(column_name, field_info);
        }

        for constraint in constraints {
            match constraint {
                TableConstraint::Unique { is_primary, columns, .. } => {
                    for column in columns {
                        column_specs.get_mut(&column.to_string()).unwrap().set_primary_key(is_primary);
                    }
                }
                TableConstraint::ForeignKey { referred_columns, foreign_table, columns, .. } => {
                    let foreign_key = ForeignKey::new(foreign_table.to_string(), referred_columns.first().unwrap().to_string());
                    for column in columns {
                        column_specs.get_mut(&column.to_string()).unwrap().set_foreign_key(foreign_key.clone());
                    }
                }
                TableConstraint::Check { .. } => {}
                TableConstraint::Index { .. } => {}
                TableConstraint::FulltextOrSpatial { .. } => {}
            }
        }

        Ok(TableInfo {
            name: value.table_name,
            fields: column_specs,
        })
    }
}

#[derive(Debug)]
pub struct DatabaseSchema {
    tables: HashMap<String, TableInfo>,
    insert_restricted: HashSet<String>,
    update_restricted: HashSet<String>,
}

impl DatabaseSchema {

    fn get_table_metadata(connection: &Connection) -> Vec<TableMetadata> {
        let mut table_inspect_query = connection.prepare("SELECT * FROM sqlite_master").unwrap();
        table_inspect_query.query_map([], |row| {
            let tp: String = row.get(0).unwrap();
            let name: String = row.get(1).unwrap();
            let table_name: String = row.get(2).unwrap();
            let sql: String = row.get(4).unwrap();

            Ok(TableMetadata {
                tp,
                name,
                table_name: table_name.to_string(),
                sql
            })
        }).unwrap()
            .map(|res| res.unwrap())
            .collect::<Vec<_>>()
    }

    fn resolve_foreign_key_types(mut tables: HashMap<String, TableInfo>) -> HashMap<String, TableInfo> {
        let mut all_resolved_field_names: Vec<(String, String, ForeignKey)> = Vec::new();
        // get a list of fields that need updating
        for (table_name, table) in &tables {
            let mut updated_foreign_key_fields: Vec<(String, String, ForeignKey)> = table.fields.iter()
                .filter(|(_, field)| field.foreign_key().as_ref().is_some_and(|inner| inner.tp().is_none()))
                .map(|(field_name, field_info)| {
                    let foreign_key = field_info.foreign_key().clone().unwrap();
                    let foreign_key_type = tables
                        .get(foreign_key.table()).unwrap()
                        .fields()
                        .get(foreign_key.field()).unwrap()
                        .tp().clone();

                    (table_name.clone(), field_name.clone(), foreign_key.with_type(foreign_key_type))
                })
                .collect::<Vec<_>>();
            all_resolved_field_names.append(&mut updated_foreign_key_fields);
        }

        // make all the updates
        for (table_name, field_name, updated_foreign_key) in all_resolved_field_names {
            tables
                .get_mut(&table_name).unwrap()
                .fields_mut().get_mut(&field_name).unwrap()
                .set_foreign_key(updated_foreign_key);
        }

        tables
    }

    pub fn new(connection: &Connection) -> DatabaseSchema {
        let table_metadata = Self::get_table_metadata(connection);

        let mut tables: HashMap<String, TableInfo> = HashMap::new();

        for metadata in table_metadata.into_iter()
            .filter_map(|metadata| TableInfo::try_from(metadata).ok()) {
            tables.insert(metadata.name.clone(), metadata);
        }

        // resolve all of the types of any foreign keys
        tables = Self::resolve_foreign_key_types(tables);

        Self {
            tables,
            insert_restricted: HashSet::new(),
            update_restricted: HashSet::new(),
        }
    }

    pub fn add_insert_restricted<StrT: Into<String>>(&mut self, tab: StrT) {
        self.insert_restricted.insert(tab.into());
    }

    pub fn add_update_restricted<StrT: Into<String>>(&mut self, tab: StrT) {
        self.update_restricted.insert(tab.into());
    }

    pub fn choose_table<RngT: Rng>(&self, rng: &mut RngT, op_kind: Option<RandomQueryStmtKind>) -> (&String, &TableInfo) {
        self.tables.iter()
            .filter(|(tab_name, _)| {
                let op_kind = op_kind.as_ref();
                match op_kind {
                    None => true,
                    Some(kind) => match kind {
                        RandomQueryStmtKind::Select => true,
                        RandomQueryStmtKind::Update => !self.update_restricted.contains(*tab_name),
                        RandomQueryStmtKind::Insert => !self.insert_restricted.contains(*tab_name),
                    }
                }
            })
            .choose(rng)
            .unwrap()
    }

    pub fn tables(&self) -> &HashMap<String, TableInfo> {
        &self.tables
    }
}
