use std::collections::{HashMap};
use rusqlite::Connection;
use rusqlite::types::Type;
use sqlparser::ast::{ColumnOption, DataType, Statement};
use sqlparser::dialect::SQLiteDialect;
use sqlparser::parser::Parser;
use sddms_shared::error::SddmsError;

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
    fields: HashMap<String, Type>,
    /// the primary key field
    primary_key: String,
}

impl TableInfo {
    pub fn name(&self) -> &str {
        &self.name
    }
    pub fn fields(&self) -> &HashMap<String, Type> {
        &self.fields
    }
    pub fn primary_key(&self) -> &str {
        &self.primary_key
    }
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

        let Statement::CreateTable { columns, .. } = create_table_statement else {
            panic!("Is not a CREATE_TABLE statement")
        };

        let mut primary_key: Option<String> = None;
        let mut column_specs: HashMap<String, Type> = HashMap::new();
        for column in columns {
            let column_name = column.name.value;

            let is_primary_key = column.options.into_iter()
                .map(|opt| opt.option)
                .any(|option| match option {
                    ColumnOption::Unique { is_primary } => is_primary,
                    _ => false
                });

            if is_primary_key {
                primary_key = Some(column_name.clone());
            }

            let column_type = TableMetadata::map_data_type_to_sqlite_type(column.data_type).unwrap();
            column_specs.insert(column_name, column_type);
        }

        Ok(TableInfo {
            name: value.table_name,
            fields: column_specs,
            primary_key: primary_key.unwrap()
        })
    }
}

#[derive(Debug)]
pub struct DatabaseSchema {
    tables: HashMap<String, TableInfo>
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

    pub fn new(connection: &Connection) -> DatabaseSchema {
        let table_metadata = Self::get_table_metadata(connection);

        let mut tables: HashMap<String, TableInfo> = HashMap::new();

        for metadata in table_metadata.into_iter()
            .filter_map(|metadata| TableInfo::try_from(metadata).ok()) {
            tables.insert(metadata.name.clone(), metadata);
        }

        Self {
            tables
        }
    }


    pub fn tables(&self) -> &HashMap<String, TableInfo> {
        &self.tables
    }
}
