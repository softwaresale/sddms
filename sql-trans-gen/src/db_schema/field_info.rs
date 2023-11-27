use std::ops::{Range, RangeInclusive};
use rusqlite::types::Type;
use sqlparser::ast::{ColumnDef, ColumnOption};
use crate::db_schema::check_parser::{extract_range_from_check_expr, NumericalRange};
use crate::db_schema::TableMetadata;

#[derive(Debug, Clone)]
pub struct ForeignKey {
    /// The table this key belongs to
    table: String,
    /// the field name on the table
    field: String,
    /// the type of this field
    tp: Option<Type>,
}

impl ForeignKey {

    pub fn new(table_name: String, field: String) -> Self {
        Self {
            table: table_name,
            field,
            tp: None
        }
    }

    pub fn with_type(mut self, tp: Type) -> Self {
        self.tp = Some(tp);
        self
    }

    pub fn table(&self) -> &str {
        &self.table
    }
    pub fn field(&self) -> &str {
        &self.field
    }
    pub fn tp(&self) -> &Option<Type> {
        &self.tp
    }
}

#[derive(Debug)]
pub struct FieldInfo {
    /// the type of the field
    tp: Type,
    /// if it's the primary key or not
    primary_key: bool,
    /// true if this field is auto incremented
    auto_inc: bool,
    /// true if this column is generated
    generated: bool,
    /// if this field references a foreign key
    foreign_key: Option<ForeignKey>,
    /// Optional integer range constraint
    int_range_constraint: Option<Range<i64>>,
    int_range_inc_constraint: Option<RangeInclusive<i64>>,
    /// Optional float range constraint
    real_range_constraint: Option<Range<f64>>,
    real_range_inc_constraint: Option<RangeInclusive<f64>>,
}

impl FieldInfo {
    pub fn tp(&self) -> &Type {
        &self.tp
    }
    pub fn primary_key(&self) -> bool {
        self.primary_key
    }
    pub fn auto_inc(&self) -> bool {
        self.auto_inc
    }
    pub fn generated(&self) -> bool {
        self.generated
    }
    pub fn foreign_key(&self) -> &Option<ForeignKey> {
        &self.foreign_key
    }
    pub fn int_range_constraint(&self) -> &Option<Range<i64>> {
        &self.int_range_constraint
    }
    pub fn real_range_constraint(&self) -> &Option<Range<f64>> {
        &self.real_range_constraint
    }
    pub fn set_foreign_key(&mut self, foreign_key: ForeignKey) {
        self.foreign_key = Some(foreign_key);
    }

    pub fn set_primary_key(&mut self, primary_key: bool) {
        self.primary_key = primary_key;
    }
    pub fn int_range_inc_constraint(&self) -> &Option<RangeInclusive<i64>> {
        &self.int_range_inc_constraint
    }
    pub fn real_range_inc_constraint(&self) -> &Option<RangeInclusive<f64>> {
        &self.real_range_inc_constraint
    }
}

impl From<ColumnDef> for FieldInfo {
    fn from(value: ColumnDef) -> Self {
        let column_type = TableMetadata::map_data_type_to_sqlite_type(value.data_type).unwrap();
        let mut info = FieldInfo {
            tp: column_type.clone(),
            primary_key: false,
            auto_inc: false,
            generated: false,
            foreign_key: None,
            int_range_constraint: None,
            int_range_inc_constraint: None,
            real_range_constraint: None,
            real_range_inc_constraint: None,
        };
        for opt in value.options {
            match opt.option {
                ColumnOption::Null => {}
                ColumnOption::NotNull => {}
                ColumnOption::Default(_) => {}
                ColumnOption::Unique { is_primary } => {
                    info.primary_key = is_primary;
                }
                ColumnOption::ForeignKey { foreign_table, referred_columns, .. } => {
                    info.foreign_key = Some(ForeignKey {
                        table: foreign_table.to_string(),
                        field: if referred_columns.is_empty() { String::default() } else { referred_columns.first().unwrap().to_string() },
                        tp: None,
                    })
                }
                ColumnOption::Check(check_expr) => {
                    let num_constraint = extract_range_from_check_expr(check_expr, &column_type);
                    if let Some(constraint) = num_constraint {
                        match constraint {
                            NumericalRange::IntRange(int_range) => info.int_range_constraint = Some(int_range),
                            NumericalRange::FloatRange(float_range) => info.real_range_constraint = Some(float_range),
                            NumericalRange::IntRangeInclusive(int_range_inclusive) => info.int_range_inc_constraint = Some(int_range_inclusive),
                            NumericalRange::FloatRangeInclusive(float_range_inclusive) => info.real_range_inc_constraint = Some(float_range_inclusive),
                        }
                    }
                }
                ColumnOption::DialectSpecific(_) => {
                    info.auto_inc = true;
                }
                ColumnOption::CharacterSet(_) => {}
                ColumnOption::Comment(_) => {}
                ColumnOption::OnUpdate(_) => {}
                ColumnOption::Generated { .. } => {
                    info.generated = true;
                }
            };
        }

        info
    }
}
