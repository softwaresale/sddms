use sqlparser::dialect::SQLiteDialect;
use sqlparser::parser::Parser;

fn main() {
    let sql = "WITH teacher_id_set AS (SELECT id as teacher_id FROM professors ORDER BY RANDOM() LIMIT 1),
VALUES_CTE(class_name,enroll_count) AS (VALUES ('P3is',79),('hriWO9kPBr',81),('Iia',47)) INSERT INTO classes (class_name,enroll_count,teacher_id) SELECT class_name, enroll_count, teacher_id FROM VALUES_CTE,teacher_id_set;";

    let dialect = SQLiteDialect {};
    let results = Parser::parse_sql(&dialect, sql).unwrap();
    println!("{:#?}", results);

    let metadata = sddms_shared::sql_metadata::parse_statements(sql).unwrap();
    for meta in metadata {
        println!("{:#?}", meta)
    }
}
