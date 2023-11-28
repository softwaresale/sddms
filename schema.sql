
-- do all of this in a transaction
BEGIN TRANSACTION;

-- create each table if they don't already exist
CREATE TABLE IF NOT EXISTS students (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT,
    gpa FLOAT CHECK ( 0.0 <= gpa <= 4.0)
);

CREATE TABLE IF NOT EXISTS professors (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT,
    years_teaching INTEGER
);

CREATE TABLE IF NOT EXISTS grades (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    course_code TEXT,
    grade INTEGER CHECK ( 0 <= grade <= 10 ),
    student_id INTEGER,
    FOREIGN KEY(student_id) REFERENCES students(id)
);

CREATE TABLE IF NOT EXISTS classes (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    class_name TEXT,
    enroll_count INTEGER,
    teacher_id INTEGER,
    FOREIGN KEY (teacher_id) REFERENCES professors(id)
);

-- delete all data from tables
DELETE FROM sqlite_sequence;
DELETE FROM students;
DELETE FROM professors;
DELETE FROM grades;
DELETE FROM classes;

-- insert default data
INSERT INTO students (name, gpa)
    VALUES
        ('test1', 3.4),
        ('test2', 3.4),
        ('test3', 3.4),
        ('test4', 3.4),
        ('test5', 3.4),
        ('test6', 3.4);

INSERT INTO professors (name, years_teaching)
    VALUES
        ('test1', 23),
        ('test2', 23),
        ('test3', 23),
        ('test4', 23),
        ('test5', 23),
        ('test6', 23);

COMMIT;

