
BEGIN TRANSACTION;

CREATE TABLE students (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT,
    gpa FLOAT CHECK ( 0.0 <= gpa <= 4.0)
);

CREATE TABLE professors (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT,
    years_teaching INTEGER
);

CREATE TABLE grades (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    course_code TEXT,
    grade INTEGER CHECK ( 0 <= grade <= 10 ),
    student_id INTEGER,
    FOREIGN KEY(student_id) REFERENCES students(id)
);

CREATE TABLE classes (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    class_name TEXT,
    enroll_count INTEGER,
    teacher_id INTEGER,
    FOREIGN KEY (teacher_id) REFERENCES professors(id)
);

COMMIT;

