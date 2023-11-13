
BEGIN TRANSACTION;

CREATE TABLE students (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT,
    gpa FLOAT
);

CREATE TABLE grades (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    course_code TEXT,
    grade INTEGER CHECK ( 0 < grade < 10 ),
    student_id INT,
    FOREIGN KEY(student_id) REFERENCES students(id)
);

COMMIT;

