import psycopg2 as pg


def create_db():  # создает таблицы
    with pg.connect(dbname='test_db', user='test', password='1234') as conn:
        with conn.cursor() as cur:
            cur.execute("""
                        create table if not exists students(
                        id serial primary key,
                        name varchar(100),
                        gpa numeric(10,2) null,
                        birth timestamp with time zone null
                        )
                        """)

            cur.execute("""
                        create table if not exists course(
                        id serial primary key,
                        name varchar(100)
                        )
                        """)

            cur.execute("""
                        create table if not exists student_course(
                        id serial primary key,
                        students_id integer references students(id),
                        course_id integer references course(id)
                        )
                        """)


def add_course(courses):  # создаёт курсы
    with pg.connect(dbname='test_db', user='test', password='1234') as conn:
        with conn.cursor() as cur:
            for course in courses:
                cur.execute("""insert into course(name) values(%s)""", (course,))


def get_students(course_id):  # возвращает студентов определенного курса
    with pg.connect(dbname='test_db', user='test', password='1234') as conn:
        with conn.cursor() as cur:
            cur.execute("""select s.id, s.name, c.name from student_course sc
                        join students s on s.id = sc.students_id
                        join course c on c.id = sc.course_id
                        where sc.course_id = %s""", (course_id,))
            return cur.fetchall()


def add_students(course_id, students):  # создает студентов и записывает их на курс
    conn = pg.connect(dbname='test_db', user='test', password='1234')
    cur = conn.cursor()
    for student in range(0, len(students)):
        students_id = add_student(students[student])
        cur.execute("""insert into student_course(students_id, course_id) values(%s, %s);""",
                    (students_id, course_id))
        conn.commit()


def add_student(student):  # просто создает студента
    with pg.connect(dbname='test_db', user='test', password='1234') as conn:
        with conn.cursor() as cur:
            if len(student['gpa']) == 0 and len(student['birth']) == 0:
                cur.execute("""insert into students(name) values(%s) returning id""", (student['name'],))
                return cur.fetchall()[0][0]
            elif len(student['gpa']) > 0 and len(student['birth']) == 0:
                cur.execute("""insert into students(name, gpa) values(%s, %s) returning id""",
                            (student['name'], student['gpa'],))
                return cur.fetchall()[0][0]
            elif len(student['gpa']) == 0 and len(student['birth']) > 0:
                cur.execute("""insert into students(name, birth) values(%s, %s) returning id""",
                            (student['name'], student['birth'],))
                return cur.fetchall()[0][0]
            elif len(student['gpa']) > 0 and len(student['birth']) > 0:
                cur.execute("""insert into students(name, gpa, birth) values(%s, %s, %s) returning id""",
                            (student['name'], student['gpa'], student['birth']))
                return cur.fetchall()[0][0]


def get_student(student_id):  # выводит студента по id
    with pg.connect(dbname='test_db', user='test', password='1234') as conn:
        with conn.cursor() as cur:
            cur.execute("""select * from students where id = %s;""", (student_id, ))
            student = cur.fetchall()
            return student


if __name__ == '__main__':
    create_db()
    add_course(['Программирование', 'Маркетинг', 'Тестирование'])
    add_students(3, [{'name': 'Anastasia', 'gpa': '4.5', 'birth': '27.11.1993'},
                     {'name': 'John', 'gpa': '4.7', 'birth': '22.12.1989'},
                     {'name': 'Bred', 'gpa': '4.6', 'birth': '02.05.1999'}])
    print(get_students(3))
    add_student({'name': 'Mikhail', 'gpa': '5', 'birth': '29.09.1990'})
    print(get_student(1))
