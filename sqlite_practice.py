import sqlite3
from sqlite3 import Error

def create_connection(db_file):
    """ create a database connection to the SQLite database
        specified by db_file
    :param db_file: database file
    :return: Connection object or None
    """
    conn = None
    try:
        conn = sqlite3.connect(db_file)
        return conn
    except Error as e:
        print(e)

    return conn

def execute_sql(conn, sql):
    """ Execute sql
    :param conn: Connection object
    :param sql: a SQL script
    :return:
    """
    try:
        c = conn.cursor()
        c.execute(sql)
    except Error as e:
        print(e)

def add_author(conn, author):
    """
    Create a new author into the authors table
    :param conn:
    :param author:
    :return: author id
    """
    sql = '''INSERT INTO authors(author_id, first_name, last_name)
                VALUES(?,?,?)'''
    cur = conn.cursor()
    cur.execute(sql, author)
    conn.commit()
    return cur.lastrowid

def add_book(conn, book):
    """
    Create a new book into the books table
    :param conn:
    :param book:
    :return: book id
    """
    sql = '''INSERT INTO books(author_id, title, genre, year)
                VALUES(?,?,?,?)'''
    cur = conn.cursor()
    cur.execute(sql, book)
    conn.commit()
    return cur.lastrowid

def select_all(conn, table):
    """
    Query all rows in the table
    :param conn: the Connection object
    :return:
    """
    cur = conn.cursor()
    cur.execute(f"SELECT * FROM {table}")
    rows = cur.fetchall()

def select_where(conn, table, **query):
    """
    Query books from table with data from **query dict
    :param conn: the Connection object
    :param table: table name
    :param query: dict of attributes and values
    :return:
    """
    cur = conn.cursor()
    qs = []
    values = ()
    for k, v in query.items():
        qs.append(f"{k}=?")
        values += (v,)
    q = " AND ".join(qs)
    cur.execute(f"SELECT * FROM {table} WHERE {q}", values)
    rows = cur.fetchall
    return rows

def update(conn, table, id, **kwargs):
    """
    update title, genre and year of a book
    :param conn:
    :param table: table name
    :param id: row id
    :return:
    """
    parameters = [f"{k} = ?" for k in kwargs]
    parameters = ", ".join(parameters)
    values = tuple(v for v in kwargs.values())
    values += (id, )

    sql = f''' UPDATE {table}
                SET {parameters}
                WHERE id = ?'''
    try:
        cur = conn.cursor()
        cur.execute(sql, values)
        conn.commit()
        print("OK")
    except sqlite3.OperationalError as e:
        print(e)

def delete_where(conn, table, **kwargs):
    """
    Delete from table where attributes from
    :param conn: Connection to the SQLite database
    :param table: table name
    :param kwargs: dict of attributes and values
    :return:
    """
    qs = []
    values = tuple()
    for k, v in kwargs.items():
        qs.append(f"{k}=?")
        values += (v,)
    q = " AND ".join(qs)

    sql = f'DELETE FROM {table} WHERE {q}'
    cur = conn.cursor()
    cur.execute(sql, values)
    conn.commit()
    print("Deleted")

def delete_all(conn, table):
    """
    Delete all rows from table
    :param conn: Connection to the SQLite database
    :param table: table name
    :return:
    """
    sql = f'DELETE FROM {table}'
    cur = conn.cursor()
    cur.execute(sql)
    conn.commit()
    print("Deleted")

if __name__ == '__main__':

    create_author_sql = """
    CREATE TABLE IF NOT EXISTS authors
    (
        id integer PRIMARY KEY,
        author_id integer NOT NULL,
        first_name text NOT NULL,
        last_name text NOT NULL
    );
    """

    create_book_sql = """
    CREATE TABLE IF NOT EXISTS books
    (
        id integer PRIMARY KEY,
        author_id integer NOT NULL,
        title test NOT NULL,
        genre text NOT NULL,
        year integer,
        FOREIGN KEY (author_id) REFERENCES author (id)
    );
    """

    db_file = "databasis.db"
    conn = create_connection(db_file)

    if conn is not None:
        execute_sql(conn, create_author_sql)
        execute_sql(conn, create_book_sql)

    author_1 = (
        2500,
        "Karol",
        "Dickens"
        )
    author_2 = (
        2501,
        "J.R.R",
        "Tolkien"
        )
    author_3 = (
        2525,
        "Lucy Maud",
        "Montgomery"
        )

    book_1 = (
        2500,
        "Opowieść wigilijna",
        "Baśń",
        1843
        )
    book_2 = (
        2501,
        "Władca Pierścieni",
        "Powieść",
        1954
        )
    book_3 = (
        2525,
        "Ania z Zielonego Wzgórza",
        "Powieść",
        1908
        )

# add_author(conn, author_1)
# add_book(conn, book_1)
# add_author(conn, author_2)
# add_book(conn, book_2)
# add_author(conn, author_3)
# add_book(conn, book_3)

# delete_where(conn, "authors", first_name="Karol")

# add_author(conn, author_1)

# update(conn, "authors", 5, first_name="Charles")

# update(conn, "books", 2, title="Władca Pierścieni")
