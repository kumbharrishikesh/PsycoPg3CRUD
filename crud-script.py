import psycopg
from psycopg import sql
import config
import traceback


def connect():
    try:
        connection = psycopg.connect(
            user=config.user,
            password=config.password,
            dbname=config.database,
            host=config.host,
            port=config.port,
            autocommit=True,
        )
        return connection
    except Exception as err:
        print("Error occurred in making connection …")
        traceback.print_exc()


def print_version(connection):
    cursor = connect().cursor()
    cursor.execute("SELECT version()")
    db_version = cursor.fetchone()
    print(db_version)
    cursor.close()
    connection.close()


def create(connection):
    cursor = connection.cursor()
    query = """
    create table person(
        first_name varchar(100),
        last_name varchar(100),
        city varchar(100)
    );
    """
    try:
        cursor.execute(query)
        connection.commit()
        print("table created successfully!")
    except Exception as err:
        print(err)
    cursor.close()
    connection.close()


def insert(connection):
    cursor = connection.cursor()

    try:
        data = [
            ("James", "Bond", "NA"),
            ("Jacob", "Monk", "NA"),
            ("James", "Billion", "NA"),
            ("Mike", "Road", "NA"),
            ("Warren", "Wick", "NA"),
            ("Stive", "Renold", "NA"),
            ("Mac", "D'cruz", "NA"),
            ("James", "White", "NA"),
            ("James", "Gomez", "NA"),
            ("James", "Williams", "NA"),
        ]
        #### Fastest COPY method ####
        # with cursor.copy(
        #     "COPY person (first_name, last_name, city) FROM STDIN"
        # ) as copy:
        #     for record in data:
        #         copy.write_row(record)

        query = "INSERT into person (first_name, last_name, city) VALUES (%s, %s,%s)"

        for row in data:
            cursor.execute(query, row)

        print("Record(s) inserted successfully!")
    except Exception as err:
        print(err)
    cursor.close()
    connection.close()


def read(connection):
    cursor = connection.cursor()
    try:
        cursor.execute("SELECT * FROM person LIMIT 500;")
        records = cursor.fetchall()
        for record in records:
            print(
                f"Read successful: name= {record[0]+' '+record[1]}, city= {record[2]}"
            )
    except Exception as err:
        print(err)
    cursor.close()
    connection.close()


def update(connection):
    cursor = connection.cursor()
    query = """
    UPDATE person SET city=%s WHERE first_name=%s;
    """
    try:
        cursor.execute(query, ("Sydney", "James"))
        cursor.execute("SELECT * FROM person WHERE first_name='James';")
        records = cursor.fetchall()
        for record in records:
            print(
                f"Update successful : name= {record[0]+' '+record[1]}, city= {record[2]}"
            )
    except Exception as err:
        print(err)
    cursor.close()
    connection.close()


def delete(connection):
    cursor = connection.cursor()
    query = """
    DELETE FROM person WHERE city='Sydney';
    """
    try:
        cursor.execute(query)
        cursor.execute("select * from person;")
        record = cursor.fetchall()
        print(record)
    except Exception as err:
        print(err)
    cursor.close()
    connection.close()


if __name__ == "__main__":
    read(connect())
