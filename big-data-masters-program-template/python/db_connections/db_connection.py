import sqlite3
if __name__ == '__main__':
    connection = sqlite3.connect("test_db")

    cursor = connection.cursor()

    cursor.execute("select * from employee")

    for record in cursor.fetchall():
        print(record)
    connection.commit()
    connection.close()