from pyhive import hive
if __name__ == '__main__':
    conn = hive.Connection(host='172.21.223.68',
                           port=10000,
                           username='npntraining',
                           password='npntraining',
                           database='default',
                           auth='CUSTOM')
    cursor = conn.cursor()
    cursor.execute("create database n1db")
