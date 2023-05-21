import mysql.connector
from mysql.connector import Error

class DbConnection:
    def database_connection():
        try:
            db_conn = mysql.connector.connect(
                host = 'localhost',
                database = 'inboundEngine_db',
                username = 'root',
                password = 'RafaelJs12345.'
            )
            if db_conn:
                return db_conn

        except Error as e:
            return e



 