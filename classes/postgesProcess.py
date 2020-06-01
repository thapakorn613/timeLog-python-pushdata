import psycopg2
import time
class PostgesProcess:
    def __init__(self,_name,_user,_password,_host,_port):
        self.name = _name
        self.user = _user
        self.password = _password
        self.host = _host
        self.port = _port
        self.connection = []
        self.cursor = []
        
    def pg_connect(self):
        try:
            print("connecting to database!")
            self.connection = psycopg2.connect(database=self.name,
                                        user=self.user,
                                        password=self.password,
                                        host=self.host,
                                        port=self.port )
            print("database connected successful!")
            self.cursor = self.connection.cursor()

        except (Exception, psycopg2.Error) as error:
            if(connection):
                print("Failed to insert record into mobile table", error)
                
    def pg_insert(self,_id,_ss,_turbid,_cod):
        if(self.connection):
            ts = time.time()
            create_table_timelogdb = """
            CREATE TABLE IF NOT EXISTS timelogdb 
            (
                id serial PRIMARY KEY,
                PK_ID NUMERIC (5, 2),
                PK_SS NUMERIC (5, 2),
                PK_TURBID NUMERIC (5, 2),
                PK_COD NUMERIC (5, 2)
            );
            """
            postgres_insert_query = """ 
            INSERT INTO timelogdb (TIMESTAMP,PK_ID, PK_SS, PK_TURBID, PK_COD) VALUES (%s,%s,%s,%s,%s)
            """
            record_to_insert = (ts,_id,_ss, _turbid, _cod)
            self.cursor.execute(postgres_insert_query, record_to_insert)
            
            self.connection.commit()
            count = self.cursor.rowcount
            print(count, "Record inserted successfully into mobile table")

    def pg_create_table(self):
        if(self.connection):
            create_table_timelogdb = """
            CREATE TABLE IF NOT EXISTS timelogdb 
            (
                id serial PRIMARY KEY,
                TIMESTAMP BIGSERIAL NOT NULL,
                PK_ID NUMERIC (5, 2),
                PK_SS NUMERIC (5, 2),
                PK_TURBID NUMERIC (5, 2),
                PK_COD NUMERIC (5, 2)
            );
            """
            self.cursor.execute(create_table_timelogdb)
            self.connection.commit()
            print("Table created successfully")

    def pg_disconnect(self):
        if(self.connection):
            self.cursor.close()
            self.connection.close()
            print("PostgreSQL connection is closed")