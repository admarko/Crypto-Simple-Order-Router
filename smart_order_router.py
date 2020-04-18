import sqlite3 as sql


class SmartOrderRouter:
    conn: sql.Connection = None
    orderbook_schema = """
						orderbook ( 
							id integer PRIMARY KEY,       
                            price bigint NOT NULL,       
                            volume bigint NOT NULL,       
                            symbol VARCHAR NOT NULL,        
                            exchange VARCHAR NOT NULL,        
                            side VARCHAR NOT NULL,        
                            purchase_date VARCHAR
                        )
                       """

    def __init__(self):
        self.create_db_connection("crypto_data.db")
        if self.conn is not None:
            self.create_table(self.conn, self.orderbook_schema)
        else:
        	print("Error! cannot create the database connection.")
        	quit()

    def create_db_connection(self, db_file: str) -> None:
    	""" create a SQL connection
	    :param db_file: string for database filename
	    :return: None
    	"""
    	try:
    		self.conn = sql.connect(db_file)
    	except sql.Error as e:
        	print(e)

    def create_table(self, conn: sql.Connection, sql_data: str) -> None:
    	""" create a SQL table
	    :param conn: Connection object
	    :param sql_data: data to be used in a CREATE TABLE statement
	    :return: None
    	"""
    	create_table_SQL = f"""CREATE TABLE IF NOT EXISTS {sql_data};"""
    	try:
    		c = self.conn.cursor()
    		c.execute(create_table_SQL)
    	except sql.Error as e:
    		print(e)


if __name__ == "__main__":
    router = SmartOrderRouter()
