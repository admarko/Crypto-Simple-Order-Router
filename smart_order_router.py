import sqlite3 as sql
from typing import Tuple, Union

import ccxt


class SmartOrderRouter:
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
    multiply_factor = 100000

    def __init__(self, symbol: str):
        self.symbol = symbol
        self.conn: sql.Connection = self.create_db_connection("crypto_data.db")

        if self.conn is not None:
            self.create_table(self.conn, self.orderbook_schema)
        else:
            print("Error! cannot create the database connection.")
            quit()

        self.find_order()

    def create_db_connection(self, db_file: str) -> sql.Connection:
        """ create a SQL connection
		:param db_file: string for database filename
		:return: None
		"""
        try:
            conn = sql.connect(db_file)
        except sql.Error as e:
            print(e)
        return conn

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

    def clear_db(self, conn: sql.Connection):
        c = conn.cursor()
        c.execute("DELETE FROM orderbook;")

    def fetch_data(self, exchange, symbol: str) -> dict:
        """
		ccxt is a wrapper library to go around many crypto exchanges
		"""
        # print(type(exchange) is ccxt.kraken.kraken)
        return exchange.fetch_order_book(symbol)

    def insert_row_into_db(
        self, row: Tuple[float, float, str, str, str, str], conn: sql.Connection
    ) -> None:
        cur = conn.cursor()
        sql = """ INSERT INTO orderbook(price,volume,symbol,exchange,side,purchase_date) VALUES(?,?,?,?,?,?) """
        cur.execute(sql, row)
        conn.commit()

    def insert_coinbase_into_db(
        self, orderbook: dict, conn: sql.Connection, symbol: str
    ) -> None:
        for ask in orderbook["asks"]:
            price = ask[0] * self.multiply_factor
            volume = ask[1] * self.multiply_factor
            row = (price, volume, symbol, "COINBASE", "ASK", None)
            self.insert_row_into_db(row, conn)

        for bid in orderbook["bids"]:
            price = ask[0] * self.multiply_factor
            volume = ask[1] * self.multiply_factor
            row = (price, volume, symbol, "COINBASE", "BID", None)
            self.insert_row_into_db(row, conn)

    def insert_kraken_into_db(
        self, orderbook: dict, conn: sql.Connection, symbol: str
    ) -> None:
        for ask in orderbook["asks"]:
            price = ask[0] * self.multiply_factor
            volume = ask[1] * self.multiply_factor
            row = (price, volume, symbol, "KRAKEN", "ASK", ask[2])
            self.insert_row_into_db(row, conn)

        for bid in orderbook["bids"]:
            price = ask[0] * self.multiply_factor
            volume = ask[1] * self.multiply_factor
            row = (price, volume, symbol, "KRAKEN", "BID", bid[2])
            self.insert_row_into_db(row, conn)

    def read_db(self, conn: sql.Connection):
        cur = conn.cursor()
        cur.execute("SELECT * FROM orderbook")
        rows = cur.fetchall()
        for row in rows:
            print(row)
        print("\n\n UPDATED DB!")

    def find_order(self):
        coinbase = ccxt.coinbasepro()
        kraken = ccxt.kraken(
            {"enableRateLimit": True, "options": {"fetchMinOrderAmounts": False}}
        )

        while True:
            self.clear_db(self.conn)
            coinbase_orderbook = self.fetch_data(coinbase, self.symbol)
            self.insert_coinbase_into_db(coinbase_orderbook, self.conn, self.symbol)
            kraken_orderbook = self.fetch_data(kraken, self.symbol)
            self.insert_kraken_into_db(kraken_orderbook, self.conn, self.symbol)
            self.read_db(self.conn)


if __name__ == "__main__":
    symbol = "BTC/USD"  # get symbol from user - check it exists
    router = SmartOrderRouter(symbol)


# buy - lowest ask
# sell - highest bid

# the following is psuedocode for step 6
# def buy():
# 	GROUP_BY asks
# 	ORDER_BY price increasing
# 	IF (avg_price < AVG_PRICE_LIMIT) AND (volume_purchased < VOLUME_LIMIT )
# 	make_purchase()
