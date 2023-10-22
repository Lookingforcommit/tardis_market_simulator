from decimal import Decimal
from typing import Union, List, Dict
from datetime import datetime
import sqlite3


class DBManager:
    __instance = None
    __unused_position_history_id = 0
    DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S.%f"
    ORDERS_DB_FORMAT = (
        "id INTEGER PRIMARY KEY,\n"
        "upd_timestamp TEXT,\n"
        "status TEXT,\n"
        "symbol TEXT,\n"
        "side TEXT,\n"
        "price TEXT,\n"
        "orig_size TEXT,\n"
        "filled_size TEXT"
    )
    POSITIONS_DB_FORMAT = (
        "symbol TEXT PRIMARY KEY,\n"
        "upd_timestamp TEXT,\n"
        "size TEXT,\n"
        "entry_price TEXT"
    )
    POSITIONS_HISTORY_DB_FORMAT = (
        "id INTEGER PRIMARY KEY,\n"
        "prev_upd_id INTEGER,\n"
        "upd_timestamp TEXT,\n"
        "symbol TEXT,\n"
        "size TEXT,\n"
        "entry_price TEXT\n"
    )

    def __new__(cls, *args, **kwargs):
        if cls.__instance is None:
            cls.__instance = super().__new__(cls)
        return cls.__instance

    def __init__(self, path_to_db=None, orderbooks=None):
        if getattr(self, "orderbooks", 0) == 0:
            self.path_to_db = path_to_db
            self.orderbooks = orderbooks
            self.prev_position_update_ids = {symbol: -1 for symbol in orderbooks.keys()}

        with sqlite3.connect(self.path_to_db) as con:
            cur = con.cursor()
            cur.execute("DROP TABLE IF EXISTS tblActiveOrders")
            cur.execute(f"CREATE TABLE tblActiveOrders ({self.ORDERS_DB_FORMAT})")
            cur.execute("DROP TABLE IF EXISTS tblOrdersHistory")
            cur.execute(f"CREATE TABLE tblOrdersHistory ({self.ORDERS_DB_FORMAT})")
            cur.execute("DROP TABLE IF EXISTS tblActivePositions")
            cur.execute(f"CREATE TABLE tblActivePositions ({self.POSITIONS_DB_FORMAT})")
            cur.execute("DROP TABLE IF EXISTS tblPositionsHistory")
            cur.execute(f"CREATE TABLE tblPositionsHistory ({self.POSITIONS_HISTORY_DB_FORMAT})")
            con.commit()

    def format_position(self, position: list) -> Dict[str, Union[datetime, str, Decimal]]:
        position = {
            "symbol": position[0],
            "upd_timestamp": datetime.strptime(position[1], self.DATETIME_FORMAT),
            "size": Decimal(position[2]),
            "entry_price": Decimal(position[3])
        }
        return position

    def format_position_update(self, update: list) -> Dict[str, Union[datetime, str, int, Decimal]]:
        new_update = {
            "id": update[0],
            "prev_upd_id": update[1],
            "upd_timestamp": datetime.strptime(update[2], self.DATETIME_FORMAT),
            "symbol": update[3],
            "size": Decimal(update[4]),
            "entry_price": Decimal(update[5])
        }
        return new_update

    def format_order(self, order: list) -> Dict[str, Union[datetime, str, int, Decimal]]:
        order = {
            "id": order[0],
            "upd_timestamp": datetime.strptime(order[1], self.DATETIME_FORMAT),
            "status": order[2],
            "symbol": order[3],
            "side": order[4],
            "price": Decimal(order[5]),
            "orig_size": Decimal(order[6]),
            "filled_size": Decimal(order[7])
        }
        return order

    def create_position_update_id(self):
        prev_id = self.__unused_position_history_id
        self.__unused_position_history_id += 1
        return prev_id

    def get_active_position(self, symbol) -> List[dict]:
        query = "SELECT * FROM tblActivePositions WHERE symbol = ?"
        with sqlite3.connect(self.path_to_db) as con:
            cur = con.cursor()
            cur.execute(query, (symbol,))
            positions = cur.fetchall()
        if len(positions) != 0:
            position = positions[0]
            new_position = self.format_position(position)
            return [new_position]
        else:
            return []

    def get_all_active_positions(self) -> List[dict]:
        query = "SELECT * FROM tblActivePositions"
        with sqlite3.connect(self.path_to_db) as con:
            cur = con.cursor()
            cur.execute(query)
            positions = cur.fetchall()
        new_positions = []
        for position in positions:
            new_position = self.format_position(position)
            new_positions.append(new_position)
        return new_positions

    def create_position(self, symbol: str, timestamp: Union[datetime, str], size: Union[Decimal, int],
                        entry_price: Union[Decimal, int]):
        size, entry_price, timestamp = str(size), str(entry_price), str(timestamp)
        query = "INSERT INTO tblActivePositions (symbol, upd_timestamp, size, entry_price) VALUES (?, ?, ?, ?)"
        with sqlite3.connect(self.path_to_db) as con:
            cur = con.cursor()
            cur.execute(query, (symbol, timestamp, size, entry_price))
            con.commit()
        self.log_position_update(symbol, timestamp, size=size, entry_price=entry_price)

    def __delete_position(self, symbol: str):
        query = "DELETE FROM tblActivePositions WHERE symbol = ?"
        with sqlite3.connect(self.path_to_db) as con:
            cur = con.cursor()
            cur.execute(query, (symbol,))
            con.commit()

    def change_position(self, symbol: str, timestamp: Union[datetime, str], **kwargs):  # TODO: add docstrings, shorts processing
        kwargs["upd_timestamp"] = timestamp
        keys, values = kwargs.keys(), kwargs.values()
        upd_columns = ",".join([f"{key} = ?" for key in keys])
        upd_values = list(map(str, values))
        upd_values.append(symbol)
        query = f"UPDATE tblActivePositions SET {upd_columns} WHERE symbol = ?"
        with sqlite3.connect(self.path_to_db) as con:
            cur = con.cursor()
            cur.execute(query, upd_values)
            con.commit()
        if kwargs["size"] == 0:
            self.__delete_position(symbol)
        self.log_position_update(symbol, timestamp, **kwargs)

    def get_position_update(self, update_id) -> List[dict]:
        query = "SELECT * FROM tblPositionsHistory WHERE id = ?"
        with sqlite3.connect(self.path_to_db) as con:
            cur = con.cursor()
            cur.execute(query, (update_id,))
            update = cur.fetchall()
        if len(update) != 0:
            update = update[0]
            new_update = self.format_position_update(update)
            return [new_update]
        else:
            return []

    def log_position_update(self, symbol: str, timestamp: Union[datetime, str], **kwargs):  # TODO: add docstrings
        timestamp = str(timestamp)
        prev_upd_id = self.prev_position_update_ids[symbol]
        prev_upd = self.get_position_update(prev_upd_id)
        upd_id = self.create_position_update_id()
        self.prev_position_update_ids[symbol] = upd_id
        if len(prev_upd) != 0:
            upd = prev_upd[0]
            upd["prev_upd_id"] = prev_upd_id
            upd["id"] = upd_id
            upd["upd_timestamp"] = timestamp
        else:
            upd = {
                "id": upd_id,
                "prev_upd_id": prev_upd_id,
                "upd_timestamp": timestamp,
                "symbol": symbol,
                "size": "",
                "entry_price": ""
            }
        for key in kwargs:
            upd[key] = kwargs[key]
        upd_values = list(map(str, upd.values()))
        query = (
            "INSERT INTO tblPositionsHistory (id, prev_upd_id, upd_timestamp, symbol, size, entry_price)"
            "VALUES (?, ?, ?, ?, ?, ?)"
        )
        with sqlite3.connect(self.path_to_db) as con:
            cur = con.cursor()
            cur.execute(query, upd_values)
            con.commit()

    def get_active_order(self, order_id) -> List[dict]:
        query = "SELECT * FROM tblActiveOrders WHERE id = ?"
        with sqlite3.connect(self.path_to_db) as con:
            cur = con.cursor()
            cur.execute(query, (order_id,))
            orders = cur.fetchall()
        if len(orders) != 0:
            order = orders[0]
            new_order = self.format_order(order)
            return [new_order]
        else:
            return []

    def get_active_orders_by_symbol(self, symbol) -> List[dict]:
        query = "SELECT * FROM tblActiveOrders WHERE symbol = ?"
        with sqlite3.connect(self.path_to_db) as con:
            cur = con.cursor()
            cur.execute(query, (symbol,))
            orders = cur.fetchall()
        new_orders = []
        for order in orders:
            new_order = self.format_order(order)
            new_orders.append(new_order)
        return new_orders

    def get_all_active_orders(self) -> List[dict]:
        query = "SELECT * FROM tblActiveOrders"
        with sqlite3.connect(self.path_to_db) as con:
            cur = con.cursor()
            cur.execute(query)
            orders = cur.fetchall()
        new_orders = []
        for order in orders:
            new_order = self.format_order(order)
            new_orders.append(new_order)
        return new_orders

    def create_order(self, order_id: int, timestamp: Union[datetime, str], status: str, symbol: str, side: str,
                     price: Union[Decimal, int], orig_size: Union[Decimal, int], filled_size: Union[Decimal, int]):
        price, orig_size, filled_size, timestamp = str(price), str(orig_size), str(filled_size), str(timestamp)
        query = (
            "INSERT INTO tblActiveOrders (id, upd_timestamp, status, symbol, side, price, orig_size, filled_size)"
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?)"
        )
        with sqlite3.connect(self.path_to_db) as con:
            cur = con.cursor()
            params = (order_id, timestamp, status, symbol, side, price, orig_size, filled_size)
            cur.execute(query, params)
            con.commit()

    def delete_order(self, order_id):
        query = "DELETE FROM tblActiveOrders WHERE id = ?"
        with sqlite3.connect(self.path_to_db) as con:
            cur = con.cursor()
            cur.execute(query, (order_id,))
            con.commit()

    def change_order(self, timestamp: Union[datetime, str], order_id, **kwargs):  # TODO: add docstrings
        kwargs["upd_timestamp"] = timestamp
        keys, values = kwargs.keys(), kwargs.values()
        upd_columns = ",".join([f"{key} = ?" for key in keys])
        upd_values = list(map(str, values))
        upd_values.append(order_id)
        query = f"UPDATE tblActiveOrders SET {upd_columns} WHERE id = ?"
        with sqlite3.connect(self.path_to_db) as con:
            cur = con.cursor()
            cur.execute(query, upd_values)
            con.commit()
        order = self.get_active_order(order_id)[0]
        if order["filled_size"] == order["orig_size"]:
            self.log_order(order_id, timestamp, "filled", order["symbol"], order["side"], order["price"],
                           order["orig_size"], order["filled_size"])
            self.delete_order(order_id)

    def log_order(self, order_id: int, timestamp: Union[datetime, str], status: str, symbol: str, side: str,
                  price: Union[Decimal, int], orig_size: Union[Decimal, int], filled_size: Union[Decimal, int]):
        price, orig_size, filled_size, timestamp = str(price), str(orig_size), str(filled_size), str(timestamp)
        query = (
            "INSERT INTO tblOrdersHistory (id, upd_timestamp, status, symbol, side, price, orig_size, filled_size)"
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?)"
        )
        with sqlite3.connect(self.path_to_db) as con:
            cur = con.cursor()
            params = (order_id, timestamp, status, symbol, side, price, orig_size, filled_size)
            cur.execute(query, params)
            con.commit()
