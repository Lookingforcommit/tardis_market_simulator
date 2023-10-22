from decimal import Decimal
from typing import Union, Tuple
from datetime import datetime
from database_management import DBManager


def calc_new_entry_price(pos_entry_price: Union[Decimal, int], pos_size: Union[Decimal, int],
                         fill_price: Union[Decimal, int], fill_size: Union[Decimal, int]):
    total_size = pos_size + fill_size
    ans = pos_entry_price * (pos_size / total_size) + fill_price * (fill_size / total_size)
    return ans


def get_prices_above(lst, price):
    lst = sorted(lst, reverse=True)
    ans = []
    for el in lst:
        if price <= el:
            ans.append(el)
        else:
            break
    return ans


def get_prices_below(lst, price):
    lst = sorted(lst)
    ans = []
    for el in lst:
        if price >= el:
            ans.append(el)
        else:
            break
    return ans


def get_opposite_side(side):
    if side == "buy":
        return "asks"
    else:
        return "bids"


def fill_order(db_manager: DBManager, timestamp: Union[datetime, str], order_id: int,
               orderbooks: dict) -> Tuple[dict, dict]:
    order = db_manager.get_active_order(order_id)[0]
    orig_size, filled_size = order["orig_size"], order["filled_size"]
    order_status, order_price, side, symbol = order["status"], order["price"], order["side"], order["symbol"]
    book_side = get_opposite_side(side)
    book = orderbooks[symbol][book_side]
    remaining_size = orig_size - filled_size
    position = db_manager.get_active_position(order["symbol"])
    if len(position) != 0:
        position = position[0]
        pos_size, pos_entry_price = position["size"], position["entry_price"]
        position_exists = True
    else:
        pos_size = pos_entry_price = Decimal("0")
        position = {"symbol": symbol}
        position_exists = False
    if side == "buy":  # TODO: Add shorts processing
        prices_below = get_prices_below(book.keys(), order_price)
        for price in prices_below:
            size = book[price]["size"]
            fill_size = min(remaining_size, size)
            remaining_size -= fill_size
            pos_entry_price = calc_new_entry_price(pos_entry_price, pos_size, price, fill_size)
            pos_size += fill_size
            if remaining_size <= 0:
                order_status = "filled"
                break
    else:
        prices_above = get_prices_above(book.keys(), order_price)
        for price in prices_above:
            size = book[price]["size"]
            fill_size = min(remaining_size, size)
            remaining_size -= fill_size
            pos_size -= fill_size
            if remaining_size <= 0:
                order_status = "filled"
                break
    new_filled_size = orig_size - remaining_size
    if new_filled_size != filled_size:
        db_manager.change_order(timestamp, order_id, filled_size=new_filled_size, status=order_status)
        if not position_exists:
            db_manager.create_position(symbol, timestamp, pos_size, pos_entry_price)
        else:
            db_manager.change_position(symbol, timestamp, size=pos_size, entry_price=pos_entry_price)
    new_order = order
    new_position = position
    new_order["filled_size"], new_order["status"] = new_filled_size, order_status
    new_position["size"], new_position["entry_price"] = pos_size, pos_entry_price
    return new_order, new_position


class OrderManager:
    __instance = None
    __unused_order_id = 0

    def __new__(cls, *args, **kwargs):
        if cls.__instance is None:
            cls.__instance = super().__new__(cls)
        return cls.__instance

    def __init__(self):
        if getattr(self, "db_manager", 0) == 0:
            self.db_manager = DBManager()

    def get_active_position(self, symbol) -> list:
        db_manager = self.db_manager
        position = db_manager.get_active_position(symbol)
        return position

    def get_all_active_positions(self) -> list:
        db_manager = self.db_manager
        positions = db_manager.get_all_active_positions()
        return positions

    def get_active_order(self, order_id) -> list:
        db_manager = self.db_manager
        order = db_manager.get_active_order(order_id)
        return order

    def get_all_active_orders(self) -> list:
        db_manager = self.db_manager
        orders = db_manager.get_all_active_orders()
        return orders

    def create_order_id(self):
        prev_id = self.__unused_order_id
        self.__unused_order_id += 1
        return prev_id

    def send_limit_order(self, timestamp: Union[datetime, str], symbol: str, side: str, price: Union[Decimal, int],
                         size: Union[Decimal, int]):
        db_manager = self.db_manager
        order_id = self.create_order_id()
        db_manager.create_order(order_id, timestamp, "open", symbol, side, price, size, 0)
        order = fill_order(db_manager, timestamp, order_id, db_manager.orderbooks)[0]
        return order

    def cancel_order(self, order_id: int):
        db_manager = self.db_manager
        order = db_manager.get_active_order(order_id)
        if len(order) != 0:
            order = order[0]
            order_id = order.pop("id")
            order["status"] = "cancelled"
            db_manager.log_order(order_id, **order)
            db_manager.delete_order(order_id)
        return order

