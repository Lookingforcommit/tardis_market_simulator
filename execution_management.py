from decimal import Decimal
from typing import Union, Tuple, Dict, Optional, Iterable
from datetime import datetime
from database_management import DBManager


def calc_new_entry_price(pos_entry_price: Decimal, pos_size: Decimal, fills: Dict[Decimal, Decimal],
                         total_filled_size: Optional[Decimal] = None):
    fill_prices = fills.keys()
    if total_filled_size is None:
        total_filled_size = sum(fill_prices)
    total_size = pos_size + total_filled_size
    ans = pos_entry_price * pos_size / total_size
    for fill_price in fill_prices:
        ans += fill_price * fills[fill_price] / total_size
    return ans


def get_prices_above(lst: Iterable[Union[int, Decimal]], price: Union[int, Decimal]):
    lst = sorted(lst, reverse=True)
    ans = []
    for el in lst:
        if price <= el:
            ans.append(el)
        else:
            break
    return ans


def get_prices_below(lst: Iterable[Union[int, Decimal]], price: Union[int, Decimal]):
    lst = sorted(lst)
    ans = []
    for el in lst:
        if price >= el:
            ans.append(el)
        else:
            break
    return ans


def get_suitable_prices(lst: Iterable[Union[int, Decimal]], price: Union[int, Decimal], side: str):
    if side == "buy":
        suitable_prices = get_prices_below(lst, price)
    else:
        suitable_prices = get_prices_above(lst, price)
    return suitable_prices


def get_opposite_side(side: str):
    if side == "buy":
        return "asks"
    else:
        return "bids"


def is_same_sign(num1, num2):
    if num1 * num2 > 0:
        return True
    else:
        return False


def update_position(db_manager: DBManager, pos_size: Decimal, pos_entry_price: Decimal, order_side: str,
                    order_fills: Dict[Decimal, Decimal], filled: Decimal, symbol: str, timestamp: Union[datetime, str]):
    position_side = "buy" if pos_size > Decimal("0") else "sell"
    new_pos_size = pos_size + filled
    if order_side == position_side:  # position incremented
        pos_entry_price = calc_new_entry_price(pos_entry_price, abs(pos_size), order_fills, abs(filled))
        db_manager.change_position(symbol, timestamp, size=new_pos_size, entry_price=pos_entry_price)
    elif is_same_sign(pos_size + filled, pos_size):  # position decremented
        db_manager.change_position(symbol, timestamp, size=new_pos_size)
    else:  # position closed
        db_manager.change_position(symbol, timestamp, size=Decimal("0"))
        if new_pos_size != Decimal("0"):  # new position created with the opposite side
            order_fills_rev = dict(sorted(order_fills.items(), reverse=True))
            remaining_size = abs(new_pos_size)
            new_order_fills = {}
            for price in order_fills_rev:  # getting the new pos_entry_price
                size = order_fills_rev[price]
                fill_size = min(remaining_size, size)
                remaining_size -= fill_size
                new_order_fills[price] = fill_size
                if remaining_size <= Decimal("0"):
                    break
            pos_entry_price = calc_new_entry_price(Decimal("0"), Decimal("0"), new_order_fills, abs(new_pos_size))
            db_manager.create_position(symbol, timestamp, new_pos_size, pos_entry_price)
    return new_pos_size, pos_entry_price


def fill_order(db_manager: DBManager, timestamp: Union[datetime, str], order_id: int,
               orderbooks: dict) -> Tuple[dict, dict]:
    order = db_manager.get_active_order(order_id)[0]
    book_side = get_opposite_side(order["side"])
    book = orderbooks[order["symbol"]][book_side]
    remaining_size = order["orig_size"] - order["filled_size"]
    position = db_manager.get_active_position(order["symbol"])
    order_side, order_price, symbol, status = order["side"], order["price"], order["symbol"], order["status"]
    if len(position) != 0:
        position = position[0]
        pos_size, pos_entry_price = position["size"], position["entry_price"]
        position_exists = True
    else:
        pos_size = pos_entry_price = Decimal("0")
        position = {"symbol": symbol}
        position_exists = False
    suitable_prices = get_suitable_prices(book.keys(), order_price, order_side)
    order_fills = {}
    for price in suitable_prices:
        size = book[price]["size"]
        fill_size = min(remaining_size, size)
        remaining_size -= fill_size
        order_fills[price] = fill_size
        if remaining_size <= Decimal("0"):
            status = "filled"
            break
    filled = order["orig_size"] - order["filled_size"] - remaining_size
    new_filled_size = order["filled_size"] + filled
    if filled > Decimal("0"):
        db_manager.change_order(timestamp, order_id, filled_size=new_filled_size, status=status)
        if order_side == "sell":
            filled = -filled
        if position_exists:
            pos_size, pos_entry_price = update_position(db_manager, pos_size, pos_entry_price, order_side, order_fills,
                                                        filled, symbol, timestamp)
        else:
            pos_size = filled
            pos_entry_price = calc_new_entry_price(Decimal("0"), Decimal("0"), order_fills, abs(pos_size))
            db_manager.create_position(symbol, timestamp, pos_size, pos_entry_price)
    new_order = order
    new_position = position
    new_order["filled_size"], new_order["status"] = new_filled_size, status
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
