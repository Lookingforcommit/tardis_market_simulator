from tardis_client import TardisClient, Channel
from abc import ABC, abstractmethod
from decimal import Decimal
from datetime import datetime
from typing import List
from execution_management import DBManager, fill_order
import requests
import json


def get_init_book(start_date: str, exchange: str, symbols: list[str], channel: str) -> List[dict]:
    filters = [{"channel": channel, "symbols": symbols}]
    params = {"from": start_date, "filters": json.dumps(filters)}
    url = "https://api.tardis.dev/v1/data-feeds/" + exchange
    response = requests.get(url, headers={}, params=params, stream=True)
    ans = []
    for line in response.iter_lines():
        # empty lines in response are being used as markers
        # for disconnect events that occurred when collecting the data
        if len(line) <= 1:
            continue
        else:
            msg = json.loads(line.decode("utf-8").split(" ")[1])
            ans.append(msg)
    return ans


class DataReplayer(ABC):
    def __init__(self, start_date: str, end_date: str, raw_channels_and_symbols: dict[str, list[str]],
                 generated_channels_and_symbols: dict[str, list[str]], path_to_db: str, strategy_class, *strategy_args,
                 **strategy_kwargs):
        self.start_date = start_date
        self.end_date = end_date
        self.channels_and_symbols = raw_channels_and_symbols
        self.generated_channels_and_symbols = generated_channels_and_symbols
        subscribed_symbols = set()
        for key in raw_channels_and_symbols.keys():
            subscribed_symbols.update(raw_channels_and_symbols[key])
        for key in generated_channels_and_symbols.keys():
            subscribed_symbols.update(generated_channels_and_symbols[key])
        self.subscribed_symbols = list(subscribed_symbols)
        self.orderbooks = {symbol: {"bids": {}, "asks": {}} for symbol in self.subscribed_symbols}
        self.channels_and_symbols[self.orderbook_update_channel] = self.subscribed_symbols
        self.db_manager = DBManager(path_to_db, self.orderbooks)  # DBManager should be initialised prior to strategy
        self.cur_timestamp = datetime(1, 1, 1)
        self.strategy = strategy_class(*strategy_args, **strategy_kwargs)

    @property
    @abstractmethod
    def orderbook_update_channel(self):
        pass

    @property
    @abstractmethod
    def exchange(self):
        pass

    @abstractmethod
    def process_message(self, message):
        pass

    @abstractmethod
    def process_book_update(self, message):
        pass

    def update_orders_status(self, strategy, symbol, timestamp):  # TODO: add dict check for channels
        db_manager = self.db_manager
        active_orders = db_manager.get_active_orders_by_symbol(symbol)
        for order in active_orders:
            new_order, new_position = fill_order(db_manager, timestamp, order["id"], self.orderbooks)
            position_upd = {"local_timestamp": timestamp, "update": new_position}
            order_upd = {"local_timestamp": timestamp, "update": new_order}
            if new_order["filled_size"] != order["filled_size"]:
                position_func = getattr(strategy, "on_position_update", 0)
                if position_func != 0:
                    position_func(position_upd)
                if new_order["status"] == "filled":
                    order_completion_func = getattr(strategy, "on_order_completion", 0)
                    if order_completion_func != 0:
                        order_completion_func(order_upd)
                else:
                    order_fill_func = getattr(strategy, "on_order_fill", 0)
                    if order_fill_func != 0:
                        order_fill_func(order_upd)

    async def replay(self):
        tardis_client = TardisClient()
        strategy = self.strategy
        generated_ch = self.generated_channels_and_symbols
        generated_book_present = "generated_orderbook" in generated_ch.keys()
        channels = self.channels_and_symbols.keys()
        messages = tardis_client.replay(
            exchange=self.exchange,
            from_date=self.start_date,
            to_date=self.end_date,
            filters=[Channel(name=key, symbols=self.channels_and_symbols[key]) for key in channels]
        )
        async for local_timestamp, message in messages:
            channel, update, symbols = self.process_message(message)
            cur_timestamp = max(self.cur_timestamp, local_timestamp)
            self.cur_timestamp = cur_timestamp
            channel_func = getattr(strategy, f"on_{channel}_update", 0)
            if channel == self.orderbook_update_channel:
                self.process_book_update(update)
                for symbol in symbols:
                    self.update_orders_status(strategy, symbol, cur_timestamp)
                    if generated_book_present and symbol in generated_ch["generated_orderbook"]:
                        generated_book_func = getattr(strategy, "on_generated_orderbook_update", 0)
                        if generated_book_func != 0:
                            msg = {"local_timestamp": local_timestamp, "update": {symbol: self.orderbooks[symbol]}}
                            generated_book_func(msg)
            if channel_func != 0:
                msg = {"local_timestamp": local_timestamp, "update": update}
                channel_func(msg)


class BitmexReplayer(DataReplayer):
    exchange = "bitmex"
    orderbook_update_channel = "orderBookL2_25"
    PARTIAL_CHANNELS = ["instrument", "trade", "settlement", "funding", "insurance", "quoteBin1m", "quoteBin5m",
                        "quoteBin1h", "quoteBin1d", "tradeBin1m", "tradeBin5m", "tradeBin1h", "tradeBin1d",
                        "orderBook10", "orderBookL2_25"]

    def __init__(self, start_date, end_date, raw_channels_and_symbols, generated_channels_and_symbols, path_to_db,
                 strategy_class, *strategy_args, **strategy_kwargs):
        super().__init__(start_date, end_date, raw_channels_and_symbols, generated_channels_and_symbols, path_to_db,
                         strategy_class, *strategy_args, **strategy_kwargs)

    @staticmethod
    def process_side(side):
        if side == "Buy":
            return "bids"
        else:
            return "asks"

    def get_level_price_by_id(self, symbol, side, level_id):  # TODO: think of a better search algo
        book = self.orderbooks[symbol][side]
        for price in book.keys():
            el = book[price]
            if el["id"] == level_id:
                return price
        return False

    def process_message(self, message):
        channel = message["table"]
        data = message["data"]
        if channel in self.PARTIAL_CHANNELS and message["action"] == "partial":
            data = [el for el in data if el["symbol"] in self.channels_and_symbols[channel]]
        message["data"] = data
        symbols = list(set([el["symbol"] for el in data]))
        return channel, message, symbols

    def process_book_update(self, message):
        action = message["action"]
        if len(message["data"]) != 0:  # Make sure that the data is not empty
            symbol = message["data"][0]["symbol"]
            if action == "partial":
                book = self.orderbooks[symbol]
                for level in message["data"]:
                    price, side = Decimal(str(level["price"])), self.process_side(level["side"])
                    new_level = {
                        "id": level["id"],
                        "size": Decimal(str(level["size"])),
                    }
                    book[side][price] = new_level
            elif action == "insert":  # TODO: check if this and partial update can be unified
                book = self.orderbooks[symbol]
                for level in message["data"]:
                    price, side = Decimal(str(level["price"])), self.process_side(level["side"])
                    new_level = {
                        "id": level["id"],
                        "size": Decimal(str(level["size"])),
                    }
                    book[side][price] = new_level
            elif action == "update":
                book = self.orderbooks[symbol]
                for level in message["data"]:
                    side, level_id = self.process_side(level["side"]), level["id"]
                    price = self.get_level_price_by_id(symbol, side, level_id)
                    old_level = book[side][price]
                    old_level["size"] = Decimal(str(level["size"]))
            elif action == "delete":
                book = self.orderbooks[symbol]
                for level in message["data"]:
                    side, level_id = self.process_side(level["side"]), level["id"]
                    price = self.get_level_price_by_id(symbol, side, level_id)
                    if price:
                        book[side].pop(price)
            else:
                assert KeyError(f"No handler specified for the {action} message in class BitmexReplayer")


class DeribitReplayer(DataReplayer):
    exchange = "deribit"
    orderbook_update_channel = "book"

    def __init__(self, start_date, end_date, raw_channels_and_symbols, generated_channels_and_symbols, path_to_db,
                 strategy_class, *strategy_args, **strategy_kwargs):
        super().__init__(start_date, end_date, raw_channels_and_symbols, generated_channels_and_symbols, path_to_db,
                         strategy_class, *strategy_args, **strategy_kwargs)

    def process_message(self, message):
        params = message["params"]
        channel_split = params["channel"].split(".")
        if "markprice.options" in params["channel"]:
            channel = "markprice_options"
        else:
            channel = channel_split[0]
        symbol = channel_split[1]
        return channel, message, [symbol]

    def process_levels(self, levels, symbol, side):
        for level in levels:
            book = self.orderbooks[symbol][side]
            action, price, size = level[0], Decimal(str(level[1])), Decimal(str(level[2]))
            if action in ["new", "change"]:
                new_level = {
                    "id": None,
                    "size": size
                }
                book[price] = new_level
            elif action == "delete":
                if price in book.keys():
                    book.pop(price)
            else:
                assert KeyError(f"No handler specified for the {action} message in class BitmexReplayer")

    def process_book_update(self, message):
        data = message["params"]["data"]
        symbol = data["instrument_name"]
        self.process_levels(data["bids"], symbol, "bids")
        self.process_levels(data["asks"], symbol, "asks")


class BinanceUSDTFuturesReplayer(DataReplayer):
    orderbook_update_channel = "depth"
    exchange = "binance-futures"

    def __init__(self, start_date, end_date, raw_channels_and_symbols, generated_channels_and_symbols, path_to_db,
                 strategy_class, *strategy_args, **strategy_kwargs):
        super().__init__(start_date, end_date, raw_channels_and_symbols, generated_channels_and_symbols, path_to_db,
                         strategy_class, *strategy_args, **strategy_kwargs)
        self.get_init_book()

    def process_levels(self, levels, symbol, side):
        for level in levels:
            book = self.orderbooks[symbol][side]
            price, size = Decimal(level[0]), Decimal(level[1])
            if size > 0:
                new_level = {
                    "id": None,
                    "size": size
                }
                book[price] = new_level
            else:
                if price in book.keys():
                    book.pop(price)

    def get_init_book(self):
        first_minute = get_init_book(self.start_date, self.exchange, self.subscribed_symbols, "depthSnapshot")
        unprocessed_symbols = set(self.subscribed_symbols)
        for update in first_minute:
            symbol = update["stream"].split("@")[0]
            if symbol in unprocessed_symbols:
                unprocessed_symbols.remove(symbol)
                data = update["data"]
                self.process_levels(data["bids"], symbol, "bids")
                self.process_levels(data["asks"], symbol, "asks")
                if len(unprocessed_symbols) == 0:
                    break

    def process_message(self, message):
        stream_split = message["stream"].split("@")
        symbol, channel = stream_split[0], stream_split[1]
        return channel, message, [symbol]

    def process_book_update(self, message):
        symbol = message["stream"].split("@")[0]
        data = message["data"]
        self.process_levels(data["b"], symbol, "bids")
        self.process_levels(data["a"], symbol, "asks")


class BinanceCOINFuturesReplayer(DataReplayer):
    orderbook_update_channel = "depth"
    exchange = "binance-delivery"

    def __init__(self, start_date, end_date, raw_channels_and_symbols, generated_channels_and_symbols, path_to_db,
                 strategy_class, *strategy_args, **strategy_kwargs):
        super().__init__(start_date, end_date, raw_channels_and_symbols, generated_channels_and_symbols, path_to_db,
                         strategy_class, *strategy_args, **strategy_kwargs)
        self.get_init_book()

    def process_levels(self, levels, symbol, side):
        for level in levels:
            book = self.orderbooks[symbol][side]
            price, size = Decimal(level[0]), Decimal(level[1])
            if size > 0:
                new_level = {
                    "id": None,
                    "size": size
                }
                book[price] = new_level
            else:
                if price in book.keys():
                    book.pop(price)

    def get_init_book(self):
        first_minute = get_init_book(self.start_date, self.exchange, self.subscribed_symbols, "depthSnapshot")
        unprocessed_symbols = set(self.subscribed_symbols)
        for update in first_minute:
            symbol = update["stream"].split("@")[0]
            if symbol in unprocessed_symbols:
                unprocessed_symbols.remove(symbol)
                data = update["data"]
                self.process_levels(data["bids"], symbol, "bids")
                self.process_levels(data["asks"], symbol, "asks")
                if len(unprocessed_symbols) == 0:
                    break

    def process_message(self, message):
        stream_split = message["stream"].split("@")
        symbol, channel = stream_split[0], stream_split[1]
        return channel, message, [symbol]

    def process_book_update(self, message):
        symbol = message["stream"].split("@")[0]
        data = message["data"]
        self.process_levels(data["b"], symbol, "bids")
        self.process_levels(data["a"], symbol, "asks")


class BinanceSpotReplayer(DataReplayer):
    orderbook_update_channel = "depth"
    exchange = "binance"

    def __init__(self, start_date, end_date, raw_channels_and_symbols, generated_channels_and_symbols, path_to_db,
                 strategy_class, *strategy_args, **strategy_kwargs):
        super().__init__(start_date, end_date, raw_channels_and_symbols, generated_channels_and_symbols, path_to_db,
                         strategy_class, *strategy_args, **strategy_kwargs)
        self.get_init_book()

    def process_levels(self, levels, symbol, side):
        for level in levels:
            book = self.orderbooks[symbol][side]
            price, size = Decimal(level[0]), Decimal(level[1])
            if size > 0:
                new_level = {
                    "id": None,
                    "size": size
                }
                book[price] = new_level
            else:
                if price in book.keys():
                    book.pop(price)

    def get_init_book(self):
        first_minute = get_init_book(self.start_date, self.exchange, self.subscribed_symbols, "depthSnapshot")
        unprocessed_symbols = set(self.subscribed_symbols)
        for update in first_minute:
            symbol = update["stream"].split("@")[0]
            if symbol in unprocessed_symbols:
                unprocessed_symbols.remove(symbol)
                data = update["data"]
                self.process_levels(data["bids"], symbol, "bids")
                self.process_levels(data["asks"], symbol, "asks")
                if len(unprocessed_symbols) == 0:
                    break

    def process_message(self, message):
        stream_split = message["stream"].split("@")
        symbol, channel = stream_split[0], stream_split[1]
        return channel, message, [symbol]

    def process_book_update(self, message):
        symbol = message["stream"].split("@")[0]
        data = message["data"]
        self.process_levels(data["b"], symbol, "bids")
        self.process_levels(data["a"], symbol, "asks")


class OkexFuturesReplayer(DataReplayer):
    exchange = "okex-futures"
    orderbook_update_channel = "futures/depth_l2_tbt"

    def __init__(self, start_date, end_date, raw_channels_and_symbols, generated_channels_and_symbols, path_to_db,
                 strategy_class, *strategy_args, **strategy_kwargs):
        super().__init__(start_date, end_date, raw_channels_and_symbols, generated_channels_and_symbols, path_to_db,
                         strategy_class, *strategy_args, **strategy_kwargs)
        self.orderbook_update_channel = "futures_depth_l2_tbt"

    def process_levels(self, levels, symbol, side):
        book = self.orderbooks[symbol][side]
        for level in levels:
            price, size = Decimal(level[0]), Decimal(level[1])
            if size > 0:
                new_level = {
                    "id": None,
                    "size": size
                }
                book[price] = new_level
            else:
                if price in book.keys():
                    book.pop(price)

    def process_message(self, message):
        channel = message["table"].replace("/", "_")
        symbols = list(set([el["instrument_id"] for el in message["data"]]))
        return channel, message, symbols

    def process_book_update(self, message):
        data = message["data"]
        for update in data:
            symbol = update["instrument_id"]
            self.process_levels(update["bids"], symbol, "bids")
            self.process_levels(update["asks"], symbol, "asks")


class OkexSwapReplayer(DataReplayer):
    exchange = "okex-swap"
    orderbook_update_channel = "swap/depth_l2_tbt"

    def __init__(self, start_date, end_date, raw_channels_and_symbols, generated_channels_and_symbols, path_to_db,
                 strategy_class, *strategy_args, **strategy_kwargs):
        super().__init__(start_date, end_date, raw_channels_and_symbols, generated_channels_and_symbols, path_to_db,
                         strategy_class, *strategy_args, **strategy_kwargs)
        self.orderbook_update_channel = "swap_depth_l2_tbt"

    def process_levels(self, levels, symbol, side):
        book = self.orderbooks[symbol][side]
        for level in levels:
            price, size = Decimal(level[0]), Decimal(level[1])
            if size > 0:
                new_level = {
                    "id": None,
                    "size": size
                }
                book[price] = new_level
            else:
                if price in book.keys():
                    book.pop(price)

    def process_message(self, message):
        channel = message["table"].replace("/", "_")
        symbols = list(set([el["instrument_id"] for el in message["data"]]))
        return channel, message, symbols

    def process_book_update(self, message):
        data = message["data"]
        for update in data:
            symbol = update["instrument_id"]
            self.process_levels(update["bids"], symbol, "bids")
            self.process_levels(update["asks"], symbol, "asks")


class OkexOptionsReplayer(DataReplayer):
    exchange = "okex-options"
    orderbook_update_channel = "option/depth_l2_tbt"

    def __init__(self, start_date, end_date, raw_channels_and_symbols, generated_channels_and_symbols, path_to_db,
                 strategy_class, *strategy_args, **strategy_kwargs):
        super().__init__(start_date, end_date, raw_channels_and_symbols, generated_channels_and_symbols, path_to_db,
                         strategy_class, *strategy_args, **strategy_kwargs)
        self.orderbook_update_channel = "option_depth_l2_tbt"

    def process_levels(self, levels, symbol, side):
        book = self.orderbooks[symbol][side]
        for level in levels:
            price, size = Decimal(level[0]), Decimal(level[1])
            if size > 0:
                new_level = {
                    "id": None,
                    "size": size
                }
                book[price] = new_level
            else:
                if price in book.keys():
                    book.pop(price)

    def process_message(self, message):
        channel = message["table"].replace("/", "_")
        symbols = list(set([el["instrument_id"] for el in message["data"]]))
        return channel, message, symbols

    def process_book_update(self, message):
        data = message["data"]
        for update in data:
            symbol = update["instrument_id"]
            self.process_levels(update["bids"], symbol, "bids")
            self.process_levels(update["asks"], symbol, "asks")


class OkexSpotReplayer(DataReplayer):
    exchange = "okex"
    orderbook_update_channel = "spot/depth_l2_tbt"

    def __init__(self, start_date, end_date, raw_channels_and_symbols, generated_channels_and_symbols, path_to_db,
                 strategy_class, *strategy_args, **strategy_kwargs):
        super().__init__(start_date, end_date, raw_channels_and_symbols, generated_channels_and_symbols, path_to_db,
                         strategy_class, *strategy_args, **strategy_kwargs)
        self.orderbook_update_channel = "spot_depth_l2_tbt"

    def process_levels(self, levels, symbol, side):
        book = self.orderbooks[symbol][side]
        for level in levels:
            price, size = Decimal(level[0]), Decimal(level[1])
            if size > 0:
                new_level = {
                    "id": None,
                    "size": size
                }
                book[price] = new_level
            else:
                if price in book.keys():
                    book.pop(price)

    def process_message(self, message):
        channel = message["table"].replace("/", "_")
        symbols = list(set([el["instrument_id"] for el in message["data"]]))
        return channel, message, symbols

    def process_book_update(self, message):
        data = message["data"]
        for update in data:
            symbol = update["instrument_id"]
            self.process_levels(update["bids"], symbol, "bids")
            self.process_levels(update["asks"], symbol, "asks")


class HuobiFuturesReplayer(DataReplayer):
    exchange = "huobi-dm"
    orderbook_update_channel = "depth"

    def __init__(self, start_date, end_date, raw_channels_and_symbols, generated_channels_and_symbols, path_to_db,
                 strategy_class, *strategy_args, **strategy_kwargs):
        super().__init__(start_date, end_date, raw_channels_and_symbols, generated_channels_and_symbols, path_to_db,
                         strategy_class, *strategy_args, **strategy_kwargs)

    def process_levels(self, levels, symbol, side):
        book = self.orderbooks[symbol][side]
        for level in levels:
            price, size = Decimal(str(level[0])), Decimal(str(level[1]))
            if size > 0:
                new_level = {
                    "id": None,
                    "size": size
                }
                book[price] = new_level
            else:
                if price in book.keys():
                    book.pop(price)

    def process_message(self, message):
        if "ch" in message.keys():
            ch = message["ch"].split(".")
        elif "topic" in message.keys():
            ch = message["topic"].split(".")
        elif "rep" in message.keys():
            ch = message["rep"].split(".")
        else:
            assert KeyError(
                f"No channel specified in the message {message}")  # TODO: read why the assertion in coro may not work
        channel, symbol = ch[2], ch[1]
        return channel, message, [symbol]

    def process_book_update(self, message):
        symbol = message["ch"].split(".")[1]
        data = message["tick"]
        self.process_levels(data["bids"], symbol, "bids")
        self.process_levels(data["asks"], symbol, "asks")


class HuobiCOINSwapReplayer(DataReplayer):
    exchange = "huobi-dm-swap"
    orderbook_update_channel = "depth"

    def __init__(self, start_date, end_date, raw_channels_and_symbols, generated_channels_and_symbols, path_to_db,
                 strategy_class, *strategy_args, **strategy_kwargs):
        super().__init__(start_date, end_date, raw_channels_and_symbols, generated_channels_and_symbols, path_to_db,
                         strategy_class, *strategy_args, **strategy_kwargs)

    def process_levels(self, levels, symbol, side):
        book = self.orderbooks[symbol][side]
        for level in levels:
            price, size = Decimal(str(level[0])), Decimal(str(level[1]))
            if size > 0:
                new_level = {
                    "id": None,
                    "size": size
                }
                book[price] = new_level
            else:
                if price in book.keys():
                    book.pop(price)

    def process_message(self, message):
        if "ch" in message.keys():
            ch = message["ch"].split(".")
        elif "topic" in message.keys():
            ch = message["topic"].split(".")
        elif "rep" in message.keys():
            ch = message["rep"].split(".")
        else:
            assert KeyError(
                f"No channel specified in the message {message}")
        channel, symbol = ch[2], ch[1]
        return channel, message, [symbol]

    def process_book_update(self, message):
        symbol = message["ch"].split(".")[1]
        data = message["tick"]
        self.process_levels(data["bids"], symbol, "bids")
        self.process_levels(data["asks"], symbol, "asks")


class HuobiUSDTSwapReplayer(DataReplayer):
    exchange = "huobi-dm-linear-swap"
    orderbook_update_channel = "depth"

    def __init__(self, start_date, end_date, raw_channels_and_symbols, generated_channels_and_symbols, path_to_db,
                 strategy_class, *strategy_args, **strategy_kwargs):
        super().__init__(start_date, end_date, raw_channels_and_symbols, generated_channels_and_symbols, path_to_db,
                         strategy_class, *strategy_args, **strategy_kwargs)

    def process_levels(self, levels, symbol, side):
        book = self.orderbooks[symbol][side]
        for level in levels:
            price, size = Decimal(str(level[0])), Decimal(str(level[1]))
            if size > 0:
                new_level = {
                    "id": None,
                    "size": size
                }
                book[price] = new_level
            else:
                if price in book.keys():
                    book.pop(price)

    def process_message(self, message):
        if "ch" in message.keys():
            ch = message["ch"].split(".")
        elif "topic" in message.keys():
            ch = message["topic"].split(".")
        elif "rep" in message.keys():
            ch = message["rep"].split(".")
        else:
            assert KeyError(
                f"No channel specified in the message {message}")
        channel, symbol = ch[2], ch[1]
        return channel, message, [symbol]

    def process_book_update(self, message):
        symbol = message["ch"].split(".")[1]
        data = message["tick"]
        self.process_levels(data["bids"], symbol, "bids")
        self.process_levels(data["asks"], symbol, "asks")


class HuobiSpotReplayer(DataReplayer):
    exchange = "huobi"
    orderbook_update_channel = "mbp"

    def __init__(self, start_date, end_date, raw_channels_and_symbols, generated_channels_and_symbols, path_to_db,
                 strategy_class, *strategy_args, **strategy_kwargs):
        super().__init__(start_date, end_date, raw_channels_and_symbols, generated_channels_and_symbols, path_to_db,
                         strategy_class, *strategy_args, **strategy_kwargs)

    def process_levels(self, levels, symbol, side):
        book = self.orderbooks[symbol][side]
        for level in levels:
            price, size = Decimal(str(level[0])), Decimal(str(level[1]))
            if size > 0:
                new_level = {
                    "id": None,
                    "size": size
                }
                book[price] = new_level
            else:
                if price in book.keys():
                    book.pop(price)

    def process_message(self, message):
        if "ch" in message.keys():
            ch = message["ch"].split(".")
        elif "topic" in message.keys():
            ch = message["topic"].split(".")
        elif "rep" in message.keys():
            ch = message["rep"].split(".")
        else:
            assert KeyError(
                f"No channel specified in the message {message}")
        channel, symbol = ch[2], ch[1]
        return channel, message

    def process_book_update(self, message):
        if "ch" in message.keys():
            symbol = message["ch"].split(".")[1]
            data = message["tick"]
            self.process_levels(data["bids"], symbol, "bids")
            self.process_levels(data["asks"], symbol, "asks")
        elif "rep" in message.keys():
            symbol = message["rep"].split(".")[1]
            data = message["data"]
            self.process_levels(data["bids"], symbol, "bids")
            self.process_levels(data["asks"], symbol, "asks")
        else:
            assert KeyError(f"Unknown format of orderbook update {message}")


class CoinbaseReplayer(DataReplayer):
    exchange = "coinbase"
    orderbook_update_channel = "l2update"

    def __init__(self, start_date, end_date, raw_channels_and_symbols, generated_channels_and_symbols, path_to_db,
                 strategy_class, *strategy_args, **strategy_kwargs):
        super().__init__(start_date, end_date, raw_channels_and_symbols, generated_channels_and_symbols, path_to_db,
                         strategy_class, *strategy_args, **strategy_kwargs)
        self.get_init_book()

    @staticmethod
    def process_side(side):
        if side == "Buy":
            return "bids"
        else:
            return "asks"

    def process_levels(self, levels, symbol, side):
        book = self.orderbooks[symbol][side]
        for level in levels:
            price, size = Decimal(level[0]), Decimal(level[1])
            if size > 0:
                new_level = {
                    "id": None,
                    "size": size
                }
                book[price] = new_level
            else:
                if price in book.keys():
                    book.pop(price)

    def get_init_book(self):
        first_minute = get_init_book(self.start_date, self.exchange, self.subscribed_symbols, "snapshot")
        unprocessed_symbols = set(self.subscribed_symbols)
        for update in first_minute:
            symbol = update["product_id"]
            if symbol in unprocessed_symbols:
                unprocessed_symbols.remove(symbol)
                self.process_levels(update["bids"], symbol, "bids")
                self.process_levels(update["asks"], symbol, "asks")
                if len(unprocessed_symbols) == 0:
                    break

    def process_message(self, message):
        channel, symbol = message["type"], message["product_id"]
        return channel, message, [symbol]

    def process_book_update(self, message):
        symbol = message["product_id"]
        data = message["changes"]
        new_levels = {"bids": [], "asks": []}
        for level in data:
            side = self.process_side(level.pop(0))
            new_levels[side].append(level)
        self.process_levels(new_levels["bids"], symbol, "bids")
        self.process_levels(new_levels["asks"], symbol, "asks")


class KrakenFuturesReplayer(DataReplayer):
    exchange = "cryptofacilities"
    orderbook_update_channel = "book"

    def __init__(self, start_date, end_date, raw_channels_and_symbols, generated_channels_and_symbols, path_to_db,
                 strategy_class, *strategy_args, **strategy_kwargs):
        super().__init__(start_date, end_date, raw_channels_and_symbols, generated_channels_and_symbols, path_to_db,
                         strategy_class, *strategy_args, **strategy_kwargs)
        self.get_init_book()

    @staticmethod
    def process_side(side):
        if side == "Buy":
            return "bids"
        else:
            return "asks"

    def process_levels(self, levels, symbol, side):
        book = self.orderbooks[symbol][side]
        for level in levels:
            price, size = Decimal(str(level["price"])), Decimal(str(level["qty"]))
            if size > 0:
                new_level = {
                    "id": None,
                    "size": size
                }
                book[price] = new_level
            else:
                if price in book.keys():
                    book.pop(price)

    def get_init_book(self):
        first_minute = get_init_book(self.start_date, self.exchange, self.subscribed_symbols, "book_snapshot")
        unprocessed_symbols = set(self.subscribed_symbols)
        for update in first_minute:
            symbol = update["product_id"]
            if symbol in unprocessed_symbols:
                unprocessed_symbols.remove(symbol)
                self.process_levels(update["bids"], symbol, "bids")
                self.process_levels(update["asks"], symbol, "asks")
                if len(unprocessed_symbols) == 0:
                    break

    def process_message(self, message):
        channel, symbol = message["feed"], message["product_id"]
        return channel, message, [symbol]

    def process_book_update(self, message):
        if "event" not in message.keys():  # not a service message
            level = {
                "price": message["price"],
                "qty": message["qty"]
            }
            levels = [level]
            symbol = message["product_id"]
            self.process_levels(levels, symbol, self.process_side(message["side"]))


class KrakenSpotReplayer(DataReplayer):
    exchange = "kraken"
    orderbook_update_channel = "book"

    def __init__(self, start_date, end_date, raw_channels_and_symbols, generated_channels_and_symbols, path_to_db,
                 strategy_class, *strategy_args, **strategy_kwargs):
        super().__init__(start_date, end_date, raw_channels_and_symbols, generated_channels_and_symbols, path_to_db,
                         strategy_class, *strategy_args, **strategy_kwargs)

    def process_levels(self, levels, symbol, side):
        book = self.orderbooks[symbol][side]
        for level in levels:
            price, size = Decimal(level[0]), Decimal(level[1])
            if size > 0:
                new_level = {
                    "id": None,
                    "size": size
                }
                book[price] = new_level
            else:
                if price in book.keys():
                    book.pop(price)

    def process_message(self, message):
        channel, symbol = message[-2], message[-1]
        if "book" in channel:
            channel = "book"
        return channel, message, [symbol]

    def process_side_update(self, update: dict, symbol: str):
        if "as" in update.keys() or "bs" in update.keys():
            if "bs" in update.keys():
                self.process_levels(update["bs"], symbol, "bids")
            if "as" in update.keys():
                self.process_levels(update["as"], symbol, "asks")
        elif "b" in update.keys():
            self.process_levels(update["b"], symbol, "bids")
        elif "a" in update.keys():
            self.process_levels(update["a"], symbol, "asks")
        else:
            assert KeyError(f"Unknown format of orderbook update {update}")

    def process_book_update(self, message):
        symbol = message[-1]
        if len(message) == 4:
            self.process_side_update(message[1], symbol)
        elif len(message) == 5:
            self.process_side_update(message[1], symbol)
            self.process_side_update(message[2], symbol)
        else:
            assert KeyError(f"Unknown format of orderbook update {message}")


class BitstampReplayer(DataReplayer):
    exchange = "bitstamp"
    orderbook_update_channel = "diff_order_book"

    def __init__(self, start_date, end_date, raw_channels_and_symbols, generated_channels_and_symbols, path_to_db,
                 strategy_class, *strategy_args, **strategy_kwargs):
        super().__init__(start_date, end_date, raw_channels_and_symbols, generated_channels_and_symbols, path_to_db,
                         strategy_class, *strategy_args, **strategy_kwargs)

    def process_levels(self, levels, symbol, side):
        book = self.orderbooks[symbol][side]
        for level in levels:
            price, size = Decimal(level[0]), Decimal(level[1])
            if size > 0:
                new_level = {
                    "id": None,
                    "size": size
                }
                book[price] = new_level
            else:
                if price in book.keys():
                    book.pop(price)

    def process_message(self, message):
        channel_split = message["channel"].split("_")
        channel, symbol = channel_split[:-1], channel_split[-1]
        channel = "_".join(channel)
        return channel, message, [symbol]

    def process_book_update(self, message):
        if message["event"] in ["data", "snapshot"]:
            data = message["data"]
            symbol = message["channel"].split("_")[-1]
            self.process_levels(data["bids"], symbol, "bids")
            self.process_levels(data["asks"], symbol, "asks")
