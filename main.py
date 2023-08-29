from tardis_client import TardisClient, Channel
from abc import ABC, abstractmethod
from decimal import Decimal
from typing import Union
import requests
import json
import asyncio


def get_init_book(start_date: str, exchange: str, symbols: list[str], channel: str) -> list[dict]:
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


#  TODO: Add API key field
#  TODO: add the service channels
START_DATE = "2021-07-01"
END_DATE = "2021-07-02"
CHANNELS_AND_SYMBOLS = {
    "live_trades": ["btcusdt", "ethusdt"],
}


class ExampleStrategy:
    def __init__(self):
        pass


class DataReplayer(ABC):
    def __init__(self, start_date: str, end_date: str, channels_and_symbols: dict[str, list[str]]):
        self.start_date = start_date
        self.end_date = end_date
        self.channels_and_symbols = channels_and_symbols
        subscribed_symbols = set()
        for key in channels_and_symbols.keys():
            subscribed_symbols.update(channels_and_symbols[key])
        self.subscribed_symbols = list(subscribed_symbols)
        self.orderbooks = {symbol: {"bids": {}, "asks": {}} for symbol in self.subscribed_symbols}
        self.channels_and_symbols[self.orderbook_update_channel] = self.subscribed_symbols

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

    async def replay(self, strategy):
        tardis_client = TardisClient()
        channels = self.channels_and_symbols.keys()
        messages = tardis_client.replay(
            exchange=self.exchange,
            from_date=self.start_date,
            to_date=self.end_date,
            filters=[Channel(name=key, symbols=self.channels_and_symbols[key]) for key in channels]
        )
        async for local_timestamp, message in messages:
            channel, update = self.process_message(message)
            strategy_func = getattr(strategy, "on_" + channel + "_update", 0)
            if channel == self.orderbook_update_channel:
                self.process_book_update(update)
            if strategy_func != 0:
                strategy_func(update)


class BitmexReplayer(DataReplayer):
    exchange = "bitmex"
    orderbook_update_channel = "orderBookL2_25"
    PARTIAL_CHANNELS = ["instrument", "trade", "settlement", "funding", "insurance", "quoteBin1m", "quoteBin5m",
                        "quoteBin1h", "quoteBin1d", "tradeBin1m", "tradeBin5m", "tradeBin1h", "tradeBin1d",
                        "orderBook10", "orderBookL2_25"]

    def __init__(self, start_date, end_date, channels_and_symbols):
        super().__init__(start_date, end_date, channels_and_symbols)

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
        return channel, message

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
            elif action == "insert": # TODO: check if this and partial update can be unified
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

    def __init__(self, start_date, end_date, channels_and_symbols):
        super().__init__(start_date, end_date, channels_and_symbols)

    def process_message(self, message):
        params = message["params"]
        if "markprice.options" in params["channel"]:
            channel = "markprice_options"
        else:
            channel = params["channel"].split(".")[0]
        return channel, message

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

    def __init__(self, start_date, end_date, channels_and_symbols):
        super().__init__(start_date, end_date, channels_and_symbols)
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
        channel = message["stream"].split("@")[1]
        return channel, message

    def process_book_update(self, message):
        symbol = message["stream"].split("@")[0]
        data = message["data"]
        self.process_levels(data["b"], symbol, "bids")
        self.process_levels(data["a"], symbol, "asks")


class BinanceCOINFuturesReplayer(DataReplayer):
    orderbook_update_channel = "depth"
    exchange = "binance-delivery"

    def __init__(self, start_date, end_date, channels_and_symbols):
        super().__init__(start_date, end_date, channels_and_symbols)  # TODO: change to exchange
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
        channel = message["stream"].split("@")[1]
        return channel, message

    def process_book_update(self, message):
        symbol = message["stream"].split("@")[0]
        data = message["data"]
        self.process_levels(data["b"], symbol, "bids")
        self.process_levels(data["a"], symbol, "asks")


class BinanceSpotReplayer(DataReplayer):
    orderbook_update_channel = "depth"
    exchange = "binance"

    def __init__(self, start_date, end_date, channels_and_symbols):
        super().__init__(start_date, end_date, channels_and_symbols)
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
        channel = message["stream"].split("@")[1]
        return channel, message

    def process_book_update(self, message):
        symbol = message["stream"].split("@")[0]
        data = message["data"]
        self.process_levels(data["b"], symbol, "bids")
        self.process_levels(data["a"], symbol, "asks")


class OkexFuturesReplayer(DataReplayer):
    exchange = "okex-futures"
    orderbook_update_channel = "futures/depth_l2_tbt"

    def __init__(self, start_date, end_date, channels_and_symbols):
        super().__init__(start_date, end_date, channels_and_symbols)
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
        return channel, message

    def process_book_update(self, message):
        data = message["data"]
        for update in data:
            symbol = update["instrument_id"]
            self.process_levels(update["bids"], symbol, "bids")
            self.process_levels(update["asks"], symbol, "asks")


class OkexSwapReplayer(DataReplayer):
    exchange = "okex-swap"
    orderbook_update_channel = "swap/depth_l2_tbt"

    def __init__(self, start_date, end_date, channels_and_symbols):
        super().__init__(start_date, end_date, channels_and_symbols)
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
        return channel, message

    def process_book_update(self, message):
        data = message["data"]
        for update in data:
            symbol = update["instrument_id"]
            self.process_levels(update["bids"], symbol, "bids")
            self.process_levels(update["asks"], symbol, "asks")


class OkexOptionsReplayer(DataReplayer):
    exchange = "okex-options"
    orderbook_update_channel = "option/depth_l2_tbt"

    def __init__(self, start_date, end_date, channels_and_symbols):
        super().__init__(start_date, end_date, channels_and_symbols)
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
        return channel, message

    def process_book_update(self, message):
        data = message["data"]
        for update in data:
            symbol = update["instrument_id"]
            self.process_levels(update["bids"], symbol, "bids")
            self.process_levels(update["asks"], symbol, "asks")


class OkexSpotReplayer(DataReplayer):
    exchange = "okex"
    orderbook_update_channel = "spot/depth_l2_tbt"

    def __init__(self, start_date, end_date, channels_and_symbols):
        super().__init__(start_date, end_date, channels_and_symbols)
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
        return channel, message

    def process_book_update(self, message):
        data = message["data"]
        for update in data:
            symbol = update["instrument_id"]
            self.process_levels(update["bids"], symbol, "bids")
            self.process_levels(update["asks"], symbol, "asks")


class HuobiFuturesReplayer(DataReplayer):
    exchange = "huobi-dm"
    orderbook_update_channel = "depth"

    def __init__(self, start_date, end_date, channels_and_symbols):
        super().__init__(start_date, end_date, channels_and_symbols)

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
        channel = ch[2]
        return channel, message

    def process_book_update(self, message):
        symbol = message["ch"].split(".")[1]
        data = message["tick"]
        self.process_levels(data["bids"], symbol, "bids")
        self.process_levels(data["asks"], symbol, "asks")


class HuobiCOINSwapReplayer(DataReplayer):
    exchange = "huobi-dm-swap"
    orderbook_update_channel = "depth"

    def __init__(self, start_date, end_date, channels_and_symbols):
        super().__init__(start_date, end_date, channels_and_symbols)

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
        channel = ch[2]
        return channel, message

    def process_book_update(self, message):
        symbol = message["ch"].split(".")[1]
        data = message["tick"]
        self.process_levels(data["bids"], symbol, "bids")
        self.process_levels(data["asks"], symbol, "asks")


class HuobiUSDTSwapReplayer(DataReplayer):
    exchange = "huobi-dm-linear-swap"
    orderbook_update_channel = "depth"

    def __init__(self, start_date, end_date, channels_and_symbols):
        super().__init__(start_date, end_date, channels_and_symbols)

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
        channel = ch[2]
        return channel, message

    def process_book_update(self, message):
        symbol = message["ch"].split(".")[1]
        data = message["tick"]
        self.process_levels(data["bids"], symbol, "bids")
        self.process_levels(data["asks"], symbol, "asks")


class HuobiSpotReplayer(DataReplayer):
    exchange = "huobi"
    orderbook_update_channel = "mbp"

    def __init__(self, start_date, end_date, channels_and_symbols):
        super().__init__(start_date, end_date, channels_and_symbols)

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
        channel = ch[2]
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

    def __init__(self, start_date, end_date, channels_and_symbols):
        super().__init__(start_date, end_date, channels_and_symbols)
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
        channel = message["type"]
        return channel, message

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

    def __init__(self, start_date, end_date, channels_and_symbols):
        super().__init__(start_date, end_date, channels_and_symbols)
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
        channel = message["feed"]
        return channel, message

    def process_book_update(self, message):
        if "event" not in message.keys():
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

    def __init__(self, start_date, end_date, channels_and_symbols):
        super().__init__(start_date, end_date, channels_and_symbols)

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
        channel = message[-2]
        if "book" in channel:
            channel = "book"
        return channel, message

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
            assert KeyError("Unknown format of orderbook update " + message)


class BitstampReplayer(DataReplayer):
    exchange = "bitstamp"
    orderbook_update_channel = "diff_order_book"

    def __init__(self, start_date, end_date, channels_and_symbols):
        super().__init__(start_date, end_date, channels_and_symbols)

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
        channel = message["channel"].split("_")[:-1]
        channel = "_".join(channel)
        return channel, message

    def process_book_update(self, message):
        if message["event"] in ["data", "snapshot"]:
            data = message["data"]
            symbol = message["channel"].split("_")[-1]
            self.process_levels(data["bids"], symbol, "bids")
            self.process_levels(data["asks"], symbol, "asks")


class OrderManager:
    def __init__(self, init_orderbooks):
        self.active_orders = {}
        self.orderbooks = init_orderbooks

    def send_limit_order(self, order_id, symbol, side, price):
        book = self.orderbooks[symbol]

    def on_orderbook_update(self, update):
        pass


strategy = ExampleStrategy()
replayer = BitstampReplayer(START_DATE, END_DATE, CHANNELS_AND_SYMBOLS)
asyncio.run(replayer.replay(strategy))
