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
    "depth": ["btcusdt", "ethusdt"],
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
            elif action == "insert":
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
            new_level = {
                "id": None,
                "size": size
            }
            if action in ["new", "change"]:
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
            new_level = {
                "id": None,
                "size": size
            }
            if size > 0:
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
            new_level = {
                "id": None,
                "size": size
            }
            if size > 0:
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
            new_level = {
                "id": None,
                "size": size
            }
            if size > 0:
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
    def __init__(self, start_date, end_date, channels_and_symbols):
        super().__init__(start_date, end_date, "okex-futures", channels_and_symbols)

    def process_message(self, message):
        channel = message["table"].split("/")[1]
        return channel, message

    def process_book_update(self, message):
        pass


class OkexSwapReplayer(DataReplayer):
    def __init__(self, start_date, end_date, channels_and_symbols):
        super().__init__(start_date, end_date, "okex-swap", channels_and_symbols)

    def process_message(self, message):
        channel = message["table"].split("/")[1]
        return channel, message

    def process_book_update(self, message):
        pass


class OkexOptionsReplayer(DataReplayer):
    def __init__(self, start_date, end_date, channels_and_symbols):
        super().__init__(start_date, end_date, "okex-options", channels_and_symbols)

    def process_message(self, message):
        channel = message["table"].split("/")[1]
        return channel, message

    def process_book_update(self, message):
        pass


class OkexSpotReplayer(DataReplayer):
    def __init__(self, start_date, end_date, channels_and_symbols):
        super().__init__(start_date, end_date, "okex", channels_and_symbols)

    def process_message(self, message):
        channel = message["table"].split("/")[1]
        return channel, message

    def process_book_update(self, message):
        pass


class HuobiFuturesReplayer(DataReplayer):
    def __init__(self, start_date, end_date, channels_and_symbols):
        super().__init__(start_date, end_date, "huobi-dm", channels_and_symbols)

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
        pass


class HuobiCOINSwapReplayer(DataReplayer):
    def __init__(self, start_date, end_date, channels_and_symbols):
        super().__init__(start_date, end_date, "huobi-dm-swap", channels_and_symbols)

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
        pass


class HuobiUSDTSwapReplayer(DataReplayer):
    def __init__(self, start_date, end_date, channels_and_symbols):
        super().__init__(start_date, end_date, "huobi-dm-linear-swap", channels_and_symbols)

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
        pass


class HuobiSpotReplayer(DataReplayer):
    def __init__(self, start_date, end_date, channels_and_symbols):
        super().__init__(start_date, end_date, "huobi", channels_and_symbols)

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
        pass


class CoinbaseReplayer(DataReplayer):
    def __init__(self, start_date, end_date, channels_and_symbols):
        super().__init__(start_date, end_date, "coinbase", channels_and_symbols)

    def process_message(self, message):
        channel = message["type"]
        return channel, message

    def process_book_update(self, message):
        pass


class KrakenFuturesReplayer(DataReplayer):
    def __init__(self, start_date, end_date, channels_and_symbols):
        super().__init__(start_date, end_date, "cryptofacilities", channels_and_symbols)

    def process_message(self, message):
        channel = message["feed"]
        return channel, message

    def process_book_update(self, message):
        pass


class KrakenSpotReplayer(DataReplayer):
    def __init__(self, start_date, end_date, channels_and_symbols):
        super().__init__(start_date, end_date, "kraken", channels_and_symbols)

    def process_message(self, message):
        channel = message[2]
        return channel, message

    def process_book_update(self, message):
        pass


class BitstampReplayer(DataReplayer):
    def __init__(self, start_date, end_date, channels_and_symbols):
        super().__init__(start_date, end_date, "bitstamp", channels_and_symbols)

    def process_message(self, message):
        channel = message["channel"].split("_")[:-1]
        channel = "_".join(channel)
        return channel, message

    def process_book_update(self, message):
        pass


class OrderManager:
    def __init__(self, init_orderbooks):
        self.active_orders = {}
        self.orderbooks = init_orderbooks

    def send_limit_order(self, order_id, symbol, side, price):
        book = self.orderbooks[symbol]

    def on_orderbook_update(self, update):
        pass


strategy = ExampleStrategy()
replayer = BinanceSpotReplayer(START_DATE, END_DATE, CHANNELS_AND_SYMBOLS)
asyncio.run(replayer.replay(strategy))
