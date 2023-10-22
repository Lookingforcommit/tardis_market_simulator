from execution_management import OrderManager
from market_replayers import BinanceUSDTFuturesReplayer
from datetime import datetime, timedelta
import asyncio

#  TODO: Add API key field
START_DATE = "2021-07-01"
END_DATE = "2021-07-02"
RAW_CHANNELS_AND_SYMBOLS = {
    "trade": ["btcusdt"],
}
GENERATED_CHANNELS_AND_SYMBOLS = {
    "generated_orderbook": ["btcusdt"],
    "position_update": ["btcusdt"]
}


class ExampleStrategy:
    def __init__(self, symbol, monitoring_timedelta=timedelta(minutes=5), orders_timedelta=timedelta(minutes=1),
                 sell_timedelta=timedelta(minutes=1), entry_drop_value=5, order_size=10):
        self.symbol = symbol
        self.start_timestamp = datetime(1, 1, 1)
        self.monitoring_timedelta = monitoring_timedelta
        self.orders_timedelta = orders_timedelta
        self.sell_timedelta = sell_timedelta
        self.entry_drop_value = entry_drop_value
        self.order_size = order_size
        self.order_manager = OrderManager()
        self.start_price = None
        self.entering_position = False
        self.active_position = False
        self.quitting_position = False
        self.orders_timestamp = None
        self.buy_order_id = None
        self.sell_order_id = None

    def on_generated_orderbook_update(self, msg):
        timestamp, book = msg["local_timestamp"], msg["update"]
        best_ask = min(book[self.symbol]['asks'].keys())
        best_bid = max(book[self.symbol]['bids'].keys())
        if not self.entering_position and not self.active_position:
            if timestamp - self.start_timestamp >= self.monitoring_timedelta:
                self.start_timestamp = timestamp
                self.start_price = best_ask
            drop_percent = 100 - 100 * best_ask / self.start_price
            if drop_percent >= self.entry_drop_value:
                order_manager = self.order_manager
                buy_order = order_manager.send_limit_order(timestamp, self.symbol, "buy", best_ask, self.order_size)
                if buy_order["status"] == "filled":
                    self.active_position = True
                elif buy_order["filled_size"] > 0:
                    self.buy_order_id = buy_order["id"]
                    self.entering_position = True
                    self.active_position = True
                else:
                    self.entering_position = True
                    self.buy_order_id = buy_order["id"]
                self.orders_timestamp = timestamp
        elif self.entering_position and timestamp - self.orders_timestamp >= self.orders_timedelta:
            order_manager = self.order_manager
            order_manager.cancel_order(self.buy_order_id)
            self.entering_position = False
        elif self.active_position and not self.quitting_position and timestamp - self.orders_timestamp >= self.sell_timedelta:
            order_manager = self.order_manager
            active_pos_size = order_manager.get_active_position(self.symbol)[0]["size"]
            sell_order = order_manager.send_limit_order(timestamp, self.symbol, "sell", best_bid, active_pos_size)
            if sell_order["status"] == "filled":
                self.active_position = False
            elif sell_order["filled_size"] > 0:
                self.sell_order_id = sell_order["id"]
                self.quitting_position = True
                self.orders_timestamp = timestamp
            else:
                self.quitting_position = True
                self.sell_order_id = sell_order["id"]
                self.orders_timestamp = timestamp
        elif self.quitting_position and timestamp - self.orders_timestamp >= self.orders_timedelta:
            order_manager = self.order_manager
            order_manager.cancel_order(self.sell_order_id)
            active_pos_size = order_manager.get_active_position(self.symbol)[0]["size"]
            sell_order = order_manager.send_limit_order(timestamp, self.symbol, "sell", best_bid, active_pos_size)
            if sell_order["status"] == "filled":
                self.active_position = False
                self.quitting_position = False
            else:
                self.sell_order_id = sell_order["id"]
                self.orders_timestamp = timestamp

    def on_position_update(self, msg):
        timestamp, position = msg["local_timestamp"], msg["update"]
        if self.entering_position:
            self.active_position = True
            if position["size"] == self.order_size:
                self.entering_position = False
                self.orders_timestamp = timestamp
        elif self.quitting_position:
            if position["size"] == 0:
                self.quitting_position = False
                self.active_position = False


replayer = BinanceUSDTFuturesReplayer(START_DATE, END_DATE, RAW_CHANNELS_AND_SYMBOLS, GENERATED_CHANNELS_AND_SYMBOLS,
                                      "database.db", ExampleStrategy, "btcusdt",
                                      timedelta(minutes=15), order_size=10, entry_drop_value=0.75)
asyncio.run(replayer.replay())
