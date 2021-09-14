from binance.client import Client
import pandas as pd
import numpy as np
from pathlib import Path
from multiprocessing import Pool
from datetime import datetime

# binance api key
api_key= ""
api_secret= ""

client = Client(api_key, api_secret)

# path for data output
data_path = Path("./data")



def download_data(symbol, interval=Client.KLINE_INTERVAL_1MINUTE):
    def to_datetime(x):
        return datetime.fromtimestamp(x / 1000)

    # date strings
    start_date = "1 Jan, 2020"
    end_date = "1 July, 2021"


    klines = client.get_historical_klines(symbol, interval,start_date ,end_date)

    klines_df = pd.DataFrame(data=klines, columns=["open_time", "open", "high", "low", "close", "volume", "close_time",
                                                   "quote_asset_volume", "number_of_trades", "taker_buy_base_volume",
                                                   "taker_buy_quote_volume", "ignore"])
    klines_df["open_time"] = klines_df["open_time"].apply(to_datetime)
    klines_df["close_time"] = klines_df["close_time"].apply(to_datetime)
    klines_df = klines_df.set_index("open_time")



    int_path = data_path/interval
    if not int_path.exists():
        Path.mkdir(int_path, parents=True)

    klines_df.to_csv(int_path / f"{symbol}_{interval}_klines.csv")

    print(f"downloaded {symbol}")






if __name__ == "__main__":


    # set interval for klines
    interval = Client.KLINE_INTERVAL_1MINUTE

    # load tickers for downloading
    exchange_info = client.get_exchange_info()

    # for filtering out stable coin pairs
    stables = ["USDT",]

    symbols = [s["symbol"] for s in exchange_info['symbols'] if any(x in s["symbol"] for x in stables)]

    with Pool(4) as p:

        p.starmap(download_data, list(map(lambda o: (o, interval), symbols)))


