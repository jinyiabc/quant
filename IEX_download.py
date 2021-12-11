import os
from pprint import pprint

import pandas as pd
import pandas_datareader.data as web
import backtrader as bt
import numpy as np
from datetime import datetime

os.environ["IEX_API_KEY"] = 'pk_e4925667bcc14b549bb491a2dc08836a'

data = pd.read_html('https://en.wikipedia.org/wiki/List_of_S%26P_500_companies')
# pprint(data)
table = data[0]
pprint(table)
tickers = table[1:]['Symbol'].tolist()
# pd.Series(tickers).to_csv("spy/tickers.csv")

from concurrent import futures

end = datetime.now()
start = datetime(end.year - 5, end.month , end.day)
bad = []

def download(ticker):
    df = web.DataReader(ticker,'iex', start, end)
    df.to_csv(f"spy/{ticker}.csv")




if __name__ == "__main__":
    main()