import os
from datetime import datetime
from pprint import pprint

import matplotlib
import pandas as pd
import pandas_datareader.data as web
import matplotlib.pyplot as plt
import matplotlib
matplotlib.use('TKAgg')

def main():
    df = pd.read_csv('survivorship-free/tickers.csv')

    sim_rsp = (
        (pd.concat(
            [pd.read_csv(f'C:\work\quant\survivorship-free\data\{ticker}.csv', index_col='date', parse_dates=True)[
                 'close'
             ].pct_change()
             for ticker in df['ticker']],
            axis=1,
            sort=True,
        ).mean(axis=1, skipna=True) + 1)
            .cumprod()
            .rename("SIM")
    )
    pprint(sim_rsp)

    # df = web.DataReader("RSP", 'yahoo', '2013-03-27', '2018-02-27')
    # pprint(df)
    rsp = (
        (web.DataReader("RSP", 'yahoo', sim_rsp.index[0], sim_rsp.index[-1])[
             "Adj Close"
         ].pct_change() + 0.002 / 252 + 1)  # 0.20% annual ER
            .cumprod()
            .rename("RSP")
    )
    pprint(rsp)
    sim_rsp.plot(legend=True, title="RSP vs. Un-Survivorship-Biased Strategy", figsize=(12, 9))
    rsp.plot(legend=True)
    plt.show()

if __name__ == "__main__":
    main()
