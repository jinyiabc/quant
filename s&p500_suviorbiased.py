import os
import re
import time
from datetime import datetime
from pprint import pprint

import matplotlib
import pandas as pd
import pandas_datareader.data as web
import matplotlib.pyplot as plt
# import matplotlib
# matplotlib.use('TKAgg')

os.environ["IEX_API_KEY"] = 'pk_e4925667bcc14b549bb491a2dc08836a'
def main():
    df = pd.read_csv('spy/tickers.csv')
    skip = pd.read_csv('spy/skips.csv')
    # Convert to list
    skip = [ticker for ticker in skip['ticker']]
    tickers = [ticker for ticker in df['ticker'] if ticker not in skip]
    sim_rsp = (
        (pd.concat(
            [pd.read_csv(f'spy/{ticker}.csv', index_col='date', parse_dates=True)[
                 'close'
             ].pct_change()
             for ticker in tickers],
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
    sim_rsp.plot(legend=True, title="RSP vs. Survivorship-Biased Strategy", figsize=(12, 9))
    rsp.plot(legend=True)
    plt.show()

def quandl_data(wiki, ticker, start, end):
    if ticker in wiki:
        df = wiki[ticker][start:end]
    else:
        ticker = fix_ticker(ticker)
        if ticker in wiki:
            df = wiki[ticker][start:end]
        else:
            return None
    df = df.drop(
        [
            "open",
            "high",
            "low",
            "close",
            "volume",
            "ex-dividend",
            "split_ratio",
            "ticker",
        ],
        axis=1,
    )
    df = df.rename(
        index=str,
        columns={
            "adj_open": "open",
            "adj_high": "high",
            "adj_low": "low",
            "adj_close": "close",
            "adj_volume": "volume",
        },
    )
    return df


def yahoo_data(ticker, start, end):
    ticker = fix_ticker(ticker)
    try:
        df = web.DataReader(ticker, "yahoo", start, end)
    except:
        time.sleep(1)
        try:
            df = web.DataReader(ticker, "yahoo", start, end)
        except:
            return None
    # adjust ohlc using adj close
    adjfactor = df["Close"] / df["Adj Close"]
    df["Open"] /= adjfactor
    df["High"] /= adjfactor
    df["Low"] /= adjfactor
    df["Close"] = df["Adj Close"]
    df["Volume"] *= adjfactor
    df = df.drop(["Adj Close"], axis=1)
    df = df.rename(str.lower, axis="columns")
    df.index.rename("date", inplace=True)
    return df


def fix_ticker(ticker):
    rename_table = {
        "-": "LPRAX",  # BlackRock LifePath Dynamic Retirement Fund
        "8686": "AFL",  # AFLAC
        "4XS": "ESRX",  # Express Scripts Holding Company
        "AAZ": "APC",  # Anadarko Petroleum Corporation
        "AG4": "AGN",  # Allergan plc
        "BFB": "BF_B",  # Brown-Forman Corporation
        "BF.B": "BF_B",  # Brown-Forman Corporation
        "BF/B": "BF_B",  # Brown-Forman Corporation
        "BLD WI": "BLD",  # TopBuild Corp.
        "BRKB": "BRK_B",  # Berkshire Hathaway Inc.
        "CC WI": "CC",  # The Chemours Company
        "DC7": "DFS",  # Discover Financial Services
        "GGQ7": "GOOG",  # Alphabet Inc. Class C
        "HNZ": "KHC",  # The Kraft Heinz Company
        "LOM": "LMT",  # Lockheed Martin Corp.
        "LTD": "LB",  # L Brands Inc.
        "LTR": "L",  # Loews Corporation
        "MPN": "MPC",  # Marathon Petroleum Corp.
        "MWZ": "MET",  # Metlife Inc.
        "MX4A": "CME",  # CME Group Inc.
        "NCRA": "NWSA",  # News Corporation
        "NTH": "NOC",  # Northrop Grumman Crop.
        "PA9": "TRV",  # The Travelers Companies, Inc.
        "QCI": "QCOM",  # Qualcomm Inc.
        "RN7": "RF",  # Regions Financial Corp
        "SLBA": "SLB",  # Schlumberger Limited
        "SYF-W": "SYF",  # Synchrony Financial
        "SWG": "SCHW",  # The Charles Schwab Corporation
        "UAC/C": "UAA",  # Under Armour Inc Class A
        "UBSFT": "UBSFY",  # Ubisoft Entertainment
        "USX1": "X",  # United States Steel Corporation
        "UUM": "UNM",  # Unum Group
        "VISA": "V",  # Visa Inc
    }
    if ticker in rename_table:
        fix = rename_table[ticker]
    else:
        fix = re.sub(r"[^A-Z]+", "", ticker)
    return fix

def main1():
    from concurrent import futures

    end = datetime.strptime('2018-03-27', "%Y-%m-%d")
    start = datetime.strptime('2013-03-28', "%Y-%m-%d")
    bad = []
    end = str(end.date())
    start = str(start.date())
    wiki = pd.read_csv('survivorship-free/WIKI_PRICES.csv', parse_dates=True)
    wiki = dict(tuple(wiki.groupby("ticker")))
    for ticker in wiki:
        wiki[ticker].set_index("date", inplace=True)


    tickers = pd.read_csv('spy/tickers.csv')
    tickers = [ticker for ticker in tickers['ticker']]
    data = {}
    skips = set()

    for company in tickers:
        if company in skips:
            continue
        df = quandl_data(wiki, company, start, end)
        if df is None:
            df = yahoo_data(company, start, end)
        if df is None:
            skips.add(company)
            continue
        df.to_csv(f"spy/{company}.csv")
        # if company in data:
        #     data[company] = data[company].append(df)
        # else:
        #     data[company] = df
    pd.DataFrame(skips).to_csv('spy/skips.csv', header=['ticker'])

if __name__ == "__main__":
    main()
