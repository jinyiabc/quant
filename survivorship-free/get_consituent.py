from pprint import pprint

import requests
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
import json
import pandas as pd
import re

# # request page
# html = requests.get("https://www.ishares.com/us/products/239726/#tabsAll").content
# soup = BeautifulSoup(html, features="lxml")
# # find available dates
# holdings = soup.find("div", {"id": "holdings"})
# dates_div = holdings.find_all("div", "component-date-list")[0]
# dates_div.find_all("option")
# dates = [option.attrs["value"] for option in dates_div.find_all("option")]
# pprint(dates)
#
# # download constituents for each date
# constituents = pd.Series(dtype='float64')
# for date in dates:
#     resp = requests.get(
#         f"https://www.ishares.com/us/products/239726/ishares-core-sp-500-etf/1467271812596.ajax?tab=all&fileType=json&asOfDate={date}"
#     ).content[3:]
#
#     tickers = json.loads(resp)
#
#     tickers = [(arr[0], arr[1]) for arr in tickers['aaData']]
#
#     date = datetime.strptime(date, "%Y%m%d")
#     constituents[date] = tickers
#     # pprint(constituents)
#
# pprint(constituents)
# constituents = constituents.iloc[::-1] # reverse into cronlogical order
# pprint(constituents)
# constituents.head()
# constituents.to_csv('constituents.csv')
constituents = pd.read_csv('constituents.csv')
constituents.set_index("date", inplace=True)
constituents.sort_index()
# constituents.head()
# pprint(constituents)

wiki = pd.read_csv("WIKI_PRICES.csv", parse_dates=True)
wiki = dict(tuple(wiki.groupby("ticker")))
for ticker in wiki:
    wiki[ticker].set_index("date", inplace=True)


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


data = {}
skips = set()
import ast

for i in range(0, len(constituents) - 1):
    start = datetime.strptime(constituents.index[i], '%Y-%m-%d')
    end = datetime.strptime(constituents.index[i + 1], '%Y-%m-%d') - timedelta(days=1)
    start = str(start.date())   # '1981-9-27'
    end = str(end.date())
    company_list = constituents.loc[constituents.index[i], 'companys']
    company_list = ast.literal_eval(company_list)

    pprint(company_list)

    for company in company_list:
        pass
        if company in skips:
            continue
        df = quandl_data(wiki, company, start, end)
        # if df is None:
        #     df = yahoo_data(company[0], start, end)
        if df is None:
            skips.add(company)
            continue
        if company in data:
            data[company] = data[company].append(df)
        else:
            data[company] = df
