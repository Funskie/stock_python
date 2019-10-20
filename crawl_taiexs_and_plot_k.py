import csv
import datetime
import fractions
import json
import os
import random
import re
import time
import urllib.parse

import loguru
import pandas
import plotly.graph_objects
from bs4 import BeautifulSoup
import requests
import requests.exceptions

import proxies_utils

class Taiex:
    def __init__(self, date, open_price, highest_price, lowest_price, close_price):
        # 日期
        self.Date = date
        # 開盤價
        self.OpenPrice = open_price
        # 最高價
        self.HighestPrice = highest_price
        # 最低價
        self.LowestPrice = lowest_price
        # 收盤價
        self.ClosePrice = close_price
    # 物件表達式
    def __repr__(self):
        return f'class Taiex {{ Date={self.Date}, OpenPrice={self.OpenPrice}, HighestPrice={self.HighestPrice}, LowestPrice={self.LowestPrice}, ClosePrice={self.ClosePrice} }}'
    
# 取得指定年月內每交易日的盤後資訊
def getTaiexs(year, month):
    proxy = proxies_utils.proxy
    taiexs = []
    while True:
        if proxy is None:
            proxy = proxies_utils.get_one_proxy()
        url = f'https://www.twse.com.tw/indicesReport/MI_5MINS_HIST?response=json&date={year}{month:02}01'
        loguru.logger.info(f'getTaiexs: month {month} url is {url}')
        loguru.logger.warning(f'getTaiexs: month {month} is downloading...')
        try:
            response = requests.get(
                url,
                proxies={
                    'https': f'https://{proxy}'
                },
                timeout=3
            )
            if response.status_code != 200:
                loguru.logger.success(f'getTaiexs: month {month} status code is not 200.')
                proxy = None
                break
            loguru.logger.success(f'getTaiexs: month {month} is downloaded.')
            body = response.json()
            stat = body['stat']
            if stat != 'OK':
                loguru.logger.error(f'getTaiexs: month {month} responses with error({stat}).')
                break
            records = body['data']
            if len(records) == 0:
                loguru.logger.success(f'getTaiexs: month {month} has no data.')
                break
            for record in records:
                date = record[0].strip()
                parts = date.split('/')
                y = int(parts[0]) + 1911
                m = int(parts[1])
                d = int(parts[2])
                date = f'{y}{m:02d}{d:02d}'
                open_price = record[1].replace(',', '').strip()
                highest_price = record[2].replace(',', '').strip()
                lowest_price = record[3].replace(',', '').strip()
                close_price = record[4].replace(',', '').strip()
                taiex = Taiex(
                    date=date,
                    open_price=open_price,
                    highest_price=highest_price,
                    lowest_price=lowest_price,
                    close_price=close_price
                )
                taiexs.append(taiex)
        except requests.exceptions.ConnectionError:
            loguru.logger.error(f'getTaiexs: proxy({proxy}) is not working (connection error).')
            proxy = None
            continue
        except requests.exceptions.ConnectTimeout:
            loguru.logger.error(f'getTaiexs: proxy({proxy}) is not working (connect timeout).')
            proxy = None
            continue
        except requests.exceptions.ProxyError:
            loguru.logger.error(f'getTaiexs: proxy({proxy}) is not working (proxy error).')
            proxy = None
            continue
        except requests.exceptions.SSLError:
            loguru.logger.error(f'getTaiexs: proxy({proxy}) is not working (ssl error).')
            proxy = None
            continue
        except Exception as e:
            loguru.logger.error(f'getTaiexs: proxy({proxy}) is not working.')
            loguru.logger.error(e)
            proxy = None
            continue
        break
    return taiexs

# 儲存傳入的盤後資訊
def saveTaiexs(filepath, taiexs):
    loguru.logger.info(f'saveTaiexs: {len(taiexs)} taiexs.')
    loguru.logger.warning(f'saveTaiexs: {filepath} is saving...')
    with open(filepath, mode='w', newline='', encoding='utf-8-sig') as f:
        writer = csv.writer(f)
        writer.writerow([
            'Date',
            'OpenPrice',
            'HighestPrice',
            'LowestPrice',
            'ClosePrice'
        ])
        for taiex in taiexs:
            writer.writerow([
                taiex.Date,
                taiex.OpenPrice,
                taiex.HighestPrice,
                taiex.LowestPrice,
                taiex.ClosePrice
            ])
    loguru.logger.success(f'main: {filepath} is saved.')

def main():
    taiexs = []
    # 取得從 2019.01 至 2019.12 的盤後資訊
    for month in range(1, 13):
        taiexs = taiexs + getTaiexs(2019, month)
    filepath = f'taiexs-2019.csv'
    saveTaiexs(filepath, taiexs)

    # 使用 Pandas 讀取下載回來的紀錄檔
    df = pandas.read_csv(filepath)
    # 將 Date 欄位按照格式轉換為 datetime 資料
    df['Date'] = pandas.to_datetime(df['Date'], format='%Y%m%d')
    
    # 建立圖表
    figure = plotly.graph_objects.Figure(
        data=[
            # Line Chart
            # 收盤價
            plotly.graph_objects.Scatter(
                x=df['Date'],
                y=df['ClosePrice'],
                name='收盤價',
                mode='lines',
                line=plotly.graph_objects.scatter.Line(
                    color='#6B99E5'
                )
            ),
            # Candlestick Chart
            # K 棒
            plotly.graph_objects.Candlestick(
                x=df['Date'],
                open=df['OpenPrice'],
                high=df['HighestPrice'],
                low=df['LowestPrice'],
                close=df['ClosePrice'],
                name='盤後資訊',
            )
        ],
        # 設定 XY 顯示格式
        layout=plotly.graph_objects.Layout(
            xaxis=plotly.graph_objects.layout.XAxis(
                tickformat='%Y-%m'
            ),
            yaxis=plotly.graph_objects.layout.YAxis(
                tickformat='.2f'
            )
        )
    )
    figure.show()

if __name__ == '__main__':
    loguru.logger.add(
        f'./log/{datetime.date.today():%Y%m%d}.log',
        rotation='1 day',
        retention='7 days',
        level='DEBUG'
    )
    main()
