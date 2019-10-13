import datetime
import json, csv
import re
import fractions
import loguru
import requests
import os, io
import ctypes
import camelot
import camelot.ext.ghostscript._gsprint

class AfterHoursInfo:
    def __init__(
        self,
        code,
        name,
        totalShare,
        totalTurnover,
        openPrice,
        highestPrice,
        lowestPrice,
        closePrice):
        # 代碼
        self.Code = code
        # 名稱
        self.Name = name
        # 成交股數
        self.TotalShare = self.checkNumber(totalShare)
        if self.TotalShare is not None:
            self.TotalShare = int(totalShare)
        # 成交金額
        self.TotalTurnover = self.checkNumber(totalTurnover)
        if self.TotalTurnover is not None:
            self.TotalTurnover = int(totalTurnover)
        # 開盤價
        self.OpenPrice = self.checkNumber(openPrice)
        if self.OpenPrice is not None:
            self.OpenPrice = fractions.Fraction(openPrice)
        # 最高價
        self.HighestPrice = self.checkNumber(highestPrice)
        if self.HighestPrice is not None:
            self.HighestPrice = fractions.Fraction(highestPrice)
        # 最低價
        self.LowestPrice = self.checkNumber(lowestPrice)
        if self.LowestPrice is not None:
            self.LowestPrice = fractions.Fraction(lowestPrice)
        # 收盤價
        self.ClosePrice = self.checkNumber(closePrice)
        if self.ClosePrice is not None:
            self.ClosePrice = fractions.Fraction(closePrice)
    # 物件表達式
    def __repr__(self):
        totalShare = self.TotalShare
        if totalShare is not None:
            totalShare = f'{totalShare}'
        totalTurnover = self.TotalTurnover
        if totalTurnover is not None:
            totalTurnover = f'{totalTurnover}'
        openPrice = self.OpenPrice
        if openPrice is not None:
            openPrice = f'{float(openPrice):.2f}'
        highestPrice = self.HighestPrice
        if highestPrice is not None:
            highestPrice = f'{float(highestPrice):.2f}'
        lowestPrice = self.LowestPrice
        if lowestPrice is not None:
            lowestPrice = f'{float(lowestPrice):.2f}'
        closePrice = self.ClosePrice
        if closePrice is not None:
            closePrice = f'{float(closePrice):.2f}'
        return (
            f'class AfterHoursInfo {{ '
            f'Code={self.Code}, '
            f'Name={self.Name}, '
            f'TotalShare={totalShare}, '
            f'TotalTurnover={totalTurnover}, '
            f'OpenPrice={openPrice}, '
            f'HighestPrice={highestPrice}, '
            f'LowestPrice={lowestPrice}, '
            f'ClosePrice={closePrice} '
            f'}}'
        )
    # 檢查數值是否有效
    def checkNumber(self, value):
        if value == '--':
            return None
        else:
            return value

class AfterHoursDailyInfo:
    def __init__(
        self,
        code,
        date,
        totalShare,
        totalTurnover,
        openPrice,
        highestPrice,
        lowestPrice,
        closePrice):
        # 代碼
        self.Code = code
        # 日期
        # 國曆年轉為西元年
        parts = date.split('/')
        date = datetime.date(int(parts[0]) + 1911, int(parts[1]), int(parts[2]))
        self.Date = date
        # 成交股數
        self.TotalShare = int(totalShare)
        # 成交金額
        self.TotalTurnover = int(totalTurnover)
        # 開盤價
        self.OpenPrice = fractions.Fraction(openPrice)
        # 最高價
        self.HighestPrice = fractions.Fraction(highestPrice)
        # 最低價
        self.LowestPrice = fractions.Fraction(lowestPrice)
        # 收盤價
        self.ClosePrice = fractions.Fraction(closePrice)
    # 物件表達式
    def __repr__(self):
        return (
            f'class AfterHoursDailyInfo {{ '
            f'Code={self.Code}, '
            f'Date={self.Date:%Y-%m-%d}, '
            f'TotalShare={self.TotalShare}, '
            f'TotalTurnover={self.TotalTurnover}, '
            f'OpenPrice={float(self.OpenPrice):.2f}, '
            f'HighestPrice={float(self.HighestPrice):.2f}, '
            f'LowestPrice={float(self.LowestPrice):.2f}, '
            f'ClosePrice={float(self.ClosePrice):.2f} '
            f'}}'
        )

def download_last_info():
    # 下載當日個股盤後資訊
    resp = requests.get(
        f'https://www.twse.com.tw/exchangeReport/MI_INDEX?' +
        f'response=json&' +
        f'type=ALLBUT0999' +
        # f'&date=20191001'
        f'&date={datetime.date.today():%Y%m%d}'
    )
    if resp.status_code != 200:
        loguru.logger.error('RESP: status code is not 200')
    loguru.logger.success('RESP: success')

    # 盤後資訊清單
    after_hours_infos = []

    # 取出 JSON 內容
    body = resp.json()
    # 取出 stat 欄位
    stat = body['stat']
    # 如果 stat 不是 OK，代表查詢日期尚無資料
    if stat != 'OK':
        loguru.logger.error(f'RESP: body.stat error is {stat}.')
        return
    # 取出第 9 表格內容
    records = body['data9']
    # 依序取出每筆盤後資訊
    for record in records:
        # 取出代碼欄位值
        code = record[0].strip()
        # 符合股票代碼規則才處理
        if re.match(r'^[1-9][0-9][0-9][0-9]$', code) is not None:
            # 取出名稱欄位值
            name = record[1].strip()
            # 取出成交股數欄位值
            total_share = record[2].replace(',', '').strip()
            # 取出成交金額欄位值
            total_turnover = record[4].replace(',', '').strip()
            # 取出開盤價欄位值
            open_price = record[5].replace(',', '').strip()
            # 取出最高價欄位值
            highest_price = record[6].replace(',', '').strip()
            # 取出最低價欄位值
            lowest_price = record[7].replace(',', '').strip()
            # 取出收盤價欄位值
            close_price = record[8].replace(',', '').strip()
            after_hours_info = AfterHoursInfo(
                code=code,
                name=name,
                totalShare=total_share,
                totalTurnover=total_turnover,
                openPrice=open_price,
                highestPrice=highest_price,
                lowestPrice=lowest_price,
                closePrice=close_price
            )
            after_hours_infos.append(after_hours_info)
    # 將每筆物件表達式輸出的字串以系統換行符號相接，讓每筆物件表達式各自獨立一行
    message = os.linesep.join([str(info) for info in after_hours_infos])
    loguru.logger.info('AFTER HOURS INFOS' + os.linesep + message)

def download_daily_info_by_code(year, month, code):
    date = f'{year}{month:02}01'
    resp = requests.get(
        f'https://www.twse.com.tw/exchangeReport/STOCK_DAY?' +
        f'response=csv&date={date}&stockNo={code}')
    if resp.status_code != 200:
        loguru.logger.error('RESP: status code is not 200')
    loguru.logger.success('RESP: success')

    # 個股每月各交易日盤後資訊清單
    after_hours_daily_infos = []
    # 取出 CSV 內容，並去除第一行及最後 5 行
    lines = io.StringIO(resp.text).readlines()
    lines = lines[1:-5]
    # 透過 CSV 讀取器載入
    reader = csv.DictReader(io.StringIO('\n'.join(lines)))
    # 依序取出每筆資料行
    for row in reader:
        # 取出日期欄位值
        date = row['日期'].strip()
        # 取出成交股數欄位值
        total_share = row['成交股數'].replace(',', '').strip()
        # 取出成交金額欄位值
        total_turnover = row['成交金額'].replace(',', '').strip()
        # 取出開盤價欄位值
        open_price = row['開盤價']
        # 取出最高價欄位值
        highest_price = row['最高價']
        # 取出最低價欄位值
        lowest_price = row['最低價']
        # 取出收盤價欄位值
        close_price = row['收盤價']
        after_hours_daily_info = AfterHoursDailyInfo(
            code=code,
            date=date,
            totalShare=total_share,
            totalTurnover=total_turnover,
            openPrice=open_price,
            highestPrice=highest_price,
            lowestPrice=lowest_price,
            closePrice=close_price
        )
        after_hours_daily_infos.append(after_hours_daily_info)
    # 將每筆物件表達式輸出的字串以系統換行符號相接，讓每筆物件表達式各自獨立一行
    message = os.linesep.join([str(info) for info in after_hours_daily_infos])
    loguru.logger.info('AFTER HOURS DAILY INFOS' + os.linesep + message)

def get_stock_basic_info(code):
    resp = requests.get(f'https://www.twse.com.tw/pdf/ch/{code}_ch.pdf')
    if resp.status_code != 200:
        loguru.logger.error('RESP: status code is not 200')
    loguru.logger.success('RESP: success')

    filename = f'{code}.pdf'
    filepath = f'stock_infos/{filename}'

    with open(filepath, 'wb') as f:
        f.write(resp.content)

    # 透過 camelot 辨識出 PDF 檔案內的表格
    tables = camelot.read_pdf(filepath)
    loguru.logger.info('DataFrame' + os.linesep + repr(tables[0].df))

    # 取出第 1 表格的 DataFrame 中的實收資本額
    paidin = tables[0].df[0][3]
    paidin.replace('新台幣', '').replace(',', '').strip()
    loguru.logger.info(f'實收資本額 {paidin}')

if __name__ == '__main__':
    loguru.logger.add(
        f'log/{datetime.date.today():%Y%m%d}.log',
        rotation='1 day',
        retention='7 days',
        level='DEBUG'
    )
    download_last_info()
    download_daily_info_by_code(2019, 10, 3045)
    get_stock_basic_info(3045)
    