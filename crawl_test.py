# -*- coding: utf-8 -*
import datetime, time
import fractions
import os
import chardet
import loguru
import requests

class StockMarketCapPercent:
    def __init__(self, sort, code, name, percent):
        # 排序
        self.Sort = int(sort)
        # 證券代碼
        self.Code = code
        # 證券名稱
        self.Name = name
        # 市值佔比
        self.Percent = fractions.Fraction(percent[:-1])
    def __repr__(self):
        return (
            f'class StockMarketCapPercent {{ '
            f'Sort={self.Sort}, '
            f'Code={self.Code}, '
            f'Name={self.Name}, '
            f'Percent={float(self.Percent):.4f}% '
            f'}}'
        )    

def main():
    resp = requests.get('https://www.taifex.com.tw/cht/9/futuresQADetail')
    if resp.status_code != 200:
        loguru.logger.error('RESP: status code is not 200')
    loguru.logger.success('RESP: success')

    txt = None
    # 對 HTTP / HTTPS 回應的二進位原始內容進行編碼判斷
    det = chardet.detect(resp.content)
    # 捕捉編碼轉換例外錯誤
    try:
        # 若判斷結果信心度超過 0.5
        if det['confidence'] > 0.5:
            # 若編碼判斷是 BIG5
            if det['encoding'] == 'big-5':
                # 因 Python 的 BIG5 編碼標示為 big5，
                # 而非 chardet 回傳的 big-5，故需另外處理
                txt = resp.content.decode('big5')
            else:
                txt = resp.content.decode(det['encoding'])
        else:
            # 若判斷信心度不足，則嘗試使用 UTF-8 解碼
            txt = resp.content.decode('utf-8')
    except Exception as e:
        # 解碼失敗
        loguru.logger.error(e)

    # 解碼失敗無法取得有效文字內容資料
    if txt is None:
        return
    # loguru.logger.info(txt)

    # 成分股市值佔比清單
    stock_markert_cap_list = []

    # 將下載回來的內容解析為 PyQuery 物件
    d = pyquery.PyQuery(txt)
    # 透過 CSS 選擇器取出所有表格行
    trs = list(d('table tr').items())
    # 去除標頭行（分析結果 1.）
    trs = trs[1:]
    # 依序取出資料行
    for tr in trs:
        # 取出所有資料格
        tds = list(tr('td').items())
        #
        # 取出資料行中第一組證券內容（分析結果 2.）
        #
        # 取出證券代碼欄位值
        code = tds[1].text().strip()
        # 若證券代碼欄位值存在資料，代表本筆資料存在，則繼續取出其他欄位
        if code != '':
            # 取出排序欄位值
            sort = tds[0].text().strip()
            # 取出證券名稱欄位值
            name = tds[2].text().strip()
            # 取出市值佔比欄位值
            percent = tds[3].text().strip()
            # 將取得資料存入成分股市值佔比清單
            stock_markert_cap_list.append(StockMarketCapPercent(
                sort=sort,
                code=code,
                name=name,
                percent=percent
            ))
        #
        # 取出資料行中第二組證券內容（分析結果 2.）
        #
        # 取出證券代碼欄位值
        code = tds[5].text().strip()
        # 若證券代碼欄位值存在資料，代表本筆資料存在，則繼續取出其他欄位
        if code != '':
            # 取出排序欄位值
            sort = tds[4].text().strip()
            # 取出證券名稱欄位值
            name = tds[6].text().strip()
            # 取出市值佔比欄位值
            percent = tds[7].text().strip()
            # 將取得資料存入成分股市值佔比清單
            stock_markert_cap_list.append(StockMarketCapPercent(
                sort=sort,
                code=code,
                name=name,
                percent=percent
            ))

    # 按證券代碼順序重新排列資列並輸出（分析結果 3.）
    stock_markert_cap_list.sort(key=lambda i: i.Code)
    # loguru.logger.info(stock_markert_cap_list)
    # 將每筆物件表達式輸出的字串以系統換行符號相接，讓每筆物件表達式各自獨立一行
    message = os.linesep.join([str(i) for i in stock_markert_cap_list])
    loguru.logger.info('Stock market cap percent' + os.linesep + message)

if __name__ == '__main__':
    loguru.logger.add(
        f"log/{datetime.date.today().strftime('%Y%m%d')}.log", 
        rotation='1 day', 
        retention='7 days', 
        level='DEBUG'
        )
    main()
    