import requests
from io import StringIO
import pandas as pd
import numpy as np
from tqdm import tqdm
import os
import pickle
import datetime

def afterIFRS(year, season):
    season2date = [ datetime.datetime(year, 5, 15),
                    datetime.datetime(year, 8, 14),
                    datetime.datetime(year, 11, 14),
                    datetime.datetime(year+1, 3, 31)]

    return pd.to_datetime(season2date[season-1].date())

def clean(year, season, balance_sheet):
    
    if len(balance_sheet) == 0:
        print('**WARRN: no data to parse')
        return balance_sheet
    balance_sheet = balance_sheet.transpose().reset_index().rename(columns={'index':'stock_id'})
    s = balance_sheet['會計項目']
    balance_sheet = balance_sheet.drop('會計項目', axis=1).apply(pd.to_numeric)
    balance_sheet['會計項目'] = s.astype(str)
    balance_sheet['date'] = afterIFRS(year, season)
    
    balance_sheet['stock_id'] = balance_sheet['stock_id'].astype(str)
    balance = balance_sheet.set_index(['stock_id', 'date'])
    return balance

def crawl_finance_statement(year, season, stock_ids):

    directory = os.path.join('data', 'financial_statement', str(year) + str(season))
    if not os.path.exists(directory):
        os.makedirs(directory)

    def download_html(year, season, stock_ids, report_type='C'):
    
        headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36'}
        sess = requests.Session()
        pbar = tqdm(stock_ids)
        for i in pbar:

            # check if the html is already parsed
            file = os.path.join(directory, str(i) + '.html')
            if os.path.exists(file) and os.stat(file).st_size > 10000:
                continue
            
            pbar.set_description('parse htmls %d season %d stock %s' % (year, season, str(i)))
                        
            # start parsing
            url = ('http://mops.twse.com.tw/server-java/t164sb01?step=1&CO_ID='
                   + i + '&SYEAR=' + str(year) + '&SSEASON='+str(season)+'&REPORT_ID=' + str(report_type))
            
            try:
                r = sess.get(url, headers=headers)
            except:
                print('**WARRN: requests cannot get stock', i, '.html')
                continue
                
            r.encoding = 'big5'
            
            # write files
            f = open(file, 'w', encoding='utf-8')

            f.write('<meta charset="UTF-8">\n')
            f.write(r.text)
            f.close()
            
            # finish
            # print(percentage, i, 'end')
            
            # sleep a while
            time.sleep(25 + random.uniform(0, 10))

    download_html(year, season, stock_ids, 'C')
    download_html(year, season, stock_ids, 'A')
    download_html(year, season, stock_ids, 'B')
    download_html(year, season, stock_ids, 'C')
    download_html(year, season, stock_ids, 'A')
    download_html(year, season, stock_ids, 'B')

def pack_htmls(year, season, directory):
    balance_sheet = {}
    income_sheet = {}
    cash_flows = {}
    income_sheet_cumulate = {}
    pbar = tqdm(os.listdir(directory))

    for i in pbar:

        # 將檔案路徑建立好
        file = os.path.join(directory, i) 
        
        # 假如檔案不是html結尾，或是太小，代表不是正常的檔案，略過
        if file[-4:] != 'html' or os.stat(file).st_size < 10000:
            continue
        
        # 顯示目前運行的狀況
        stock_id = i.split('.')[0]
        pbar.set_description('parse htmls %d season %d stock %s' % (year, season, stock_id))
        
        # 讀取html
        dfs = pd.read_html(file)
        
        # 假如html不完整，則略過
        if len(dfs) < 4:
            print('**WARRN html file broken', year, season, i)
            continue
        
        # 取得 balance sheet
        df = dfs[1].copy().drop_duplicates(subset=0, keep='last')
        df = df.set_index(0)
        balance_sheet[stock_id] = df[1].dropna()
        #balance_sheet = combine(balance_sheet, df[1].dropna(), stock_id)

        # 取得 income statement
        df = dfs[2].copy().drop_duplicates(subset=0, keep='last')
        df = df.set_index(0)
        
        # 假如有4個columns，則第1與第3條column是單季跟累計的income statement
        if len(df.columns) == 4:
            income_sheet[stock_id] = df[1].dropna()
            income_sheet_cumulate[stock_id] = df[3].dropna()
        # 假如有2個columns，則代表第3條column為累計的income statement，單季的從缺
        elif len(df.columns) == 2:
            income_sheet_cumulate[stock_id] = df[1].dropna()
            
            # 假如是第一季財報 累計 跟單季 的數值是一樣的
            if season == 1:
                income_sheet[stock_id] = df[1].dropna()

        # 取得 cash_flows
        df = dfs[3].copy().drop_duplicates(subset=0, keep='last')
        df = df.set_index(0)
        cash_flows[stock_id] = df[1].dropna()
    
    # 將dictionary整理成dataframe
    balance_sheet = pd.DataFrame(balance_sheet)
    income_sheet = pd.DataFrame(income_sheet)
    income_sheet_cumulate = pd.DataFrame(income_sheet_cumulate)
    cash_flows = pd.DataFrame(cash_flows)
    
    # 做清理
    ret = {'balance_sheet':clean(year, season, balance_sheet), 'income_sheet':clean(year, season, income_sheet), 
            'income_sheet_cumulate':clean(year, season, income_sheet_cumulate), 'cash_flows':clean(year, season, cash_flows)}
    
    # 假如是第一季的話，則 單季 跟 累計 是一樣的
    if season == 1:
        ret['income_sheet'] = ret['income_sheet_cumulate'].copy()

    ret['income_sheet_cumulate'].columns = '累計' + ret['income_sheet_cumulate'].columns
    
    pickle.dump(ret, open('data/financial_statement/pack' + str(year) + str(season) + '.pickle', 'wb'))
    
    return ret

def get_all_pickles(directory):
    ret = {}
    for i in os.listdir(directory):
        if i[:4] != 'pack':
            continue
        ret[i[4:9]] = pickle.load(open(os.path.join(directory, i), 'rb'))
    return ret

def combine(d):

    tnames = ['balance_sheet',
            'cash_flows',
            'income_sheet',
            'income_sheet_cumulate']

    tbs = {t:pd.DataFrame() for t in tnames}

    for i, dfs in d.items():
        for tname in tnames:
            tbs[tname] = tbs[tname].append(dfs[tname])
    return tbs

   
def fill_season4(tbs):
    # copy income sheet (will modify it later)
    income_sheet = tbs['income_sheet'].copy()
    
    # calculate the overlap columns
    c1 = set(tbs['income_sheet'].columns)
    c2 = set(tbs['income_sheet_cumulate'].columns)
    
    overlap_columns = []
    for i in c1:
        if '累計' + i in c2:
            overlap_columns.append('累計' + i)

    # get all years
    years = set(tbs['income_sheet_cumulate'].index.levels[1].year)
    
    for y in years:
        
        # get rows of the dataframe that is season 4
        ys = tbs['income_sheet_cumulate'].reset_index('stock_id').index.year == y
        ds4 = tbs['income_sheet_cumulate'].reset_index('stock_id').index.month == 3
        df4 = tbs['income_sheet_cumulate'][ds4 & ys].apply(lambda s: pd.to_numeric(s, errors='corece')).reset_index('date')
        
        # get rows of the dataframe that is season 3
        yps = tbs['income_sheet_cumulate'].reset_index('stock_id').index.year == y - 1
        ds3 = tbs['income_sheet_cumulate'].reset_index('stock_id').index.month == 11
        df3 = tbs['income_sheet_cumulate'][ds3 & yps].apply(lambda s: pd.to_numeric(s, errors='corece')).reset_index('date')
        
        # calculate the differences of income_sheet_cumulate to get income_sheet single season
        diff = df4 - df3
        diff = diff.drop(['date'], axis=1)[overlap_columns]
        
        # remove 累計
        diff.columns = diff.columns.str[2:]
        
        # 加上第四季的日期
        diff['date'] = pd.to_datetime(str(y) + '-03-31')
        diff = diff[list(c1) + ['date']].reset_index().set_index(['stock_id','date'])
        
        # 新增資料於income_sheet尾部
        income_sheet = income_sheet.append(diff)
        
    # 排序好並更新tbs
    income_sheet = income_sheet.reset_index().sort_values(['stock_id', 'date']).set_index(['stock_id', 'date'])
    tbs['income_sheet'] = income_sheet

def to_db(tbs):
    import sqlite3
    print('save table to db')
    conn = sqlite3.connect(os.path.join('data', 'data.db'))
    for i, df in tbs.items():
        print('  ', i)
        df = df.reset_index().sort_values(['stock_id', 'date']).drop_duplicates(['stock_id', 'date']).set_index(['stock_id', 'date'])
        df[df.count().nlargest(900).index].to_sql(i, conn, if_exists='replace')
        
        
def html2db(date):
    year = date.year
    if date.month == 3:
        season = 4
        year = year - 1
        month = 11
    elif date.month == 5:
        season = 1
        month = 2
    elif date.month == 8:
        season = 2
        month = 5
    elif date.month == 11:
        season = 3
        month = 8
    else:
        return None
    
    pack_htmls(year, season, os.path.join('data', 'financial_statement', str(year) + str(season)))
    d = get_all_pickles(os.path.join('data', 'financial_statement'))
    tbs = combine(d)
    fill_season4(tbs)
    to_db(tbs)
    return {}
    
