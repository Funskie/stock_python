import sqlite3
import pandas as pd
import os
import datetime

class Data():
    
    def __init__(self):
        self.conn = sqlite3.connect(os.path.join('data', "data.db"))
        cursor = self.conn.execute('SELECT name FROM sqlite_master WHERE type = "table"')
        table_names = [t[0] for t in list(cursor)]
        self.col2table = {}
        for tname in table_names:
            c = self.conn.execute('PRAGMA table_info(' + tname + ');')
            for cname in [i[1] for i in list(c)]:
                self.col2table[cname] = tname
                
        self.date = datetime.datetime.now().date()
        
        # for cache mode
        self.cache = False
        self.data = {}
        
        # for dates
        #print('Data: pre-loading dates...')
        self.dates = {}
        for tname in table_names:
            c = self.conn.execute('PRAGMA table_info(' + tname + ');')
            cnames = [i[1] for i in list(c)]
            if 'date' in cnames:
                if tname == 'price':
                    s1 = ("""SELECT DISTINCT date FROM %s where stock_id='0050'"""%('price'))
                    s2 = ("""SELECT DISTINCT date FROM %s where stock_id='1101'"""%('price'))
                    s3 = ("""SELECT DISTINCT date FROM %s where stock_id='2330'"""%('price'))
                    df = (pd.read_sql(s1, self.conn)
                          .append(pd.read_sql(s2, self.conn))
                          .append(pd.read_sql(s3, self.conn))
                          .drop_duplicates('date').sort_values('date'))
                    df['date'] = pd.to_datetime(df['date'])
                    df = df.set_index('date')
                    self.dates[tname] = df
                else:
                    s = ("""SELECT DISTINCT date FROM '%s'"""%(tname))
                    self.dates[tname] = pd.read_sql(s, self.conn, parse_dates=['date'], index_col=['date']).sort_index()
        #print('Data: done')
        
        
    def get(self, name, n):
        
        if name not in self.col2table or n == 0:
            print('Data: **ERROR: cannot find', name, 'in database')
            return pd.DataFrame()
        
        df = self.dates[self.col2table[name]].loc[:self.date].iloc[-n:]
        try:
            startdate = df.index[-1]
            enddate = df.index[0]
        except:
            print('Data: **WARRN: data cannot be retrieve completely:', name)
            enddate = df.iloc[0]
        
        # use cache
        if name in self.data and self.contain_date(name, enddate, startdate):
            return self.data[name][enddate:startdate]
        
        # obtain from database
        s = ("""SELECT stock_id, date, [%s] FROM %s WHERE date BETWEEN '%s' AND '%s'"""%(name, 
            self.col2table[name], str(enddate.strftime('%Y-%m-%d')), 
            str((self.date + datetime.timedelta(days=1)).strftime('%Y-%m-%d'))))
        ret = pd.read_sql(s, self.conn, parse_dates=['date']).pivot(index='date', columns='stock_id')[name]
        
        # save the cache
        if self.cache:
            self.data[name] = ret

        return ret
    
    def contain_date(self, name, startdate, enddate):
        #print(startdate, enddate)
        if name not in self.data:
            return False
        if self.data[name].index[0] <= startdate <= enddate <= self.data[name].index[-1]:
            return True
        
        return False
        
    def get3(self, name):
        s = ("""SELECT stock_id, %s FROM %s """%(name, self.col2table[name]))
        return pd.read_sql(s, self.conn, index_col=['stock_id'])