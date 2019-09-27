import datetime, time
import os

import requests
import requests.exceptions
from bs4 import BeautifulSoup
import urllib.parse

import json, csv
import js2py

import random

import loguru

# 取得模組執行當下時間
now = datetime.datetime.now()
# 透過全域變數共用代理清單
proxies = []
proxy = None

def get_proxies_from_ProxyNova():
    # proxies = []
    # 按照網站規則使用各國代碼傳入網址取得各國 IP 代理
    countries = [
        'tw',
        'jp',
        'kr',
        'id',
        'my',
        'th',
        'vn',
        'ph',
        'hk',
        'uk',
        'us'
    ]
    for country in countries:
        url = f"https://www.proxynova.com/proxy-server-list/country-{country}/"
        loguru.logger.debug(f'getProxiesFromProxyNova: {url}')
        loguru.logger.warning(f'getProxiesFromProxyNova: downloading...')
        response = requests.get(url)
        if response.status_code != 200:
            loguru.logger.debug(f'getProxiesFromProxyNova: status code is not 200')
            continue
        loguru.logger.success(f'getProxiesFromProxyNova: downloaded.')

        soup = BeautifulSoup(response.text)
        table = soup.table
        rows = table.tbody.find_all('tr')
        loguru.logger.warning(f'getProxiesFromProxyNova: scanning...')
        for row in rows:
            try:
                # 取出 IP 欄位內的 JavaScript 程式碼
                js = row.find('script').string
                # 去除 JavaScript 程式碼開頭的 document.write( 字串與結尾的 ); 字串，
                # 再與可供 js2py 執行後回傳指定變數的 JavaScript 程式碼相結合
                js = 'let x = %s; x' % (js.replace('document.write(', '').replace(');', ''))
                # 透過 js2py 執行取得還原後的 IP
                ip = js2py.eval_js(js).strip()
                # 取出 Port 欄位值
                port = row.find_all('td')[1].text.strip()
                # 組合 IP 代理
                proxy = f'{ip}:{port}'
                proxies.append(proxy)
            except Exception as e:
                loguru.logger.warning(e)
        loguru.logger.success(f'getProxiesFromProxyNova: scanned.')
        loguru.logger.debug(f'getProxiesFromProxyNova: {len(proxies)} proxies is found.')
        # 每取得一個國家代理清單就休息一秒，避免頻繁存取導致代理清單網站封鎖
        time.sleep(1)
    return proxies

def get_proxies_from_GatherProxy():
    # proxies = []
    countries = [
        'Taiwan',
        'Japan',
        'United States',
        'Thailand',
        'Vietnam',
        'Indonesia',
        'Singapore',
        'Philippines',
        'Malaysia',
        'Hong Kong'
    ]
    for country in countries:
        url = f"http://www.gatherproxy.com/proxylist/country/?c={urllib.parse.quote(country)}/"
        loguru.logger.debug(f'getProxiesFromGatherProxy: {url}')
        loguru.logger.warning(f'getProxiesFromGatherProxy: downloading...')
        response = requests.get(url)
        if response.status_code != 200:
            loguru.logger.debug(f'getProxiesFromGatherProxy: status code is not 200')
            continue
        loguru.logger.success(f'getProxiesFromGatherProxy: downloaded.')

        soup = BeautifulSoup(response.text)
        table = soup.table
        # 取出 script 標簽中的 JavaScript 原始碼
        rows = table.find_all('script')
        loguru.logger.warning(f'getProxiesFromGatherProxy: scanning...')
        for row in rows:
            # 去除 JavaScript 程式碼開頭的 gp.insertPrx( 字串與結尾的 ); 字串
            script = row.text.strip().replace('gp.insertPrx(', '').replace(');', '')
            # 將參數物件以 JSON 方式解析
            script = json.loads(script)
            # 取出 IP 欄位值
            ip = script['PROXY_IP'].strip()
            # 取出 Port 欄位值，並從 16 進位表示法解析為 10 進位表示法
            port = int(script['PROXY_PORT'].strip(), 16)
            # 組合 IP 代理
            proxy = f'{ip}:{port}'
            proxies.append(proxy)
        loguru.logger.success(f'getProxiesFromGatherProxy: scanned.')
        loguru.logger.debug(f'getProxiesFromGatherProxy: {len(proxies)} proxies is found.')
        # 每取得一個國家代理清單就休息一秒，避免頻繁存取導致代理清單網站封鎖
        time.sleep(1)
    return proxies

def get_proxies_from_FreeProxyList():
    # proxies = []
    url = 'https://free-proxy-list.net/'
    loguru.logger.debug(f'getProxiesFromFreeProxyList: {url}')
    loguru.logger.warning(f'getProxiesFromFreeProxyList: downloading...')
    response = requests.get(url)
    if response.status_code != 200:
        loguru.logger.debug(f'getProxiesFromFreeProxyList: status code is not 200')
        return
    loguru.logger.success(f'getProxiesFromFreeProxyList: downloaded.')
    soup = BeautifulSoup(response.text)
    trs = soup.table.find_all('tr')[1:-1]
    loguru.logger.warning(f'getProxiesFromFreeProxyList: scanning...')
    for tr in trs:
        # 取出所有資料格
        tds = tr.find_all('td')
        # 取出 IP 欄位值
        ip = tds[0].text.strip()
        # 取出 Port 欄位值
        port = tds[1].text.strip()
        # 組合 IP 代理
        proxy = f'{ip}:{port}'
        proxies.append(proxy)
    loguru.logger.success(f'getProxiesFromFreeProxyList: scanned.')
    loguru.logger.debug(f'getProxiesFromFreeProxyList: {len(proxies)} proxies is found.')
    return proxies
# 下載代理清單
def download_proxies(hour):
    global proxies
    proxies = proxies + get_proxies_from_ProxyNova()
    proxies = proxies + get_proxies_from_GatherProxy()
    proxies = proxies + get_proxies_from_FreeProxyList()
    # proxies = list(dict.fromkeys(proxies))
    loguru.logger.debug(f'download_proxies: {len(proxies)} proxies is found.')
# 取得代理清單
def get_proxies():
    global proxies
    now = datetime.datetime.now()
    hour = f'{now:%Y%m%d%H}'
    file_name = f'proxies-{hour}.csv'
    file_path = os.path.join('proxies', file_name)
    if os.path.isfile(file_path):
        # 如果本小時的紀錄檔案存在，直接載入代理清單
        loguru.logger.info(f'getProxies: {file_name} exists.')
        loguru.logger.warning(f'getProxies: {file_name} is loading...')
        with open(file_path, 'r', newline='', encoding='utf-8-sig') as f:
            reader = csv.DictReader(f)
            for row in reader:
                proxy = row['Proxy']
                proxies.append(proxy)
        loguru.logger.success(f'getProxies: {file_name} is loaded.')
    else:
        # 如果本小時的紀錄檔案存在，重新下載代理清單並保存
        loguru.logger.info(f'getProxies: {file_name} does not exist.')
        download_proxies(hour)
        loguru.logger.warning(f'getProxies: {file_name} is saving...')
        with open(file_path, 'w', newline='', encoding='utf-8-sig') as f:
            writer = csv.writer(f)
            writer.writerow([
                'Proxy'
            ])
            for proxy in proxies:
                writer.writerow([
                    proxy
                ])
        loguru.logger.success(f'getProxies: {file_name} is saved.')
# 隨機取出一組代理
def get_one_proxy():
    global proxies
    # 若代理清單內已無代理，則重新下載
    if len(proxies) == 0:
        get_proxies()
    proxy = random.choice(proxies)
    loguru.logger.debug(f'getProxy: {proxy}')
    proxies.remove(proxy)
    loguru.logger.debug(f'getProxy: {len(proxies)} proxies is unused.')
    return proxy

def test_request():
    global proxy
    # 持續更換代理直到連線請求成功為止
    while True:
        # 若無上一次連線請求成功的代理資訊，則重新取出一組代理資訊
        if proxy is None:
            proxy = get_one_proxy()
        try:
            url = f'https://www.google.com/'
            loguru.logger.info(f'testRequest: url is {url}')
            loguru.logger.warning(f'testRequest: downloading...')
            response = requests.get(
                                    url,
                                    # 指定 HTTPS 代理資訊
                                    proxies={'https': f'https://{proxy}'},
                                    # 指定連限逾時限制
                                    timeout=5
                                    )
            if response.status_code != 200:
                loguru.logger.debug(f'testRequest: status code is not 200.')
                # 請求發生錯誤，清除代理資訊，繼續下個迴圈
                proxy = None
                continue
            loguru.logger.success(f'testRequest: downloaded.')
        # 發生以下各種例外時，清除代理資訊，繼續下個迴圈
        except requests.exceptions.ConnectionError:
            loguru.logger.error(f'testRequest: proxy({proxy}) is not working (connection error).')
            proxy = None
            continue
        except requests.exceptions.ConnectTimeout:
            loguru.logger.error(f'testRequest: proxy({proxy}) is not working (connect timeout).')
            proxy = None
            continue
        except requests.exceptions.ProxyError:
            loguru.logger.error(f'testRequest: proxy({proxy}) is not working (proxy error).')
            proxy = None
            continue
        except requests.exceptions.SSLError:
            loguru.logger.error(f'testRequest: proxy({proxy}) is not working (ssl error).')
            proxy = None
            continue
        except Exception as e:
            loguru.logger.error(f'testRequest: proxy({proxy}) is not working.')
            loguru.logger.error(e)
            proxy = None
            continue
        # 成功完成請求，離開迴圈
        break

if __name__ == '__main__':
    loguru.logger.add(
        f"log/{datetime.date.today().strftime('%Y%m%d')}.log", 
        rotation='1 day', 
        retention='7 days', 
        level='DEBUG'
        )
    test_request()
