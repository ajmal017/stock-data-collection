import alpaca_trade_api as Alpaca
import pandas as pd
import holidays
from datetime import datetime, timedelta, date
import os
from time import sleep, mktime, localtime
import asyncio
import uvloop
import concurrent
import requests
from datetime import datetime
import json
import pickle
from threading import Thread


def read_json(f):
    with open(f, 'r') as df:
        return json.loads(df.read())

def dump_json(f, d):
    with open(f, 'w') as df:
        json.dump(d, df, indent=4)

def read_bin(f):
    with open(f, 'rb') as df:
        return pickle.load(df)

def dump_bin(f, d):
    with open(f, 'wb') as df:
        pickle.dump(d, df)


def from_unix(unix):
    t = datetime.fromtimestamp(unix)
    return str(t)



us_holidays = holidays.UnitedStates()
KEY = 'POLYGON_KEY'
SECRET = 'POLYGON_SECRET'

symbols = read_json('../data/symbols.json')
api = Alpaca.REST(KEY, SECRET)

def get_days(s, e):
    days = []
    sy, sm, sd = [int(d) for d in s.split('-')]
    ey, em, ed = [int(d) for d in e.split('-')]
    start = datetime(sy, sm, sd)
    end = datetime(ey, em, ed)
    delta = end - start
    for i in range(delta.days + 1):
        days.append(str(start + timedelta(i))[:10])
    return days


def unix(date):
    y, m, d = [int(d) for d in date.split('-')]
    o = int(datetime(y, m, d, 9, 30).strftime("%s")) * 1000000000
    c = int(datetime(y, m, d, 16).strftime("%s")) * 1000000000
    return o, c

def get_bars(symbols, dates):
    dates = sorted([(str(date).split(' ')[0]).split('-') for date in dates if date not in us_holidays and date.isoweekday() in range(1, 6)])   
    for sym in symbols:
        if not os.path.exists(f'minute/{sym}'):
            os.mkdir(f'minute/{sym}')
        for date in dates:
            y, m, d = date
            _d = f'{y}-{m}-{d}'
            print(f'{sym} minute {_d}')
            start = pd.Timestamp(year=int(y), month=int(m), day=int(d), hour=9, minute=30, tz='US/Eastern')
            end = pd.Timestamp(year=int(y), month=int(m), day=int(d), hour=16, tz='US/Eastern')
            bars = api.polygon.historic_agg_v2(sym, 1, 'minute', _from=_d, to=_d)
            s, e = 0, 0
            for b in range(len(bars)):
                if bars[b].timestamp >= start and s == 0:
                    s = b
                if e == 0:
                    if bars[b].timestamp == end:
                        e = b
                        break
                    elif bars[b].timestamp > end:
                        e = b - 1
            try:
                _bars = [[b.volume, b.open, b.close, b.high, b.low] for b in bars[s:e]]
                _bars[-1]
                dump_bin(f'minute/{sym}/{_d}', _bars)
            except:
                pass
            

asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())

def get_quotes(symbol, date):
    url = f'https://api.polygon.io/v2/ticks/stocks/trades/{symbol}/{date}?apiKey={KEY}&limit=50000'
    t_offset = 0
    data = []
    o, c = unix(date)
    filled = False
    while True:
        try:
            k = requests.get(url + '&timestamp={}'.format(t_offset))
            if k.status_code != 200:
                break
            k = k.json()
            if not k['success']:
                break
            _res = k['results']
            res = []
            for r in _res:
                fo = int((r['t']) - o)/1e9
                if fo >= 0:
                    res.append((fo, r['s'], r['p']))
                if fo >= 23400:
                    filled = True
                    break
            data.extend(res)
            if k['results_count'] < 50000 or filled is True:
                break
            tp = localtime(_res[-1]['t']/1e9)
            if tp.tm_hour >= 16:
                break
            t_offset = _res[-1]['t']
        except Exception as e:
            print(e)

    return data

def worker(s, d):
    print('Trying %s:%s...' % (s, d))
    r = get_quotes(s, d)
    if r:
        if not os.path.exists(f'ticks/{s}'):
            os.mkdir(f'ticks/{s}')
        with open(f'ticks/{s}/{d}', 'wb') as f:
            pickle.dump(r, f)


async def main(symbols, dates):
    with concurrent.futures.ThreadPoolExecutor(max_workers=100) as executor:
        loop = asyncio.get_event_loop()
        futures = [loop.run_in_executor(None, worker, s, d) for s in symbols for d in dates]
        for _ in await asyncio.gather(*futures):
            pass

def premarket_index(s, e=None):
    if e is None: e = str(datetime.now())[:10]
    
    tk = ['QQQ', 'DIA', 'SPY']
    for day in get_days(s, e):
        results = {'QQQ': [], 'DIA': [], 'SPY': []}
        for sym in tk:
            req_url = f'https://api.polygon.io/v2/aggs/ticker/{sym}/range/1/minute/{day}/{day}?apiKey={KEY}'
            raw = requests.get(req_url).text
            data = json.loads(raw)
            if data['resultsCount'] != 0:
                for r in data['results']:
                    t = int(r['t']/1000)
                    date, time = from_unix(t)[:10], from_unix(t)[11:]
                    hrs, min_ = [float(s) for s in time.split(':')[:-1]]
                    rel = hrs + min_/100 
                    if rel >= 4 and rel < 9.3:
                        v, o, c, h, l = r['v'], r['o'], r['c'], r['h'], r['l']
                        results[sym].append([v, o, c, h, l])
        if len(results['QQQ']) > 0:
            print(f'Got Premarket {day}')
            dump_bin(f'premarket/index/{day}', results)

def post_market_index(s, e=None):
    if e is None: e = str(datetime.now())[:10]
    
    tk = ['QQQ', 'DIA', 'SPY']
    for day in get_days(s, e):
        results = {'QQQ': [], 'DIA': [], 'SPY': []}
        for sym in tk:
            req_url = f'https://api.polygon.io/v2/aggs/ticker/{sym}/range/1/minute/{day}/{day}?apiKey={KEY}'
            raw = requests.get(req_url).text
            data = json.loads(raw)
            if data['resultsCount'] != 0:
                for r in data['results']:
                    t = int(r['t']/1000)
                    date, time = from_unix(t)[:10], from_unix(t)[11:]
                    hrs, min_ = [float(s) for s in time.split(':')[:-1]]
                    rel = hrs + min_/100 
                    if rel >= 16:
                        v, o, c, h, l = r['v'], r['o'], r['c'], r['h'], r['l']
                        results[sym].append([v, o, c, h, l])
        if len(results['QQQ']) > 0:
            print(f'Got Premarket {day}')
            dump_bin(f'postmarket/index/{day}', results)


def premarket_sym(symbols, s, e=None):
    if e is None: e = str(datetime.now())[:10]
    
    for sym in symbols:
        if not os.path.exists(f'premarket/{sym}'):
            os.mkdir(f'premarket/{sym}')
        for day in get_days(s, e):
            if os.path.exists(f'premarket/{sym}/{day}'):
                continue
            else:
                bars = []
                req_url = f'https://api.polygon.io/v2/aggs/ticker/{sym}/range/1/minute/{day}/{day}?apiKey={KEY}'
                raw = requests.get(req_url).text
                data = json.loads(raw)
                try:
                    if data['resultsCount'] != 0:
                        for r in data['results']:
                            t = int(r['t']/1000)
                            date, time = from_unix(t)[:10], from_unix(t)[11:]
                            hrs, min_ = [float(s) for s in time.split(':')[:-1]]
                            rel = hrs + min_/100 
                            if rel >= 4 and rel < 9.3:
                                v, o, c, h, l = r['v'], r['o'], r['c'], r['h'], r['l']
                                bars.append([v, o, c, h, l])
                    if len(bars) > 0:
                        print(f'Got PreMarket {sym} {day}')
                        dump_bin(f'premarket/{sym}/{day}', bars)
                except:
                    pass

def post_market_sym(symbols, s, e=None):
    if e is None: e = str(datetime.now())[:10]
    
    for sym in symbols:
        if not os.path.exists(f'postmarket/{sym}'):
            os.mkdir(f'postmarket/{sym}')
        for day in get_days(s, e):
            if os.path.exists(f'postmarket/{sym}/{day}'):
                continue
            else:
                bars = []
                req_url = f'https://api.polygon.io/v2/aggs/ticker/{sym}/range/1/minute/{day}/{day}?apiKey={KEY}'
                raw = requests.get(req_url).text
                data = json.loads(raw)
                try:
                    if data['resultsCount'] != 0:
                        for r in data['results']:
                            t = int(r['t']/1000)
                            date, time = from_unix(t)[:10], from_unix(t)[11:]
                            hrs, min_ = [float(s) for s in time.split(':')[:-1]]
                            rel = hrs + min_/100 
                            if rel >= 16:
                                v, o, c, h, l = r['v'], r['o'], r['c'], r['h'], r['l']
                                bars.append([v, o, c, h, l])
                    if len(bars) > 0:
                        print(f'Got Postmarket {sym} {day}')
                        dump_bin(f'postmarket/{sym}/{day}', bars)
                except:
                    pass

if __name__ == '__main__':

    sortedymbols = sorted(symbols)

    us_holidays = holidays.UnitedStates()
    dates = [datetime.today() - timedelta(days=i) for i in range(2, 4)]
    
    get_bars(symbols, dates)
    dates = sorted([str(date).split(' ')[0] for date in dates if date not in us_holidays and date.isoweekday() in range(1, 6)])
    
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main(symbols, dates))
    

    Thread(target=post_market_index, args=(dates[0],)).start()
    Thread(target=premarket_index, args=(dates[0],)).start()
    Thread(target=post_market_index, args=(dates[0] ,)).start()
    Thread(target=premarket_sym, args=(sortedymbols, dates[0])).start()
    Thread(target=post_market_sym, args=(sortedymbols, dates[0])).start()