from fastapi import FastAPI
from threading import Thread
import requests
import time
from sqlalchemy import create_engine, text
from datetime import datetime
import uvicorn
from typing import Callable, Any



### ПАРСИНГ И ЗАПИСЬ В БД ###
# Формат json responseble = {'symbol': 'BTCUSDT', 'price': '89833.10000000'}

engine = create_engine("postgresql://postgres:123456@localhost:5432/price_monitor")

def func_parisng(link: str):
    responseble = (requests.get(link)).json()
    return responseble

def convertor_binance(data_json: dict):
    data_ready = {
            'site': 'binance.com',
            'currency': data_json['symbol'],
            'price': data_json['price'],
            'date_price': datetime.now()
           }
    return data_ready

def convertor_kraken(data_json: dict): 
    name_currency = list(data_json['result'].keys())[0] # Название валюты вытаскиваю из json
    price = data_json['result'][name_currency]['c'][0]
    data_ready = {
            'site': 'kraken.com',
            'currency': name_currency,
            'price': price,
            'date_price': datetime.now()
           }
    return data_ready

def convertor_bybit(data_json: dict): 
    name_currency = data_json['result']['list'][0]['symbol']
    price = data_json['result']['list'][0]['lastPrice']
    data_ready = {
            'site': 'bybit.com',
            'currency': name_currency,
            'price': price,
            'date_price': datetime.now()
           }
    return data_ready

def insert_data(data_ready: dict):
    with engine.connect() as conn:
            conn.execute(text(f"""
            INSERT INTO price(site, currency, price, date_price) 
            values 
            ('{data_ready['site']}', '{data_ready['currency']}', {data_ready['price']}, '{data_ready['date_price']}')
            """))
            conn.commit()



def parsing_insert_data():

    parsing_sources = {
            'binance.com': {
                    'URL':'https://api.binance.com/api/v3/ticker/price?symbol=',
                    'symbol': ["BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT", "ADAUSDT", "DOGEUSDT", "XRPUSDT", "DOTUSDT", "LTCUSDT", "MATICUSDT"]
                    },
            'kraken.com': {
                    'URL': 'https://api.kraken.com/0/public/Ticker?pair=',
                    'symbol': ['BTCUSDT', 'ETHUSDT', 'SOLUSDT', 'ADAUSDT', 'DOGEUSDT', 'XRPUSDT', 'DOTUSDT', 'LTCUSDT']
                    },
            'bybit.com': {
                    'URL': 'https://api.bybit.com/v5/market/tickers?category=spot&symbol=',
                    'symbol': ['BTCUSDT', 'ETHUSDT', 'SOLUSDT', 'ADAUSDT', 'DOGEUSDT', 'XRPUSDT', 'DOTUSDT', 'LTCUSDT']
                    }
        }


    while True:
        # time.sleep(30)
        print('начинаю запросы по всем валютам', datetime.now())
        for name in parsing_sources:
            for currency in parsing_sources[name]['symbol']:
                proto_url = parsing_sources[name]['URL']
                correct_url = proto_url + currency

                if name == 'binance.com':
                    raw_response = func_parisng(correct_url)
                    db_data = convertor_binance(raw_response)
                    insert_data(db_data)

                    time.sleep(10)
                    print('запрос binance.com выполнен', datetime.now()) 


                if name == 'kraken.com':    
                    raw_response = func_parisng(correct_url)
                    db_data = convertor_kraken(raw_response)
                    insert_data(db_data)

                    time.sleep(10)
                    print('запрос kraken.com выполнен', datetime.now()) 

                if name == 'bybit.com':    
                    raw_response = func_parisng(correct_url)
                    db_data = convertor_bybit(raw_response)
                    insert_data(db_data)

                    time.sleep(10)
                    print('запрос bybit.com выполнен', datetime.now())

        print('закончил запросы по всем валютам', datetime.now(), '\n') 






### FASTAPI ЧАСТЬ ###

app = FastAPI()

@app.get("/api/v1/prices")
def get_all_prices():
    lst = []
    with engine.connect() as conn:
        result = conn.execute(text(f"SELECT * FROM price"))
        for i in result.mappings().all():
            lst.append(i)
    return lst
        







### ТОЧКА ВХОДА ###
if __name__ == "__main__":
    t = Thread(target = parsing_insert_data) # Дал задаче t мою функцию
    t.start() # Запустил задачу с моей функции в первом потоке    
    uvicorn.run(app, host="127.0.0.1", port=8000)      