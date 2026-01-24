from fastapi import FastAPI
from threading import Thread
import requests
import time
from sqlalchemy import create_engine, text
from datetime import datetime
import uvicorn
from typing import Callable, Any


data_json = {
  "retCode": 0,
  "retMsg": "OK",
  "result": {
    "category": "spot",
    "list": [
      {
        "symbol": "BTCUSDT",
        "bid1Price": "89676",
        "bid1Size": "0.261079",
        "ask1Price": "89676.1",
        "ask1Size": "1.282655",
        "lastPrice": "89676",
        "prevPrice24h": "90032.1",
        "price24hPcnt": "-0.0040",
        "highPrice24h": "90336",
        "lowPrice24h": "88513",
        "turnover24h": "842799755.41366598",
        "volume24h": "9411.564379",
        "usdIndexPrice": "89581.685793"
      }
    ]
  },
  "retExtInfo": {

  },
  "time": 1769149429273
}

# print(  d['result']["XXBTZUSD"]['c'][0]   )
print(data_json['result']['list'][0]['lastPrice'])