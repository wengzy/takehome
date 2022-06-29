#!/usr/bin/env python3
#=========================================================
# File Name : BinanceTask.py
# Purpose :
# Creation Date : 2022-06-28 20:52:59
# Last Modified : 2022-06-29 23:04:16
# Created By :  John Weng 
#=========================================================
import requests
import time
import json
import heapq
from prometheus_client import start_http_server, Gauge


class BinanceTask:

    API_URL = 'https://api.binance.com/api'

    def __init__(self):
        self.API_URL = self.API_URL
        self.current_price = {}

    # Question 1 & 2 
    def top_symbols(self, coin, key, top_number ):
        """
        Return the top N symbols with quote asset BTC and the highest volume 
        over the last 24 hours in descending order in data frames.
        [{'symbol': 'VETBTC', 'priceChange': '-0.00000003',...},
        {'symbol': 'DOGEBTC', 'priceChange': '-0.00000015',...}, 
        {...},]
        """

        uri = "/v3/ticker/24hr"
        response = requests.get(self.API_URL + uri )
        # Check for HTTP codes other than 200
        if response.status_code != 200:
            print('Status:', response.status_code, 'Headers:', response.headers, 'Error Response:',response.json())
            exit()

        # Decode the JSON response into a dictionary and use the data
        data = response.json()

        # Filter python objects with list comprehensions
        filted_dict = [x for x in data if x['symbol'].endswith(coin)  ]

        topN =  heapq.nlargest (top_number, filted_dict , key=lambda s: float(s[key]))

        print("Top %i symbols  %s with the highest %s " %(top_number,coin,key))
        for n in topN:
            print(n['symbol'],n[key])
        print("""
              """)
        return(topN)

    # Question 3
    def total_notional(self,symbols , top_number):
        """
        Return total notional value of the top_number bids and asks 
        {'VETBTC': {'bid_notional': 74.39859864999994, 'ask_notional': 162.38389860999987}, 
        'DOGEBTC': {'bid_notional': 104.49938716999993, 'ask_notional': 92.47865982000002},
        {...}}
        """

        uri = "/v3/depth"
        notional_dict = {}

        for symbol_dict in symbols:
            # get depth of each symbol via API
            symbol = symbol_dict['symbol']
            payload = { 'symbol' : symbol, 'limit' : top_number }
            response = requests.get(self.API_URL + uri, params=payload)
            if response.status_code != 200:
                print('Status:', response.status_code, 'Headers:', response.headers, 'Error Response:',response.json())
                exit()

            data = response.json()

            bid_notional = 0
            for bid in data['bids']:
                bid_notional += float(bid[0]) * float(bid[1])

            ask_notional = 0
            for ask in data['asks']:
                ask_notional += float(ask[0]) * float(ask[1])
            
            notional_dict[symbol] = {'bid_notional' : bid_notional ,'ask_notional' : ask_notional}
            

        print("Total Notional value") 
        print(notional_dict)
        print("""
              """)
        return notional_dict

    # Question 4 
    def price_spread(self, symbols,onScreen=False):
        """
        Return the price spread for each symbols in dictionary format

        {
        'BTCUSDT': 0.010000000002037268,
        'ETHUSDT': 0.07999999999992724,
        ...
        }
        """

        uri = '/v3/ticker/bookTicker'
        spread_dict = {}
        symbol_list =[]
        for symbol_dict in symbols:
            symbol_list.append( symbol_dict['symbol'])


        symbols_str ='['+ ','.join(f'"{w}"' for w in symbol_list)+']'

        # get bookTicker for all symbols
        payload = { 'symbols' : symbols_str }
        response = requests.get(self.API_URL + uri, params=payload)
        if response.status_code != 200:
            print('Status:', response.status_code, 'Headers:', response.headers, 'Error Response:',response.json())
            exit()

        data = response.json()
        for book in data:
            spread_dict[book['symbol']] = float(book['askPrice']) - float(book['bidPrice'])

        if onScreen:
            print("Price Spread ")
            print(spread_dict)
            print("""
                  """)
        return spread_dict

    # Question 5 
    def spread_delta(self, symbols,onScreen=False):
        """
        Return the absolute price delta for each symbols in dictionary format
        """

        # if no previous price spread
        if not self.current_price:
            self.current_price = self.price_spread(symbols,False)
            time.sleep(10)
        
        delta = {}
        new_price = self.price_spread(symbols,False)

        for key in new_price:
            delta[key] = abs(self.current_price[key]-new_price[key])

        self.current_price = new_price
        if onScreen:
            print("Absolute Delta")
            print(delta)
            print("""
                  """)

        return(delta)



if __name__ == "__main__":
    client = BinanceTask()

    # To Print Details
    print("Questions 1:")
    Q1=client.top_symbols('BTC','volume',5)

    print("Questions 2:")
    Q2=client.top_symbols('USDT', 'count',5)

    print("Questions 3:")
    Q3=client.total_notional(Q1, 200 )

    print("Questions 4:")
    Q4=client.price_spread(Q2,True)


    # Question 5 
    print("Questions 5:")
    start_http_server(8080)
    print("Prometheus metrics For Absolute Delta on port 8080\n")
    ABSOLUTE_DELTA = Gauge('absolute_delta', 'Absolute Delta', ['symbol'])
    while True:
        metrics = client.spread_delta(Q2,False)
        for j in metrics:
            ABSOLUTE_DELTA.labels(j).set(metrics[j])
        time.sleep(10)
