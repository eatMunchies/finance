"""
Class for helping retrieve and clean data
"""

import pandas as pd
import requests as r
import dotenv
import os

dotenv.load_dotenv()

class DataHandler:

    def __init__(self, api : str = "STOCK_DATA"):
        
        self.api = api
        self.base = "./data/"

        if not os.path.exists(self.base):
            os.makedirs(self.base)

        if api == 'STOCK_DATA':
            self.key = os.getenv(api + "_KEY")
            self.adjusted = os.getenv(api + "_ADJUSTED")
            self.unadjusted = os.getenv(api + "_UNADJUSTED")
        elif api == 'ALPHA':
            self.key = os.getenv(api + "_KEY")
            self.url = os.getenv(api + "_URL")
        else:
            raise ValueError("Bad API selection!")
    
    def get_data(self, options : dict = {}):
        df = self.get_data_raw(options)
        df = self.clean_data(df)
        return df
    
    def get_data_raw(self, options : dict = {}):
        if self.api == "STOCK_DATA":
            params = {
                "api_token": self.key,
                "symbols": options['symbols'],
                "sort": "asc",
                "key_by_date": True,
            }
            if options['date_from']: params['date_from'] = options['date_from']
            if options['date_to']: params['date_to'] = options['date_to']

            response = r.get(self.unadjusted, params)
            return pd.DataFrame(dict(response.json())['data'])
        elif self.api == 'ALPHA':
            params = {
                "function": "TIME_SERIES_INTRADAY",
                "symbol": options['symbol'],
                "apikey": self.key,
                "outputsize": "compact",
            }
            if options['adjusted']: params['adjusted'] = options['adjusted']
            if options['interval']: params['interval'] = options['interval']
            if options['extended_hours']: params['extended_hours'] = True
            # if options['month']: params['month'] = options['month']
            # lets loop and get data since forever
            df = None
            year_i = 2009
            month_i = 1
            while year_i != 2025 or month_i != 8: # it is august 2025 right now
                month = f"{year_i}-{month_i}"
                print("Getting data for:", month)
                params['month'] = month
                response = r.get(self.url, params=params)
                print("Got response:", response.json())
                data = response.json()['Time Series (1min)']
                print("Got data:", data)
                # first loop check
                if month == "2009-1":
                    df = pd.DataFrame(data)
                else:
                    df = pd.concat([df, pd.DataFrame(data)])
                
                month_i += 1
                if month_i > 12:
                    year_i += 1
                    month_i = 1
            return df.T
        else:
            raise RuntimeError("Bad API selection!")
    
    def save_to_csv(self, fp : str, df : pd.DataFrame) -> bool:
        try:
            df.to_csv(self.base + fp)
            return True
        except Exception as e:
            print(f"Exception occurred: {e}")
            return False
    
    def get_from_csv(self, fp : str) -> pd.DataFrame:
        try:
            df = pd.read_csv(self.base + fp)
            return df
        except Exception as e:
            print(f"Exception occurred: {e}")
            return None
    
    def clean_data(self, df : pd.DataFrame) -> pd.DataFrame:
        
        if self.api == "STOCK_DATA":
            df_clean = pd.concat(
                [df.drop(columns=["data"]),
                 pd.json_normalize(df["data"])],
                 axis=1
            ).reset_index()
            return df_clean
        elif self.api == "ALPHA":
            df_clean = df.T.rename(columns={
                '1. open': 'open',
                '2. high': 'high',
                '3. low': 'low',
                '4. close': 'close',
                '5. volume': 'volume'
            })
            for col in df_clean.columns:
                df_clean[col] = df_clean[col].astype(float)
            return df_clean
        else:
            raise RuntimeError("Bad API selection!")
    
    def to_string(self, df : pd.DataFrame) -> None:

        if self.api == "STOCK_DATA":
            # hopefully structure is consistent with the api chosen
            print("Earliest record: " + df['date'][0])
            print("Latest record: " + df['date'][1488])
            print("Number of records: " + len(df))
            print("Columns: ", df.columns)
            print("Head: ", df.head(5))
        else:
            raise RuntimeError("Bad API selection!")