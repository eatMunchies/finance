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
        else:
            raise ValueError("Bad API selection!")
    
    def get_data(self, symbols : list, date_from : str = "", date_to : str = ""):
        df = self.get_data_raw(symbols, date_from, date_to)
        df = self.clean_data(df)
        return df
    
    def get_data_raw(self, symbols : list, date_from : str = "", date_to : str = ""):
        if self.api == "STOCK_DATA":
            params = {
                "api_token": self.key,
                "symbols": symbols,
                "sort": "asc",
                "key_by_date": True,
            }
            if date_from != "": params['date_from'] = date_from
            if date_to != "": params['date_to'] = date_to

            response = r.get(self.unadjusted, params)
            return pd.DataFrame(dict(response.json())['data'])
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
            )
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