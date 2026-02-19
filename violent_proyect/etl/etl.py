print("hello world")
print("Proceso etl")

import pandas as pd 
import numpy as np 
import csv
import os 
import random 
import matplotlib.pyplot as plt
from pathlib import Path 

#all local 

#datos = r"C:\Users\zabu\desktop\violent_proyect\Violent_Crime___Property_Crime_by_County__1975_to_Present.csv"
#impios = r"C:\Users\zabu\desktop\violent_proyect\violent_proyect_limpio.csv"
#limpio_long = r"C:\Users\zabu\desktop\violent_proyect\violent_long_clean.csv"

BASE_DIR = Path(__file__).resolve().parent.parent

raw_data = BASE_DIR / "data" / "raw" / "violent_crime_1975_to_2020.csv"
clean_data = BASE_DIR / "data" / "clean" / "violent_clean.csv"
clean_long = BASE_DIR / "data" / "clean" / "violent_long.csv"



def etl(raw_data, clean_data, clean_long): 
    try:
        
        df = pd.read_csv(raw_data)
        print(df.info())
        #print(df['VIOLENT CRIME RATE PERCENT CHANGE PER 100,000'].dtype)

        percent_cols = [
           'PERCENT CHANGE',
           'VIOLENT CRIME PERCENT CHANGE',
           'PROPERTY CRIME PERCENT CHANGE',
           'OVERALL PERCENT CHANGE PER 100,000 PEOPLE',
           'VIOLENT CRIME RATE PERCENT CHANGE PER 100,000 PEOPLE',
           'PROPERTY CRIME RATE PERCENT CHANGE PER 100,000 PEOPLE',
           'MURDER  RATE PERCENT CHANGE PER 100,000 PEOPLE',
           'RAPE RATE PERCENT CHANGE PER 100,000 PEOPLE',
           'ROBBERY RATE PERCENT CHANGE PER 100,000 PEOPLE',
           'AGG. ASSAULT  RATE PERCENT CHANGE PER 100,000 PEOPLE',
           'B & E RATE PERCENT CHANGE PER 100,000 PEOPLE',
           'LARCENY THEFT  RATE PERCENT CHANGE PER 100,000 PEOPLE',
           'M/V THEFT  RATE PERCENT CHANGE PER 100,000 PEOPLE'
        ]

        for col in percent_cols:
            df[col] = pd.to_numeric(df[col], errors='coerce')
            df[col] = df[col].fillna(df[col].mean())

        crime_cols = [
            'MURDER', 
            'RAPE',
            'ROBBERY',
            'AGG. ASSAULT',
            'B & E',
            'LARCENY THEFT',
            'M/V THEFT',    
        ]

        df_long = df.melt(
            id_vars=['JURISDICTION', 'YEAR', 'POPULATION'],
            value_vars=crime_cols,
            var_name='crime_type',
            value_name='crime_count'
        )

        severity_map = {
            'MURDER': 'HIGH',
            'RAPE': 'HIGH', 
            'ROBBERY': 'MEDIUM', 
            'AGG. ASSAULT': 'MEDIUM',
            'B & E': 'LOW', 
            'LARCENY THEFT': 'LOW', 
            'M/V THEFT': 'LOW'
        }

        df_long['severity'] = df_long['crime_type'].map(severity_map)
        
        df_long['rate_per_100k'] = (
            df_long['crime_count'] / df_long['POPULATION'] * 100_000
        )

        df_long['event_timestamp'] = pd.to_datetime(
            df_long['YEAR'].astype(str)
        ) + pd.to_timedelta(
            np.random.randint(0, 365, size=len(df_long)), unit='D' 
        )

        df_long['anomaly_flag'] = (
            df_long.groupby(['JURISDICTION', 'crime_type'])['crime_count']
                   .transform(lambda x: x > x.mean() + 2*x.std())
        )

        df['POPULATION'].hist()
        
        df.to_csv(clean_data, index=False, encoding='utf-8')
        df_long.to_csv(clean_long, index=False, encoding='utf-8')

        return True 

    except Exception as e: 
        print(f"el proceso etl fallo: {e}")
        return False 

if __name__ == "__main__":
    etl(raw_data, clean_data, clean_long)