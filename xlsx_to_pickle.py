from posixpath import split
import pandas as pd
import pickle
import os
# This script gets a folder as path and changes each xlsx file in the folder to 
# a pickle object and saves it in the same folder. this is done so to increase the
# speed of loading of the file in other scripts 
# Note: files has to be in this format: HistoricalTrades_BTCUSDT_{endId}____{startId}.xlsx

PATH = "../../Data/HistoricalTrades/"

files = os.listdir(PATH)
for file in files:
    if file.endswith(".xlsx"):
        df = pd.read_excel(PATH + file)
        df.to_pickle(os.path.abspath(PATH) +"\\"+ file.replace(".xlsx", ".pickle"),)
print("Done!")