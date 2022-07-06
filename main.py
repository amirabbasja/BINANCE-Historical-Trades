from http import client
import string
import pandas as pd
from binance import Client
from tqdm import tqdm
import os



def save_dataframe(df:pd.DataFrame, name:string):
    # Writing the dataframe to excel file. The name string should have an xlsx extension

    #Make a historical tades directory
    exists = os.path.exists(DATAPATH)
    if not exists:
        os.makedirs(DATAPATH)

    with pd.ExcelWriter(name) as writer:
        df.to_excel(writer, index = False)

def get_downloaded_trades(Path, direction):
    # this function gets the downloaded trades and 
    # The direction variable specifies the direction of getting the data
    # If it equals to "forward", the latest data will be gathered 
    # but if it equals backward, the historic data will be gathered starting
    # from the most aged trade
    # Also remember that the file names has to be in the following format:
    # f"/HistoricalTrades_{symbol}_{startId}____{endId}.xlsx"  
    ids = []
    for files  in os.listdir(Path):
        ls = files.replace(".xlsx","").split("_")
        # print(ls[2] + " => " + str(ls[6]))
        ids.append(int(ls[2]))
        ids.append(int(ls[6]))
    
    
    if(len(os.listdir(Path)) != 0):
        if(direction == "backward"):
            return min(ids)
        elif(direction == "forward"):
            return max(ids)
    else:
        return -1

def getDiscontinuity(path: string, symbol):
    """
    Some times the downloaded historical tarnsactions files are not continuous.
    This function returns the range that the historical transactions are missing.

    Arguments
    ---------
    path: The location of histocial transactions file.
    symbol: The symbol of the desired asset.

    Returns
    -------
    A list of tuples containing the start and end of the discontinuity. The returned 
    numbers are tardeIDs.
    
    Note* The file names should be in the following format:
    "HistoricalTrades_{symbol}_(\d+)____(\d+).pkl"
    
    """
    from collections import OrderedDict
    files = os.listdir(path)

    dis = {}

    for file in files:
        z = re.match(f"HistoricalTrades_{symbol}_(\d+)____(\d+).pkl", file)
        if z != None:
            dis[z.group(1)] = z.group(2)

    dis = OrderedDict(sorted(dis.items()))
    a = dis.items()

    out = []
    temp = [0, 0]

    for i, val in enumerate(a):
        if i != 0 and temp[1] != val[0]:
            out.append([temp[1] , val[0]])
        temp = val

    return out

client = Client(
    api_key = "gcJLtOs6tZYTOBEyIYY0JZBDagmFMkc5SY5T8R792cCUgBn7YCLfG7pOJZG3J36x",
    api_secret = 'EYyaqoqRyPxozdDQQmlL7xQiHaqDIxgGAav68UoYAznTAOI6darEG132kavhI3Vh')

DATAPATH = "../../Data/HistoricalTrades" # The folder to get and restore t from
SYMBOL = "BTCUSDT" 
DIRECTION = "backward"

if(DIRECTION == "backward"):
    
    lastTradeId = get_downloaded_trades(DATAPATH, DIRECTION)
    if(lastTradeId == -1):
        initialize = client.get_historical_trades(symbol=SYMBOL , limit = 1)
        endId = lastTradeId = initialize[0]["id"]
    else:
        endId = lastTradeId

    while True:
        trades = pd.DataFrame(columns = ["id","time","price","qty","isBuyerMaker","isBestMatch"])
        startId = 0
        for i in tqdm(range(20)):
            # Getting 1000 transactions in each request. in the end of loop, we get 1m transactions
            Batch = client.get_historical_trades(symbol=SYMBOL , limit = 1000, fromId = lastTradeId-1000*i)
            
            trades = pd.concat([pd.DataFrame(Batch, columns =["id","time","price","qty","isBuyerMaker","isBestMatch"]), trades], ignore_index = True)
            
            startId = Batch[0]["id"]
        
        save_dataframe(trades, DATAPATH + f"/HistoricalTrades_{SYMBOL}_{startId}____{endId}.xlsx")
        endId = trades.iloc[0,0]
        lastTradeId = endId


elif(DIRECTION == "forward"):
    pass

print("Done!")
