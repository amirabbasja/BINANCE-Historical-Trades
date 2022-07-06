This application downloads the historical transactions from biannce and saves them to a pickle file.
By default, each file contains 1m transactions, but for any reason if you want to change that, you 
can change the range of tqdm function in the main loop, each loop contains 1k trasnactions (The Biannce's
limit).
The saved files will be in the following format:
f"/HistoricalTrades_{symbol}_{startId}____{endId}.pkl"

---------
TODO: The backward data gathering direction works fine, but complete the forward data gathering as well
TODO: The patching process has to be implemented. Some times the downloadad files has discontinuities, 
    write a program that finds discontinuities (Already done) and patches them (Not done).