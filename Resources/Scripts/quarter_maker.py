import pandas as pd
import numpy as np

cols = ['Start_Date','Start_Epoch','End_Date','End_Epoch']
years_quarters = [[2007 + i for i in range(0,16)],['Q1','Q2','Q3','Q4']]
idx = pd.MultiIndex.from_product(years_quarters, names=["Year", "Quarter"])
quarters = pd.DataFrame(index=idx,columns=cols)

start_dates = []
end_dates = []

for year in idx.levels[0].to_list():
    start_dates.append(str(year) + '-01-01')
    end_dates.append(str(year) + '-04-01')
    start_dates.append(str(year) + '-04-01')
    end_dates.append(str(year) + '-07-01')
    start_dates.append(str(year) + '-07-01')
    end_dates.append(str(year) + '-10-01')
    start_dates.append(str(year) + '-10-01')
    end_dates.append(str(year + 1) + '-01-01')

quarters['Start_Date'] = start_dates
quarters['Start_Date'] = pd.to_datetime(quarters['Start_Date'])
quarters['End_Date'] = end_dates
quarters['End_Date'] = pd.to_datetime(quarters['End_Date'])

quarters['Start_Epoch'] = quarters.Start_Date.astype(np.int64) // 10 ** 9
quarters['End_Epoch'] = quarters.End_Date.astype(np.int64) // 10 ** 9

quarters.to_csv('C:\\Kodex\\Recommenders\\pushshift_python\\Resources\\Data\\yearly_quarters.csv')