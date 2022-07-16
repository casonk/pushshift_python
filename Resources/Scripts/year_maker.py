import pandas as pd
import numpy as np

end_dates = pd.Series(pd.date_range(start="2007-01-01", end="2022-05-15", freq="Y"))

start_dates = end_dates - pd.Timedelta(days=365)

cal_year = end_dates.apply(lambda x: x.year)

years = pd.DataFrame({
    'Year' : cal_year,
})

years['Start_Date'] = start_dates
years['Start_Epoch'] = years.Start_Date.astype(np.int64) // 10 ** 9
years['End_Date'] = end_dates
years['End_Epoch'] = years.End_Date.astype(np.int64) // 10 ** 9
years['IDX'] = years.Year.astype(str)

years.to_csv('F:\\Funded\\Ethical_Reccomendations\\Python\\pushshift_python\\Resources\\Data\\years.csv', index=False)