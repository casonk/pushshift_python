import pandas as pd
import numpy as np

end_dates = pd.Series(pd.date_range(start="2007-01-01", end="2022-05-15", freq="W"))

start_dates = end_dates - pd.Timedelta(days=7)

cal_week = start_dates.index.to_series() % 52 + 1

cal_year = end_dates.apply(lambda x: x.year)

weeks = pd.DataFrame({
    'Year' : cal_year,
    'Week' : cal_week,
})

weeks['Start_Date'] = start_dates
weeks['Start_Epoch'] = weeks.Start_Date.astype(np.int64) // 10 ** 9
weeks['End_Date'] = end_dates
weeks['End_Epoch'] = weeks.End_Date.astype(np.int64) // 10 ** 9
weeks['IDX'] = weeks.Year.astype(str) + '_' + weeks.Week.astype(str)

weeks.to_csv('F:\\Funded\\Ethical_Reccomendations\\pushshift_python\\Resources\\Data\\yearly_weeks.csv', index=False)