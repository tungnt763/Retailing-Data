import pandas as pd
from datetime import datetime, timedelta

start_date = datetime(2000, 1, 1)
end_date = datetime(2029, 12, 31)

dates = pd.date_range(start=start_date, end=end_date)

data = []
for d in dates:
    date_key = int(d.strftime("%Y%m%d"))
    full_date = d.strftime("%Y-%m-%d")
    day_of_week = d.strftime("%A")
    weekday_indicator = "Weekend" if d.weekday() >= 5 else "Weekday"
    holiday_indicator = "Non-Holiday"  # Cập nhật sau nếu có danh sách ngày lễ

    day_in_month = d.day
    day_in_year = d.timetuple().tm_yday
    week_in_year = int(d.strftime("%V"))
    is_last_day_in_month = (d + timedelta(days=1)).month != d.month

    calendar_month = d.month
    calendar_month_name = d.strftime("%B")
    calendar_quarter = (d.month - 1) // 3 + 1
    calendar_year = d.year

    fiscal_month = (d.month + 2) % 12 + 1
    fiscal_quarter = (fiscal_month - 1) // 3 + 1
    fiscal_year = d.year if d.month >= 3 else d.year - 1

    data.append([
        date_key, full_date, day_of_week, weekday_indicator, holiday_indicator,
        day_in_month, day_in_year, week_in_year, is_last_day_in_month,
        calendar_month, calendar_month_name, calendar_quarter, calendar_year,
        fiscal_month, fiscal_quarter, fiscal_year,
    ])

df = pd.DataFrame(data, columns=[
    'date_key', 'full_date', 'day_of_week', 'weekday_indicator', 'holiday_indicator',
    'day_in_month', 'day_in_year', 'week_in_year', 'is_last_day_in_month',
    'calendar_month', 'calendar_month_name', 'calendar_quarter', 'calendar_year',
    'fiscal_month', 'fiscal_quarter', 'fiscal_year',
])

df.to_csv("date.csv", index=False)
