CREATE TABLE dim_date (
    date_key INT PRIMARY KEY,                         -- YYYYMMDD
    full_date DATE,                                   -- 2000-01-01
    day_of_week VARCHAR(10),                          -- Monday
    weekday_indicator VARCHAR(10),                    -- Weekday / Weekend
    holiday_indicator VARCHAR(20),                    -- Holiday / Non-Holiday (cần cập nhật tay)
    
    day_in_month INT,                                 -- 1 → 31
    day_in_year INT,                                  -- 1 → 366
    week_in_year INT,                                 -- ISO week
    is_last_day_in_month BOOLEAN,
    
    calendar_month INT,
    calendar_quarter INT,
    calendar_year INT,
    
    fiscal_month INT,
    fiscal_quarter INT,
    fiscal_year INT,

);
