INSERT INTO joined_wind_data
SELECT 
    t1.Time AS LeftTableTime,
    t2.Datetime AS RightTableTime,
    t2.Wind AS WindForecast,
    t2.Wind_Gust AS GustForecast,
    t2.Temperature AS TempForecast,
    t2.Precipitation AS PrecipitationForecast,
    t2.Cloud_Cover AS CloudForecast,
    t2.Wind_Direction AS WindDirForecast,
    t2.WindDirBin AS WindDirBinForecast,
    t1.WindSpeed AS WindMeasured,
    t1.WindGust AS GustMeasured,
    t1.Temp AS TempMeasured,
    t1.WindDir AS WindDirMeasured,
    t1.Baro AS BaroMeasured,
    t2.Month AS Month
FROM
    measurments_rewa t1
JOIN
    historical_forecast t2
ON
    t2.Datetime = (
        SELECT MAX(Datetime)
        FROM historical_forecast
        WHERE Datetime BETWEEN DATE_ADD(t1.Time, INTERVAL 0 HOUR) AND DATE_ADD(t1.Time, INTERVAL 2 HOUR)
    )
