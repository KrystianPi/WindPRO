SELECT 
    t1.Time AS LeftTableTime,
    t2.Datetime AS RightTableTime,
    t2.Wind AS WindForecast,
    t2.Wind_Gust AS GustForecast,
    t2.Temperature AS TempForecast,
    t2.Precipitation AS PrecipitationForecast,
    t2.Cloud_Cover AS CloudForecast,
    t2.Wind_Direction AS WindDirForecast,
    t1.WindSpeed AS WindMeasured,
    t1.WindGust AS GustMeasured,
    t1.Temp AS TempMeasured,
    t1.WindDir AS WindDirMeasured,
    t1.Baro AS BaroMeasured
FROM
    measurments_rewa t1
JOIN
    historical_forecast t2
ON
    t2.Datetime BETWEEN DATE_ADD(t1.Time, INTERVAL 0 HOUR) AND DATE_ADD(t1.Time, INTERVAL 2 HOUR)
LIMIT 100;