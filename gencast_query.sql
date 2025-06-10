-- GenCast 2m Temperature Query with Ensemble Averaging
-- Adjust table name and column names based on your actual schema

SELECT 
    forecast_date,
    forecast_time,
    latitude,
    longitude,
    AVG(temperature_2m) as avg_temperature_2m,
    COUNT(*) as num_ensemble_members,
    STDDEV(temperature_2m) as temp_stddev
FROM 
    `your-project.weathernext_gen_fo.126478713_1_0`  -- Adjust table name
WHERE 
    -- Filter for specific date and time
    forecast_date = '2024-01-01'  -- Replace with your target date
    AND forecast_time = '12:00:00'  -- 12:00:00 UTC
    
    -- Filter for specific location (adjust coordinates as needed)
    AND latitude BETWEEN 40.700 AND 40.750  -- NYC area example
    AND longitude BETWEEN -74.050 AND -74.000
    
    -- Limit to 50 ensemble members
    AND ensemble_member <= 50
    
GROUP BY 
    forecast_date,
    forecast_time,
    latitude,
    longitude
ORDER BY 
    latitude,
    longitude;


-- Alternative query if you want to get the nearest grid point to exact coordinates
WITH nearest_point AS (
  SELECT 
    forecast_date,
    forecast_time,
    latitude,
    longitude,
    ensemble_member,
    temperature_2m,
    -- Calculate distance from target point
    ST_DISTANCE(
      ST_GEOGPOINT(longitude, latitude),
      ST_GEOGPOINT(-74.0060, 40.7128)  -- NYC coordinates
    ) as distance_meters
  FROM 
    `your-project.weathernext_gen_fo.126478713_1_0`
  WHERE 
    forecast_date = '2024-01-01'
    AND forecast_time = '12:00:00'
    AND ensemble_member <= 50
)
SELECT 
    forecast_date,
    forecast_time,
    latitude,
    longitude,
    AVG(temperature_2m) as avg_temperature_2m,
    COUNT(*) as num_ensemble_members,
    STDDEV(temperature_2m) as temp_stddev,
    MIN(distance_meters) as distance_to_target_meters
FROM nearest_point
WHERE distance_meters = (
    SELECT MIN(distance_meters) 
    FROM nearest_point
)
GROUP BY 
    forecast_date,
    forecast_time,
    latitude,
    longitude; 