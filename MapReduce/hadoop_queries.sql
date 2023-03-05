CREATE EXTERNAL TABLE delay_flights (
              year int,
              month int,
              day_of_month int,
              day_of_week int,
              dep_time int,
              crs_dep_time int,
              arr_time int,
              crs_arr_time int,
              unique_carrier string,
              flight_num string,
              tail_num string,
              act_elapsed_time int,
              crs_elapsed_time int,
              air_time int,
              arr_delay int,
              dep_delay int,
              origin string,
              dest string,
              distance int,
              taxi_in int,
              taxi_out int,
              cancelled int,
              cancellation_code string,
              diverted int,
              carrier_delay int,
              weather_delay int,
              nas_delay int,
              security_delay int,
              aircraft_delay int
       )
       ROW FORMAT DELIMITED
       FIELDS TERMINATED BY ','
       LINES TERMINATED BY '\n'
       STORED AS TEXTFILE
       LOCATION "s3://flightsdelay/";



SELECT year, avg((carrier_delay /arr_delay)*100) from delay_flights GROUP BY year

SELECT year, avg((nas_delay /arr_delay)*100) from delay_flights GROUP BY year

SELECT year, avg((weather_delay /arr_delay)*100) from delay_flights GROUP BY year

SELECT year, avg((aircraft_delay /arr_delay)*100) from delay_flights GROUP BY year

SELECT year, avg((security_delay /arr_delay)*100) from delay_flights GROUP BY year