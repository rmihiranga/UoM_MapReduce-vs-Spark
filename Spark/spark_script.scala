val results = spark.sql("CREATE EXTERNAL TABLE delay_flights (year int, month int, day_of_month int, day_of_week int, dep_time int,crs_dep_time int,arr_time int,crs_arr_time int,unique_carrier string,flight_num string,tail_num string,act_elapsed_time int,crs_elapsed_time int,air_time int,arr_delay int,dep_delay int,origin string,dest string,distance int,taxi_in int,taxi_out int,cancelled int,cancellation_code string,diverted int,carrier_delay int,weather_delay int,nas_delay int,security_delay int,aircraft_delay int ) ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' STORED AS TEXTFILE LOCATION \"s3://flightsdelay/\"")



##########################  carrier delay ##########################

val t1 = System.nanoTime;

val result1 = spark.sql("SELECT year, avg((carrier_delay /arr_delay)*100) from delay_flights GROUP BY year");
result1.show();

val duration1 = (System.nanoTime - t1) / 1e9d;

println("Time taken to run carrier delay query 1 -> " + duration1);

val t2 = System.nanoTime;

val result2 = spark.sql("SELECT year, avg((carrier_delay /arr_delay)*100) from delay_flights GROUP BY year");
result2.show();

val duration2 = (System.nanoTime - t2) / 1e9d;

println("Time taken to run carrier delay query 2 -> " + duration2);

val t3 = System.nanoTime;

val result3 = spark.sql("SELECT year, avg((carrier_delay /arr_delay)*100) from delay_flights GROUP BY year");
result3.show();

val duration3 = (System.nanoTime - t3) / 1e9d;

println("Time taken to run carrier delay query 3 -> " + duration3);

val t4 = System.nanoTime;

val result4 = spark.sql("SELECT year, avg((carrier_delay /arr_delay)*100) from delay_flights GROUP BY year");
result4.show();

val duration4 = (System.nanoTime - t4) / 1e9d;

println("Time taken to run carrier delay query 4 -> " + duration4);

val t5 = System.nanoTime;

val result5 = spark.sql("SELECT year, avg((carrier_delay /arr_delay)*100) from delay_flights GROUP BY year");
result3.show();

val duration5 = (System.nanoTime - t5) / 1e9d;

println("Time taken to run carrier delay query 5 -> " + duration5);

#####################################################

##########################  NAS delay ##########################

val t1 = System.nanoTime;

val result1 = spark.sql("SELECT year, avg((nas_delay /arr_delay)*100) from delay_flights GROUP BY year");
result1.show();

val duration1 = (System.nanoTime - t1) / 1e9d;

println("Time taken to run NAS delay query 1 -> " + duration1);

val t2 = System.nanoTime;

val result2 = spark.sql("SELECT year, avg((nas_delay /arr_delay)*100) from delay_flights GROUP BY year");
result2.show();

val duration2 = (System.nanoTime - t2) / 1e9d;

println("Time taken to run NAS delay query 2 -> " + duration2);

val t3 = System.nanoTime;

val result3 = spark.sql("SELECT year, avg((carrier_delay /arr_delay)*100) from delay_flights GROUP BY year");
result3.show();

val duration3 = (System.nanoTime - t3) / 1e9d;

println("Time taken to run NAS delay query 3 -> " + duration3);

val t4 = System.nanoTime;

val result4 = spark.sql("SELECT year, avg((nas_delay /arr_delay)*100) from delay_flights GROUP BY year");
result4.show();

val duration4 = (System.nanoTime - t4) / 1e9d;

println("Time taken to run NAS delay query 4 -> " + duration4);

val t5 = System.nanoTime;

val result5 = spark.sql("SELECT year, avg((nas_delay /arr_delay)*100) from delay_flights GROUP BY year");
result3.show();

val duration5 = (System.nanoTime - t5) / 1e9d;

println("Time taken to run NAS delay query 5 -> " + duration5);

#####################################################

##########################  Weather delay ##########################

val t1 = System.nanoTime;

val result1 = spark.sql("SELECT year, avg((weather_delay /arr_delay)*100) from delay_flights GROUP BY year");
result1.show();

val duration1 = (System.nanoTime - t1) / 1e9d;

println("Time taken to run weather delay query 1 -> " + duration1);

val t2 = System.nanoTime;

val result2 = spark.sql("SELECT year, avg((weather_delay /arr_delay)*100) from delay_flights GROUP BY year");
result2.show();

val duration2 = (System.nanoTime - t2) / 1e9d;

println("Time taken to run weather delay query 2 -> " + duration2);

val t3 = System.nanoTime;

val result3 = spark.sql("SELECT year, avg((weather_delay /arr_delay)*100) from delay_flights GROUP BY year");
result3.show();

val duration3 = (System.nanoTime - t3) / 1e9d;

println("Time taken to run weather delay query 3 -> " + duration3);

val t4 = System.nanoTime;

val result4 = spark.sql("SELECT year, avg((weather_delay /arr_delay)*100) from delay_flights GROUP BY year");
result4.show();

val duration4 = (System.nanoTime - t4) / 1e9d;

println("Time taken to run weather delay query 4 -> " + duration4);

val t5 = System.nanoTime;

val result5 = spark.sql("SELECT year, avg((weather_delay /arr_delay)*100) from delay_flights GROUP BY year");
result3.show();

val duration5 = (System.nanoTime - t5) / 1e9d;

println("Time taken to run weather delay query 5 -> " + duration5);

#####################################################

##########################  Aircraft delay ##########################

val t1 = System.nanoTime;

val result1 = spark.sql("SELECT year, avg((aircraft_delay /arr_delay)*100) from delay_flights GROUP BY year");
result1.show();

val duration1 = (System.nanoTime - t1) / 1e9d;

println("Time taken to run aircraft delay query 1 -> " + duration1);

val t2 = System.nanoTime;

val result2 = spark.sql("SELECT year, avg((aircraft_delay /arr_delay)*100) from delay_flights GROUP BY year");
result2.show();

val duration2 = (System.nanoTime - t2) / 1e9d;

println("Time taken to run aircraft delay query 2 -> " + duration2);

val t3 = System.nanoTime;

val result3 = spark.sql("SELECT year, avg((aircraft_delay /arr_delay)*100) from delay_flights GROUP BY year");
result3.show();

val duration3 = (System.nanoTime - t3) / 1e9d;

println("Time taken to run aircraft delay query 3 -> " + duration3);

val t4 = System.nanoTime;

val result4 = spark.sql("SELECT year, avg((aircraft_delay /arr_delay)*100) from delay_flights GROUP BY year");
result4.show();

val duration4 = (System.nanoTime - t4) / 1e9d;

println("Time taken to run aircraft delay query 4 -> " + duration4);

val t5 = System.nanoTime;

val result5 = spark.sql("SELECT year, avg((aircraft_delay /arr_delay)*100) from delay_flights GROUP BY year");
result3.show();

val duration5 = (System.nanoTime - t5) / 1e9d;

println("Time taken to run aircraft delay query 5 -> " + duration5);

#####################################################

##########################  Security delay ##########################

val t1 = System.nanoTime;

val result1 = spark.sql("SELECT year, avg((security_delay /arr_delay)*100) from delay_flights GROUP BY year");
result1.show();

val duration1 = (System.nanoTime - t1) / 1e9d;

println("Time taken to run security delay query 1 -> " + duration1);

val t2 = System.nanoTime;

val result2 = spark.sql("SELECT year, avg((security_delay /arr_delay)*100) from delay_flights GROUP BY year");
result2.show();

val duration2 = (System.nanoTime - t2) / 1e9d;

println("Time taken to run security delay query 2 -> " + duration2);

val t3 = System.nanoTime;

val result3 = spark.sql("SELECT year, avg((security_delay /arr_delay)*100) from delay_flights GROUP BY year");
result3.show();

val duration3 = (System.nanoTime - t3) / 1e9d;

println("Time taken to run security delay query 3 -> " + duration3);

val t4 = System.nanoTime;

val result4 = spark.sql("SELECT year, avg((security_delay /arr_delay)*100) from delay_flights GROUP BY year");
result4.show();

val duration4 = (System.nanoTime - t4) / 1e9d;

println("Time taken to run security delay query 4 -> " + duration4);

val t5 = System.nanoTime;

val result5 = spark.sql("SELECT year, avg((security_delay /arr_delay)*100) from delay_flights GROUP BY year");
result3.show();

val duration5 = (System.nanoTime - t5) / 1e9d;

println("Time taken to run security delay query 5 -> " + duration5);

#####################################################
