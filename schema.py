from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType,TimestampType
org_schema = StructType([
    StructField("timestamp", TimestampType(), True),
    StructField("TP2", DoubleType(), True),
    StructField("TP3", DoubleType(), True),
    StructField("H1", DoubleType(), True),
    StructField("DV_pressure", DoubleType(), True),
    StructField("Reservoirs", DoubleType(), True),
    StructField("Oil_temperature", DoubleType(), True),
    StructField("Flowmeter", DoubleType(), True),
    StructField("Motor_current", DoubleType(), True),
    StructField("COMP", IntegerType(), True),
    StructField("DV_eletric", IntegerType(), True),
    StructField("Towers", IntegerType(), True),
    StructField("MPG", IntegerType(), True),
    StructField("LPS", IntegerType(), True),
    StructField("Pressure_switch", IntegerType(), True),
    StructField("Oil_level", IntegerType(), True),
    StructField("Caudal_impulses", IntegerType(), True),
    StructField("gpsLong", DoubleType(), True),
    StructField("gpsLat", DoubleType(), True),
    StructField("gpsSpeed", IntegerType(), True),
    StructField("gpsQuality", IntegerType(), True)
])


agg_schema = StructType([
    StructField("window", StructType([
        StructField("start", TimestampType(), True),
        StructField("end", TimestampType(), True)
    ]), True),
    StructField("rolling_mean_TP2", DoubleType(), True),
    StructField("rolling_min_TP2", DoubleType(), True),
    StructField("rolling_max_TP2", DoubleType(), True),
    StructField("rolling_mean_TP3", DoubleType(), True),
    StructField("rolling_min_TP3", DoubleType(), True),
    StructField("rolling_max_TP3", DoubleType(), True),
    StructField("rolling_mean_H1", DoubleType(), True),
    StructField("rolling_min_H1", DoubleType(), True),
    StructField("rolling_max_H1", DoubleType(), True),
    StructField("rolling_mean_DV_pressure", DoubleType(), True),
    StructField("rolling_min_DV_pressure", DoubleType(), True),
    StructField("rolling_max_DV_pressure", DoubleType(), True),
    StructField("rolling_mean_Reservoirs", DoubleType(), True),
    StructField("rolling_min_Reservoirs", DoubleType(), True),
    StructField("rolling_max_Reservoirs", DoubleType(), True),
    StructField("rolling_mean_Oil_temperature", DoubleType(), True),
    StructField("rolling_min_Oil_temperature", DoubleType(), True),
    StructField("rolling_max_Oil_temperature", DoubleType(), True),
    StructField("rolling_mean_Motor_current", DoubleType(), True),
    StructField("rolling_min_Motor_current", DoubleType(), True),
    StructField("rolling_max_Motor_current", DoubleType(), True),
    StructField("record_count", IntegerType(), True),
    StructField("window_start", TimestampType(), True),
    StructField("window_end", TimestampType(), True)
])