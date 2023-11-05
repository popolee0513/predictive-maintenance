from schema import org_schema
import argparse
from streaming import *
from pyspark.sql.types import TimestampType



if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="")
    parser.add_argument("--outputmode" , help="set outputmode")
    parser.add_argument("--checkpoint" , help="set checkpoint location")
    parser.add_argument("--input_topic" , help="set input topic")
    parser.add_argument("--output_topic" , help="set output topic")
    args = parser.parse_args()
    spark = create_spark_session('feature_agg')
    raw_df = process_kafka_data(spark, args.input_topic, org_schema)
    raw_df = raw_df.withColumn("timestamp", raw_df["timestamp"].cast(TimestampType()))

    processed_df = preprocess_data(raw_df)
    final_df = create_kafka_write_stream(processed_df,args.outputmode, args.output_topic, args.checkpoint)
    final_df.start()\
            .awaitTermination()