# First Spark job:
# spark-submit command for the first job
spark-submit --conf spark.cores.max=3 \
              --driver-memory 5G \
              --executor-memory 5G \
              --master spark://mlops.us-west4-b.c.mlops-404108.internal:7077 \
              --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0 \
              --py-files schema.py,streaming.py \
              feature_agg.py \
              --outputmode update \
              --checkpoint temp/app1 \
              --input_topic raw \
              --output_topic aggregation
              

# Second Spark job:
# spark-submit command for the second job

spark-submit --conf spark.cores.max=3 \
              --driver-memory 5G \
              --executor-memory 5G \
              --master spark://mlops.us-west4-b.c.mlops-404108.internal:7077 \
              --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0 \
              --py-files schema.py,streaming.py \
              final_output.py \
              --outputmode append \
              --checkpoint temp/app2 \
              --input_topic_1 raw \
              --input_topic_2 aggregation \
              --output_topic final_output