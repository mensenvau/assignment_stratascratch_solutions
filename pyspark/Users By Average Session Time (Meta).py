from pyspark.sql.functions import date_format, col, when, max, min, avg, expr

# Processing the dataframe
df = facebook_web_log \
    .withColumn('date', date_format('timestamp', 'yyyyMMdd')) \
    .withColumn('page_load', when(col("action") == "page_load", col("timestamp"))) \
    .withColumn('page_exit', when(col("action") == "page_exit", col("timestamp"))) \
    .groupBy(["user_id", "date"]).agg(
        max("page_load").alias("max_page_load"),
        min("page_exit").alias("max_page_exit")
    ).withColumn('sum',  col("max_page_exit").cast("long") - col("max_page_load").cast("long")) \
    .groupBy(["user_id"]).agg(
        avg("sum").alias("avg_session_duration")
    ) \
    .filter(col("avg_session_duration") > 0)
 
# Convert to Pandas DataFrame
df_pd = df.toPandas()

# Display the result
df_pd
