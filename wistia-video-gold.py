# Databricks notebook source
# Read all three bronze datasets
metadata_df = spark.read.option("multiLine", "true").json(
    "abfss://raw@wistiaadls.dfs.core.windows.net/metadata/*/*/*.json"
)
stats_df = spark.read.option("multiLine", "true").json(
    "abfss://raw@wistiaadls.dfs.core.windows.net/stats_by_date/*/*/*.json"
)
events_df = spark.read.option("multiLine", "true").json(
    "abfss://raw@wistiaadls.dfs.core.windows.net/events/*/*/*.json"
)
print("✅ Bronze data loaded")
print(f"Metadata: {metadata_df.count()} rows")
print(f"Stats:    {stats_df.count()} rows")
print(f"Events:   {events_df.count()} rows")

# COMMAND ----------

from pyspark.sql.functions import col, to_timestamp, when, regexp_extract, upper
from pyspark.sql.functions import count

dim_media = metadata_df.select(
    col("hashed_id").alias("media_id"),
    col("name").alias("title"),
    col("duration").alias("duration_secs"),
    to_timestamp(col("created")).alias("created_at"),
    # Extract channel from title
    when(upper(col("name")).contains("FACEBOOK"), "Facebook")
    .when(upper(col("name")).contains("YOUTUBE"), "YouTube")
    .otherwise("Unknown").alias("channel")
    # need to join to events to get embed url 
).dropDuplicates(["media_id"])

# Get most common embed_url per media_id from events
embed_urls = events_df.groupBy("media_id", "embed_url") \
    .agg(count("*").alias("cnt")) \
    .orderBy("media_id", col("cnt").desc()) \
    .dropDuplicates(["media_id"]) \
    .select("media_id", "embed_url")

# Join back to dim_media
dim_media = dim_media.join(embed_urls, on="media_id", how="left")


display(dim_media.limit(30))
print(f"dim_media rows: {dim_media.count()}")


# COMMAND ----------

from pyspark.sql.functions import col

dim_visitor = events_df.select(
    col("visitor_key").alias("visitor_id"),
    col("ip").alias("ip_address"),
    col("country"),
    col("region"),
    col("city"),
    col("user_agent_details.browser").alias("browser"),
    col("user_agent_details.platform").alias("platform"),
    col("user_agent_details.mobile").alias("is_mobile")
).dropDuplicates(["visitor_id"])

print(f"dim_visitor rows: {dim_visitor.count()}")
display(dim_visitor.limit(30))

# COMMAND ----------

from pyspark.sql.functions import col, to_timestamp, to_date, round

# Get duration_secs from dim_media to calculate watch time
duration_lookup = dim_media.select("media_id", "duration_secs")

fact_visitor_events = events_df.select(
    col("event_key"),
    col("media_id"),
    col("visitor_key").alias("visitor_id"),
    to_timestamp(col("received_at")).alias("received_at"),
    to_date(col("received_at")).alias("date"),
    col("percent_viewed").alias("watched_percent_visitor"),
    col("embed_url").alias("url")
).join(duration_lookup, on="media_id", how="left") \
 .withColumn(
    "total_watch_time_hrs_visitor",
    round((col("watched_percent_visitor") * col("duration_secs")) / 3600, 6)
).drop("duration_secs")

print(f"fact_visitor_events rows: {fact_visitor_events.count()}")
display(fact_visitor_events.limit(30))

# COMMAND ----------

from pyspark.sql.functions import col, regexp_extract, avg, round, countDistinct

# Read stats with media_id extracted from file path
stats_with_media = spark.read.option("multiLine", "true").json(
    "abfss://raw@wistiaadls.dfs.core.windows.net/stats_by_date/*/*/*.json"
).withColumn(
    "media_id",
    regexp_extract(col("_metadata.file_path"), r"media_id=([^/]+)", 1)
).select(
    col("media_id"),
    col("date").cast("date"),
    col("load_count").alias("load_count_daily"),
    col("play_count").alias("play_count_daily"),
    col("hours_watched").alias("total_watch_time_hrs_daily")
)

# Aggregate visitor metrics from events
events_agg = events_df.groupBy(
    col("media_id"),
    to_date(col("received_at")).alias("date")
).agg(
    countDistinct("visitor_key").alias("visitor_count_daily"),
    avg("percent_viewed").alias("avg_watched_percent_daily")
)

# Join and calculate play_rate
fact_media_engagement_daily = stats_with_media.join(
    events_agg, on=["media_id", "date"], how="left"
).withColumn(
    "play_rate",
    round(col("play_count_daily") / col("load_count_daily"), 4)
).select(
    "media_id",
    "date",
    "load_count_daily",
    "play_count_daily",
    "visitor_count_daily",
    "play_rate",
    "total_watch_time_hrs_daily",
    "avg_watched_percent_daily"
)

# See row count + sample in one shot
print(f"fact_media_engagement_daily rows: {fact_media_engagement_daily.count()}")
display(fact_media_engagement_daily.limit(20))

# COMMAND ----------

GOLD_PATH = "abfss://gold@wistiaadls.dfs.core.windows.net"

# Write as parquet — columnar format, efficient for Spark reads
dim_media.write.mode("overwrite").parquet(f"{GOLD_PATH}/dim_media")
dim_visitor.write.mode("overwrite").parquet(f"{GOLD_PATH}/dim_visitor")
fact_visitor_events.write.mode("overwrite").parquet(f"{GOLD_PATH}/fact_visitor_events")
fact_media_engagement_daily.write.mode("overwrite").parquet(f"{GOLD_PATH}/fact_media_engagement_daily")

print("✅ All gold tables written to ADLS")

# COMMAND ----------

