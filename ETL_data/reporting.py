
from pyspark.sql import functions as F
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
from pyspark.sql.window import Window
from pyspark.sql.functions import lag
from pyspark.sql.functions import countDistinct
from pyspark.sql.functions import datediff

spark = SparkSession.builder \
    .appName("Staging") \
    .getOrCreate()

stop_and_search=spark.sql("select * from stage.stop_and_search ")
outcomes=spark.sql("select * from stage.outcomes ")
street=spark.sql("select * from stage.street ")

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, to_date, year, month, dayofmonth, max

# Initialize Spark Session
spark = SparkSession.builder.appName("CrimeInsights").getOrCreate()

# Load Data
# file_path = "path_to_your_file.csv"  # Replace with your file path
# data = spark.read.csv(file_path, header=True, inferSchema=True)
data=spark.sql("""select s.*,o.outcome_type as final_outcome_type from stage.outcomes o inner join 
stage.street s on o.crime_id= s.crime_id and o.LSOA_code=s.LSOA_code and s.period_id=o.period_id and s.id_Reported_By=o.id_Reported_by
and s.id_Falls_within=o.id_Falls_within
 order by o.crime_id desc """)
# Feature Engineering: Extract Date Information
data = data.withColumn("Date", to_date(col("Month"), "yyyy-MM-dd"))
data = data.withColumn("Year", year("Date"))
data = data.withColumn("Month_Num", month("Date"))

data.write.format("delta").mode("overwrite").save(f"Tables/reporting_x/raw_reporting")
# Insight 1: Crime Type Distribution
crime_type_distribution = data.groupBy("Crime_type").count().orderBy(col("count").desc())

crime_type_distribution.write.format("delta").mode("overwrite").save(f"Tables/reporting_x/crime_type_distribution")

# Insight 2: Monthly Crime Trends with crimetype
monthly_crime_trends = data.groupBy("Year", "Month_Num","Crime_type").count().orderBy("Year", "Month_Num")


monthly_crime_trends.write.format("delta").mode("overwrite").save(f"Tables/reporting_x/monthly_crime_trends")

window_spec = Window.partitionBy("LSOA_name","Crime_type").orderBy(F.desc("Crime_Count"))

top_crimes_by_LSOA = (
    data.groupBy("LSOA_name", "Crime_type")
    .count()
    .withColumnRenamed("count", "Crime_Count")
    .withColumn("rank", F.row_number().over(window_spec))  # Rank within each Year and Month_Num
    # .filter(F.col("rank") <= 5)  # Keep only top 5
    .orderBy("LSOA_name", "Crime_type", col("rank"))
)
top_crimes_by_LSOA.write.format("delta").mode("overwrite").save(f"Tables/reporting_x/top_crimes_by_LSOA")



outcome_type_crime_count_df = (
    data.groupBy("id_falls_within","Final_outcome_type")
    .count()
)
total_crime_count_df=data.groupBy("id_falls_within").count().withColumnRenamed("count","count_total")
perf_of_each_force=outcome_type_crime_count_df.join(total_crime_count_df,["id_falls_within"],"inner")

perf_of_each_force.write.format("delta").mode("overwrite").save(f"Tables/reporting_x/force_final_outcome")


top_locations_by_crime = (
    data.groupBy("Crime_type", "LSOA_code")
    .count()
    .withColumnRenamed("count", "Crime_Count")
    .orderBy("Crime_type", col("Crime_Count").desc())
)
top_locations_by_crime.write.format("delta").mode("overwrite").save(f"Tables/reporting_x/top_locations_by_crime")


crime_density = data.groupBy("Longitude", "Latitude","LSOA_name").count().orderBy(col("count").desc())
crime_density.write.format("delta").mode("overwrite").save(f"Tables/reporting_x/crime_density")


stop_and_search=spark.sql("select * from stage.stop_and_search")
stop_and_search.write.format("delta").mode("overwrite").save(f"Tables/reporting_x/stop_and_search")