
from pyspark.sql import functions as F
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

spark = SparkSession.builder \
    .appName("Staging") \
    .getOrCreate()

period_df=spark.sql("select * from dimension.period ")
force_df=spark.sql("select * from dimension.force ")

def load_stage_stop_and_search():

    landing_sas_df=spark.sql("select * from landing.STOP_AND_SEARCH ")
    landing_sas_df_dim = landing_sas_df.join(
        period_df,
        (landing_sas_df['Date'] >= period_df['start_date']) & (landing_sas_df['Date'] <= period_df['end_date']),
        'left'
    ).select(landing_sas_df["*"], period_df["period_id"])

    landing_sas_df_dim=landing_sas_df_dim.withColumn("Force",F.split(F.split(F.col("file_name"),".csv")[0],"/")[4])


    def match_id(force, ids):
        for id_val in ids:
            if id_val in force:
                return id_val
        return None

    ids_list = [row["id"] for row in force_df.collect()]

    # Register the UDF
    match_id_udf = udf(lambda force: match_id(force, ids_list), StringType())

    landing_sas_df_dim=landing_sas_df_dim.withColumn("force_id", match_id_udf(landing_sas_df_dim["force"]))

    try:
        stage_sas_df=spark.sql("select * from stage.STOP_AND_SEARCH")
    except:
        print("table does not exists")
        first_stage_sas=landing_df_dim.drop('file_name', 'Force')
        first_stage_sas.write.format("delta").mode("overwrite").save("Tables/stage/STOP_AND_SEARCH")
    else:
        # Step 1: Filter rows in staging that are NOT in landing
        stage_sas_only_df = stage_sas_df.join(landing_sas_df_dim, on=["period_id", "force_id"], how="left_anti")

        # Step 2: Union with landing data
        stage_sas_write_df = stage_sas_only_df.union(landing_sas_df_dim.select(stage_sas_only_df.columns))

        # # Show the final result
        # display(stage_sas_write_df)
        stage_sas_write_df.write.format("delta").mode("overwrite").save("Tables/stage/STOP_AND_SEARCH")


def load_stage_outcomes():

    landing_outcomes_df=spark.sql("select * from landing.OUTCOMES")
    landing_outcomes_df_dim = landing_outcomes_df.join(
        period_df,
        (landing_outcomes_df['Month'] >= period_df['start_date']) & (landing_outcomes_df['Month'] <= period_df['end_date']),
        'left'
    ).select(landing_outcomes_df["*"], period_df["period_id"]).drop("period_name","start_date","end_date")

    landing_outcomes_df_dim2=landing_outcomes_df_dim.join(
        force_df,
        (landing_outcomes_df_dim['Reported_by'] == force_df['name']),
        'left'
    ).select(landing_outcomes_df_dim["*"], force_df["id"]).withColumnRenamed("id","id_Reported_By").drop("name")

    print(landing_outcomes_df_dim2.columns)
    print(landing_outcomes_df_dim2.select(landing_outcomes_df_dim2["*"]))
    # landing_outcomes_df_dim
    landing_outcomes_df_dim3=landing_outcomes_df_dim2.join(
        force_df,
        (landing_outcomes_df_dim2['Falls_within'] == force_df['name']),
        'left'
    ).select(landing_outcomes_df_dim2["*"], "id").withColumnRenamed("id","id_Falls_within").drop("name","file_name","Reported_by","Falls_within")
    print(landing_outcomes_df_dim3.count())

    try:
        stage_outcomes_df=spark.sql("select * from stage.OUTCOMES")
    except:
        print("table does not exists")
        first_stage_outcomes=landing_outcomes_df_dim3
        first_stage_outcomes.write.format("delta").mode("overwrite").save("Tables/stage/OUTCOMES")
    else:
        # Step 1: Filter rows in staging that are NOT in landing
        stage_outcomes_only_df = stage_outcomes_df.join(landing_outcomes_df_dim3, on=["period_id", "id_Reported_By","id_Falls_within"], how="left_anti")

        # Step 2: Union with landing data
        stage_outcomes_write_df = stage_outcomes_only_df.union(landing_outcomes_df_dim3.select(stage_outcomes_only_df.columns))

        # # Show the final result
        # display(stage_sas_write_df)
        stage_outcomes_write_df.write.format("delta").mode("overwrite").save("Tables/stage/OUTCOMES")


def load_stage_street():
    landing_street_df=spark.sql("select * from landing.STREET")
    landing_street_df_dim = landing_street_df.join(
        period_df,
        (landing_street_df['Month'] >= period_df['start_date']) & (landing_street_df['Month'] <= period_df['end_date']),
        'left'
    ).select(landing_street_df["*"], period_df["period_id"]).drop("period_name","start_date","end_date")

    landing_street_df_dim2=landing_street_df_dim.join(
        force_df,
        (landing_street_df_dim['Reported_by'] == force_df['name']),
        'left'
    ).select(landing_street_df_dim["*"], force_df["id"]).withColumnRenamed("id","id_Reported_By").drop("name")

    print(landing_street_df_dim2.columns)
    print(landing_street_df_dim2.select(landing_street_df_dim2["*"]))
    # landing_outcomes_df_dim
    landing_street_df_dim3=landing_street_df_dim2.join(
        force_df,
        (landing_street_df_dim2['Falls_within'] == force_df['name']),
        'left'
    ).select(landing_street_df_dim2["*"], "id").withColumnRenamed("id","id_Falls_within").drop("name","file_name","Reported_by","Falls_within")
    print(landing_street_df_dim3.count())

    try:
        stage_street_df=spark.sql("select * from stage.STREET")
    except:
        print("table does not exists")
        first_stage_street=landing_street_df_dim3.filter(F.col("crime_id").isNotNull())
        first_stage_street.write.format("delta").mode("overwrite").save("Tables/stage/STREET")
    else:
        # Step 1: Filter rows in staging that are NOT in landing
        stage_street_only_df = stage_street_df.join(landing_street_df_dim3, on=["period_id", "id_Reported_By","id_Falls_within"], how="left_anti")

        # Step 2: Union with landing data
        stage_street_write_df = stage_street_only_df.union(landing_street_df_dim3.select(stage_street_only_df.columns))

        # # Show the final result
        # display(stage_sas_write_df)
        stage_street_write_df.filter(F.col("crime_id").isNotNull()).write.format("delta").mode("overwrite").save("Tables/stage/STREET")


load_stage_stop_and_search()
load_stage_outcomes()
load_stage_street()