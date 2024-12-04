from pyspark.sql import SparkSession
import sys
from pyspark.sql import functions as F
import json
from pyspark.sql.functions import input_file_name
# Create a SparkSession
spark = SparkSession.builder \
    .appName("Landing") \
    .getOrCreate()


def load_data_for_period(period,source_type):
    """
    Function to load UK crime data for a specific period.

    :param period: String in the format "YYYY-MM"
    :return: Spark DataFrame with the loaded data
    """
    base_path = f"wasbs://input@ukpolicedata.blob.core.windows.net/input_data/{period}/*{source_type}.csv"  # Update this to your directory structure
    df=None

    folder_path = base_path
    try:
        df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(folder_path, header=True, inferSchema=True).withColumn("file_name", input_file_name())
    except Exception as e:
        print(e)

    return df

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



args=sys.argv
print(args)
job_req = json.loads(args[1])
storage_account_key=job_req['STORAGE_ACCOUNT_KEY']
print(storage_account_key)
spark.conf.set("fs.azure.account.key.ukpolicedata.blob.core.windows.net", storage_account_key)
# period=job_req['PERIOD']
# source_type=job_req['SOURCE_TYPE']


period_list=[ item[0] for item in spark.sql("select distinct period_id from dimension.period order by period_id asc").collect()]
print(period_list)
sources=["stop-and-search","outcomes","stop-and-search"]

for period_data in period_list:
    period=period_data
    print(period)
    for sources_data in sources:
        source_type=sources_data
        print(source_type)
        landing_data = load_data_for_period(period,source_type)

        Column_rename_mapping_sas={
            'Type': 'Type',
            'Date': 'Date',
            'Part of a policing operation': 'Part_of_a_policing_operation',
            'Policing operation': 'Policing_operation',
            'Latitude': 'Latitude',
            'Longitude': 'Longitude',
            'Gender': 'Gender',
            'Age range': 'Age_range',
            'Self-defined ethnicity': 'Self_defined_ethnicity',
            'Officer-defined ethnicity': 'Officer_defined_ethnicity',
            'Legislation': 'Legislation',
            'Object of search': 'Object_of_search',
            'Outcome': 'Outcome',
            'Outcome linked to object of search': 'Outcome_linked_to_object_of_search',
            'Removal of more than just outer clothing': 'Removal_of_more_than_just_outer_clothing',
            'file_name':'file_name'
        }
        Column_rename_mapping_outcomes={'Crime ID': 'Crime_ID', 'Month': 'Month', 'Reported by': 'Reported_by', 'Falls within': 'Falls_within', 'Longitude': 'Longitude', 'Latitude': 'Latitude', 'Location': 'Location', 'LSOA code': 'LSOA_code', 'LSOA name': 'LSOA_name', 'Outcome type': 'Outcome_type', 'file_name': 'file_name'}
        Column_rename_mapping_street={'Crime ID': 'Crime_ID', 'Month': 'Month', 'Reported by': 'Reported_by', 'Falls within': 'Falls_within', 'Longitude': 'Longitude', 'Latitude': 'Latitude', 'Location': 'Location', 'LSOA code': 'LSOA_code', 'LSOA name': 'LSOA_name', 'Crime type': 'Crime_type', 'Last outcome category': 'Last_outcome_category', 'Context': 'Context', 'file_name': 'file_name'}

        if source_type=="stop-and-search":
            Column_rename_mapping=Column_rename_mapping_sas
            landing_table_name="STOP_AND_SEARCH"
        elif source_type=="outcomes":
            Column_rename_mapping=Column_rename_mapping_outcomes
            landing_table_name="OUTCOMES"
        elif source_type=="street":
            Column_rename_mapping=Column_rename_mapping_street
            landing_table_name="STREET"
        landing_data=landing_data.select(
            [F.col(old_name).alias(new_name) if old_name in Column_rename_mapping else F.col(old_name)
             for old_name, new_name in Column_rename_mapping.items()])
        landing_data.write.format("delta").mode("overwrite").save(f"Tables/landing/{landing_table_name}")
    period_df=spark.sql("select * from dimension.period ")
    force_df=spark.sql("select * from dimension.force ")
    load_stage_stop_and_search()
    load_stage_outcomes()
    load_stage_street()