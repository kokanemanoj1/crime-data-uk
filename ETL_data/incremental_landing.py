from pyspark.sql import SparkSession
import sys
from pyspark.sql import functions as F
import json
from pyspark.sql.functions import input_file_name
# Create a SparkSession
spark = SparkSession.builder \
    .appName("Landing") \
    .getOrCreate()
args=sys.argv
print(args)
job_req = json.loads(args[1])
storage_account_key=job_req['STORAGE_ACCOUNT_KEY']
print(storage_account_key)
spark.conf.set("fs.azure.account.key.ukpolicedata.blob.core.windows.net", storage_account_key)
period=job_req['PERIOD']
source_type=job_req['SOURCE_TYPE']


def load_data_for_period(period,source_type):
    """
    Function to load UK crime data for a specific period.

    :param period: String in the format "YYYY-MM"
    :return: Spark DataFrame with the loaded data
    """
    base_path = f"wasbs://input@ukpolicedata.blob.core.windows.net/{period}/*{source_type}.csv"  # Update this to your directory structure
    df=None

    folder_path = base_path
    try:
        df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load(folder_path, header=True, inferSchema=True).withColumn("file_name", input_file_name())
    except Exception as e:
        print(e)

    return df



landing_data = load_data_for_period(period,source_type)


# Show the first few rows of the loaded data
landing_data.count()

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
