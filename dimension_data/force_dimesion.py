import requests
import pandas as pd
# API endpoint
url = "https://data.police.uk/api/forces"



# Make the GET request
response = requests.get(url, params=params)

# Check the response status code
if response.status_code == 200:
    # Parse the response JSON
    data = response.json()
    print("API Response:", data)
else:
    print(f"Failed to fetch data. Status code: {response.status_code}")
forces_df = pd.DataFrame(data)
# display(spark.createDataFrame(forces_df))
spark.createDataFrame(forces_df).write.format("delta").mode("overwrite").save("Tables/dimension/force")