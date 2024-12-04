from datetime import datetime, timedelta
import pandas as pd

# Define the start and end period
start_year = 2015
current_year = datetime.now().year
current_month = datetime.now().month

# Generate a list of period IDs, names, start dates, and end dates
periods = []
current_date = datetime(start_year, 1, 1)

while current_date.year < current_year or (current_date.year == current_year and current_date.month <= current_month):
    period_id = current_date.strftime("%Y-%m")  # Format as YYYY-MM
    period_name = current_date.strftime("%B %Y")
    start_date = current_date
    # Calculate the last day of the month
    next_month = current_date.replace(day=28) + timedelta(days=4)  # Ensure we're in the next month
    end_date = next_month - timedelta(days=next_month.day)
    periods.append({
        "period_id": period_id,
        "period_name": period_name,
        "start_date": start_date,
        "end_date": end_date.replace(hour=23, minute=59, second=59)
    })
    # Move to the next month
    current_date += timedelta(days=32)  # Jump to the next month
    current_date = current_date.replace(day=1)


period_df = pd.DataFrame(periods)


print(period_df)

# Write to Spark DataFrame and save as Delta format
spark.createDataFrame(period_df).write.format("delta").mode("overwrite").save("Tables/dimension/period")
