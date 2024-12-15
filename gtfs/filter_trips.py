import psycopg2
import pandas as pd
from tqdm import tqdm
import os

# Ensure 'filtered' directory exists
output_dir = "filtered"
os.makedirs(output_dir, exist_ok=True)

# Define connection details
conn = psycopg2.connect(
    host="",
    database="",
    user="",
    password=""
)

# Define the desired time range for filtering
start_time = '08:00:00'
end_time = '11:00:00'

# Query to filter stop_times based on the specified time range
stop_times_query = f"""
SELECT *
FROM stop_times
WHERE arrival_time BETWEEN '{start_time}' AND '{end_time}'
   OR departure_time BETWEEN '{start_time}' AND '{end_time}'
"""

# Execute the query and load the results into a DataFrame
print("Querying filtered stop times from PostgreSQL...")
filtered_stop_times_df = pd.read_sql_query(stop_times_query, conn)

# Display progress while loading
for _ in tqdm(range(len(filtered_stop_times_df)), desc="Loading data"):
    pass  # Simulate a progress bar as the data is already loaded

# Extract unique trip_ids
filtered_trip_ids = filtered_stop_times_df['trip_id'].unique()
print(f"Found {len(filtered_trip_ids)} unique trip_ids within the specified time range.")

# Save the filtered stop_times DataFrame to stop_times.txt in the filtered directory
filtered_stop_times_df.to_csv(os.path.join(output_dir, "stop_times.txt"), index=False)
print("stop_times.txt file created successfully!")

# Define and save trips.txt
trips_query = "SELECT * FROM trips WHERE trip_id IN %s"
filtered_trips_df = pd.read_sql_query(trips_query, conn, params=(tuple(filtered_trip_ids),))
filtered_trips_df.to_csv(os.path.join(output_dir, "trips.txt"), index=False)
print("trips.txt file created successfully!")

# Get unique route_ids and service_ids for filtering routes and calendars
filtered_route_ids = filtered_trips_df['route_id'].unique()
filtered_service_ids = filtered_trips_df['service_id'].unique()

# Define and save routes.txt
routes_query = "SELECT * FROM routes WHERE route_id IN %s"
filtered_routes_df = pd.read_sql_query(routes_query, conn, params=(tuple(filtered_route_ids),))
filtered_routes_df.to_csv(os.path.join(output_dir, "routes.txt"), index=False)
print("routes.txt file created successfully!")

# Define and save stops.txt
# we shouldnt edit this
# stop_ids = filtered_stop_times_df['stop_id'].unique()
# stops_query = "SELECT * FROM stops WHERE stop_id IN %s"
# filtered_stops_df = pd.read_sql_query(stops_query, conn, params=(tuple(stop_ids),))
# filtered_stops_df.to_csv(os.path.join(output_dir, "stops.txt"), index=False)
# print("stops.txt file created successfully!")

# Define and save calendar.txt with correct date format
calendar_query = "SELECT * FROM calendar WHERE service_id IN %s"
filtered_calendar_df = pd.read_sql_query(calendar_query, conn, params=(tuple(filtered_service_ids),))

# Format start_date and end_date to YYYYMMDD
filtered_calendar_df['start_date'] = pd.to_datetime(filtered_calendar_df['start_date']).dt.strftime('%Y%m%d')
filtered_calendar_df['end_date'] = pd.to_datetime(filtered_calendar_df['end_date']).dt.strftime('%Y%m%d')

filtered_calendar_df.to_csv(os.path.join(output_dir, "calendar.txt"), index=False)
print("calendar.txt file created successfully!")

# Define and save calendar_dates.txt with correct date format
calendar_dates_query = "SELECT * FROM calendar_dates WHERE service_id IN %s"
filtered_calendar_dates_df = pd.read_sql_query(calendar_dates_query, conn, params=(tuple(filtered_service_ids),))

# Format date to YYYYMMDD
filtered_calendar_dates_df['date'] = pd.to_datetime(filtered_calendar_dates_df['date']).dt.strftime('%Y%m%d')

filtered_calendar_dates_df.to_csv(os.path.join(output_dir, "calendar_dates.txt"), index=False)
print("calendar_dates.txt file created successfully!")

# Define and save shapes.txt (if applicable)
shape_ids = filtered_trips_df['shape_id'].dropna().unique()
if len(shape_ids) > 0:
    shapes_query = "SELECT * FROM shapes WHERE shape_id IN %s"
    filtered_shapes_df = pd.read_sql_query(shapes_query, conn, params=(tuple(shape_ids),))
    filtered_shapes_df.to_csv(os.path.join(output_dir, "shapes.txt"), index=False)
    print("shapes.txt file created successfully!")
# Close the database connection
conn.close()