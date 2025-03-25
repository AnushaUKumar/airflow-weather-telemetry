# from airflow import DAG
# from airflow.providers.http.hooks.http import HttpHook
# from airflow.providers.postgres.hooks.postgres import PostgresHook
# from airflow.decorators import task
# from airflow.utils.dates import days_ago

# #based on lat and long from a weather API

# LAT= '51.5074'
# LONG= '-0.1278'

# POSTGRES_CONN_ID = "postgres_default"
# API_CONN_ID  = 'open_meteo_api'

# default_args = {
#     'owner' : 'airflow',
#     'start_date': days_ago(1)

# }

# ## create dag

# with DAG(dag_id = 'weather_etl_pipeline',
#          default_arge = default_args,
#          schedule_interval = '@daily',
#         catchup = False) as dags:
#     @task()
#     def extract_weather_data():
#         """Extract weather data from Open - Meteo  API using Aiirflow connection"""
#         #Use HTTP hook to get the weather data
#         http_hook = HTTPHook( http_conn_id = API_CONN_ID , method = 'GET')
        
#         # Build the API endpoint
#         ## https://api.open-meteo.com/v1/forecast?latitude=51.5074&longitude=-0.1278&current_weather=true
#         endpoint=f'/v1/forecast?latitude={LAT}&longitude={LONG}&current_weather=true'

#         response = http_hook.run(endpoint)

#         if response.status_code ==200:
#             return response.json()
#         else:
#             raise Exception(f"Failed to fetch weather data: {response.status_code}")

#     #transforming weather data  
#     @task                 
#     def transform_weather_data(weather_data):
#         """Transform the extracted weather data"""
#         current_weather = weather_data['current_weather']
#         transformed_data = {
#             "latitude" : LAT,
#             "longitude" : LONG,
#             "temperature": current_weather["temperature"],
#             "windspeed":current_weather["windspeed"],
#             "winddirection":current_weather["winddirection"],
#             "weathercode":current_weather["weathercode"]
#         }
#         return transformed_data

#     #pushing data to postgreshook
#     @task
#     def load_weather_data(transformed_data):
#         """Load transformed data into PostgreSQL"""
#         pg_hook = PostgresHook(postgres_conn_id = POSTGRES_CONN_ID)
#         conn = pg_hook.get_conn()
#         cursor = conn.cursor()
#         cursor.execute("""
#         CREATE TABLE IF NOT EXISTS weather_data(
#                     latitude float,
#                     longitude float,
#                     temperature float,
#                     windspeed float,
#                     winddirection float,
#                     weathercode int,
#                     timestamp timestamp default current_timestamp                       
#                 );
#             """)
#         cursor.execute("""
#         Insert into weather_data(latitude,longitude,temperature,windspeed,winddirection,weathercode
#         ) values (%s,%s,%s,%s,%s,%s,%s )
#         """,
#         (
#             transformed_data['latitude'],
#             transformed_data['longitude'],
#             transformed_data['temperature'],
#             transformed_data['windspeed'],
#             transformed_data['winddirection'],
#             transformed_data['weathercode']
#         ) )
#         conn.commit()
#         cursor.close()

#     ##DAG Workflow - ETL pipeline
#     weather_data = extract_weather_data()
#     transformed_data= transform_weather_data()
#     load_weather_data(transformed_data)
from airflow import DAG
from airflow.decorators import task
from airflow.utils.dates import days_ago
from airflow.exceptions import AirflowSkipException
from airflow.providers.http.hooks.http import HttpHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta, timezone
# All 50 states + DC + Puerto Rico with approximate coordinates
US_STATES = [
    {"state": "Alabama", "lat": 32.806671, "lon": -86.791130},
    {"state": "Alaska", "lat": 61.370716, "lon": -152.404419},
    {"state": "Arizona", "lat": 33.729759, "lon": -111.431221},
    {"state": "Arkansas", "lat": 34.969704, "lon": -92.373123},
    {"state": "California", "lat": 36.116203, "lon": -119.681564},
    {"state": "Colorado", "lat": 39.059811, "lon": -105.311104},
    {"state": "Connecticut", "lat": 41.597782, "lon": -72.755371},
    {"state": "Delaware", "lat": 39.318523, "lon": -75.507141},
    {"state": "Florida", "lat": 27.766279, "lon": -81.686783},
    {"state": "Georgia", "lat": 33.040619, "lon": -83.643074},
    {"state": "Hawaii", "lat": 21.094318, "lon": -157.498337},
    {"state": "Idaho", "lat": 44.240459, "lon": -114.478828},
    {"state": "Illinois", "lat": 40.349457, "lon": -88.986137},
    {"state": "Indiana", "lat": 39.849426, "lon": -86.258278},
    {"state": "Iowa", "lat": 42.011539, "lon": -93.210526},
    {"state": "Kansas", "lat": 38.526600, "lon": -96.726486},
    {"state": "Kentucky", "lat": 37.668140, "lon": -84.670067},
    {"state": "Louisiana", "lat": 31.169546, "lon": -91.867805},
    {"state": "Maine", "lat": 44.693947, "lon": -69.381927},
    {"state": "Maryland", "lat": 39.063946, "lon": -76.802101},
    {"state": "Massachusetts", "lat": 42.230171, "lon": -71.530106},
    {"state": "Michigan", "lat": 43.326618, "lon": -84.536095},
    {"state": "Minnesota", "lat": 45.694454, "lon": -93.900192},
    {"state": "Mississippi", "lat": 32.741646, "lon": -89.678696},
    {"state": "Missouri", "lat": 38.456085, "lon": -92.288368},
    {"state": "Montana", "lat": 46.921925, "lon": -110.454353},
    {"state": "Nebraska", "lat": 41.125370, "lon": -98.268082},
    {"state": "Nevada", "lat": 38.313515, "lon": -117.055374},
    {"state": "New Hampshire", "lat": 43.452492, "lon": -71.563896},
    {"state": "New Jersey", "lat": 40.298904, "lon": -74.521011},
    {"state": "New Mexico", "lat": 34.840515, "lon": -106.248482},
    {"state": "New York", "lat": 42.165726, "lon": -74.948051},
    {"state": "North Carolina", "lat": 35.630066, "lon": -79.806419},
    {"state": "North Dakota", "lat": 47.528912, "lon": -99.784012},
    {"state": "Ohio", "lat": 40.388783, "lon": -82.764915},
    {"state": "Oklahoma", "lat": 35.565342, "lon": -96.928917},
    {"state": "Oregon", "lat": 44.572021, "lon": -122.070938},
    {"state": "Pennsylvania", "lat": 40.590752, "lon": -77.209755},
    {"state": "Rhode Island", "lat": 41.680893, "lon": -71.511780},
    {"state": "South Carolina", "lat": 33.856892, "lon": -80.945007},
    {"state": "South Dakota", "lat": 44.299782, "lon": -99.438828},
    {"state": "Tennessee", "lat": 35.747845, "lon": -86.692345},
    {"state": "Texas", "lat": 31.054487, "lon": -97.563461},
    {"state": "Utah", "lat": 40.150032, "lon": -111.862434},
    {"state": "Vermont", "lat": 44.045876, "lon": -72.710686},
    {"state": "Virginia", "lat": 37.769337, "lon": -78.169968},
    {"state": "Washington", "lat": 47.400902, "lon": -121.490494},
    {"state": "West Virginia", "lat": 38.491226, "lon": -80.954453},
    {"state": "Wisconsin", "lat": 44.268543, "lon": -89.616508},
    {"state": "Wyoming", "lat": 42.755966, "lon": -107.302490},
    {"state": "District of Columbia", "lat": 38.897438, "lon": -77.026817},
    {"state": "Puerto Rico", "lat": 18.220833, "lon": -66.590149}
]

POSTGRES_CONN_ID = "postgres_default"
API_CONN_ID = 'open_meteo_api'

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1)
}

with DAG(dag_id='us_weather_etl_pipeline',
         default_args=default_args,
         schedule_interval='@daily',
         catchup=False,
         tags=["weather", "USA"]) as dag:
    @task()
    def flatten(list_of_lists):
        flat = [item for sublist in list_of_lists for item in sublist]
        print(f"🟢 Flattened {len(flat)} records")
        return flat


    @task()
    def extract_weather(state_info):
        import time
        import random
        

        try:
            time.sleep(random.uniform(0.2, 1.0))  # Avoid rate-limiting
            http = HttpHook(http_conn_id=API_CONN_ID, method='GET')
            endpoint = f"/v1/forecast?latitude={state_info['lat']}&longitude={state_info['lon']}&current_weather=true"
            response = http.run(endpoint)

            if response.status_code != 200:
                raise ValueError(f"HTTP {response.status_code}: {response.text}")

            data = response.json()
            if 'current_weather' not in data:
                raise AirflowSkipException(f"No 'current_weather' data for {state_info['state']}")

            data["state"] = state_info["state"]
            data["latitude"] = state_info["lat"]
            data["longitude"] = state_info["lon"]

            return data

        except Exception as e:
            print(f"Error for {state_info['state']}: {e}")
            raise

    @task()
    def transform_multiple_days(data):
        current = data["current_weather"]
        base_record = {
            "state": data["state"],
            "latitude": data["latitude"],
            "longitude": data["longitude"],
            "temperature": current["temperature"],
            "windspeed": current["windspeed"],
            "winddirection": current["winddirection"],
            "weathercode": current["weathercode"]
        }

        # Simulate records for the past 5 days
        records = []
        for i in range(5):
            record = base_record.copy()
            record["timestamp"] = datetime.now(timezone.utc) - timedelta(days=i)
            records.append(record)
        return records


    @task()
    def load(record):
        pg = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = pg.get_conn()
        cur = conn.cursor()

        cur.execute("""
            CREATE TABLE IF NOT EXISTS us_weather_data (
                state TEXT,
                latitude FLOAT,
                longitude FLOAT,
                temperature FLOAT,
                windspeed FLOAT,
                winddirection FLOAT,
                weathercode INT,
                timestamp TIMESTAMP
            );
        """)

        try:
            cur.execute("""
                INSERT INTO us_weather_data(state, latitude, longitude, temperature, windspeed, winddirection, weathercode, timestamp)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
            """, (
                record["state"],
                float(record["latitude"]),
                float(record["longitude"]),
                float(record["temperature"]),
                float(record["windspeed"]),
                float(record["winddirection"]),
                int(record["weathercode"]),
                record["timestamp"]
            ))
            print(f"✅ Inserted for {record['state']} @ {record['timestamp']}")
        except Exception as e:
            print(f"❌ Error inserting data for {record['state']} @ {record['timestamp']}: {e}")

        conn.commit()
        cur.close()
    raw_data = extract_weather.expand(state_info=US_STATES)
    transformed_multi = transform_multiple_days.expand(data=raw_data)
    flattened = flatten(transformed_multi)
    load.expand(record=flattened)
