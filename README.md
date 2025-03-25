# Weather Telemetry ETL Using Airflow and Postgres

A distributed ETL pipeline that collects, transforms, and stores real-time weather telemetry for all 50 U.S. states, Washington D.C., and Puerto Rico. This pipeline uses **Apache Airflow** for orchestration, **Open-Meteo API** for weather data, and **PostgreSQL** for storage. It simulates multi-day weather ingestion for advanced time-series analysis in **Tableau** (integration in progress).
---

## Features

- **Dynamic Task Mapping** with Airflow 2.3+ to scale across 52 states
- **Multi-day Simulation** per state for time-series readiness
- Uses [Open-Meteo](https://open-meteo.com/) for real-time current weather data
- Geographic coverage: 50 U.S. states + D.C. + Puerto Rico
- Data warehousing with PostgreSQL
- Tableau Dashboard (work in progress): trends, alerts, and regional analysis

---

## Tech Stack

| Layer         | Tools & Services               |
|---------------|--------------------------------|
| Orchestration | Apache Airflow                 |
| API Client    | Open-Meteo (HTTPS)             |
| Database      | PostgreSQL                     |
| Language      | Python (Airflow DAGs)          |
| Visualization | Tableau (Work in Progress)     |

---

## DAG Structure

```text
extract_weather (mapped) --> transform_multiple_days (mapped)
                                  ↓
                             flatten (list)
                                    ↓
                              load (mapped)
```
- extract_weather: Retrieves real-time weather for each U.S. state using Airflow's HttpHook.

- transform_multiple_days: Simulates the last 5 days of weather records per state.

- flatten: Combines nested lists from all states into a single iterable.

- load: Inserts each record into the us_weather_data table in PostgreSQL.


## Setup Instructions

### 1. Clone the Repository
```bash
git clone https://github.com/your-username/distributed-weather-etl.git
cd distributed-weather-etl
```

### 2. Airflow Setup
In the Airflow UI:
 - HTTP Connection: open_meteo_api
   - Host: https://api.open-meteo.com

 - Postgres Connection: postgres_default
   - Match credentials to your PostgreSQL instance.

### 3. Run the DAG
- Trigger us_weather_etl_pipeline manually or schedule it to run daily
- Monitor task mapping and logs in the Airflow UI

### 4. View Data
Connect to your PostgreSQL instance and run:
```bash
SELECT * FROM us_weather_data ORDER BY timestamp DESC LIMIT 10;
```
## Tableau Dashboards

building Tableau dashboards to visualize:
- Weather trends by U.S. region (Midwest, West, South, etc.)
- Metric over time: temperature, windspeed, direction
- High wind or abnormal temperature alerts
- Day-wise aggregation using moving averages

These dashboards will enable interactive filtering, regional drilldowns, and time-series visual analysis for stakeholders.

## Output Schema (us_weather_data)

| Column        | Type      | Description                           |
|---------------|-----------|---------------------------------------|
| `state`       | TEXT      | U.S. state name                       |
| `latitude`    | FLOAT     | Latitude of the state centroid        |
| `longitude`   | FLOAT     | Longitude of the state centroid       |
| `temperature` | FLOAT     | Temperature in Celsius                |
| `windspeed`   | FLOAT     | Windspeed in km/h                     |
| `winddirection` | FLOAT   | Wind direction in degrees             |
| `weathercode` | INT       | Weather code from Open-Meteo          |
| `timestamp`   | TIMESTAMP | Time of record (UTC, simulated 5-day) |

## Real-World Use Cases
- Emergency forecasting & climate alert systems
- Operations teams planning around adverse weather
- Logistics route planning based on regional wind/temperature trends
- Public dashboards for weather anomaly tracking

