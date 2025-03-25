import folium
from folium.plugins import HeatMap
import pandas as pd

# Load your weather data from PostgreSQL or a CSV
df = pd.read_csv("us_weather_data.csv")  # should contain lat, lon, temperature

# Create base map
m = folium.Map(location=[39.5, -98.35], zoom_start=4)

# Prepare heat data (lat, lon, weight)
heat_data = [[row['latitude'], row['longitude'], row['temperature']] for index, row in df.iterrows()]

# Add HeatMap layer
HeatMap(heat_data, min_opacity=0.4, radius=25, blur=15, max_zoom=1).add_to(m)

# Save to HTML
m.save("us_weather_heatmap.html")
