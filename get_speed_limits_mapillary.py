
# Online Python - IDE, Editor, Compiler, Interpreter

import requests
import json

# Mapillary API credentials
ACCESS_TOKEN = ""

# Function to calculate bounding box
def get_bounding_box(lat1, lon1, lat2, lon2):
    return min(lon1, lon2), min(lat1, lat2), max(lon1, lon2), max(lat1, lat2)

# Function to get speed limits from Mapillary
def get_speed_limits(lat1, lon1, lat2, lon2):
    min_lon, min_lat, max_lon, max_lat = get_bounding_box(lat1, lon1, lat2, lon2)
    url = f"https://graph.mapillary.com/map_features?access_token={ACCESS_TOKEN}&fields=id,object_value,aligned_direction,geometry&bbox={min_lon},{min_lat},{max_lon},{max_lat}&layers=trafficsigns"
    
    response = requests.get(url)
    
    if response.status_code == 200:
        data = response.json()

        # with open('./traffic_sign_response_data.json', 'w') as json_file:
        #     json.dump(data, json_file, indent=4)

        speed_limits = [
            item for item in data.get("data", [])
            if "maximum-speed-limit" in item.get("object_value", "")
        ]
        
        return speed_limits
    else:
        print(f"Error fetching data: {response.status_code}, {response.text}")
        return []

# Example: Start and End GPS Coordinates
start_lat, start_lon = 29.715858, -95.745292  # Example start location - parent's house
end_lat, end_lon = 29.789550, -95.774188  # Example end location - katy asia town

# Get speed limits in the driving path
speed_limits = get_speed_limits(start_lat, start_lon, end_lat, end_lon)

# Print results
for sign in speed_limits:
    coord = sign["geometry"]["coordinates"]
    limit_value = sign["object_value"]
    print(f"Speed Limit: {limit_value}, Location: {coord}")