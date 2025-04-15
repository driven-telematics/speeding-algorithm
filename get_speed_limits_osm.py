import requests
import json

# Overpass API endpoint
OVERPASS_URL = ""

# Function to query Overpass API for road segments near a GPS coordinate
def get_road_segments(lat, lon, radius=1000):
    query = f"""
    [out:json];
    way(around:{radius},{lat},{lon})[highway];
    out geom;
    """
    
    response = requests.get(OVERPASS_URL, params={"data": query})
    
    if response.status_code == 200:
        data = response.json()
        with open('./osm_speed_response_data.json', 'w') as json_file:
            json.dump(data, json_file, indent=4)

        road_segments = []
        
        for element in data.get("elements", []):
            road = {
                "id": element["id"],
                "name": element["tags"].get("name", "Unnamed Road"),
                "speed_limit": element["tags"].get("maxspeed", "Unknown"),
                "geometry": element.get("geometry", [])
            }
            road_segments.append(road)
        
        return road_segments
    else:
        print(f"Error fetching data: {response.status_code}")
        return []

lat, lon = 29.715858, -95.745292  # Example start location - parent's house
# lat, lon = 34.854317, -82.241088 # returns speed limit in South Carolina
road_segments = get_road_segments(lat, lon)

# Print results
for road in road_segments:
    print(f"Road: {road['name']}, Speed Limit: {road['speed_limit']}")
    print(f"Geometry: {road['geometry']}\n")
