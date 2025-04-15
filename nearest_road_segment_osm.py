import json
from geopy.distance import geodesic

# Load OSM data from the Overpass API response file
with open("./osm_speed_response_data.json", "r") as file:
    osm_data = json.load(file)

# Sample user GPS location (Replace with actual live GPS data)
user_gps = (29.735230, -95.734709)  # Example lat, lon

# Function to find the nearest road segment
def find_nearest_road(user_location, osm_data):
    closest_road = None
    min_distance = float("inf")

    for element in osm_data.get("elements", []):
        if element["type"] == "way" and "geometry" in element:
            road_name = element["tags"].get("name", "Unnamed Road")
            highway_type = element["tags"].get("highway", "Unknown")
            speed_limit = element["tags"].get("maxspeed", "Unknown")

            for point in element["geometry"]:
                road_point = (point["lat"], point["lon"])
                distance = geodesic(user_location, road_point).meters  # Calculate distance in meters

                if distance < min_distance:
                    min_distance = distance
                    closest_road = {
                        "road_name": road_name,
                        "highway_type": highway_type,
                        "speed_limit": speed_limit,
                        "distance_meters": round(min_distance, 2),
                        "nearest_point": road_point
                    }

    return closest_road

# Find and print the nearest road segment
nearest_road = find_nearest_road(user_gps, osm_data)

if nearest_road:
    print(f"📍 Nearest Road: {nearest_road['road_name']}")
    print(f"🛣️ Road Type: {nearest_road['highway_type']}")
    print(f"🚦 Speed Limit: {nearest_road['speed_limit']}")
    print(f"📏 Distance to Road: {nearest_road['distance_meters']} meters")
    print(f"🗺️ Nearest Road Point: {nearest_road['nearest_point']}")
else:
    print("No road found near this location.")
