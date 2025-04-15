import requests
import json
from geopy.distance import geodesic

# API Credentials
MAPILLARY_ACCESS_TOKEN = ""
OVERPASS_URL = ""

# Function to query Overpass API for road segments near a GPS coordinate
def get_road_segments(lat, lon, radius=50):
    query = f"""
    [out:json];
    way(around:{radius},{lat},{lon})[highway];
    out geom;
    """
    response = requests.get(OVERPASS_URL, params={"data": query})
    if response.status_code == 200:
        return response.json().get("elements", [])
    return []

# Function to find the nearest road segment
def find_nearest_road(lat, lon, road_segments):
    user_location = (lat, lon)
    closest_road = None
    min_distance = float("inf")
    for road in road_segments:
        if "geometry" in road:
            for point in road["geometry"]:
                road_point = (point["lat"], point["lon"])
                distance = geodesic(user_location, road_point).meters
                if distance < min_distance:
                    min_distance = distance
                    speed_limit = road["tags"].get("maxspeed", "Unknown")
                    if speed_limit != "Unknown" and " mph" in speed_limit:
                        speed_limit = float(speed_limit.replace(" mph", ""))
                    closest_road = {
                        "road_name": road["tags"].get("name", "Unnamed Road"),
                        "road_type": road["tags"].get("highway", "Unknown"),
                        "speed_limit": speed_limit,
                        "distance_meters": round(min_distance, 2),
                        "nearest_point": road_point
                    }
    return closest_road

# Function to get speed limits from Mapillary if OSM data is missing
def get_mapillary_speed_limits(lat, lon):
    bbox = f"{lon-0.0001},{lat-0.0001},{lon+0.0001},{lat+0.0001}"
    url = f"https://graph.mapillary.com/map_features?access_token={MAPILLARY_ACCESS_TOKEN}&fields=id,object_value,geometry&bbox={bbox}&layers=trafficsigns"
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json().get("data", [])
        for item in data:
            if "maximum-speed-limit" in item.get("object_value", ""):
                return item["object_value"].split("-")[-2]  # Extract speed limit value
    return "Unknown"

# Function to process the input file and detect speeding events
def process_data_file(input_file):
    with open(input_file, "r") as file:
        data = file.read().strip().split("|")
        for row in data:
            lat, lon, distracted, traveling_speed, timestamp = row.split(",")
            lat, lon, traveling_speed = float(lat), float(lon), float(traveling_speed)
            
            road_segments = get_road_segments(lat, lon)
            nearest_road = find_nearest_road(lat, lon, road_segments)
            
            if nearest_road:
                speed_limit = nearest_road["speed_limit"]
                if speed_limit == "Unknown":
                    print(f"Nearest Road Speed Limit - Mapillary: {speed_limit}")
                    speed_limit = get_mapillary_speed_limits(lat, lon)
                
                speeding = traveling_speed > speed_limit if isinstance(speed_limit, float) else "Unknown"
                
                print(f"Timestamp: {timestamp}")
                print(f"📍 GPS Location: {lat}, {lon}")
                print(f"🛣️ Road: {nearest_road['road_name']} ({nearest_road['road_type']})")
                print(f"🚦 Speed Limit: {speed_limit} mph")
                print(f"🚗 Traveling Speed: {traveling_speed} mph")
                print(f"⚠️ Speeding: {'Yes' if speeding == True else 'No' if speeding == False else 'Unknown'}\n")

# Run the script with an example file
# process_data_file("./JameyTrips/trial_1.txt")
process_data_file("./JameyTrips/debug_trial_1.txt")
