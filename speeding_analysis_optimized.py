import requests
import json
from geopy.distance import geodesic
from datetime import datetime

### WHAT IS THIS ALGO DOING
"""
Input:
    - Raw Driving Data File (idk the extension)
    - lat, long, distracted (0/1), travelling speed, timestamp |
    - | seperates data points
Functionality
    1. Determines min lat, min long, max lat, max long across all data point ✅ 
    2. Create Bounding Box with points ✅
    3. Make API call to OSM using Bounding Box ✅
        - Returns List of Road Segments 
        - Id, Road Name, Road Type, Geometry[lat, long], Speed if available
            - Geometry are points that make up road shape
    4. Make API call to Mapillary using Bounding Box
        - Retrieves all traffic signs
        - Returns Filtered list of only speed limit signs
    5. Traverse Through Data Input File
        1. Find the Nearest Road Segment
            - Using lat, long & OSM Road Segment,
            finds the nearest road by comparing user lat, long
            to every Road Segment's Geometry (🆘 could be heavy operation)
            - Returns: closest_road {
                "id": road["id"],
                "road_name": road["tags"].get("name", "Unnamed Road"),
                "road_type": road["tags"].get("highway", "Unknown"),
                "speed_limit": speed_limit,
                "distance_meters": round(min_distance, 2),
                "nearest_point": road_point # Nearest Road Segment GPS Point
            }
        2. Get Speed Limit Available from OSM, if not find nearest speed sign
        from Mapillary Data
        3. Compare Travelling Speed to Speed Limit
"""

# API Credentials
MAPILLARY_ACCESS_TOKEN = ""
OVERPASS_URL = ""

def convert_timestamp(timestamp):
    timestamp = int(timestamp)
    return datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d %H:%M:%S')

# Function to query Overpass API for road segments within bounding box
def get_road_segments(lat_min, lon_min, lat_max, lon_max):
    query = f"""
    [out:json];
    way({lat_min},{lon_min},{lat_max},{lon_max})[highway];
    out geom;
    """
    response = requests.get(OVERPASS_URL, params={"data": query})
    
    if response.status_code == 200:
        return response.json().get("elements", [])
    return []

# Function to get speed limits from Mapillary within bounding box
def get_mapillary_speed_limits(lat_min, lon_min, lat_max, lon_max):
    bbox = f"{lon_min},{lat_min},{lon_max},{lat_max}"
    url = f"https://graph.mapillary.com/map_features?access_token={MAPILLARY_ACCESS_TOKEN}&fields=id,object_value,geometry&bbox={bbox}&layers=trafficsigns"
    response = requests.get(url)
    if response.status_code == 200:
        # return response.json().get("data", [])
        data = response.json()

        # with open('./traffic_sign_response_data.json', 'w') as json_file:
        #     json.dump(data, json_file, indent=4)

        speed_limits = [
            item for item in data.get("data", [])
            if "regulatory--maximum-speed-limit" in item.get("object_value", "")
        ]
        
        return speed_limits
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
                        "id": road["id"],
                        "road_name": road["tags"].get("name", "Unnamed Road"),
                        "road_type": road["tags"].get("highway", "Unknown"),
                        "speed_limit": speed_limit,
                        "distance_meters": round(min_distance, 2),
                        "nearest_point": road_point # Nearest Road Segment GPS Point
                    }
    return closest_road

def find_nearest_speed_sign(lat, lon, speed_signs):
    user_location = (lat, lon)
    closest_sign = None
    min_distance = float("inf")

    for sign in speed_signs:
        object_value = sign["object_value"]
        
        # Filter only speed signs with "maximum-speed-limit"
        if "regulatory--maximum-speed-limit" in object_value:
            sign_location = tuple(sign["geometry"]["coordinates"][::-1])  # Reverse order to (lat, lon)
            distance = geodesic(user_location, sign_location).meters

            if distance < min_distance:
                min_distance = distance
                # print(f"Object Value: {object_value}")
                
                # Extract speed limit (assumes format "regulatory--maximum-speed-limit-XX--gX")
                parts = object_value.split("-")
                # print(f"Parts: {parts}")  # Debugging output
                
                if len(parts) < 4:
                    print(f"Skipping invalid format: {object_value}")  # Debugging output
                    continue

                speed_limit_str = parts[-3]  # Extract the second-to-last part
                # print(f"Speed Limit Str: {speed_limit_str}")  # Debugging output
                
                try:
                    speed_limit = float(speed_limit_str)  # Convert to float
                except ValueError:
                    print(f"Failed to convert speed limit: {speed_limit_str}")  # Debugging output
                    continue  # Skip if extraction fails

                closest_sign = {
                    "id": sign["id"],
                    "speed_limit": speed_limit,
                    "distance_meters": round(min_distance, 2),
                    "coordinates": sign_location
                }

    # print(f"Closest Sign: {closest_sign}")  # Debugging output
    return closest_sign



# Function to process the input file and detect speeding events
def process_data_file(input_file):
    with open(input_file, "r") as file:
        data = file.read().strip().split("|")
        points = [row.split(",") for row in data]
        latitudes = [float(p[0]) for p in points]
        longitudes = [float(p[1]) for p in points]
        lat_min, lat_max = min(latitudes), max(latitudes)
        lon_min, lon_max = min(longitudes), max(longitudes)
        print(f"Bounding Box (lat_min, lon_min, lat_max, lon_max): {lat_min}, {lon_min}, {lat_max}, {lon_max}")

        road_segments = get_road_segments(lat_min, lon_min, lat_max, lon_max)
        print(f"Road Segments Found: {len(road_segments)}")
        traffic_signs = get_mapillary_speed_limits(lat_min, lon_min, lat_max, lon_max)
        print(f"Mapillary Traffic Signs Found: {len(traffic_signs)}")
        # for i, sign in enumerate(speed_signs[:1000]):
        #     if "maximum-speed-limit" in sign.get("object_value", ""):
        #         print(f"Speed Sign {i+1}: {sign}")

        for lat, lon, distracted, traveling_speed, timestamp in points:
            lat, lon, traveling_speed = float(lat), float(lon), float(traveling_speed)
            nearest_road = find_nearest_road(lat, lon, road_segments)
            mapillary_service_used = False
            
            if nearest_road:
                speed_limit = nearest_road["speed_limit"]
                print(f"Speed Limit - found using OSM: {speed_limit}")                

                if speed_limit == "Unknown":
                    # Find nearest speed limit sign from Mapillary data
                    nearest_speed_sign = find_nearest_speed_sign(lat, lon, traffic_signs)
                    if nearest_speed_sign != None:
                        mapillary_service_used = True
                        print(f"Speed Limit - found using Mapillary: {nearest_speed_sign['speed_limit']} mph")
                        speed_limit = nearest_speed_sign["speed_limit"]
                    else:
                        print(f"No speed limit sign found near this location.")
                        speed_limit = "Unknown"
                
                speeding = traveling_speed > speed_limit if isinstance(speed_limit, float) else "Unknown"
                
                print(f"⏱️  Driving Timestamp: {convert_timestamp(timestamp)}")
                print(f"📍 Driver's GPS Location: {lat}, {lon}")
                print(f"📏  Distance from Driver to Nearest Road: {nearest_road['distance_meters']} m")
                print(f"🪪  Road Segment ID: {nearest_road['id']}")
                print(f"🛣️  Road Segment: {nearest_road['road_name']}")
                print(f"🚧  Road Type: {nearest_road['road_type']}")
                print(f"🚦 Speed Limit: {speed_limit} mph")
                print(f"🦮 Mapillary Service Used?: {'Yes' if mapillary_service_used else 'No'}")
                print(f"🚗 Traveling Speed: {traveling_speed} mph")
                print(f"⚠️  Speeding: {'Yes' if speeding == True else 'No' if speeding == False else 'Unknown'}\n")

# Run the script with an example file
process_data_file("./JameyTrips/trial_1.txt")