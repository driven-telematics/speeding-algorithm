from datetime import datetime
import requests
import re
import math
from geopy.distance import geodesic
from shapely.geometry import Point, LineString
from shapely.ops import nearest_points

### WHAT SHOULD THIS ALGO BE DOING
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
    4. Create a subset of Road Segment Data where speed limit is unknown
        - segment ID, name, type, and geometry
    5. Make API call to Mapillary using Bounding Box
        - Retrieves all traffic signs
        - Returns Filtered list of only speed limit signs
    6. Traverse through Unknown Speed Road Segment Data 
    6.1 Determine min lat, long and max lat, long to create Bounding Box ???
        - I think there is already data on the bounds
        "bounds": {
            "minlat": 29.7132341,
            "minlon": -95.7471085,
            "maxlat": 29.7135192,
            "maxlon": -95.7468311
        },
    6.2 Compare each speed sign's coordinates to each unknown road segment bounding box
        - if min_lat(Road Segment) <= lat(speed sign) <= max_lat(Road Segment) &
        min_long(Road Segment) <= long(speed sign) <= max_long(Road Segment),
        we can assume this speed sign is within the bounds of the road segment
        - calculate perpendicular distance between speed sign
        and a line of a road segment
            - the geometry of the road segment is made up of multiple lines
            i.e. from one coord to the next is a line
    7. Map Mapillary Speed Sign Data to OSM Road Segment
        - if distance between sign and road segment < 10-20 meters, assign speed sign
        to road segment
        - create new field in Mapillary Object called "speed_limit" and
        assign it the parsed speed limit from "object_value"
        - Append Mapillary Object to OSM Road Segment Object
        - Mapillary Speed Sign Object {
            "id": "914976236015494",
            "object_value": "regulatory--maximum-speed-limit-40--g3",
            "aligned_direction": 189.90952,
            "geometry": {
                "type": "Point",
                "coordinates": [
                    -95.751699668911,
                    29.787045844674
                ]
            }
        },
        - OSM Road Segment {
            "type": "way",
            "id": 20900806,
            "bounds": {
                "minlat": 29.7132341,
                "minlon": -95.7471085,
                "maxlat": 29.7135192,
                "maxlon": -95.7468311
            },
            "nodes": [
                224487956,
                224555488,
                1158292770,
                1158292641,
                1158292520,
                1158292753,
                1158292609
            ],
            "geometry": [
                {
                    "lat": 29.7132341,
                    "lon": -95.7468723
                },
                {
                    "lat": 29.7134304,
                    "lon": -95.7468311
                },
                {
                    "lat": 29.7134834,
                    "lon": -95.7468325
                },
                {
                    "lat": 29.713517,
                    "lon": -95.746874
                },
                {
                    "lat": 29.7135192,
                    "lon": -95.7469258
                },
                {
                    "lat": 29.7134929,
                    "lon": -95.7469881
                },
                {
                    "lat": 29.7133206,
                    "lon": -95.7471085
                }
            ],
            "tags": {
                "highway": "residential",
                "name": "Athea Glen Circle",
                "oneway": "yes",
                "tiger:cfcc": "A41",
                "tiger:county": "Fort Bend, TX",
                "tiger:name_base": "Athea Glen",
                "tiger:name_type": "Cir",
                "tiger:reviewed": "no",
                "tiger:separated": "no",
                "tiger:source": "tiger_import_dch_v0.6_20070829",
                "tiger:tlid": "78323369",
                "tiger:zip_left": "77450",
                "tiger:zip_right": "77450"
            }
        },
    8. Traverse through Driving Data Point File
    9. Compute Perpendicular Distance between user's (lat, long) and 
    a line (2 geometry points)
    10. Return Road Segment/ Assign Data Point to the Road Segment
    - if min_lat(Road Segment) <= lat(user lat) <= max_lat(Road Segment) &
        min_long(Road Segment) <= long(user long) <= max_long(Road Segment),
        we can assume this speed sign is within the bounds of the road segment 
    - If the distance is less than 10/20 meters, return the road segment
Output:
    1. Driving Data Point Timestamp
    2. User's GPS Location
    3. Distance to Nearest Road? (Depends on how we are finding
    what road segment the Driver is on. If comparing user's location to
    be inside bounds, we don't need to)
    4. Road Segment ID
    5. Road Segment Name
    6. Road Type
    7. Speed Limit
    8. Mapillary or OSM Speed Limit used
    9. Traveling Speed
    10. Speeding?
"""




# API Credentials
MAPILLARY_ACCESS_TOKEN = ""
MAPQUEST_API_KEY = ""
OVERPASS_URL = ""
BATCH_SIZE = 20

def convert_timestamp(timestamp):
    timestamp = int(timestamp)
    return datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d %H:%M:%S')

def convert_to_lat_lon(coords):
    return [(entry['lat'], entry['lon']) for entry in coords]

def extract_float(value: str) -> float:
    """
    Extracts the numeric value from a string and converts it to a float.
    
    :param value: A string containing a number with possible text.
    :return: The extracted number as a float.
    """
    match = re.search(r"\d+\.?\d*", value)
    return float(match.group()) if match else None

def get_bounding_box(points):
    latitudes = [p[0] for p in points]
    longitudes = [p[1] for p in points]
    return min(latitudes), max(latitudes), min(longitudes), max(longitudes)

def expand_bounding_box(min_lat, max_lat, min_lon, max_lon, meters=10):
    """
    Expands a bounding box by a given number of meters in each direction.
    
    :param min_lat: Minimum latitude of the bounding box
    :param max_lat: Maximum latitude of the bounding box
    :param min_lon: Minimum longitude of the bounding box
    :param max_lon: Maximum longitude of the bounding box
    :param meters: Distance in meters to expand in each direction (default is 10m)
    :return: New bounding box as (new_min_lat, new_max_lat, new_min_lon, new_max_lon)
    """
    # Constants for degree-to-meter conversion
    METERS_PER_DEGREE_LAT = 111320  # Approximate meters per degree of latitude
    avg_lat = (min_lat + max_lat) / 2  # Use the average latitude for longitude calculation
    METERS_PER_DEGREE_LON = 111320 * abs(math.cos(math.radians(avg_lat)))  # Adjust for latitude
    
    # Convert meters to degrees
    lat_offset = meters / METERS_PER_DEGREE_LAT
    lon_offset = meters / METERS_PER_DEGREE_LON
    
    # Expand the bounding box
    new_min_lat = min_lat - lat_offset
    new_max_lat = max_lat + lat_offset
    new_min_lon = min_lon - lon_offset
    new_max_lon = max_lon + lon_offset
    
    return new_min_lat, new_max_lat, new_min_lon, new_max_lon

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
    else:
        print(f"Error fetching data: {response.status_code}, {response.text}")
        return []

def get_unknown_speed_road_segments(road_segments):
    unknown_speed_road_segments = []
    for road in road_segments:
        speed_limit = road["tags"].get("maxspeed", "Unknown")
        if speed_limit == "Unknown":
            unknown_speed_road_segments.append(road)
    return unknown_speed_road_segments

# Function to parse speed limits from Mapillary
def parse_mapillary_speed_limit(object_value):
    parts = object_value.split("-")
    if len(parts) < 4:
        return None
    try:
        return float(parts[-3])
    except ValueError:
        return None

# Function to get speed limits from Mapillary within bounding box
def get_mapillary_speed_limits(lat_min, lon_min, lat_max, lon_max):
    bbox = f"{lon_min},{lat_min},{lon_max},{lat_max}"
    url = f"https://graph.mapillary.com/map_features?access_token={MAPILLARY_ACCESS_TOKEN}&fields=id,object_value,geometry&bbox={bbox}&layers=trafficsigns"
    # response = requests.get(url)

    # if response.status_code == 200:
    #     print(f"Success: {response.status_code}")
    #     data = response.json()
    #     speed_limits = [
    #         item for item in data.get("data", [])
    #         if "regulatory--maximum-speed-limit" in item.get("object_value", "")
    #     ]
    #     print(speed_limits)
    #     return speed_limits
    # print(f"Failure: {response.status_code}")
    # return []
    try:
        response = requests.get(url)
        if response.status_code == 200:
            return [
                item for item in response.json().get("data", [])
                if "regulatory--maximum-speed-limit" in item.get("object_value", "")
            ]
        else:
            print(f"Mapillary API Error: {response.status_code}, {response.text}")
            return []
    except requests.RequestException as e:
        print(f"Mapillary API Request Failed: {e}")
        return []

# Helper function to find distance between speed sign and road segment
def calculate_distance_to_road_segment(sign_coords, road_coords):
    """
    Calculate the minimum perpendicular distance from a speed limit sign to a road segment.

    :param sign_coords: Tuple (latitude, longitude) of the speed sign.
    :param road_coords: List of tuples [(lat1, lon1), (lat2, lon2), ...] representing the road segment.
    :return: Minimum distance in meters.
    """
    sign_location = Point(sign_coords[1], sign_coords[0])  # Convert to (lon, lat) for Shapely
    min_distance = float('inf')

    for i in range(len(road_coords) - 1):
        segment = LineString([(road_coords[i][1], road_coords[i][0]), (road_coords[i+1][1], road_coords[i+1][0])])

        # Convert degrees to meters using geopy
        # reference_point = segment.interpolate(segment.project(sign_location))
        # distance_meters = geodesic((sign_coords[0], sign_coords[1]), (reference_point.y, reference_point.x)).meters
        nearest_point = nearest_points(segment, sign_location)[0]  
        distance_meters = geodesic((sign_coords[0], sign_coords[1]), (nearest_point.y, nearest_point.x)).meters

        min_distance = min(min_distance, distance_meters)

    return min_distance

def map_speed_sign_to_nearest_road(nearest_road, speed_signs):
    minlat, maxlat = nearest_road['bounds']['minlat'], nearest_road['bounds']['maxlat']
    minlon, maxlon = nearest_road['bounds']['minlon'], nearest_road['bounds']['maxlon']
    road_coords = [(point["lat"], point["lon"]) for point in nearest_road["geometry"]]
    # Ensure road segment has a "speed_signs" field
    nearest_road.setdefault("mapillary_speed_signs", [])

    for sign in speed_signs:
        sign_coords = (sign["geometry"]["coordinates"][1], sign["geometry"]["coordinates"][0])  # (lat, lon)
        distance = calculate_distance_to_road_segment(sign_coords, road_coords)

        if distance < 10 and minlat <= sign_coords[0] <= maxlat and minlon <= sign_coords[1] <= maxlon:  # Assign sign if within 10 meters
        # if distance < 20:  # Assign sign if within 10 meters
            nearest_road["mapillary_speed_signs"].append(
                {
                    "sign_id": sign["id"],
                    "object_value": sign["object_value"],
                    "speed_limit": parse_mapillary_speed_limit(sign["object_value"]), 
                    "sign_coords": sign_coords,
                    "distance": distance
                }
            )

    return nearest_road

def map_speed_signs_to_unknown_segments(unknown_road_segments, speed_signs):
    """
    Assigns speed limit signs to the closest road segment if within a 10-meter threshold.

    :param unknown_road_segments: List of road segments with geometry data.
    :param speed_signs: List of speed signs with latitude and longitude coordinates.
    :return: Updated unknown_road_segments with assigned speed signs.
    """
    for road in unknown_road_segments:
        minlat, maxlat = road['bounds']['minlat'], road['bounds']['maxlat']
        minlon, maxlon = road['bounds']['minlon'], road['bounds']['maxlon']
        road_coords = [(point["lat"], point["lon"]) for point in road["geometry"]]
        
        # Ensure road segment has a "speed_signs" field
        road.setdefault("mapillary_speed_signs", [])

        for sign in speed_signs:
            sign_coords = (sign["geometry"]["coordinates"][1], sign["geometry"]["coordinates"][0])  # (lat, lon)
            distance = calculate_distance_to_road_segment(sign_coords, road_coords)

            if distance < 10 and minlat <= sign_coords[0] <= maxlat and minlon <= sign_coords[1] <= maxlon:  # Assign sign if within 10 meters
            # if distance < 20:  # Assign sign if within 10 meters
                road["mapillary_speed_signs"].append(
                    {
                        "sign_id": sign["id"],
                        "object_value": sign["object_value"],
                        "speed_limit": parse_mapillary_speed_limit(sign["object_value"]), 
                        "sign_coords": sign_coords,
                        "distance": distance,
                        "speed_service_used": "Mapillary"
                    }
                )

    return unknown_road_segments


# Helper function to find distance between user and road segment
def calculate_distance_user_to_road_segment(user_coords, road_coords):
    """
    Calculate the minimum perpendicular distance from a speed limit sign to a road segment.

    :param sign_coords: Tuple (latitude, longitude) of the speed sign.
    :param road_coords: List of tuples [(lat1, lon1), (lat2, lon2), ...] representing the road segment.
    :return: Minimum distance in meters.
    """
    sign_location = Point(user_coords[1], user_coords[0])  # Convert to (lon, lat) for Shapely
    min_distance = float('inf')

    for i in range(len(road_coords) - 1):
        segment = LineString([(road_coords[i][1], road_coords[i][0]), (road_coords[i+1][1], road_coords[i+1][0])])

        # Convert degrees to meters using geopy
        nearest_point = nearest_points(segment, sign_location)[0]  
        distance_meters = geodesic((user_coords[0], user_coords[1]), (nearest_point.y, nearest_point.x)).meters

        min_distance = min(min_distance, distance_meters)

    return min_distance

"""
Previous method has params: updated_road_segments
"""
def find_nearest_road(user_coords, road_segments):
    # Create a dictionary from shorter_list using 'id' as the key for quick lookup
    # speed_signs_map = {item["id"]: item for item in updated_road_segments}
    closest_road = None
    min_distance = float("inf")

    for road in road_segments:
        road_coords = road_coords = [(point["lat"], point["lon"]) for point in road["geometry"]]
        distance = calculate_distance_user_to_road_segment(user_coords, road_coords)

        if distance < min_distance:
            min_distance = distance
            speed_limit = road["tags"].get("maxspeed", "Unknown")
            parsed_speed_limit = extract_float(speed_limit) if "mph" in speed_limit else speed_limit
            
            closest_road = {
                "id": road.get("id"),
                "road_name": road["tags"].get("name", "Unnamed Road"),
                "road_type": road["tags"].get("highway", "Unknown"),
                "speed_limit": parsed_speed_limit,
                "distance_meters": round(distance, 2),
                "geometry": road["geometry"],
                "bounds": road["bounds"],
                "road_segment_data": road,
                "speed_service_used": "OSM"
            }
            
            # elif speed_limit == "Unknown":
            #     if road_segment_id in speed_signs_map and len(speed_signs_map[road_segment_id]['mapillary_speed_signs']) > 0:
            #         closest_road = {
            #             "id": road_segment_id,
            #             "road_name": speed_signs_map[road_segment_id]["tags"].get("name", "Unnamed Road"),
            #             "road_type": speed_signs_map[road_segment_id]["tags"].get("highway", "Unknown"),
            #             "distance_meters": round(distance, 2),
            #             "speed_limit": speed_signs_map[road_segment_id]['mapillary_speed_signs'][0]['speed_limit'],
            #             "road_segment_data": speed_signs_map[road_segment_id],
            #             "speed_service_used": "Mapillary"
            #         }
                # else: # Remove this to only show user coords where speed limit is known
                #     closest_road = {
                #         "id": road_segment_id,
                #         "road_name": road["tags"].get("name", "Unnamed Road"),
                #         "road_type": road["tags"].get("highway", "Unknown"),
                #         "road_geometry": road["geometry"],
                #         "speed_limit": speed_limit,
                #         "distance_meters": round(distance, 2),
                #         "road_segment_data": road,
                #         "speed_service_used": "OSM"
                #     }
    return closest_road
                    

def get_mapquest_speed_limit(user_coord):
    url = f"https://www.mapquestapi.com/geocoding/v1/reverse"
    params = {
        "key": MAPQUEST_API_KEY,
        "location": f"{user_coord[0]},{user_coord[1]}",
        "includeRoadMetadata": "true"
    }
    
    response = requests.get(url, params=params)
    
    if response.status_code == 200:
        data = response.json()
        try:
            road_metadata = data["results"][0]["locations"][0].get("roadMetadata", {})
            speed_limit = "Unknown"
            if road_metadata:
                speed_limit = road_metadata.get("speedLimit", "Unknown")

                if speed_limit:
                 speed_limit = float(speed_limit)

            return speed_limit
        except (IndexError, KeyError):
            return "Unexpected response format from API."
    else:
        return f"Error: {response.status_code} - {response.text}"                    

                

# Function to process the input file and detect speeding events
def process_data_file(input_file):
    with open(input_file, "r") as file:
        data = file.read().strip().split("|")
        points = [(float(p[0]), float(p[1]), int(p[2]), float(p[3]), p[4]) for p in (row.split(",") for row in data)]
        latitudes = [float(p[0]) for p in points]
        longitudes = [float(p[1]) for p in points]
        session_lat_min, session_lat_max = min(latitudes), max(latitudes)
        session_lon_min, session_lon_max = min(longitudes), max(longitudes)
    
    speed_signs = get_mapillary_speed_limits(session_lat_min, session_lon_min, session_lat_max, session_lon_max)
    print(speed_signs)
    road_segments_cache = {}
    total_points = len(points)
    batch_start = 0

    while batch_start < total_points:
        batch_end = min(batch_start + BATCH_SIZE, total_points)
        batch = points[batch_start:batch_end]
        lat_min, lat_max, lon_min, lon_max = get_bounding_box(batch)
        # speed_signs = get_mapillary_speed_limits(lat_min, lon_min, lat_max, lon_max)
        # print(speed_signs)

        key = (lat_min, lat_max, lon_min, lon_max)
        if key not in road_segments_cache:
            road_segments_cache[key] = get_road_segments(lat_min, lon_min, lat_max, lon_max)
        road_segments = road_segments_cache[key]
        
        for lat, lon, distracted, traveling_speed, timestamp in batch:
            user_coords = (lat, lon)
            nearest_road = find_nearest_road(user_coords, road_segments)
            # print(nearest_road)
            speed_limit = None
            if nearest_road:
                if nearest_road['speed_limit'] != 'Unknown':
                    speed_limit = nearest_road['speed_limit']
                elif nearest_road['speed_limit'] == 'Unknown':
                    #speed_signs = get_mapillary_speed_limits(lat_min, lat_max, lon_min, lon_max)
                    updated_segment_with_mapillary_speed = map_speed_sign_to_nearest_road(nearest_road, speed_signs)
                    if len(updated_segment_with_mapillary_speed['mapillary_speed_signs']) > 0 :
                        speed_limit = updated_segment_with_mapillary_speed['mapillary_speed_signs'][0]['speed_limit']
                        nearest_road['speed_service_used'] = 'Mapillary'
                    else:
                        speed_limit = get_mapquest_speed_limit(user_coords)
                        nearest_road['speed_service_used'] = 'MapQuest'
                else:
                    speed_limit = "Unknown"

                speeding = traveling_speed > speed_limit if isinstance(speed_limit, float) else "Unknown"

                print(f"⏱️  Driving Timestamp: {convert_timestamp(timestamp)}")
                print(f"📍 Driver's GPS Location: {lat}, {lon}")
                print(f"📏  Distance from Driver to Nearest Road: {nearest_road['distance_meters']} m")
                # print(f"🙋  Closest Road Segment GPS Point: {nearest_road['nearest_point']}")
                print(f"🪪  Road Segment ID: {nearest_road['id']}")
                print(f"🛣️  Road Segment: {nearest_road['road_name']}")
                print(f"🚧  Road Type: {nearest_road['road_type']}")
                # print(f"🧮  Road Segment Geometry: {convert_to_lat_lon(nearest_road['road_geometry'])}")
                print(f"🚦 Speed Limit: {speed_limit} mph")
                print(f"🦮 Speed Service Used?: {nearest_road['speed_service_used']}")
                print(f"🚗 Traveling Speed: {traveling_speed} mph")
                print(f"⚠️  Speeding: {'Yes' if speeding == True else 'No' if speeding == False else 'Unknown'}\n")

            else:
                print(f"No road segment found for {lat}, {lon} at {convert_timestamp(timestamp)}")

        batch_start += BATCH_SIZE

    """
        vvvvv WORKING ALGORITHM BELOW vvvvv
    """

    # with open(input_file, "r") as file:
    #     data = file.read().strip().split("|")
    #     points = [row.split(",") for row in data]
    #     latitudes = [float(p[0]) for p in points]
    #     longitudes = [float(p[1]) for p in points]
    #     lat_min, lat_max = min(latitudes), max(latitudes)
    #     lon_min, lon_max = min(longitudes), max(longitudes)
    #     print(f"Original BB Coords: {lat_min, lat_max, lon_min, lon_max}")

    #     road_segments = get_road_segments(lat_min, lon_min, lat_max, lon_max)
    #     print(f"# of OSM Road Segments: {len(road_segments)}")
    #     unknown_speed_road_segments = get_unknown_speed_road_segments(road_segments)
    #     print(f"# of segments w/ unknown speed: {len(unknown_speed_road_segments)}")
    #     #print(f"Sample Segment Data: {unknown_speed_segments[0]}")
    #     speed_signs = get_mapillary_speed_limits(lat_min, lon_min, lat_max, lon_max)
    #     print(f"# of Mapillary Speed Signs: {len(speed_signs)}")
    #     # print(speed_signs)
    #     #print(f"Sample Speed Sign Data: {speed_signs[0]}")

    #     updated_road_segments = map_speed_signs_to_unknown_segments(unknown_speed_road_segments, speed_signs)
    #     # print(f"Updated Road Segment: {updated_road_segments[0]}")
    #     # for road in updated_road_segments:
    #     #     if len(road['mapillary_speed_signs']) > 0:
    #             # print(f"Road Segment ID: {road['id']}")
    #             # print(f"Road Name: {road['tags'].get('name', 'Unknown')}")
    #             # print(f"Road Type: {road['tags']['highway']}")
    #             # print(f"All Speed Signs Nearby: {road['mapillary_speed_signs']}\n")

    #     for lat, lon, distracted, traveling_speed, timestamp in points:
    #         lat, lon, traveling_speed = float(lat), float(lon), float(traveling_speed)
    #         user_coords = (lat, lon)
    #         nearest_road = find_nearest_road(user_coords, road_segments, updated_road_segments)
            
            
    #         if nearest_road:
    #             speed_limit = nearest_road['speed_limit']
    #             speeding = traveling_speed > speed_limit if isinstance(speed_limit, float) else "Unknown"
    #             print(f"⏱️  Driving Timestamp: {convert_timestamp(timestamp)}")
    #             print(f"📍 Driver's GPS Location: {lat}, {lon}")
    #             print(f"📏  Distance from Driver to Nearest Road: {nearest_road['distance_meters']} m")
    #             # print(f"🙋  Closest Road Segment GPS Point: {nearest_road['nearest_point']}")
    #             print(f"🪪  Road Segment ID: {nearest_road['id']}")
    #             print(f"🛣️  Road Segment: {nearest_road['road_name']}")
    #             print(f"🚧  Road Type: {nearest_road['road_type']}")
    #             # print(f"🧮  Road Segment Geometry: {convert_to_lat_lon(nearest_road['road_geometry'])}")
    #             print(f"🚦 Speed Limit: {speed_limit} mph")
    #             print(f"🦮 Speed Service Used?: {nearest_road['speed_service_used']}")
    #             print(f"🚗 Traveling Speed: {traveling_speed} mph")
    #             print(f"⚠️  Speeding: {'Yes' if speeding == True else 'No' if speeding == False else 'Unknown'}\n")
    #         else:
    #             print(f"No Nearest Road for user @ {user_coords} - {convert_timestamp(timestamp)}")

# Run the script with an example file
process_data_file("./JameyTrips/trial_5.txt")
