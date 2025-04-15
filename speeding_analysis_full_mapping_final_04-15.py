from datetime import datetime, timezone
import requests
import re
import json
import time
import pandas as pd
from geopy.distance import geodesic
from shapely.geometry import Point, LineString
from shapely.ops import nearest_points
import boto3
from decimal import Decimal
from collections import defaultdict


# API Credentials
MAPILLARY_ACCESS_TOKEN = ""
MAPQUEST_API_KEY = ""
# OVERPASS_URL = ""
DRIVEN_OVERPASS_URL = ""
BATCH_SIZE = 20 # OSM Calls
DB_BATCH_SIZE = 25 # DB Calls

# Initialize the DynamoDB client
dynamodb = boto3.resource('dynamodb')
# table = dynamodb.Table('drivenDB_road_segment_info')

# Function to batch fetch items from DynamoDB
def batch_get_items(keys):
    results = {}
    for batch in (keys[i : i + BATCH_SIZE] for i in range(0, len(keys), BATCH_SIZE)):
        response = dynamodb.batch_get_item(
            RequestItems={
                'drivenDB_road_segment_info': {
                    'Keys': [{'road_segment_id': segment_id} for segment_id in batch]
                }
            }
        )
        items = response.get('Responses', {}).get('drivenDB_road_segment_info', [])
        results.update({item['road_segment_id']: item for item in items})
    return results

def batch_write_all(table_name, items, batch_size=DB_BATCH_SIZE):
    for i in range(0, len(items), batch_size):
        batch = items[i:i + batch_size]  
        request_items = {table_name: batch}

        dynamodb.batch_write_item(RequestItems=request_items)
        # response = dynamodb.batch_write_item(RequestItems=request_items)

        # while 'UnprocessedItems' in response and response['UnprocessedItems']:
        #     unprocessed = response['UnprocessedItems'].get(table_name, [])
        #     if unprocessed:
        #         print(f"Retrying {len(unprocessed)} unprocessed items...")
        #         response = dynamodb.batch_write_item(RequestItems={table_name: unprocessed})

def write_speed_data_to_file(file_path, lat, lon, distracted, traveling_speed,
                            osm_speed_limit, mapillary_speed_limit, mapquest_speed_limit, highway_type, timestamp):

    speed_limit_sources = [osm_speed_limit, mapillary_speed_limit, mapquest_speed_limit]
    valid_speed_limit = next((speed_limit for speed_limit in speed_limit_sources if speed_limit != 0), 0)  

    with open(file_path, "a") as file:
        file.write(
            f"{lat},{lon},{distracted},{traveling_speed},{valid_speed_limit},{highway_type},{timestamp}|\n"
        )

def count_segment_occurrences(geocode_to_segment):
    segment_count = defaultdict(int)

    # Count occurrences of each segment_id
    for value in geocode_to_segment.values():
        segment_id = value["segment_id"]
        segment_count[segment_id] += 1
    
    # print(segment_count)
    # print(f"Length of original segment count: {len(segment_count)}")

    # Filter out segments with count < n number of occurences
    filtered_segment_count = {seg_id: count for seg_id, count in segment_count.items() if count >= 5}
    removed_segment_count = {seg_id: count for seg_id, count in segment_count.items() if count < 5}

    return filtered_segment_count, removed_segment_count, len(segment_count)

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

# Function to query Overpass API for road segments within bounding box
def get_road_segments(lat_min, lon_min, lat_max, lon_max):
    query = f"""
    [out:json];
    way({lat_min},{lon_min},{lat_max},{lon_max})[highway];
    out geom;
    """
    response = requests.get(DRIVEN_OVERPASS_URL, params={"data": query})
    
    if response.status_code == 200:
        if not response.text.strip():  # Check if response is empty
            print("Error: Received empty response from Overpass API")
            return []
        try:
            return response.json().get("elements", [])
        except requests.exceptions.JSONDecodeError:
            print(f"Error decoding JSON: {response.text}")
            return []
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
                "osm_speed_limit": parsed_speed_limit,
                "distance_meters": round(distance, 2),
                "geometry": road["geometry"],
                "bounds": road["bounds"],
                "mapillary_speed_limit": -1.0,
                "mapquest_speed_limit": -1.0   
            }
            
    return closest_road
                    

def get_mapquest_speed_limit(coord):
    url = f"https://www.mapquestapi.com/geocoding/v1/reverse"
    params = {
        "key": MAPQUEST_API_KEY,
        "location": f"{coord[0]},{coord[1]}",
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


def calculate_distance_and_duration(coords):
    """
    Calculate total distance (in miles) and duration (in seconds) from GPS data.
    
    :param coords: List of tuples [(lat, lon, distracted, speed, timestamp), ...]
    :return: Total distance (miles), total duration (seconds)
    """
    if len(coords) < 2:
        return 0, 0  # Not enough data points

    # Extract relevant fields and sort by timestamp
    parsed_data = [(lat, lon, datetime.fromtimestamp(int(timestamp), tz=timezone.utc)) for lat, lon, _, _, timestamp in coords]
    parsed_data.sort(key=lambda x: x[2])  # Sort by timestamp (if not already sorted)

    total_distance = 0
    start_time = parsed_data[0][2]
    end_time = parsed_data[-1][2]

    for i in range(1, len(parsed_data)):
        point1 = (parsed_data[i - 1][0], parsed_data[i - 1][1])
        point2 = (parsed_data[i][0], parsed_data[i][1])
        total_distance += geodesic(point1, point2).miles  # Compute great-circle distance

    total_seconds = int((end_time - start_time).total_seconds())  # Convert to int
    minutes = total_seconds // 60
    seconds = total_seconds % 60

    return total_distance, (minutes, seconds)
                

# Function to process the input file and detect speeding events
def process_data_file(input_file):
    algo_start_time = time.time()
    reading_file_start_time = time.time()

    with open(input_file, "r") as file:
        data = file.read().strip().split("|")
        points = [(float(p[0]), float(p[1]), int(p[2]), float(p[3]), p[4]) for p in (row.split(",") for row in data)]
        latitudes = [float(p[0]) for p in points]
        longitudes = [float(p[1]) for p in points]
        session_lat_min, session_lat_max = min(latitudes), max(latitudes)
        session_lon_min, session_lon_max = min(longitudes), max(longitudes)
    
    reading_file_end_time = time.time()
    elapsed_reading_file_time = reading_file_end_time - reading_file_start_time
    
    mapillary_api_call_start_time = time.time()
    speed_signs = get_mapillary_speed_limits(session_lat_min, session_lon_min, session_lat_max, session_lon_max)
    # print(speed_signs)
    # Track unique travelled segments across all batches
    travelled_segments = {}
    geocode_to_segment = {}
    total_points = len(points)
    batch_start = 0
    osm_api_call = 0

    mapillary_api_call_end_time = time.time()
    elapsed_mapillary_api_call_time = mapillary_api_call_end_time - mapillary_api_call_start_time

    determine_travelled_segments_start_time = time.time()
    while batch_start < total_points:
        batch_end = min(batch_start + BATCH_SIZE, total_points)
        batch = points[batch_start:batch_end]
        lat_min, lat_max, lon_min, lon_max = get_bounding_box(batch)

        road_segments = get_road_segments(lat_min, lon_min, lat_max, lon_max)
        osm_api_call += 1
        
        for lat, lon, distracted, traveling_speed, timestamp in batch:
            user_coords = (lat, lon)
            nearest_road = find_nearest_road(user_coords, road_segments)
            
            if nearest_road:
                segment_id = str(nearest_road['id'])
                if segment_id not in travelled_segments:
                    travelled_segments[segment_id] = nearest_road
                geocode_to_segment[(lat, lon, timestamp)] = {
                    "segment_id": segment_id,
                    "distance_meters": nearest_road['distance_meters']
                }

        batch_start += BATCH_SIZE

    filtered_geocode_to_segment, removed_segments, unique_segments_count = count_segment_occurrences(geocode_to_segment)
        
    determine_travelled_segments_end_time = time.time()
    elapsed_determine_travelled_segments = determine_travelled_segments_end_time - determine_travelled_segments_start_time


    resolve_speed_limits_start_time = time.time()

    segment_ids = list(travelled_segments.keys())
    db_existing_segments = batch_get_items(segment_ids)
    db_items_to_write = []

    mapquest_api_counter = 0
    segments_with_unknown_speeds = 0
    updated_at_timestamp = int(time.time())

    # Resolve speed limits for all travelled segments
    for segment_id, road in travelled_segments.items():
        if segment_id in filtered_geocode_to_segment:
            if segment_id in db_existing_segments:
                # Use existing record data
                item = db_existing_segments[segment_id]
                road['osm_speed_limit'] = float(item.get('osm_speed_limit', Decimal(0)))
                road['mapillary_speed_limit'] = float(item.get('mapillary_speed_limit', Decimal(0)))
                road['mapquest_speed_limit'] = float(item.get('mapquest_speed_limit', Decimal(0)))
                
            else:
                speed_limit = road['osm_speed_limit']
                road_segment_info = {
                    "road_segment_id": segment_id,
                    "osm_road_name": road['road_name'],
                    "osm_road_type": road['road_type'],
                    "osm_speed_limit": Decimal(str(speed_limit)) if isinstance(speed_limit, (int, float)) else speed_limit,
                    "mapillary_speed_limit": Decimal(0),
                    "mapquest_speed_limit": Decimal(0),
                    "avg_contextual_speed_30_day": Decimal(0), 
                    "avg_contextual_speed_60_day": Decimal(0), 
                    "avg_contextual_speed_180_day": Decimal(0),
                    "updated_at": updated_at_timestamp  
                }
                
                if speed_limit == "Unknown": # Speed Limit from OSM is not present
                    road['osm_speed_limit'] = 0
                    road_segment_info['osm_speed_limit'] = Decimal(0)
                    
                    segments_with_unknown_speeds += 1
                    # Check Mapillary
                    segment_with_mapillary_speed = map_speed_sign_to_nearest_road(road, speed_signs)
                    if len(segment_with_mapillary_speed['mapillary_speed_signs']) > 0: # speed sign mapped to road
                        speed_limit = segment_with_mapillary_speed['mapillary_speed_signs'][0]['speed_limit']
                        road['mapillary_speed_limit'] = speed_limit  # for printing to console
                        road_segment_info['mapillary_speed_limit'] = Decimal(speed_limit)
                    else:
                        # Call MapQuest if still unknown
                        mid_index = len(road['geometry']) // 2  # Get the middle index
                        middle_road_coord = (road['geometry'][mid_index]['lat'], road['geometry'][mid_index]['lon'])
                        speed_limit = get_mapquest_speed_limit(middle_road_coord)
                        mapquest_api_counter += 1
                        road['mapquest_speed_limit'] = speed_limit  # for printing to console
                        if speed_limit != "Unknown":
                            road_segment_info['mapquest_speed_limit'] = Decimal(speed_limit)
                        else:
                            road_segment_info['mapquest_speed_limit'] = Decimal(0)
                    road_segment_info['osm_speed_limit'] = Decimal(0)

                db_items_to_write.append({"PutRequest": {"Item": road_segment_info}})
                # Write in batches
                if len(db_items_to_write) == BATCH_SIZE:
                    dynamodb.batch_write_item(RequestItems={'drivenDB_road_segment_info': db_items_to_write})
                    db_items_to_write = []  # Reset batch
        # else:
        #     # print(f"Segment ID {segment_id} not found in geocode_to_segment_counter")
    print(f"# of Items to write to DB: {len(db_items_to_write)}")
    print(f"Items Content: {db_items_to_write}")
                 
    if db_items_to_write:
        dynamodb.batch_write_item(RequestItems={'drivenDB_road_segment_info': db_items_to_write})

    resolve_speed_limits_end_time = time.time()
    elapsed_resolve_speed_limits = resolve_speed_limits_end_time - resolve_speed_limits_start_time

    final_output_functionality_start_time = time.time()

    speeding_events = []
    user_id = 31399 # Example user ID for testing

    # Output user geocode results
    for (lat, lon, timestamp), segment_data in geocode_to_segment.items():
        segment_id = segment_data['segment_id']
        distance_meters = segment_data['distance_meters']
        segment = travelled_segments[segment_id]
        print(segment)
        osm_speed_limit = segment['osm_speed_limit'] if segment['osm_speed_limit'] and segment['osm_speed_limit'] != 'Unknown' else 0
        mapillary_speed_limit = segment['mapillary_speed_limit'] if segment['mapillary_speed_limit'] > 0 else 0
        mapquest_speed_limit = (
            segment['mapquest_speed_limit']
            if isinstance(segment['mapquest_speed_limit'], (int, float)) and segment['mapquest_speed_limit'] > 0
            else 0
        )

        traveling_speed = next((p[3] for p in points if p[0] == lat and p[1] == lon and p[4] == timestamp), 0)
        distracted = next((p[2] for p in points if p[0] == lat and p[1] == lon and p[4] == timestamp), "Unknown")

        print(f"⏱️  Timestamp: {convert_timestamp(timestamp)}")
        print(f"📍 Location: {lat}, {lon}")
        print(f"📏  Distance from Driver to Nearest Road: {distance_meters} m")
        print(f"🚧 Road Segment ID: {segment['id'] if segment else 'None'}")
        print(f"🛣️  Road Segment: {segment['road_name']}")
        print(f"🚧  Road Type: {segment['road_type']}")
        print(f"🚦 OSM Speed Limit: {osm_speed_limit} mph")
        print(f"🚦 Mapillary Speed Limit: {mapillary_speed_limit} mph")
        print(f"🚦 MapQuest Speed Limit: {mapquest_speed_limit} mph")
        print(f"🚗 Traveling Speed: {traveling_speed} mph\n")

        speed_limit_sources = [osm_speed_limit, mapillary_speed_limit, mapquest_speed_limit]
        valid_posted_speed_limit = next((speed_limit for speed_limit in speed_limit_sources if speed_limit != 0), Decimal(0))  

        if traveling_speed > valid_posted_speed_limit and valid_posted_speed_limit > 0:
            speeding_events.append({
                "PutRequest": {
                    "Item": {
                        "road_segment_id": str(segment['id']),
                        "timestamp#user_id": f"{str(timestamp)}#{str(user_id)}",
                        "traveling_speed": Decimal(traveling_speed),
                        "posted_speed_limit": Decimal(valid_posted_speed_limit)
                    }
                }
            })

        # Example usage
        write_speed_data_to_file(
            "speed_data.txt",
            lat, lon, distracted, traveling_speed,
            osm_speed_limit, mapillary_speed_limit, mapquest_speed_limit,
            segment["road_type"], timestamp
        )

    if speeding_events:
        batch_write_all("users_speeding_events", speeding_events)

    distance, (duration_minutes, duration_seconds) = calculate_distance_and_duration(points)

    final_output_functionality_end_time = time.time()
    elapsed_final_output_functionality_time = final_output_functionality_end_time - final_output_functionality_start_time
   
    algo_end_time = time.time()
    elapsed_algo_time = algo_end_time - algo_start_time

    # Monitoring/Perfomance Logging
    print("======= DRIVING DATA METRICS =======")
    print(f"Total Distance: {distance:.2f} miles, Total Duration: {duration_minutes} minutes and {duration_seconds} seconds")
    print(f"# of User Geocodes: {len(points)}")
    print(f"# of travelled road segments: {len(travelled_segments)}\n")
    print(f"# of Unique Segments: {unique_segments_count}")
    print(f"# of Segments where geocode > 5: {len(filtered_geocode_to_segment)}")
    # print(filtered_geocode_to_segment)
    print(f"# of Segments Ignored (geocode < 5): {len(removed_segments)}")
    # print(f"Removed Segments: {removed_segments}")
    print(f"# of Geocodes considered for speeding: {len(speeding_events)}")
    print("======= ALGO PERFORMANCE METRICS =======")
    print(f"# of segments with unknown speeds: {segments_with_unknown_speeds}")
    print(f"# of OSM API calls: {osm_api_call}")
    print(f"# of Mapillary speed signs {len(speed_signs)}")
    print(f"# of MapQuest API calls: {mapquest_api_counter}\n")
    print(f"Time to complete reading file: {elapsed_reading_file_time:.4f} seconds")
    print(f"Time to complete Mapillary API call: {elapsed_mapillary_api_call_time:.4f} seconds")
    print(f"Time to complete determine_travelled_segments: {elapsed_determine_travelled_segments:.4f} seconds")
    print(f"Time to complete find_speed_limits: {elapsed_resolve_speed_limits:.4f} seconds")
    print(f"Time to complete final_output_functionality: {elapsed_final_output_functionality_time:.4f} seconds")
    print(f"Time to complete full algorithm: {elapsed_algo_time:.4f} seconds")

# Run the script with an example file
process_data_file("./JameyTrips/trial_5.txt")
