import requests

# API Credentials
MAPILLARY_ACCESS_TOKEN = ""
OVERPASS_URL = ""

def get_road_segments(lat_min, lon_min, lat_max, lon_max):
    query = f"""
    [out:json];
    way({lat_min},{lon_min},{lat_max},{lon_max})[highway];
    out geom;
    """
    response = requests.get(OVERPASS_URL, params={"data": query})
    
    if response.status_code == 200:
        data = response.json()
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

def process_data_file(input_file):
    with open(input_file, "r") as file:
        data = file.read().strip().split("|")
        points = [row.split(",") for row in data]
        latitudes = [float(p[0]) for p in points]
        longitudes = [float(p[1]) for p in points]
        lat_min, lat_max = min(latitudes), max(latitudes)
        lon_min, lon_max = min(longitudes), max(longitudes)
        print(f"Original BB Coords: {lat_min, lat_max, lon_min, lon_max}")
        road_segments = get_road_segments(lat_min, lon_min, lat_max, lon_max)
        print(f"# of OSM Road Segments: {len(road_segments)}")
        # Print results
        for road in road_segments:
            print(f"Road: {road['name']}, Speed Limit: {road['speed_limit']}")


# Run the script with an example file
process_data_file("./JameyTrips/trial_5.txt")