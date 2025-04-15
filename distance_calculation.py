from datetime import datetime, timezone
import pandas as pd
from geopy.distance import geodesic

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

    for i in range(1, len(parsed_data)):
        point1 = (parsed_data[i - 1][0], parsed_data[i - 1][1])
        point2 = (parsed_data[i][0], parsed_data[i][1])
        total_distance += geodesic(point1, point2).kilometers  # Compute great-circle distance

    return total_distance

def process_data_file(input_file):

    with open(input_file, "r") as file:
        data = file.read().strip().split("|")
        points = [(float(p[0]), float(p[1]), int(p[2]), float(p[3]), p[4]) for p in (row.split(",") for row in data)]

    distance = calculate_distance_and_duration(points)
    with open("./distances.txt", "a") as file:
        file.write(f"{distance:.2f}\n")
    print(f"Trip Distance: {distance:.2f} km")

# Run the script with an example file
process_data_file("./JameyTrips/trial_30.txt")