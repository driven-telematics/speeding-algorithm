def extract_coordinates(file_path):
    coordinates = []
    with open(file_path, 'r') as file:
        data = file.read().strip()
        data_points = data.split('|')
        for point in data_points:
            values = point.split(',')
            lat, long = float(values[0]), float(values[1])
            coordinates.append((lat,long))
    return coordinates

def format_coordinates_osm_get_by_id(data):
    return [(item["lat"], item["lon"]) for item in data]

# Example usage:
# file_path = './JameyTrips/trial_1.txt'
# print(f"# of coords: {len(extract_coordinates(file_path))}")
# print(extract_coordinates(file_path))
# coords = extract_coordinates(file_path)

coordinates = [
                {
                    "lat": 29.7847754,
                    "lon": -95.6455106
                },
                {
                    "lat": 29.7847695,
                    "lon": -95.6441317
                }
            ]

coords = format_coordinates_osm_get_by_id(coordinates)

# Format the data
formatted_blue_circle = [f"{lat},{lon},red,circle,\"\"" for lat, lon in coords]

# Join lines
output_blue_circle = "\n".join(formatted_blue_circle)

# Save to file
blue_circle_file_path = "./route_geocodes_plotter.txt"

with open(blue_circle_file_path, "w") as file:
    file.write(output_blue_circle)