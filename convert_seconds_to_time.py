def convert_seconds_to_time(seconds_list):
    total_seconds = sum(seconds_list)

    # years = total_seconds // (365 * 24 * 3600)
    # remaining_seconds = total_seconds % (365 * 24 * 3600)

    # months = remaining_seconds // (30 * 24 * 3600)
    # remaining_seconds %= (30 * 24 * 3600)

    # days = remaining_seconds // 86400
    # remaining_seconds %= 86400

    hours = total_seconds // 3600
    remaining_seconds = total_seconds % 3600

    minutes = remaining_seconds // 60
    seconds = remaining_seconds % 60

    return f"{hours} hours, {minutes} minutes, {seconds} seconds"

# Example usage
example_seconds = [
    22052922214, 12893536798, 14241539348, 10588719763, 12595441014,
    14724040832, 15186640037, 9578948025, 8959950733, 9631083431,
    10170721802, 10604662069, 9635249760, 9465072474, 10271195862,
    9154064992, 9525786302, 9126457734, 9348068551, 9557586073,
    9403264496, 9534801413, 9355563731, 982126241
]
result = convert_seconds_to_time(example_seconds)
print(result)
