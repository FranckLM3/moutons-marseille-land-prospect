import json

# Try to parse manually to find the error
with open('mos_2022.geojson', 'r') as f:
    for i, line in enumerate(f, 1):
        if i == 199880:  # Error line
            print(f"Problem line {i}: {line[:200]}...")
            break