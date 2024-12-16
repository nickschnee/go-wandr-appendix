from models import POIPreferences, LakeSize, UrbanSize
from db import get_db_connection

def filter_by_lake(cur, stations, lake_preferences):
    if not lake_preferences:
        return stations

    # Extract station IDs from the list of stations
    station_ids = [station[0] for station in stations]

    # Execute a single query to get all matching station IDs
    cur.execute("""
        SELECT DISTINCT stop_id
        FROM stop_has_poi
        WHERE stop_id = ANY(%s)
        AND poi_type = 'lake'
        AND poi_distance <= %s
        AND poi_size_closest = ANY(%s)
    """, (
        station_ids,
        lake_preferences.lake_distance,
        [size.value for size in lake_preferences.lake_sizes]
    ))

    # Fetch the matching station IDs
    matching_station_ids = set(row[0] for row in cur.fetchall())

    # Filter the original stations list based on matching IDs
    filtered_stations = [station for station in stations if station[0] in matching_station_ids]

    return filtered_stations


def filter_by_urban(cur, stations, urban_preferences):
    """
    Filter stations based on urban avoidance preferences.
    Returns filtered list of stations that match urban criteria.
    """
    if not urban_preferences:
        return stations

    filtered_stations = []
    for station in stations:
        station_id = station[0]
        cur.execute("""
            SELECT 1
            FROM stop_has_poi
            WHERE stop_id = %s 
            AND poi_type = 'urban'
            AND (
                poi_size_cumulated IN ('S', 'M', 'L')
                OR poi_distance IS NULL
                OR poi_distance > %s
            )
        """, (
            station_id,
            urban_preferences.urban_distance
        ))
        if cur.fetchone():
            filtered_stations.append(station)
    
    return filtered_stations

def filter_by_restaurant(cur, stations, restaurant_preferences):
    """
    Filter stations based on restaurant/guesthouse preferences.
    Returns filtered list of stations that match restaurant criteria.
    """
    if not restaurant_preferences:
        return stations

    # Extract station IDs from the list of stations
    station_ids = [station[0] for station in stations]

    # Execute query to get stations with required restaurant density
    cur.execute("""
        SELECT DISTINCT stop_id
        FROM stop_has_poi
        WHERE stop_id = ANY(%s)
        AND poi_type = 'restaurant_guesthouse'
        AND poi_distance <= %s
        AND poi_density >= %s
    """, (
        station_ids,
        restaurant_preferences.restaurant_distance,
        restaurant_preferences.min_restaurant_density
    ))

    # Fetch the matching station IDs
    matching_station_ids = set(row[0] for row in cur.fetchall())

    # Filter the original stations list based on matching IDs
    filtered_stations = [station for station in stations if station[0] in matching_station_ids]

    return filtered_stations

def find_poi(start_stations, poi_preferences: POIPreferences = None):
    """
    Filter stations based on POI preferences.
    Returns filtered list of stations that match POI criteria.
    """
    if not poi_preferences:
        return start_stations

    if not (poi_preferences.lake or poi_preferences.avoid_urban or poi_preferences.restaurant):
        return start_stations

    print(f"Finding POIs with preferences: {poi_preferences}")
    filtered_stations = start_stations.copy()
    
    conn = get_db_connection()
    cur = conn.cursor()

    try:
        # Check lake criteria if enabled
        if poi_preferences.lake:
            filtered_stations = filter_by_lake(cur, filtered_stations, poi_preferences)

        # Check urban avoidance criteria if enabled
        if poi_preferences.avoid_urban:
            filtered_stations = filter_by_urban(cur, filtered_stations, poi_preferences)
            
        # Check restaurant criteria if enabled
        if poi_preferences.restaurant:
            filtered_stations = filter_by_restaurant(cur, filtered_stations, poi_preferences)

        print(f"Found {len(filtered_stations)} stations matching POI criteria")
        return filtered_stations

    finally:
        cur.close()
        conn.close()