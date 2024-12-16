import requests
import json
from psycopg2 import sql
import time
from typing import Tuple, List
import random
from poi import find_poi
from models import POIPreferences

def find_nearest_oev_stations(cur, user_lat, user_lon, radius_km=10):
    cur.execute("""
        SELECT xtf_id, name
        FROM stops
        WHERE ST_DWithin(
            geom,
            ST_Transform(ST_SetSRID(ST_MakePoint(%s, %s), 4326), 2056),
            %s
        )
        ORDER BY ST_Distance(
            geom,
            ST_Transform(ST_SetSRID(ST_MakePoint(%s, %s), 4326), 2056)
        );
    """, (user_lon, user_lat, radius_km * 1000, user_lon, user_lat))
    
    return cur.fetchall()


def find_isochrone(cur, user_lat, user_lon, cutoff=60, is_sunday=False):
    """Entry point function for finding isochrones"""
    print(f"Finding isochrone for {user_lat}, {user_lon} with cutoff {cutoff} minutes...")
    
    # Use the new get_isochrone function which handles caching
    return get_isochrone(cur, user_lat, user_lon, cutoff, is_sunday)

def get_isochrone(cur, user_lat, user_lon, cutoff=60, is_sunday=False):
    """Main function to get isochrone, first checking cache then falling back to API"""
    print(f"Getting isochrone for {user_lat}, {user_lon} with cutoff {cutoff} minutes...")
    
    # Create unique key for this request
    unique_key = f"{user_lat}_{user_lon}_{cutoff}_{is_sunday}"
    
    # Check cache first with corrected distance check
    cur.execute("""
        SELECT multipolygon 
        FROM isochrone_cache 
        WHERE ST_DWithin(
            ST_Transform(start_coordinates, 2056),
            ST_Transform(ST_SetSRID(ST_MakePoint(%s, %s), 4326), 2056),
            200  -- 200 meters in Swiss coordinate system (CH1903+/LV95)
        )
        AND cutoff = %s
        AND is_sunday = %s
        ORDER BY created_at DESC
        LIMIT 1;
    """, (user_lon, user_lat, cutoff, is_sunday))
    
    cached_result = cur.fetchone()
    
    if cached_result:
        print("Found cached isochrone!")
        multipolygon_geom = cached_result[0]
    else:
        print("No cached isochrone found, fetching from API...")
        multipolygon_geojson = fetch_isochrone(user_lat, user_lon, cutoff, is_sunday)
        
        # Convert GeoJSON to PostGIS geometry with SRID
        cur.execute(
            "SELECT ST_SetSRID(ST_GeomFromGeoJSON(%s), 4326)",
            [multipolygon_geojson]
        )
        multipolygon_geom = cur.fetchone()[0]
        save_isochrone(cur, user_lat, user_lon, multipolygon_geojson, cutoff, is_sunday, unique_key)
    
    # Query stops within the isochrone
    cur.execute("""
        SELECT xtf_id, name
        FROM stops
        WHERE ST_Within(
            geom,
            ST_Transform(ST_SetSRID(%s::geometry, 4326), 2056)
        );
    """, (multipolygon_geom,))
    
    return cur.fetchall()

def fetch_isochrone(user_lat, user_lon, cutoff, is_sunday=False):
    """Fetch isochrone from API"""
    base_url = "http://localhost:8080/otp/traveltime/isochrone"
    
    location = f"{user_lat},{user_lon}"
    print(f"Location: {location}")
    
    # Choose date based on is_sunday flag
    time_str = "2025-04-13T08:10:00+02:00" if is_sunday else "2025-04-11T08:10:00+02:00"
    
    params = {
        "batch": "true",
        "location": location,
        "time": time_str,
        "modes": "WALK,TRANSIT",
        "arriveBy": "false",
        "cutoff": f"{int(cutoff)}M"
    }
    
    print(f"Making request to: {base_url} with params: {params}")

    max_retries = 3
    retry_delay = 2
    timeout = 10

    for attempt in range(max_retries):
        try:
            response = requests.get(base_url, params=params, timeout=timeout)
            print(f"Response status code: {response.status_code}")
            print(f"Response content: {response.text[:200]}...")
            
            if not response.text:
                if attempt < max_retries - 1:
                    print(f"Empty response received, retrying in {retry_delay} seconds...")
                    time.sleep(retry_delay)
                    continue
                else:
                    raise ValueError("Empty response received from isochrone API after all retries")

            try:
                response_data = response.json()
            except requests.exceptions.JSONDecodeError:
                if attempt < max_retries - 1:
                    print(f"Invalid JSON response, retrying in {retry_delay} seconds...")
                    time.sleep(retry_delay)
                    continue
                else:
                    raise ValueError("Invalid JSON response from isochrone API after all retries")

            multipolygon_geojson = json.dumps(response_data['features'][0]['geometry'])
            return multipolygon_geojson

        except requests.exceptions.Timeout:
            if attempt < max_retries - 1:
                print(f"Request timed out, retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)
            else:
                raise ValueError(f"Isochrone API request timed out after {max_retries} attempts")
        except requests.exceptions.RequestException as e:
            if attempt < max_retries - 1:
                print(f"Request failed: {str(e)}, retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)
            else:
                raise ValueError(f"Failed to fetch isochrone data: {str(e)}")

    raise ValueError("Failed to get valid response from isochrone API after all retries")

def save_isochrone(cur, user_lat, user_lon, multipolygon_geojson, cutoff, is_sunday, unique_key):
    """Save isochrone to cache"""
    print(f"Saving isochrone to cache for {user_lat}, {user_lon} with cutoff {cutoff} minutes...")
    
    cur.execute("""
        INSERT INTO isochrone_cache 
        (start_coordinates, multipolygon, cutoff, is_sunday, unique_key)
        VALUES (
            ST_SetSRID(ST_MakePoint(%s, %s), 4326),
            ST_SetSRID(ST_GeomFromGeoJSON(%s), 4326),
            %s,
            %s,
            %s
        )
        ON CONFLICT (unique_key) DO NOTHING;
    """, (user_lon, user_lat, multipolygon_geojson, cutoff, is_sunday, unique_key))
    
    # Get connection from cursor and commit
    cur.connection.commit()

def get_isochrone_geojson(cur, user_lat, user_lon, cutoff=60, is_sunday=False):
    """
    Get isochrone as GeoJSON, first checking cache then falling back to API
    This is for the intermediate step when we just want to show the isochrone on the map
    """
    print(f"Getting isochrone GeoJSON for {user_lat}, {user_lon} with cutoff {cutoff} minutes...")
    
    # Create unique key for this request
    unique_key = f"{user_lat}_{user_lon}_{cutoff}_{is_sunday}"
    
    # Check cache first with corrected distance check
    cur.execute("""
        SELECT ST_AsGeoJSON(multipolygon)
        FROM isochrone_cache 
        WHERE ST_DWithin(
            ST_Transform(start_coordinates, 2056),
            ST_Transform(ST_SetSRID(ST_MakePoint(%s, %s), 4326), 2056),
            200  -- 200 meters in Swiss coordinate system (CH1903+/LV95)
        )
        AND cutoff = %s
        AND is_sunday = %s
        ORDER BY created_at DESC
        LIMIT 1;
    """, (user_lon, user_lat, cutoff, is_sunday))
    
    cached_result = cur.fetchone()
    
    if cached_result:
        print("Found cached isochrone!")
        return json.loads(cached_result[0])
    else:
        print("No cached isochrone found, fetching from API...")
        multipolygon_geojson = fetch_isochrone(user_lat, user_lon, cutoff, is_sunday)
        
        # Save to cache
        save_isochrone(cur, user_lat, user_lon, multipolygon_geojson, cutoff, is_sunday, unique_key)
        
        return json.loads(multipolygon_geojson)

def find_start_stop(
    cur,
    start_coords: List[float],
    radius: float,
    is_minutes: bool = False,
    is_sunday: bool = False,
    poi_preferences: POIPreferences = None
) -> Tuple[str, str]:
    """
    Find a suitable start stop based on given coordinates and preferences.
    
    Args:
        cur: Database cursor
        start_coords: [lat, lon] coordinates
        radius: Search radius (in minutes if is_minutes=True, else in meters)
        is_minutes: Whether radius is in minutes
        is_sunday: Whether the search is for a Sunday
        poi_preferences: Optional POI preferences for filtering stations
        
    Returns:
        Tuple of (stop_id, stop_name)
        
    Raises:
        ValueError: If no suitable stations are found
    """
    print("Finding nearest stations...")
    if is_minutes:
        start_stations = find_isochrone(cur, start_coords[0], start_coords[1], radius, is_sunday)
    else:
        start_stations = find_nearest_oev_stations(cur, start_coords[0], start_coords[1], radius/1000)

    if not start_stations:
        radius_unit = "minutes" if is_minutes else "meters"
        raise ValueError(f"No OEV stations found within {radius} {radius_unit} of the start point")

    print(f"Found {len(start_stations)} nearby start stations.")
    print("\n")  # Visual separation

    # Apply POI preferences if provided
    if poi_preferences:
        start_stations = find_poi(start_stations, poi_preferences)
        print(f"Found {len(start_stations)} start stations with POI preferences.")
        print("\n")  # Visual separation

    if not start_stations:
        raise ValueError("No stations found matching POI preferences")

    # Randomly select start station
    start_fid, start_name = random.choice(start_stations)
    print(f"Randomly selected start station: FID: {start_fid}, Name: {start_name}")

    return start_fid, start_name

def find_end_stop(
    cur,
    end_coords: List[float],  # [lon, lat]
    is_sunday: bool = False,
    initial_radius: int = 1000,
    max_radius: int = 5000,
    radius_increment: int = 500
) -> Tuple[str, str, float, float, float]:  # Returns (fid, name, type, lon, lat)
    """
    Find the nearest public transport stop to the end point.
    Gradually increases search radius until a stop is found.
    """
    current_radius = initial_radius
    
    while current_radius <= max_radius:
        print(f"Searching for end station within {current_radius}m...")
        
        # Query for stations within current radius
        cur.execute("""
            SELECT 
                xtf_id, 
                name, 
                verkehrsmittel_bezeichnung,
                ST_X(ST_Transform(geom, 4326)) as lon,
                ST_Y(ST_Transform(geom, 4326)) as lat,
                ST_Distance(
                    geom,
                    ST_Transform(ST_SetSRID(ST_MakePoint(%s, %s), 4326), 2056)
                ) as distance
            FROM stops 
            WHERE ST_DWithin(
                geom,
                ST_Transform(ST_SetSRID(ST_MakePoint(%s, %s), 4326), 2056),
                %s
            )
            ORDER BY distance ASC
            LIMIT 1;
        """, (end_coords[0], end_coords[1], end_coords[0], end_coords[1], current_radius))
        
        result = cur.fetchone()
        
        if result:
            print(f"Found end station: {result[1]} at distance {result[5]:.2f}m")
            return result
            
        print(f"No stations found within {current_radius}m, increasing radius...")
        current_radius += radius_increment
    
    raise ValueError(f"No public transport stops found within {max_radius}m of end point")