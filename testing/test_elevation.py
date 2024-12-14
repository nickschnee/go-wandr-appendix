import pytest
import requests
import statistics
from typing import List, Dict
import time
from datetime import datetime
import json

BASE_URL = "http://127.0.0.1:8001"

def make_route_request(params: Dict) -> Dict:
    """Make request to route API with retry logic and delay"""
    max_retries = 3
    retry_delay = 2
    
    for attempt in range(max_retries):
        try:
            response = requests.post(f"{BASE_URL}/hike", json=params)
            response.raise_for_status()
            # Add delay to prevent overloading
            time.sleep(1)
            return response.json()
        except requests.exceptions.RequestException as e:
            if attempt == max_retries - 1:
                raise
            time.sleep(retry_delay)

def collect_elevation_data(elevation_type: str, num_samples: int = 10) -> List[float]:
    """Collect elevation data for multiple route requests"""
    base_params = {
        "start": {
            #rigi klösterli
            "lat": 47.0426107,
            "lon": 8.4872653
            #bern
            # "lat": 46.93703929969877,
            # "lon": 7.43319760631916
        },
        "desired_length": 2000,
        "radius": 10000,
        "elevation": elevation_type,
        "surface_weight": 0,
        "elevation_weight": 1,
        "trail_weight": 0,
        "prefer_hard_surface": False,
        "preferred_trail_type": "hiking",
        "mode": "explore",
        "is_minutes": False,
        "is_sunday": False,
        "poi_preferences": {
            "lake": False,
            "lake_sizes": ["M"],
            "lake_distance": 5000,
            "avoid_urban": False,
            "urban_distance": 5000,
            "restaurant": False,
            "restaurant_distance": 1000,
            "min_restaurant_density": 1
        }
    }
    
    elevation_differences = []
    
    for _ in range(num_samples):
        result = make_route_request(base_params)
        elevation_differences.append(result["net_elevation_difference"])
        
    return elevation_differences

def log_test_results(elevation_type: str, stats: Dict):
    timestamp = datetime.now().isoformat()
    log_entry = {
        "timestamp": timestamp,
        "elevation_type": elevation_type,
        "average_elevation": round(stats["avg_elevation"], 1),
        "standard_deviation": round(stats["std_dev"], 2),
        "sample_size": stats["sample_size"],
        "elevation_differences": stats["elevation_differences"]
    }
    
    # Append to log file
    with open("test_results.jsonl", "a") as f:
        f.write(json.dumps(log_entry) + "\n")

@pytest.mark.parametrize("elevation_type,expected_min_avg", [
    ("gain", 1),    # Expect average elevation gain of at least 50m
    ("loss", -1),   # Expect average elevation loss of at least 50m
    ("level", -50)   # Expect relatively flat routes (±10m average)
])
def test_elevation_optimization(elevation_type: str, expected_min_avg: float):
    # Collect elevation data
    elevation_differences = collect_elevation_data(elevation_type)
    
    # Calculate statistics
    avg_elevation = statistics.mean(elevation_differences)
    std_dev = statistics.stdev(elevation_differences)
    
    # Create stats dictionary
    stats = {
        "avg_elevation": avg_elevation,
        "std_dev": std_dev,
        "sample_size": len(elevation_differences),
        "elevation_differences": elevation_differences
    }
    
    # Log results
    log_test_results(elevation_type, stats)
    
    print(f"\nResults for {elevation_type}:")
    print(f"Average elevation difference: {avg_elevation:.2f}m")
    print(f"Standard deviation: {std_dev:.2f}m")
    print(f"Sample size: {len(elevation_differences)}")
    print(f"All elevation differences: {elevation_differences}")
    
    if elevation_type == "gain":
        assert avg_elevation > expected_min_avg, f"Expected average elevation gain > {expected_min_avg}m, got {avg_elevation:.2f}m"
    elif elevation_type == "loss":
        assert avg_elevation < expected_min_avg, f"Expected average elevation loss < {expected_min_avg}m, got {avg_elevation:.2f}m"
    else:  # level
        assert abs(avg_elevation) < abs(expected_min_avg), f"Expected average elevation change within ±{abs(expected_min_avg)}m, got {avg_elevation:.2f}m"