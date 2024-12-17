import pytest
import requests
import statistics
from typing import List, Dict
import time
from datetime import datetime
import json
import pandas as pd
import os

# Configuration
BASE_URL = "http://127.0.0.1:8001"
NUM_SAMPLES = 100  # Number of routes to generate for each combination
RESULTS_DIR = "tests/results"

def calculate_surface_metrics(features):
    """Calculate percentage of hard vs natural surface"""
    total_length = 0
    hard_surface_length = 0
    
    for feature in features:
        length = feature['properties']['length']
        total_length += length
        if feature['properties']['belagsart'] == 'Hart':
            hard_surface_length += length
    
    hard_surface_percentage = (hard_surface_length / total_length * 100) if total_length > 0 else 0
    natural_surface_percentage = 100 - hard_surface_percentage
    
    return {
        'hard_surface_percentage': round(hard_surface_percentage, 2),
        'natural_surface_percentage': round(natural_surface_percentage, 2)
    }

def calculate_trail_metrics(features):
    """Calculate percentage of different trail types"""
    total_length = 0
    trail_lengths = {'Wanderweg': 0, 'Bergwanderweg': 0, 'Street': 0}
    
    for feature in features:
        length = feature['properties']['length']
        total_length += length
        trail_type = feature['properties']['trail_type']
        
        # Add length to appropriate category
        if trail_type in ['Wanderweg', 'Bergwanderweg']:
            trail_lengths[trail_type] += length
        else:
            trail_lengths['Street'] += length
    
    # Calculate percentages
    hiking_percentage = (trail_lengths['Wanderweg'] / total_length * 100) if total_length > 0 else 0
    mountain_percentage = (trail_lengths['Bergwanderweg'] / total_length * 100) if total_length > 0 else 0
    street_percentage = (trail_lengths['Street'] / total_length * 100) if total_length > 0 else 0
    
    return {
        'hiking_percentage': round(hiking_percentage, 2),
        'trail_percentage': round(mountain_percentage, 2),
        'street_percentage': round(street_percentage, 2)
    }

def make_route_request(params: Dict) -> Dict:
    """Make request to route API and log any errors"""
    start_time = time.time()
    try:
        response = requests.post(f"{BASE_URL}/hike", json=params)
        response.raise_for_status()
        # Add small delay to prevent overloading
        time.sleep(1)
        result = response.json()
        duration = time.time() - start_time
        
        # Calculate surface and trail metrics
        surface_metrics = calculate_surface_metrics(result['geojson']['features'])
        trail_metrics = calculate_trail_metrics(result['geojson']['features'])
        
        result.update(surface_metrics)
        result.update(trail_metrics)
        
        return result, duration
    except requests.exceptions.RequestException as e:
        print(f"Error making request: {str(e)}")
        raise

def collect_route_data(location: str, desired_length: int, num_samples: int = NUM_SAMPLES) -> List[Dict]:
    """Collect route data for multiple requests"""
    locations = {
        "rigi": {"lat": 47.0426107, "lon": 8.4872653},
        "bern": {"lat": 46.93703929969877, "lon": 7.43319760631916}
    }
    
    # Base parameters
    base_params = {
        "start": locations[location],
        "desired_length": desired_length,
        "radius": max(desired_length * 2, 5000),  # Adjust radius based on desired length
        "elevation_weight": 0,
        "surface_weight": 1,  # Always optimize for surface
        "trail_weight": 0,
        "prefer_hard_surface": False,  # Always prefer natural surface
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
    
    routes_data = []
    for i in range(num_samples):
        print(f"\nGenerating route {i+1}/{num_samples} for {location} - {desired_length}m")
        try:
            result, duration = make_route_request(base_params)
            route_data = {
                "location": location,
                "iteration": i + 1,
                "desired_length": desired_length,
                "actual_length": result["total_distance"],
                "length_difference": result["total_distance"] - desired_length,
                "length_difference_percent": ((result["total_distance"] - desired_length) / desired_length * 100),
                "request_duration": round(duration, 2)
            }
            routes_data.append(route_data)
        except Exception as e:
            print(f"Failed to generate route {i+1}: {str(e)}")
            continue
    
    return routes_data

def log_test_results(all_routes_data: List[Dict]):
    """Log test results in multiple formats for analysis"""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # Save detailed JSON results
    os.makedirs(RESULTS_DIR, exist_ok=True)
    json_path = os.path.join(RESULTS_DIR, f"length_test_results_{timestamp}.json")
    summary_path = os.path.join(RESULTS_DIR, f"length_test_summary_{timestamp}.txt")
    
    with open(json_path, "w") as f:
        json.dump(all_routes_data, f, indent=2)
    
    # Create DataFrame for analysis
    df = pd.DataFrame(all_routes_data)
    
    # Generate summary report
    with open(summary_path, "w") as f:
        f.write("Route Length Test Results\n")
        f.write(f"Generated on: {datetime.now().isoformat()}\n")
        f.write(f"Total routes generated: {len(all_routes_data)}\n")
        f.write(f"Routes per combination: {NUM_SAMPLES}\n\n")
        
        for location in ['rigi', 'bern']:
            f.write(f"\nResults for {location.upper()}:\n")
            f.write("=" * 50 + "\n")
            
            location_data = df[df['location'] == location]
            for desired_length in [1000, 5000, 10000, 15000, 20000]:
                length_data = location_data[location_data['desired_length'] == desired_length]
                f.write(f"\nDesired length: {desired_length}m\n")
                f.write(f"Number of routes: {len(length_data)}\n")
                
                # Write statistics
                f.write(f"Actual length: {length_data['actual_length'].mean():.2f} ± {length_data['actual_length'].std():.2f}m\n")
                f.write(f"Length difference: {length_data['length_difference'].mean():.2f} ± {length_data['length_difference'].std():.2f}m\n")
                f.write(f"Length difference: {length_data['length_difference_percent'].mean():.1f}% ± {length_data['length_difference_percent'].std():.1f}%\n")
                f.write(f"Request duration: {length_data['request_duration'].mean():.2f} ± {length_data['request_duration'].std():.2f}s\n")
                f.write("-" * 50 + "\n")

@pytest.mark.parametrize("location,desired_length", [
    ("rigi", 1000), ("rigi", 5000), ("rigi", 10000), ("rigi", 15000), ("rigi", 20000),
    ("bern", 1000), ("bern", 5000), ("bern", 10000), ("bern", 15000), ("bern", 20000)
])
def test_route_lengths(location: str, desired_length: int):
    """Test route generation with different lengths"""
    routes_data = collect_route_data(location, desired_length)
    test_route_lengths.all_routes_data = getattr(
        test_route_lengths, 'all_routes_data', []
    ) + routes_data
    assert isinstance(routes_data, list)

def test_all_lengths(tmp_path):
    """Run all length tests and generate comprehensive report"""
    os.makedirs(RESULTS_DIR, exist_ok=True)
    all_routes_data = getattr(test_route_lengths, 'all_routes_data', [])
    log_test_results(all_routes_data)
    assert len(all_routes_data) > 0, "No route data was collected"

if __name__ == "__main__":
    test_all_lengths()