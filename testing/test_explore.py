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

def collect_route_data(location: str, optimization_type: str, num_samples: int = NUM_SAMPLES) -> List[Dict]:
    """Collect route data for multiple requests"""
    locations = {
        "rigi": {"lat": 47.0426107, "lon": 8.4872653},
        "bern": {"lat": 46.93703929969877, "lon": 7.43319760631916}
    }
    
    # Base parameters
    base_params = {
        "start": locations[location],
        "desired_length": 5000,
        "radius": 5000,
        "elevation_weight": 0,
        "surface_weight": 0,
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
    
    # Set optimization parameters
    if optimization_type in ['gain', 'loss', 'level']:
        base_params['elevation'] = optimization_type
        base_params['elevation_weight'] = 1
    elif optimization_type in ['hard', 'natural']:
        base_params['surface_weight'] = 1
        base_params['prefer_hard_surface'] = (optimization_type == 'hard')
    elif optimization_type in ['hiking', 'trail']:
        base_params['trail_weight'] = 1
        base_params['preferred_trail_type'] = optimization_type
    
    routes_data = []
    for i in range(num_samples):
        print(f"\nGenerating route {i+1}/{num_samples} for {location} - {optimization_type}")
        try:
            result, duration = make_route_request(base_params)
            route_data = {
                "location": location,
                "iteration": i + 1,
                "optimization_type": optimization_type,
                "net_elevation_difference": result["net_elevation_difference"],
                "total_elevation_gain": result["total_elevation_gain"],
                "total_elevation_loss": result["total_elevation_loss"],
                "total_distance": result["total_distance"],
                "hard_surface_percentage": result["hard_surface_percentage"],
                "natural_surface_percentage": result["natural_surface_percentage"],
                "hiking_percentage": result["hiking_percentage"],
                "trail_percentage": result["trail_percentage"],
                "street_percentage": result["street_percentage"],
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
    json_path = os.path.join(RESULTS_DIR, f"test_results_{timestamp}.json")
    summary_path = os.path.join(RESULTS_DIR, f"test_summary_{timestamp}.txt")
    
    with open(json_path, "w") as f:
        json.dump(all_routes_data, f, indent=2)
    
    # Create DataFrame for analysis
    df = pd.DataFrame(all_routes_data)
    
    # Generate summary report
    with open(summary_path, "w") as f:
        f.write("Route Generation Test Results\n")
        f.write(f"Generated on: {datetime.now().isoformat()}\n")
        f.write(f"Total routes generated: {len(all_routes_data)}\n")
        f.write(f"Routes per combination: {NUM_SAMPLES}\n\n")
        
        for location in ['rigi', 'bern']:
            f.write(f"\nResults for {location.upper()}:\n")
            f.write("=" * 50 + "\n")
            
            location_data = df[df['location'] == location]
            for optimization_type in ['gain', 'loss', 'level', 'hard', 'natural', 'hiking', 'mountain']:
                type_data = location_data[location_data['optimization_type'] == optimization_type]
                f.write(f"\n{optimization_type.upper()} optimization:\n")
                f.write(f"Number of routes: {len(type_data)}\n")
                
                # For each metric, write mean ± std
                f.write(f"Elevation difference: {type_data['net_elevation_difference'].mean():.2f} ± {type_data['net_elevation_difference'].std():.2f}m\n")
                f.write(f"Elevation gain: {type_data['total_elevation_gain'].mean():.2f} ± {type_data['total_elevation_gain'].std():.2f}m\n")
                f.write(f"Elevation loss: {type_data['total_elevation_loss'].mean():.2f} ± {type_data['total_elevation_loss'].std():.2f}m\n")
                f.write(f"Distance: {type_data['total_distance'].mean():.2f} ± {type_data['total_distance'].std():.2f}m\n")
                f.write(f"Hard surface: {type_data['hard_surface_percentage'].mean():.2f} ± {type_data['hard_surface_percentage'].std():.2f}%\n")
                f.write(f"Natural surface: {type_data['natural_surface_percentage'].mean():.2f} ± {type_data['natural_surface_percentage'].std():.2f}%\n")
                f.write(f"Hiking trail: {type_data['hiking_percentage'].mean():.2f} ± {type_data['hiking_percentage'].std():.2f}%\n")
                f.write(f"Mountain trail: {type_data['trail_percentage'].mean():.2f} ± {type_data['trail_percentage'].std():.2f}%\n")
                f.write(f"Street: {type_data['street_percentage'].mean():.2f} ± {type_data['street_percentage'].std():.2f}%\n")
                f.write(f"Request duration: {type_data['request_duration'].mean():.2f} ± {type_data['request_duration'].std():.2f}s\n")
                f.write("-" * 50 + "\n")

@pytest.mark.parametrize("location,optimization_type", [
    ("rigi", "gain"), ("rigi", "loss"), ("rigi", "level"),
    ("rigi", "hard"), ("rigi", "natural"),
    ("rigi", "hiking"), ("rigi", "mountain"),
    ("bern", "gain"), ("bern", "loss"), ("bern", "level"),
    ("bern", "hard"), ("bern", "natural"),
    ("bern", "hiking"), ("bern", "mountain")
])
def test_elevation_optimization(location: str, optimization_type: str):
    """Test route generation with different optimization types"""
    routes_data = collect_route_data(location, optimization_type)
    # Store data for the comprehensive report
    test_elevation_optimization.all_routes_data = getattr(
        test_elevation_optimization, 'all_routes_data', []
    ) + routes_data
    # Assert we got some data (basic validation)
    assert isinstance(routes_data, list)

def test_all_elevations(tmp_path):
    """Run all elevation tests and generate comprehensive report"""
    # Create results directory if it doesn't exist
    os.makedirs(RESULTS_DIR, exist_ok=True)
    
    # Get all collected data from individual tests
    all_routes_data = getattr(test_elevation_optimization, 'all_routes_data', [])
    
    # Log all results
    log_test_results(all_routes_data)
    
    # Basic validation
    assert len(all_routes_data) > 0, "No route data was collected"

if __name__ == "__main__":
    test_all_elevations()