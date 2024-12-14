import networkx as nx
import pickle
import os
import numpy as np
from typing import Dict, List, Tuple, Optional
from models import ElevationType, TrailType
from cost_utils import calculate_cost
from datetime import datetime, timedelta
import math

class GraphManager:
    CACHE_DURATION = timedelta(hours=24)  # Cache valid for 24 hours
    ELEVATION_SENSITIVITY = 5.0  # Add this line back
    
    def __init__(self, cache_file='hiking_graph.pickle'):
        self.G = nx.DiGraph()
        self.graph_built = False
        self.cache_file = cache_file
        
    def _is_cache_valid(self):
        """Check if cache file exists"""
        return os.path.exists(self.cache_file)
        
    def build_graph(self, cur) -> None:
        """Build graph from database edges and cache it"""
        if self.graph_built:
            return
            
        # Try to load from cache first
        if self._is_cache_valid():
            try:
                with open(self.cache_file, 'rb') as f:
                    self.G = pickle.load(f)
                    self.graph_built = True
                    print(f"Loaded graph from cache: {self.cache_file}")
                    return
            except Exception as e:
                print(f"Error loading cache: {e}")
        
        # If cache invalid or loading failed, build from database
        print("Building graph from database...")
        # Get all edges with their attributes
        cur.execute("""
            SELECT id, source, target, length, elevation_difference, 
                   belagsart, wanderwege, tobler_duration, geom
            FROM wanderwege_edges_3
        """)
        
        # Add edges with attributes
        for edge in cur.fetchall():
            self.G.add_edge(
                edge[1],  # source
                edge[2],  # target
                edge_id=edge[0],
                length=edge[3],
                elevation_diff=edge[4],
                surface=edge[5],
                trail_type=edge[6],
                duration=edge[7],
                geom=edge[8]
            )
        
        # Save to cache
        try:
            with open(self.cache_file, 'wb') as f:
                pickle.dump(self.G, f)
            print(f"Saved graph to cache: {self.cache_file}")
        except Exception as e:
            print(f"Error saving cache: {e}")
            
        self.graph_built = True
    
    def find_exploration_path(self, 
                            cur,
                            start_vertex: int, 
                            desired_length: float,
                            cost_weights: Dict[str, float],
                            tolerance: float,
                            avoid_vertices: List[int],
                            elevation_type: ElevationType,
                            prefer_hard_surface: bool,
                            preferred_trail_type: TrailType) -> Dict:
        """Find path that meets length criteria with lowest cost"""
        
        min_length = desired_length * (1 - tolerance)
        max_length = desired_length * (1 + tolerance)
        
        def cost_function(u, v, d):
            # Calculate individual costs
            elevation_cost = self._calculate_elevation_cost(d, elevation_type)
            surface_cost = self._calculate_surface_cost({'surface': d.get('surface'), 
                                                       'prefer_hard_surface': prefer_hard_surface})
            trail_cost = self._calculate_trail_cost({'trail_type': d.get('trail_type'), 
                                                       'preferred_trail_type': preferred_trail_type})
            
            # Calculate weighted sum
            weighted_cost = (
                cost_weights.get('elevation', 1.0) * elevation_cost +
                cost_weights.get('surface', 0.0) * surface_cost +
                cost_weights.get('trail', 0.0) * trail_cost
            )
            
            # Apply avoidance penalty if needed
            if avoid_vertices and (u in avoid_vertices or v in avoid_vertices):
                weighted_cost *= 10
            
            # Log detailed cost breakdown
            print(f"\nExplore Edge {u}->{v}:")
            print(f"  Optimization type: {elevation_type.name}")
            print(f"  Elevation diff: {d.get('elevation_diff', 0)}")
            print(f"  Surface type: {d.get('surface')}")
            print(f"  Trail type: {d.get('trail_type')}")
            print(f"  Costs:")
            print(f"    Elevation: {elevation_cost} (weight: {cost_weights.get('elevation', 1.0)})")
            print(f"    Surface: {surface_cost} (weight: {cost_weights.get('surface', 0.0)})")
            print(f"    Trail: {trail_cost} (weight: {cost_weights.get('trail', 0.0)})")
            print(f"    Final weighted cost: {weighted_cost}")
            
            return max(0.000001, weighted_cost)
        
        # Use ego_graph to get subgraph within max_length distance
        subgraph = nx.ego_graph(
            self.G,
            start_vertex,
            radius=max_length,
            distance='length'
        )
        
        # Use NetworkX's single_source_dijkstra on the subgraph
        distances, paths = nx.single_source_dijkstra(
            subgraph,
            start_vertex,
            weight=cost_function,
            cutoff=max_length
        )
        
        # Filter paths within desired length range
        valid_paths = {}
        for target, path in paths.items():
            path_length = self._calculate_path_length(path)
            if min_length <= path_length <= max_length:
                valid_paths[target] = (distances[target], path)
        
        if not valid_paths:
            raise ValueError(f"No paths found within length range {min_length}-{max_length}")
            
        # Find path with minimum cost
        best_target = min(valid_paths.keys(), key=lambda k: valid_paths[k][0])
        best_cost, best_path = valid_paths[best_target]
        
        return {
            'end_vertex': best_target,
            'total_length': self._calculate_path_length(best_path),
            'path': best_path
        }
    
    def _calculate_path_length(self, path: List[int]) -> float:
        """Calculate total length of a path"""
        return sum(
            self.G[u][v]['length']
            for u, v in zip(path[:-1], path[1:])
        )
    
    def _calculate_elevation_cost(self, edge_data: Dict, elevation_type: ElevationType = ElevationType.ELEVATION_GAIN) -> float:
        """Calculate elevation-based cost based on preference type using logistic functions"""
        elevation_diff = edge_data.get('elevation_diff', 0)
        sensitivity = 5  # sensitivity factor s from the formulas
        
        if elevation_diff == 0:
            return 0.5
        
        if elevation_type == ElevationType.ELEVATION_GAIN:
            # Cost approaches 0 for uphill (positive elevation_diff)
            # Cost approaches 1 for downhill (negative elevation_diff)
            return 1 / (1 + math.exp(elevation_diff/sensitivity))
        
        elif elevation_type == ElevationType.ELEVATION_LOSS:
            # Inverted logistic function
            # Cost approaches 0 for downhill (negative elevation_diff)
            # Cost approaches 1 for uphill (positive elevation_diff)
            return 1 / (1 + math.exp(-elevation_diff/sensitivity))
        
        else:  # ElevationType.ELEVATION_LEVEL
            # Exponential decay function
            # Cost approaches 0 for flat segments
            # Cost approaches 1 for large elevation differences
            return 1 - math.exp(-abs(elevation_diff)/sensitivity)
    
    def _calculate_surface_cost(self, edge_data: Dict) -> float:
        """Calculate surface-based cost"""
        surface = edge_data.get('surface')
        
        if surface == 'Hart':
            return 0.0 if edge_data.get('prefer_hard_surface', False) else 1.0
        elif surface == 'Natur':
            return 1.0 if edge_data.get('prefer_hard_surface', False) else 0.0
        else:
            return 0.5
    
    def _calculate_trail_cost(self, edge_data: Dict) -> float:
        """Calculate trail type based cost"""
        trail_type = edge_data.get('trail_type')
        preferred_type = edge_data.get('preferred_trail_type', TrailType.HIKING)
        
        trail_mapping = {
            TrailType.ALPINE: "Alpinwanderweg",
            TrailType.MOUNTAIN: "Bergwanderweg",
            TrailType.HIKING: "Wanderweg"
        }
        
        if trail_type == trail_mapping.get(preferred_type):
            return 0.0
        elif trail_type is None:
            return 0.5
        else:
            return 1.0
    
    def find_path_to_target(self,
                           start_vertex: int,
                           target_vertex: int,
                           cost_weights: Dict[str, float],
                           search_radius: float,
                           elevation_type: ElevationType,
                           prefer_hard_surface: bool,
                           preferred_trail_type: TrailType) -> Dict:
        """Find shortest path between two vertices using custom cost function"""
        
        def cost_function(u, v, d):
            # Calculate individual costs
            elevation_cost = self._calculate_elevation_cost(d, elevation_type)
            surface_cost = self._calculate_surface_cost({'surface': d.get('surface'), 
                                                       'prefer_hard_surface': prefer_hard_surface})
            trail_cost = self._calculate_trail_cost({'trail_type': d.get('trail_type'), 
                                                   'preferred_trail_type': preferred_trail_type})
            
            # Calculate weighted sum
            weighted_cost = (
                cost_weights.get('elevation', 1.0) * elevation_cost +
                cost_weights.get('surface', 0.0) * surface_cost +
                cost_weights.get('trail', 0.0) * trail_cost
            )
            
            # Log detailed cost breakdown
            print(f"\nBounce Edge {u}->{v}:")
            print(f"  Optimization type: {elevation_type.name}")
            print(f"  Elevation diff: {d.get('elevation_diff', 0)}")
            print(f"  Surface type: {d.get('surface')}")
            print(f"  Trail type: {d.get('trail_type')}")
            print(f"  Costs:")
            print(f"    Elevation: {elevation_cost} (weight: {cost_weights.get('elevation', 1.0)})")
            print(f"    Surface: {surface_cost} (weight: {cost_weights.get('surface', 0.0)})")
            print(f"    Trail: {trail_cost} (weight: {cost_weights.get('trail', 0.0)})")
            print(f"    Final weighted cost: {weighted_cost}")
            
            return max(0.000001, weighted_cost)
        
        # Create subgraph within search radius around both vertices
        subgraph = nx.ego_graph(
            self.G,
            start_vertex,
            radius=search_radius,
            distance='length'
        )
        
        if target_vertex not in subgraph:
            raise ValueError(f"Target vertex {target_vertex} not found within {search_radius}m of start vertex")
        
        try:
            # Find shortest path using dijkstra
            distance, path = nx.single_source_dijkstra(
                subgraph,
                start_vertex,
                target=target_vertex,
                weight=cost_function
            )
            
            return {
                'end_vertex': target_vertex,
                'total_length': self._calculate_path_length(path),
                'path': path
            }
            
        except nx.NetworkXNoPath:
            raise ValueError(f"No path found between vertices {start_vertex} and {target_vertex}")