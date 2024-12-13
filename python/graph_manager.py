import networkx as nx
import pickle
import os
import numpy as np
from typing import Dict, List, Tuple, Optional
from models import ElevationType, TrailType
from cost_utils import calculate_cost
from datetime import datetime, timedelta

class GraphManager:
    CACHE_DURATION = timedelta(hours=24)  # Cache valid for 24 hours
    ELEVATION_SENSITIVITY = 5.0  # Add this line back
    
    def __init__(self, cache_file='hiking_graph.pickle'):
        self.G = nx.DiGraph()
        self.graph_built = False
        self.cache_file = cache_file
        
    def _is_cache_valid(self):
        """Check if cache file exists and is recent enough"""
        if not os.path.exists(self.cache_file):
            return False
            
        # Check file age
        file_time = datetime.fromtimestamp(os.path.getmtime(self.cache_file))
        return datetime.now() - file_time < self.CACHE_DURATION
        
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
                            cur,  # Pass cursor instead of using stored connection
                            start_vertex: int, 
                            desired_length: float,
                            cost_weights: Dict[str, float],
                            tolerance: float = 0.1) -> Dict:
        """Find path that meets length criteria with lowest cost"""
        
        min_length = desired_length * (1 - tolerance)
        max_length = desired_length * (1 + tolerance)
        
        def cost_function(u, v, d):
            return (
                cost_weights['elevation'] * self._calculate_elevation_cost(d) +
                cost_weights['surface'] * self._calculate_surface_cost(d) +
                cost_weights['trail'] * self._calculate_trail_cost(d)
            )
        
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
    
    def _calculate_elevation_cost(self, edge_data: Dict) -> float:
        """Calculate elevation-based cost similar to SQL implementation"""
        elevation_diff = edge_data.get('elevation_diff', 0)
        
        if elevation_diff == 0:
            return 0.5
        
        # Sigmoid-like function similar to SQL implementation
        return 1.0 / (1.0 + np.exp(elevation_diff / self.ELEVATION_SENSITIVITY))
    
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
                           search_radius: float = 20000,
                           **kwargs) -> Dict:
        """Find shortest path between two vertices using custom cost function"""
        
        def cost_function(u, v, d):
            return calculate_cost(u, v, d, cost_weights)
        
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