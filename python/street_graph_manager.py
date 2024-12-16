from typing import Dict, List, Tuple, Optional
import networkx as nx
import numpy as np
from graph_manager import GraphManager
import pickle

class StreetGraphManager(GraphManager):
    def __init__(self):
        super().__init__(cache_file='street_graph.pickle')
        
    def build_graph(self, cur) -> None:
        """Build graph from street network database edges and cache it"""
        if self.graph_built:
            return
            
        # Try to load from cache first
        if self._is_cache_valid():
            try:
                with open(self.cache_file, 'rb') as f:
                    self.G = pickle.load(f)
                    self.graph_built = True
                    print(f"Loaded street graph from cache: {self.cache_file}")
                    
                    # Add connectivity check
                    weak_components = list(nx.weakly_connected_components(self.G))
                    strong_components = list(nx.strongly_connected_components(self.G))
                    print(f"Graph has {len(weak_components)} weakly connected components")
                    print(f"Graph has {len(strong_components)} strongly connected components")
                    print(f"Largest weak component has {len(max(weak_components, key=len))} nodes")
                    print(f"Largest strong component has {len(max(strong_components, key=len))} nodes")
                    return
            except Exception as e:
                print(f"Error loading street graph cache: {e}")
        
        print("Building street graph from database...")
        # Get all edges with their attributes
        cur.execute("""
            SELECT 
                id, source, target, length, geom,
                ST_IsValid(geom) as is_valid,
                ST_AsText(ST_StartPoint(geom)) as start_point,
                ST_AsText(ST_EndPoint(geom)) as end_point
            FROM strasse_clear_edges_2
            WHERE length > 0  -- Ensure we only get valid edges
        """)
        
        edges = cur.fetchall()
        print(f"Found {len(edges)} edges in street graph")
        
        # Check for potential geometry issues
        invalid_geoms = sum(1 for edge in edges if not edge[5])
        print(f"Found {invalid_geoms} invalid geometries")
        
        # Add edges (no need to double them as they're already bidirectional)
        edge_count = 0
        for edge in edges:
            if not edge[5]:  # Skip invalid geometries
                continue
            
            self.G.add_edge(
                edge[1],  # source
                edge[2],  # target
                edge_id=edge[0],
                length=edge[3],
                geom=edge[4],
                start_point=edge[6],
                end_point=edge[7]
            )
            edge_count += 1
        
        print(f"Added {edge_count} edges to street graph")
        print(f"Graph has {len(self.G.nodes)} nodes and {len(self.G.edges)} edges")
        
        # Check connectivity
        weak_components = list(nx.weakly_connected_components(self.G))
        strong_components = list(nx.strongly_connected_components(self.G))
        print(f"Graph has {len(weak_components)} weakly connected components")
        print(f"Graph has {len(strong_components)} strongly connected components")
        print(f"Largest weak component has {len(max(weak_components, key=len))} nodes")
        print(f"Largest strong component has {len(max(strong_components, key=len))} nodes")
        
        # Print some statistics about the components
        weak_sizes = [len(c) for c in weak_components]
        print(f"Weak component size distribution:")
        print(f"  Min: {min(weak_sizes)}")
        print(f"  Max: {max(weak_sizes)}")
        print(f"  Average: {sum(weak_sizes)/len(weak_sizes):.2f}")
        print(f"  Number of single-node components: {sum(1 for s in weak_sizes if s == 1)}")
        
        # Optionally, connect nearby vertices to increase connectivity
        for node in self.G.nodes:
            neighbors = list(self.G.neighbors(node))
            for neighbor in neighbors:
                if not self.G.has_edge(node, neighbor):
                    self.G.add_edge(node, neighbor, length=0)  # Add zero-length edge to connect
                    self.G.add_edge(neighbor, node, length=0)  # Ensure bidirectional
        
        # Save to cache
        try:
            with open(self.cache_file, 'wb') as f:
                pickle.dump(self.G, f)
            print(f"Saved street graph to cache: {self.cache_file}")
        except Exception as e:
            print(f"Error saving street graph cache: {e}")
        
        self.graph_built = True

    def find_shortest_path(self, start_vertex_id: int, end_vertex_id: int) -> Dict:
        """Find the shortest path between two vertices"""
        try:
            # First check if vertices are in the same component
            if not nx.has_path(self.G, start_vertex_id, end_vertex_id):
                # Try finding closest connected vertex if direct path doesn't exist
                start_component = nx.node_connected_component(self.G.to_undirected(), start_vertex_id)
                end_component = nx.node_connected_component(self.G.to_undirected(), end_vertex_id)
                
                if start_component == end_component:
                    path = nx.shortest_path(self.G, start_vertex_id, end_vertex_id, weight='length')
                else:
                    raise ValueError(f"Vertices {start_vertex_id} and {end_vertex_id} are in different components")
            else:
                path = nx.shortest_path(self.G, start_vertex_id, end_vertex_id, weight='length')
            
            total_length = sum(self.G[path[i]][path[i+1]]['length'] for i in range(len(path)-1))
            
            return {
                'path': path,
                'total_length': total_length
            }
        except nx.NetworkXNoPath:
            raise ValueError(f"No path found between vertices {start_vertex_id} and {end_vertex_id}")
        except Exception as e:
            print(f"Error finding path: {str(e)}")
            raise