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