def find_bounce_path(cur, start_vertex_id, desired_length, elevation_type, 
                    surface_weight, elevation_weight, trail_weight,
                    prefer_hard_surface, preferred_trail_type,
                    bounce_factor=0.4, poi_preferences=None):
    max_attempts = 10
    attempts = 0
    last_error = None
    
    # Create weights dictionary
    weights = {
        'elevation': elevation_weight,
        'surface': surface_weight,
        'trail': trail_weight
    }
    
    # Create kwargs dictionary for additional parameters
    path_kwargs = {
        'mode': 'bounce',
        'prefer_hard_surface': prefer_hard_surface,
        'preferred_trail_type': preferred_trail_type
    }
    
    print("poi_preferences in find bounce III:", poi_preferences)
    
    # Get bounce vertices generator
    bounce_vertices = choose_bounce_vertices_generator(
        cur, 
        start_vertex_id, 
        desired_length * bounce_factor,
        poi_preferences
    )
    
    while attempts < max_attempts:
        attempts += 1
        
        try:
            # Get next bounce vertex
            target_vertex, lake_distance = next(bounce_vertices)
            
            # Get bounce vertex coordinates
            cur.execute(f"""
                SELECT 
                    ST_X(ST_Transform(vertex, 4326)) as lon,
                    ST_Y(ST_Transform(vertex, 4326)) as lat
                FROM {VERTICES_TABLE}
                WHERE vertex_id = %s
            """, (target_vertex,))
            
            bounce_coords = cur.fetchone()
            if bounce_coords:
                bounce_lon, bounce_lat = bounce_coords
            else:
                raise ValueError(f"Could not find coordinates for vertex {target_vertex}")
            
            print(f"\nAttempt {attempts}: Trying target vertex {target_vertex} (distance to lake: {round(lake_distance) if lake_distance else 'N/A'}m)")
            
            # Find path to bounce point
            outbound_result = find_path_to_target(
                cur,
                start_vertex_id,
                target_vertex,
                weights,
                elevation_type,
                prefer_hard_surface,
                preferred_trail_type,
                search_radius=20000
            )
            
            # Calculate remaining length with more flexibility
            remaining_length = max(
                desired_length * 0.2,  # Minimum 20% of desired length (was 30%)
                min(
                    desired_length * 0.6,  # Maximum 60% of desired length
                    desired_length - outbound_result['total_length']
                )
            )
            
            print(f"Outbound length: {outbound_result['total_length']}m")
            print(f"Remaining length target: {remaining_length}m")
            
            # Get the outbound path vertices to avoid
            vertices_to_avoid = outbound_result['path']
            
            # From the bounce point, find a path using regular explore mode
            explore_result = find_end_vertex(
                cur,
                target_vertex,  # Start from bounce point
                remaining_length,
                elevation_type,
                surface_weight=surface_weight,
                elevation_weight=elevation_weight,
                trail_weight=trail_weight,
                prefer_hard_surface=prefer_hard_surface,
                preferred_trail_type=preferred_trail_type,
                poi_preferences=poi_preferences,
                avoid_vertices=vertices_to_avoid  # Pass vertices to avoid
            )
            
            # If successful, return the combined result
            return {
                'total_length': outbound_result['total_length'] + explore_result['total_length'],
                'path': outbound_result['path'] + explore_result['path'],
                'bounce_coordinates': [bounce_lon, bounce_lat],
                'bounce_poi_type': 'restaurant' if poi_preferences and poi_preferences.restaurant 
                                  else 'lake' if poi_preferences and poi_preferences.lake 
                                  else None
            }
            
        except (ValueError, StopIteration) as e:
            last_error = e
            print(f"Failed: {str(e)}")
            continue
    
    raise ValueError(f"Could not find any valid bounce path after {max_attempts} attempts. Last error: {last_error}")

def choose_bounce_vertices_generator(cur, start_vertex_id, target_distance, poi_preferences):
    """
    Generator that yields suitable bounce points (vertex_id, poi_distance) based on:
    - Distance from start vertex (approximately target_distance)
    - Proximity to specified POI type (lake or restaurant)
    """
    # Determine POI type based on preferences
    poi_type = 'restaurant_guesthouse' if poi_preferences.restaurant else 'lake'
    
    # Query vertices that meet both distance and POI criteria
    query = """
    SELECT 
        v.vertex_id,
        p.poi_distance
    FROM wanderwege_vertices_3 v
    JOIN vertex_has_poi p ON v.vertex_id = p.vertex_id
    WHERE 
        p.poi_type = %s AND
        p.poi_distance <= %s AND
        ST_DWithin(v.vertex, 
            (SELECT vertex FROM wanderwege_vertices_3 WHERE vertex_id = %s),
            %s)
    ORDER BY 
        (p.poi_distance / %s) +  -- Normalize POI distance
        (ABS(ST_Distance(v.vertex, 
            (SELECT vertex FROM wanderwege_vertices_3 WHERE vertex_id = %s)) 
            - %s) / %s) ASC      -- Normalize target distance
    """
    
    cur.execute(query, (
        poi_type,
        current_poi_distance,
        start_vertex_id,
        current_distance * 1.2,  # Allow 20% flexibility
        current_poi_distance,
        start_vertex_id,
        target_distance,
        target_distance
    ))
    
    if row := cur.fetchone():
        yield row[0], row[1]  # vertex_id, poi_distance