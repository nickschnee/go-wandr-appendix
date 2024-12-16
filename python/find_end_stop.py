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