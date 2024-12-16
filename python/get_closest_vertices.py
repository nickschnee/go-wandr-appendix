def get_closest_vertices(cur, start_point_wkt, end_point_wkt, vertices_table=VERTICES_TABLE):
    cur.execute(f"""
        SELECT vertex_id, ST_AsText(vertex), ST_Distance(vertex, ST_GeomFromText(%s, 2056)) AS dist
        FROM {vertices_table}
        ORDER BY dist ASC LIMIT 1;
    """, (start_point_wkt,))
    closest_start_vertex = cur.fetchone()

    cur.execute(f"""
        SELECT vertex_id, ST_AsText(vertex), ST_Distance(vertex, ST_GeomFromText(%s, 2056)) AS dist
        FROM {vertices_table}
        ORDER BY dist ASC LIMIT 1;
    """, (end_point_wkt,))
    closest_end_vertex = cur.fetchone()

    return closest_start_vertex, closest_end_vertex