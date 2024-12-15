CREATE INDEX ON wanderwege_edges_3 USING GIST(geom);
CREATE INDEX ON planet_osm_polygon USING GIST(way);

-- Create a table to store the edge-to-POI relationships
CREATE TABLE edge_has_poi (
    edge_id INTEGER,                -- The edge ID
    poi_type TEXT,                  -- Type of POI (e.g., 'lake')
    poi_distance FLOAT,             -- Distance to the closest POI
    PRIMARY KEY (edge_id, poi_type) -- Composite key to avoid duplicates
);

-- Insert missing edges into the edge_has_poi table
INSERT INTO edge_has_poi (edge_id, poi_type)
SELECT id, 'lake'
FROM wanderwege_edges_3
WHERE id NOT IN (
    SELECT edge_id
    FROM edge_has_poi
    WHERE poi_type = 'lake'
);


CREATE OR REPLACE PROCEDURE calculate_edge_poi_metrics(distance FLOAT)
LANGUAGE plpgsql
AS $$
BEGIN
    -- Log the start of the procedure
    RAISE NOTICE 'Starting calculation of edge POI metrics with distance: % meters', distance;

    -- Pre-transform lakes to avoid repeated transformations
    CREATE TEMP TABLE transformed_lakes AS
    SELECT osm_id, ST_Transform(way, 2056) AS geom
    FROM planet_osm_polygon
    WHERE "natural" = 'water' AND water = 'lake';

    CREATE INDEX ON transformed_lakes USING GIST(geom);

    -- Calculate the closest lake distance in batches
    FOR batch IN 1..10 LOOP
        RAISE NOTICE 'Processing batch % of 10...', batch;
        
        UPDATE edge_has_poi
        SET poi_distance = subquery.min_distance
        FROM (
            SELECT 
                e.id AS edge_id,
                ROUND(MIN(ST_Distance(e.geom, l.geom))) AS min_distance
            FROM wanderwege_edges_3 e
            JOIN transformed_lakes l
            ON ST_DWithin(e.geom, l.geom, distance)
            WHERE e.id % 10 = batch - 1
            GROUP BY e.id
        ) AS subquery
        WHERE edge_has_poi.edge_id = subquery.edge_id 
        AND edge_has_poi.poi_type = 'lake';
        
        COMMIT;
    END LOOP;

    -- Clean up
    DROP TABLE transformed_lakes;

    -- Log the end of the procedure
    RAISE NOTICE 'POI metrics calculation completed successfully for distance: % meters', distance;
END;
$$;

CALL calculate_edge_poi_metrics(5000);