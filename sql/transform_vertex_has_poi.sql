CREATE INDEX ON wanderwege_vertices_3 USING GIST(vertex);
CREATE INDEX ON planet_osm_polygon USING GIST(way);

-- Create a table to store the vertex-to-POI relationships
CREATE TABLE vertex_has_poi (
    vertex_id INTEGER,               -- The vertex ID
    poi_type TEXT,                  -- Type of POI (e.g., 'lake')
    poi_distance FLOAT,             -- Distance to the closest POI
    PRIMARY KEY (vertex_id, poi_type) -- Composite key to avoid duplicates
);

-- Insert missing vertices into the vertex_has_poi table
INSERT INTO vertex_has_poi (vertex_id, poi_type)
SELECT vertex_id, 'lake'
FROM wanderwege_vertices_3
WHERE vertex_id NOT IN (
    SELECT vertex_id
    FROM vertex_has_poi
    WHERE poi_type = 'lake'
);

CREATE OR REPLACE PROCEDURE calculate_vertex_poi_metrics(distance FLOAT)
LANGUAGE plpgsql
AS $$
BEGIN
    -- Log the start of the procedure
    RAISE NOTICE 'Starting calculation of vertex POI metrics with distance: % meters', distance;

    -- Pre-transform lakes to avoid repeated transformations
    CREATE TEMP TABLE transformed_lakes AS
    SELECT osm_id, ST_Transform(way, 2056) AS geom
    FROM planet_osm_polygon
    WHERE "natural" = 'water' AND water = 'lake';

    CREATE INDEX ON transformed_lakes USING GIST(geom);

    -- Calculate the closest lake distance in batches
    FOR batch IN 1..10 LOOP
        RAISE NOTICE 'Processing batch % of 10...', batch;
        
        UPDATE vertex_has_poi
        SET poi_distance = subquery.min_distance
        FROM (
            SELECT 
                v.vertex_id,
                ROUND(MIN(ST_Distance(v.vertex, l.geom))) AS min_distance
            FROM wanderwege_vertices_3 v
            JOIN transformed_lakes l
            ON ST_DWithin(v.vertex, l.geom, distance)
            WHERE v.vertex_id % 10 = batch - 1
            GROUP BY v.vertex_id
        ) AS subquery
        WHERE vertex_has_poi.vertex_id = subquery.vertex_id 
        AND vertex_has_poi.poi_type = 'lake';
        
        COMMIT;
    END LOOP;

    -- Clean up
    DROP TABLE transformed_lakes;

    -- Log the end of the procedure
    RAISE NOTICE 'POI metrics calculation completed successfully for distance: % meters', distance;
END;
$$;

CALL calculate_vertex_poi_metrics(5000);

CREATE INDEX idx_vertex_has_poi_lake_distance 
ON vertex_has_poi(poi_type, poi_distance) 
WHERE poi_type = 'lake';


-- restaurants
-- restaurants
-- restaurants
-- restaurants
-- restaurants
-- restaurants
-- restaurants
-- restaurants
-- restaurants
-- restaurants
-- restaurants
-- restaurants

-- Create spatial index on planet_osm_point
CREATE INDEX ON planet_osm_point USING GIST(way);

-- Insert missing restaurant vertices
INSERT INTO vertex_has_poi (vertex_id, poi_type)
SELECT vertex_id, 'restaurant_guesthouse'
FROM wanderwege_vertices_3
WHERE vertex_id NOT IN (
    SELECT vertex_id
    FROM vertex_has_poi
    WHERE poi_type = 'restaurant_guesthouse'
);

-- Create a procedure to calculate restaurant distances
CREATE OR REPLACE PROCEDURE calculate_vertex_restaurant_metrics(distance FLOAT)
LANGUAGE plpgsql
AS $$
BEGIN
    -- Log the start of the procedure
    RAISE NOTICE 'Starting calculation of vertex restaurant metrics with distance: % meters', distance;

    -- Pre-transform restaurants to avoid repeated transformations
    CREATE TEMP TABLE transformed_restaurants AS
    SELECT osm_id, ST_Transform(way, 2056) AS geom
    FROM planet_osm_point
    WHERE amenity IN ('restaurant', 'guesthouse');

    CREATE INDEX ON transformed_restaurants USING GIST(geom);

    -- Calculate the closest restaurant distance in batches
    FOR batch IN 1..10 LOOP
        RAISE NOTICE 'Processing batch % of 10...', batch;
        
        UPDATE vertex_has_poi
        SET poi_distance = subquery.min_distance
        FROM (
            SELECT 
                v.vertex_id,
                ROUND(MIN(ST_Distance(v.vertex, r.geom))) AS min_distance
            FROM wanderwege_vertices_3 v
            JOIN transformed_restaurants r
            ON ST_DWithin(v.vertex, r.geom, distance)
            WHERE v.vertex_id % 10 = batch - 1
            GROUP BY v.vertex_id
        ) AS subquery
        WHERE vertex_has_poi.vertex_id = subquery.vertex_id 
        AND vertex_has_poi.poi_type = 'restaurant_guesthouse';
        
        COMMIT;
    END LOOP;

    -- Clean up
    DROP TABLE transformed_restaurants;

    -- Log the end of the procedure
    RAISE NOTICE 'Restaurant metrics calculation completed successfully for distance: % meters', distance;
END;
$$;

CALL calculate_vertex_restaurant_metrics(5000);

-- Create index for restaurant distances
CREATE INDEX idx_vertex_has_poi_restaurant_distance 
ON vertex_has_poi(poi_type, poi_distance) 
WHERE poi_type = 'restaurant_guesthouse';