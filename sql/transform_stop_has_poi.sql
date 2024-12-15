CREATE TABLE stop_has_poi (
    stop_id INTEGER,                -- The stop ID
    poi_type TEXT,                  -- Type of POI (e.g., 'lake')
    poi_distance FLOAT,             -- Distance to the closest POI
    poi_density INTEGER,            -- Number of POIs within a specified radius
    poi_size_closest TEXT,          -- Size category of the closest POI ('S', 'M', 'L', 'XL')
    poi_size_cumulated TEXT,        -- Cumulative size category of all POIs within the radius ('S', 'M', 'L', 'XL')
    PRIMARY KEY (stop_id, poi_type) -- Composite key to avoid duplicates
);

--- make indexes
CREATE INDEX stops_geom_idx ON stops USING GIST(geom);
CREATE INDEX lakes_geom_idx ON planet_osm_polygon USING GIST(way);

CREATE INDEX idx_stop_id ON stop_has_poi(stop_id);
CREATE INDEX idx_poi_type ON stop_has_poi(poi_type);

CREATE INDEX idx_stop_id_poi_distance ON stop_has_poi(stop_id, poi_distance);
CREATE INDEX idx_poi_type_poi_distance ON stop_has_poi(poi_type, poi_distance);

CREATE INDEX idx_poi_distance ON stop_has_poi(poi_distance);

--- lakes
--- lakes
--- lakes
--- lakes
--- lakes

--- Insert Missing Stops into stop_has_poi

INSERT INTO stop_has_poi (stop_id, poi_type)
SELECT xtf_id, 'lake'
FROM stops
WHERE xtf_id NOT IN (
    SELECT stop_id
    FROM stop_has_poi
    WHERE poi_type = 'lake'
);

--- update procedure

CREATE OR REPLACE PROCEDURE calculate_poi_metrics(distance FLOAT)
LANGUAGE plpgsql
AS $$
BEGIN
    -- Log the start of the procedure
    RAISE NOTICE 'Starting calculation of POI metrics with distance: % meters', distance;

    -- Combine Query 1 (Distance) and Query 3 (Closest Lake Size)
    RAISE NOTICE 'Calculating closest lake distance and size category...';
    UPDATE stop_has_poi
    SET 
        poi_distance = subquery.min_distance,
        poi_size_closest = subquery.size_category
    FROM (
        SELECT 
            s.id AS stop_id,
            ROUND(MIN(ST_Distance(s.geom, ST_Transform(l.way, 2056)))) AS min_distance, -- Closest distance
            CASE 
                WHEN MIN(l.way_area) < 10000 THEN 'S'
                WHEN MIN(l.way_area) < 100000 THEN 'M'
                WHEN MIN(l.way_area) < 1000000 THEN 'L'
                ELSE 'XL'
            END AS size_category -- Closest lake size category
        FROM stops s
        JOIN planet_osm_polygon l
        ON l."natural" = 'water' AND l.water = 'lake'
        WHERE ST_DWithin(s.geom, ST_Transform(l.way, 2056), distance) -- Use distance parameter
        GROUP BY s.id
    ) AS subquery
    WHERE stop_has_poi.stop_id = subquery.stop_id AND stop_has_poi.poi_type = 'lake';

    -- Log completion of the first update
    RAISE NOTICE 'Finished calculating closest lake distance and size category.';

    -- Combine Query 2 (Density) and Query 4 (Cumulative Size)
    RAISE NOTICE 'Calculating lake density and cumulative size category...';
    UPDATE stop_has_poi
    SET 
        poi_density = subquery.lake_count,
        poi_size_cumulated = subquery.cumulated_size_category
    FROM (
        SELECT 
            s.id AS stop_id,
            COUNT(l.osm_id) AS lake_count, -- Number of lakes
            CASE 
                WHEN SUM(l.way_area) < 10000 THEN 'S'
                WHEN SUM(l.way_area) < 100000 THEN 'M'
                WHEN SUM(l.way_area) < 1000000 THEN 'L'
                ELSE 'XL'
            END AS cumulated_size_category -- Cumulative size category
        FROM stops s
        JOIN planet_osm_polygon l
        ON l."natural" = 'water' AND l.water = 'lake'
        WHERE ST_DWithin(s.geom, ST_Transform(l.way, 2056), distance) -- Use distance parameter
        GROUP BY s.id
    ) AS subquery
    WHERE stop_has_poi.stop_id = subquery.stop_id AND stop_has_poi.poi_type = 'lake';

    -- Log completion of the second update
    RAISE NOTICE 'Finished calculating lake density and cumulative size category.';

    -- Log the end of the procedure
    RAISE NOTICE 'POI metrics calculation completed successfully for distance: % meters', distance;

END;
$$;


CALL calculate_poi_metrics(5000); -- 5 km radius



CREATE OR REPLACE PROCEDURE calculate_restaurant_guesthouse_poi_metrics(distance FLOAT)
LANGUAGE plpgsql
AS $$
BEGIN
    -- Log the start of the procedure
    RAISE NOTICE 'Starting calculation of Restaurant/Guesthouse POI metrics with distance: % meters', distance;

    -- Step 1: Insert missing restaurant and guesthouse POI rows
    RAISE NOTICE 'Inserting missing restaurant and guesthouse POI rows...';
    INSERT INTO stop_has_poi (stop_id, poi_type)
    SELECT DISTINCT CAST(s.xtf_id AS character varying), 'restaurant_guesthouse'
    FROM stops s
    WHERE NOT EXISTS (
        SELECT 1 
        FROM stop_has_poi shp
        WHERE shp.stop_id = CAST(s.xtf_id AS character varying) AND shp.poi_type = 'restaurant_guesthouse'
    );

    -- Step 2: Calculate closest restaurant/guesthouse distance
    RAISE NOTICE 'Calculating closest restaurant/guesthouse distance...';
    UPDATE stop_has_poi
    SET 
        poi_distance = subquery.min_distance,
        poi_size_closest = NULL -- Leaving this empty
    FROM (
        SELECT 
            CAST(s.xtf_id AS character varying) AS stop_id,
            ROUND(MIN(ST_Distance(s.geom, ST_Transform(p.way, 2056)))) AS min_distance -- Closest distance
        FROM stops s
        JOIN planet_osm_point p
        ON p.amenity IN ('restaurant', 'guesthouse') -- Filter for restaurants and guesthouses
        WHERE ST_DWithin(s.geom, ST_Transform(p.way, 2056), distance) -- Use distance parameter
        GROUP BY s.xtf_id
    ) AS subquery
    WHERE stop_has_poi.stop_id = subquery.stop_id AND stop_has_poi.poi_type = 'restaurant_guesthouse';

    -- Step 3: Calculate density
    RAISE NOTICE 'Calculating restaurant/guesthouse density...';
    UPDATE stop_has_poi
    SET 
        poi_density = subquery.poi_count,
        poi_size_cumulated = NULL -- Leaving this empty
    FROM (
        SELECT 
            CAST(s.xtf_id AS character varying) AS stop_id,
            COUNT(p.osm_id) AS poi_count -- Number of POIs
        FROM stops s
        JOIN planet_osm_point p
        ON p.amenity IN ('restaurant', 'guesthouse') -- Filter for restaurants and guesthouses
        WHERE ST_DWithin(s.geom, ST_Transform(p.way, 2056), distance) -- Use distance parameter
        GROUP BY s.xtf_id
    ) AS subquery
    WHERE stop_has_poi.stop_id = subquery.stop_id AND stop_has_poi.poi_type = 'restaurant_guesthouse';

    -- Log the end of the procedure
    RAISE NOTICE 'Restaurant/Guesthouse POI metrics calculation completed successfully for distance: % meters', distance;

END;
$$;

-- Call the procedure with a 5 km radius
CALL calculate_restaurant_guesthouse_poi_metrics(5000);
