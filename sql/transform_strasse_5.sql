CREATE OR REPLACE PROCEDURE transform_strassen(
    source_table text DEFAULT 'strasse_clear',
    target_vertices_table text DEFAULT 'strasse_clear_vertices',
    target_edges_table text DEFAULT 'strasse_clear_edges'
)
LANGUAGE plpgsql
AS $$
DECLARE
    create_vertices_sql text;
    create_edges_sql text;
BEGIN
    RAISE NOTICE 'Starting strassen transformation procedure...';

    -- Create vertices table
    RAISE NOTICE 'Creating % table...', target_vertices_table;
    create_vertices_sql := format('
        CREATE TABLE %I AS
        SELECT DISTINCT ST_StartPoint(ST_GeometryN(geom, 1)) AS vertex FROM %I
        UNION
        SELECT DISTINCT ST_EndPoint(ST_GeometryN(geom, ST_NumGeometries(geom))) AS vertex FROM %I',
        target_vertices_table, source_table, source_table
    );
    EXECUTE create_vertices_sql;

    -- Create edges table
    RAISE NOTICE 'Creating % table...', target_edges_table;
    create_edges_sql := format('
        CREATE TABLE %I AS
        SELECT
            id,
            ST_StartPoint(ST_GeometryN(geom, 1)) AS source,
            ST_EndPoint(ST_GeometryN(geom, ST_NumGeometries(geom))) AS target,
            ST_Length(geom) AS length,
            geom
        FROM %I',
        target_edges_table, source_table
    );
    EXECUTE create_edges_sql;

    -- Create initial indexes
    RAISE NOTICE 'Creating initial indexes...';
    EXECUTE format('CREATE INDEX %I_vertex_idx ON %I USING GIST (vertex)', target_vertices_table, target_vertices_table);
    EXECUTE format('CREATE INDEX %I_source_idx ON %I USING GIST (source)', target_edges_table, target_edges_table);
    EXECUTE format('CREATE INDEX %I_target_idx ON %I USING GIST (target)', target_edges_table, target_edges_table);

    -- Add vertex_id to vertices table
    RAISE NOTICE 'Adding vertex_id to %...', target_vertices_table;
    EXECUTE format('ALTER TABLE %I ADD COLUMN vertex_id SERIAL PRIMARY KEY', target_vertices_table);

    -- Add source and target vertex ID columns to edges table
    RAISE NOTICE 'Adding source and target vertex ID columns to %...', target_edges_table;
    EXECUTE format('ALTER TABLE %I ADD COLUMN source_vertex_id INTEGER', target_edges_table);
    EXECUTE format('ALTER TABLE %I ADD COLUMN target_vertex_id INTEGER', target_edges_table);

    -- Update source vertex IDs
    RAISE NOTICE 'Updating source vertex IDs...';
    EXECUTE format('
        UPDATE %I e
        SET source_vertex_id = v.vertex_id
        FROM %I v
        WHERE ST_Equals(e.source, v.vertex)',
        target_edges_table, target_vertices_table
    );

    -- Update target vertex IDs
    RAISE NOTICE 'Updating target vertex IDs...';
    EXECUTE format('
        UPDATE %I e
        SET target_vertex_id = v.vertex_id
        FROM %I v
        WHERE ST_Equals(e.target, v.vertex)',
        target_edges_table, target_vertices_table
    );

    -- Drop old columns and rename new ones
    RAISE NOTICE 'Dropping old columns and renaming new ones...';
    EXECUTE format('
        ALTER TABLE %I 
        DROP COLUMN source,
        DROP COLUMN target',
        target_edges_table
    );

    EXECUTE format('
        ALTER TABLE %I 
        RENAME COLUMN source_vertex_id TO source',
        target_edges_table
    );

    EXECUTE format('
        ALTER TABLE %I 
        RENAME COLUMN target_vertex_id TO target',
        target_edges_table
    );

    -- Insert reverse edges
    RAISE NOTICE 'Inserting reverse edges...';
    EXECUTE format('
        INSERT INTO %I (source, target, length, geom)
        SELECT 
            target AS source, 
            source AS target, 
            length, 
            ST_Reverse(geom) AS geom
        FROM %I e1
        WHERE NOT EXISTS (
            SELECT 1 
            FROM %I e2
            WHERE e2.source = e1.target 
            AND e2.target = e1.source
            AND e2.length = e1.length
        )',
        target_edges_table, target_edges_table, target_edges_table
    );

    -- Add metadata columns
    RAISE NOTICE 'Adding metadata columns...';
    EXECUTE format('
        ALTER TABLE %I
        ADD COLUMN datum_aend date,
        ADD COLUMN datum_erst date,
        ADD COLUMN erstellung bigint,
        ADD COLUMN grund_aend character varying(50),
        ADD COLUMN herkunft character varying(50),
        ADD COLUMN objektart character varying(50),
        ADD COLUMN revision_j bigint,
        ADD COLUMN revision_m bigint,
        ADD COLUMN kunstbaute character varying(30),
        ADD COLUMN belagsart character varying(10),
        ADD COLUMN eigentueme character varying(50),
        ADD COLUMN strassenna character varying(254),
        ADD COLUMN wanderwege character varying(50),
        ADD COLUMN befahrbark character varying(10),
        ADD COLUMN stufe character varying(5),
        ADD COLUMN richtungsg character varying(10),
        ADD COLUMN verkehrsbe character varying(50),
        ADD COLUMN kreisel character varying(10)',
        target_edges_table
    );

    -- Update edges with metadata
    RAISE NOTICE 'Updating edges with metadata...';
    EXECUTE format('
        UPDATE %I e
        SET 
            datum_aend = s.datum_aenderung,
            datum_erst = s.datum_erstellung,
            erstellung = s.erstellung_jahr,
            grund_aend = s.grund_aenderung,
            herkunft = s.herkunft,
            objektart = s.objektart,
            revision_j = s.revision_jahr,
            revision_m = s.revision_monat,
            kunstbaute = s.kunstbaute,
            belagsart = s.belagsart,
            eigentueme = s.eigentuemer,
            strassenna = s.strassenname,
            wanderwege = s.wanderwege,
            befahrbark = s.befahrbarkeit,
            stufe = s.stufe,
            richtungsg = s.richtungsgetrennt,
            verkehrsbe = s.verkehrsbedeutung,
            kreisel = s.kreisel
        FROM 
            %I s
        WHERE 
            ST_Intersects(e.geom::geometry, s.geom::geometry)',
        target_edges_table, source_table
    );

    -- Create metadata index
    RAISE NOTICE 'Creating metadata index...';
    EXECUTE format('CREATE INDEX %I_idx_strassenna ON %I(strassenna)', target_edges_table, target_edges_table);

    -- Replace id column
    RAISE NOTICE 'Replacing id column...';
    EXECUTE format('ALTER TABLE %I DROP COLUMN id', target_edges_table);
    EXECUTE format('ALTER TABLE %I ADD COLUMN id SERIAL PRIMARY KEY', target_edges_table);

    -- Create additional indexes
    RAISE NOTICE 'Creating additional indexes...';
    EXECUTE format('CREATE INDEX %I_geom_idx ON %I USING GIST (geom)', target_edges_table, target_edges_table);
    EXECUTE format('CREATE INDEX %I_source_idx ON %I(source)', target_edges_table, target_edges_table);
    EXECUTE format('CREATE INDEX %I_target_idx ON %I(target)', target_edges_table, target_edges_table);
    EXECUTE format('CREATE INDEX idx_%I_path ON %I(source, target, length)', target_edges_table, target_edges_table);

    RAISE NOTICE 'Strassen transformation procedure completed successfully.';
    COMMIT;
END;
$$;


CALL transform_strassen();