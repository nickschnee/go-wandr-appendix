-- transform haltestellen-oev_2056_de.gpkg
--- downloaded from https://www.bav.admin.ch/bav/de/home/allgemeine-themen/fachthemen/geoinformation/geobasisdaten/haltestellen-des-oeffentlichen-verkehrs.html

CREATE TABLE stops AS
SELECT *
FROM haltestellen_1
WHERE Betriebspunkttyp_Bezeichnung = 'Haltestelle'
AND geom IS NOT NULL
AND name IS NOT NULL;

--- make xtf id unique
ALTER TABLE stops ADD CONSTRAINT unique_xtf_id UNIQUE (xtf_id);

CREATE INDEX stops_geom_idx ON stops USING GIST(geom);