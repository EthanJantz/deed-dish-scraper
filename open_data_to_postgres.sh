psql -h localhost -U postgres -d deeds_db -c "CREATE EXTENSION IF NOT EXISTS postgis;"

ogr2ogr -f "PostgreSQL" PG:"dbname=deeds_db host=localhost user=deeds_admin" \
  -nln address_points \
  -lco GEOMETRY_NAME=geom \
  -lco SPATIAL_INDEX=GIST \
  -a_srs EPSG:4326 \
  "data/Address_Points.geojson"

ogr2ogr -f "PostgreSQL" PG:"dbname=deeds_db host=localhost user=deeds_admin" \
  -nln vacant_buildings \
  -lco GEOMETRY_NAME=geom \
  -lco SPATIAL_INDEX=GIST \
  -a_srs EPSG:4326 \
  "data/Vacant and Abandoned Buildings - Violations_20250721.geojson"
