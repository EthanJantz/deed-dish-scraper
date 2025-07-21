ogr2ogr -f "PostgreSQL" PG:"dbname=recordings host=localhost user=ethan" \
  -nln address_points \
  -lco GEOMETRY_NAME=geom \
  -lco SPATIAL_INDEX=GIST \
  -a_srs EPSG:4326 \
  "data/Address_Points.geojson"

ogr2ogr -f "PostgreSQL" PG:"dbname=recordings host=localhost user=ethan" \
  -nln vacant_buildings \
  -lco GEOMETRY_NAME=geom \
  -lco SPATIAL_INDEX=GIST \
  -a_srs EPSG:4326 \
  "data/Vacant and Abandoned Buildings - Violations_20250721.geojson"
