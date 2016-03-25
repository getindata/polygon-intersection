# polygon-intersection
Spark App to calculate polygon intersection

To convert shapefile to CSV, use [ogr2ogr](http://www.gdal.org/ogr2ogr.html) tool:

```
ogr2ogr -f CSV -lco GEOMETRY=AS_WKT -t_srs wgs84 -dim 2 data.csv input.shp
```
