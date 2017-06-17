-- Create PostGIS database 'pm'
-- Fill in 'path','username', 'passwd' with the appropriate value
-- where username is the db username, and passwd is the db password
-- SRID: EPSG: 4326 -- WGS84 - World Geodetic System 1984 (https://epsg.io/4326) 
-- SRID: EPSG: 102012 -- Asia Lambert Conformal Conic (https://epsg.io/102012) 
-- Check the coordinate system of the shapefile to import and change
-- the shp2pgsql accordingly

-- open a terminal, and create a new db and add postgis extension
createdb -h localhost -p 5432 -U cchoirat pm
psql -h localhost -p 5432 -U cchoirat -d pm -c "CREATE EXTENSION postgis;"

-- by default postgis does not have projection coordinate system for Asia, so you need to add it 
-- using INSERT into the spatial_ref_sys table

INSERT into spatial_ref_sys (srid, auth_na	me, auth_srid, proj4text, srtext) values (102012, 'esri', 102012, '+proj=lcc +lat_1=30 +lat_2=62 +lat_0=0 +lon_0=105 +x_0=0 +y_0=0 +ellps=WGS84 +datum=WGS84 +units=m +no_defs ', 'PROJCS["Asia_Lambert_Conformal_Conic",GEOGCS["GCS_WGS_1984",DATUM["WGS_1984",SPHEROID["WGS_1984",6378137,298.257223563]],PRIMEM["Greenwich",0],UNIT["Degree",0.017453292519943295]],PROJECTION["Lambert_Conformal_Conic_2SP"],PARAMETER["False_Easting",0],PARAMETER["False_Northing",0],PARAMETER["Central_Meridian",105],PARAMETER["Standard_Parallel_1",30],PARAMETER["Standard_Parallel_2",62],PARAMETER["Latitude_Of_Origin",0],UNIT["Meter",1],AUTHORITY["EPSG","102012"]]');

-- Fixing quotes

INSERT into spatial_ref_sys (srid, auth_name, auth_srid, proj4text, srtext) values (102012, 'esri', 102012, '+proj=lcc +lat_1=30 +lat_2=62 +lat_0=0 +lon_0=105 +x_0=0 +y_0=0 +ellps=WGS84 +datum=WGS84 +units=m +no_defs ', 'PROJCS[''Asia_Lambert_Conformal_Conic'',GEOGCS[''GCS_WGS_1984'',DATUM[''WGS_1984'',SPHEROID[''WGS_1984'',6378137,298.257223563]],PRIMEM[''Greenwich'',0],UNIT[''Degree'',0.017453292519943295]],PROJECTION[''Lambert_Conformal_Conic_2SP''],PARAMETER[''False_Easting'',0],PARAMETER[''False_Northing'',0],PARAMETER[''Central_Meridian'',105],PARAMETER[''Standard_Parallel_1'',30],PARAMETER[''Standard_Parallel_2'',62],PARAMETER[''Latitude_Of_Origin'',0],UNIT[''Meter'',1],AUTHORITY[''EPSG'',''102012'']]');

-- pm data import and settings
-- create a table pmkorea

create table pmkorea (receptor character varying, date character varying, lat double precision, lng double precision, height double precision, torder character varying, tid character varying);

-- import data

COPY pmkorea from '/Users/cchoirat/Dropbox/Korea/from_monitor_with_id.csv' DELIMITERS ',' CSV header;

-- add the geometry column

alter table pmkorea add column geom geometry (Point, 4326);

-- update the geometry column

update pmkorea set geom = ST_SetSRID(ST_MakePoint(lng,lat),4326);

-- create spatial index

CREATE INDEX pmkorea_gix ON pmkorea2 USING GIST (geom);

CREATE INDEX pmkorea_gix ON pmkorea2 USING GIST (geom);

-- re-project the table to EPSG: 102012 (Asia Lambert Conformal Conic)

ALTER TABLE pmkorea ALTER COLUMN geom TYPE geometry(Point,102012) USING ST_Transform(geom, 102012);

--

CREATE INDEX pmkorea_gix ON  pmkorea USING GIST (geom);

SELECT * FROM pmkorea2 LIMIT 20;

/* import world coutries shapefile using shp2pgsql, note that the countries.shp was previsouly re-project 
in Asia Lambert Conformal Conic projection using desktop gis*/

shp2pgsql -c -D -I -s 102012 /Users/cchoirat/Documents/LocalGit/Korea/data/countries/countries.shp countries | psql -d pm -h localhost -U cchoirat

SELECT pmkorea.tid, ST_MakeLine(pmkorea.geom) as geom into pmlines from pmkorea GROUP BY pmkorea.tid;

CREATE INDEX countries_gix ON countries USING GIST (geom);

-- create a pmline by t-id

SELECT pmkorea.tid, ST_MakeLine(pmkorea.geom) as geom into pmlines from pmkorea GROUP BY pmkorea.tid;

SELECT * FROM countries limit 2;

SELECT * FROM pmline limit 2;

SELECT * FROM pmlink limit 2;

-- create a spatial index

CREATE INDEX pmline_gix ON pmlines USING GIST (geom);

-- calculate pmlines that intersect countries boundaries and percentage

CREATE TABLE percentage AS SELECT a.name, a.iso2, b.tid, ST_LENGTH(ST_Intersection(a.geom, b.geom)) as pmlength, 
sum(ST_LENGTH(ST_Intersection(a.geom, b.geom))) over(partition by b.tid) as tidlength,
(ST_LENGTH(ST_Intersection(a.geom, b.geom))/sum(ST_LENGTH(ST_Intersection(a.geom, b.geom))) over(partition by b.tid)) * 100 as perc
FROM countries a, pmlines b
WHERE ST_Intersects(a.geom, b.geom) group by b.tid, a.name, a.iso2, a.geom, b.geom;

CREATE TABLE pmlink AS SELECT a.name, a.iso2, b.tid, ST_LENGTH(ST_Intersection(a.geom, b.geom)) as pmlength, sum(ST_LENGTH(ST_Intersection(a.geom, b.geom))) over(partition by b.tid) as tidlength, (ST_LENGTH(ST_Intersection(a.geom, b.geom))/sum(ST_LENGTH(ST_Intersection(a.geom, b.geom))) over(partition by b.tid)) * 100 as perc FROM countries a  pmlines  b WHERE ST_Intersects(a.geom, b.geom) group by b.tid, a.name, a.iso2, a.geom, b.geom;

SELECT * FROM percentage LIMIT 5;

DROP TABLE percentage;


CREATE TABLE percentage2 AS select tbpm.name, tbpm.iso2, tbpm.tid, (tbpm.pmlength/sum(tbpm.pmlength) over(partition by tbpm.tid))*100 as perc from (SELECT a.name, a.iso2, b.tid, ST_LENGTH(ST_Intersection(a.geom, b.geom)) as pmlength FROM countries a, pmlines b WHERE ST_Intersects(a.geom, b.geom) group by b.tid, a.name, a.iso2, a.geom, b.geom) as tbpm;

SELECT * FROM percentage2 LIMIT 50;

/* calculate points that intersect countries boundaries and percentage without buffer */
/*select countries.iso2 as iso2, countries.name as cname, pmkorea.tid as pmdate, count(*) AS totalpm, sum(count(*)) over(partition by pmkorea.tid) as sumtotalpm, (count(*)/sum(count(*)) over(partition by pmkorea.tid))*100 as percpm FROM countries, pmkorea WHERE 
st_contains(countries.geom,pmkorea.geom) GROUP BY countries.name, countries.iso2, pmdate*/

/* calculate points that intersect countries boundaries and percentage with buffer */
/*select countries.iso2 as iso2, countries.name as cname, pmkorea.tid as pmdate, count(*) AS totalpm, sum(count(*)) over(partition by pmkorea.tid) as sumtotalpm, (count(*)/sum(count(*)) over(partition by pmkorea.tid))*100 as percpm FROM countries, pmkorea WHERE 
st_dwithin(countries.geom,pmkorea.geom, 1000) GROUP BY countries.name, countries.iso2, pmdate*/
