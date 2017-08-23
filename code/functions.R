##----- Create spatial database

create_trajectory_db <- function() {
  system(paste("createdb", dbname))
  db <- src_postgres(dbname = dbname, host = host, port = port, user = user)
  dbSendQuery(db$con, "CREATE EXTENSION POSTGIS")
  dbDisconnect(db$con)
}

##----- Add projection coordinate system for Asia

add_asia_projection_to_db <- function() {
  db <- src_postgres(dbname = dbname, host = host, port = port, user = user)
  cmd <- "INSERT into spatial_ref_sys (srid, auth_name, auth_srid, proj4text, srtext) values (102012, 'esri', 102012, '+proj=lcc +lat_1=30 +lat_2=62 +lat_0=0 +lon_0=105 +x_0=0 +y_0=0 +ellps=WGS84 +datum=WGS84 +units=m +no_defs ', 'PROJCS[''Asia_Lambert_Conformal_Conic'',GEOGCS[''GCS_WGS_1984'',DATUM[''WGS_1984'',SPHEROID[''WGS_1984'',6378137,298.257223563]],PRIMEM[''Greenwich'',0],UNIT[''Degree'',0.017453292519943295]],PROJECTION[''Lambert_Conformal_Conic_2SP''],PARAMETER[''False_Easting'',0],PARAMETER[''False_Northing'',0],PARAMETER[''Central_Meridian'',105],PARAMETER[''Standard_Parallel_1'',30],PARAMETER[''Standard_Parallel_2'',62],PARAMETER[''Latitude_Of_Origin'',0],UNIT[''Meter'',1],AUTHORITY[''EPSG'',''102012'']]');"
  dbGetQuery(db$con, cmd)
  dbDisconnect(db$con)
}

##----- Import world coutries shapefile using shp2pgsql, note that the countries.shp was previsouly re-project 
##----- in Asia Lambert Conformal Conic projection

import_world_shapefiles <- function() {
  cmd <- paste("shp2pgsql -c -D -I -s 102012",
               dir_world_shapefiles,
                "countries | psql -d", dbname, "-h localhost -U", user);
  system(cmd)
}

##----- Copy data to database as 2D points and change the CRS

copy_to_db_points <- function(table, table_name = NULL) {
  db <- src_postgres(dbname = dbname, host = host, port = port, user = user)
  if (is.null(table_name))
    table_name <- deparse(substitute(table))
  copy_to(dest = db, df = table, name = table_name, temporary = FALSE)
  #-- add the geometry column
  dbGetQuery(db$con, paste("ALTER TABLE", table_name, "ADD COLUMN geom geometry(POINT, 4326);"))
  #-- update the geometry column
  dbGetQuery(db$con, paste("UPDATE", table_name, "set geom = ST_SetSRID(ST_MakePoint(lng, lat), 4326);"))
  #-- create spatial index
  dbGetQuery(db$con, paste("CREATE INDEX pmkorea_gix ON", table_name, "USING GIST (geom);"))
  #-- re-project the table to EPSG: 102012 (Asia Lambert Conformal Conic)
  dbGetQuery(db$con, paste("ALTER TABLE", table_name, "ALTER COLUMN geom TYPE geometry(Point, 102012) USING ST_Transform(geom, 102012);"))
  dbDisconnect(db$con)
}

##----- Add identifier to trajectory

preprocess_trajectory_korea <- function(file_trajectory, min_height = 0, max_height = 500) {
  trajectory <- fread(file_trajectory)
  nb_unique_trajectories <- nrow(unique(trajectory, by = "date"))
  nb_days <- 5 # by construction of the HYSPLIT trajectories
  nb_obs_per_trajectory <- 24 * nb_days + 1 # hourly observations
  traj_ID <- data.table(expand.grid(1:nb_obs_per_trajectory, 1:nb_unique_trajectories))
  traj_ID[, Var2 := paste0("ID-", Var2)]
  trajectory[, ID := traj_ID$Var2]
  trajectory[, Order := traj_ID$Var1]
  d <- trajectory[, .(receptor, date, lat, lon, height, Order, ID)]
  names(d) <- c("receptor", "date", "lat", "lng", "height", "order", "tid")
  d <- as_tibble(d)
  d %>% 
    group_by(tid) %>%
    mutate(first_above = min(which(height > max_height | row_number() == n()))) %>%
    mutate(first_below = min(which(height < min_height | row_number() == n()))) %>% 
    filter(row_number() < min(first_above, first_below)) %>% 
    select(-order, first_above, first_below) -> D
  return(data.table(d))
}

##----- Spatial linkage

percentage_trajectories <- function(table_pm = "pmkorea", table_link = "pmlink", table_lines = "pmlines") {
  db <- src_postgres(dbname = dbname, host = host, port = port, user = user)
  #-- create a pmline by t-id
  cmd <- paste0("SELECT ", table_pm, ".tid, ST_MakeLine(", table_pm, ".geom) as geom into ", table_lines, " from ", table_pm, " GROUP BY ", table_pm, ".tid;")
  dbGetQuery(db$con, cmd)
  #-- create a spatial index
  cmd <- paste("CREATE INDEX pmline_gix ON", table_lines, "USING GIST (geom);")
  dbGetQuery(db$con, cmd)
  #-- calculate pmlines that intersect countries boundaries and percentage
  cmd <- paste("CREATE TABLE", table_link, "AS SELECT a.name, a.iso2, b.tid, ST_LENGTH(ST_Intersection(a.geom, b.geom)) as pmlength, sum(ST_LENGTH(ST_Intersection(a.geom, b.geom))) over(partition by b.tid) as tidlength, (ST_LENGTH(ST_Intersection(a.geom, b.geom))/sum(ST_LENGTH(ST_Intersection(a.geom, b.geom))) over(partition by b.tid)) * 100 as perc FROM countries a, ", table_lines, " b WHERE ST_Intersects(a.geom, b.geom) group by b.tid, a.name, a.iso2, a.geom, b.geom;")
  cmd
  dbGetQuery(db$con, cmd)
  link <- collect(tbl(db, table_link))
  remove_table(table_lines)
  remove_table(table_link)
  dbDisconnect(db$con)
  return(data.table(link))
}

##----- Remove table

remove_table <- function(table_name) {
  db <- src_postgres(dbname = dbname, host = host, port = port, user = user)
  dbSendQuery(db$con, paste("DROP TABLE",  table_name))
  dbDisconnect(db$con)
}

