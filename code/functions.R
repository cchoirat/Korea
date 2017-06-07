create_trajectory_db <- function() {
  system(paste("createdb", dbname))
  db <- src_postgres(dbname = dbname, host = host, port = port, user = user)
  dbSendQuery(db$con, "CREATE EXTENSION POSTGIS")
  dbDisconnect(db$con)
}

##----- Add identifier to trajectory

preprocess_trajectory <- function(file_trajectory, drop_hours_pre = 24, dismiss_above = dismiss_above) {
  trajectory <- fread(file_trajectory)
  nb_unique_trajectories <- nrow(unique(trajectory, by = "date"))
  nb_days <- 7 # by construction of the HYSPLIT trajectories
  nb_obs_per_trajectory <- 24 * nb_days + 1 # hourly observations
  traj_ID <- data.table(expand.grid(1:nb_obs_per_trajectory, 1:nb_unique_trajectories))
  traj_ID[, Var2 := paste0("ID-", Var2)]
  trajectory[, ID := traj_ID$Var2]
  trajectory[, Order := traj_ID$Var1]
  d <- trajectory[, .(receptor, date, lat, lon, height, Order, ID)]
  names(d) <- c("receptor", "date", "lat", "lng", "height", "order", "tid")
  d <- d[order > drop_hours_pre]
  if (dismiss_above) {
    d$above <- 0
    d[, above := as.numeric(height > max_height)]
    d <- d[, .SD[order < match(1, above)], by = tid]
    d[, above := NULL]
  }
  return(d)
}

##----- Transform list of targets to data frame

process_linkage_output <- function(d) {
  l <- stringr::str_split(d$target, " _ ")
  # lu <- lapply(l, unique)
  pu <- plyr::ldply(l, rbind)
  
  D <- cbind(d, pu)
  D$target <- NULL
  
  D %>% gather(Idx, target, -receptor, -date, -tid) %>% na.omit() %>% arrange(tid) -> DD
  DD$Idx <- NULL
  return(data.table(DD))
}

##----- Copy data to database as 2D points and change the CRS

copy_to_db_points <- function(table, table_name = NULL) {
  db <- src_postgres(dbname = dbname, host = host, port = port, user = user)
  if (is.null(table_name))
    table_name <- deparse(substitute(table))
  copy_to(dest = db, df = table, name = table_name, temporary = FALSE)
  dbGetQuery(db$con, paste("ALTER TABLE", table_name, "ADD COLUMN gid serial PRIMARY KEY;"))
  dbGetQuery(db$con, paste("ALTER TABLE", table_name, "ADD COLUMN geom geometry(POINT, 2163);"))
  dbGetQuery(db$con, paste("UPDATE", table_name, "SET geom = ST_Transform(ST_SetSRID(ST_MakePoint(lng, lat), 4326), 2163);"))
  dbDisconnect(db$con)
}

##----- Add a spatial index

add_spatial_index <- function(table_name) {
  db <- src_postgres(dbname = dbname, host = host, port = port, user = user)
  dbGetQuery(db$con, paste("CREATE INDEX sidx ON", table_name, "USING GIST (geom);"))
  dbDisconnect(db$con)
}

##----- Transform the 

create_line_geom <- function() {
  db <- src_postgres(dbname = dbname, host = host, port = port, user = user)
  q <- "CREATE TABLE l_trajectory(
            id serial,
            receptor character varying,
            date character varying,
            tid character varying,
            geom geometry(LINESTRING, 2163));
        INSERT INTO l_trajectory(
            receptor,
            date,
            tid,
            geom)
        SELECT
            receptor,
            date,
            tid,
            ST_MakeLine(geom) AS newgeom FROM p_trajectory
        GROUP BY
            receptor,
            date,
            tid;"
  dbGetQuery(db$con, q)
  dbDisconnect(db$con)
}

intersection_point_linestring <- function() {
  db <- src_postgres(dbname = dbname, host = host, port = port, user = user)
  q <- paste("SELECT
            p2.receptor,
            p2.date,
            p2.tid,
            string_agg(target, ' _ ') AS target FROM receptor AS p1
        JOIN
            l_trajectory as p2
        ON
            ST_DWithin(p1.geom, p2.geom,", buffer_size, ")
        GROUP BY
            p2.receptor,
            p2.date,
            p2.tid;")
  out <- dbGetQuery(db$con, q)
  dbDisconnect(db$con)
  return(data.table(out))
}

remove_table <- function(table_name) {
  db <- src_postgres(dbname = dbname, host = host, port = port, user = user)
  dbSendQuery(db$con, paste("DROP TABLE",  table_name))
  dbDisconnect(db$con)
}

