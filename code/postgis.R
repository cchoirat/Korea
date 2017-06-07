##----- Install Postgres and PostGIS with homebrew
# brew install postgres
# brew install postgis

##----- Start Postgres
# pg_ctl -D /usr/local/var/postgres start &
##----- Stop Postgres
# pg_ctl -D /usr/local/var/postgres stop &

library(tidyr)
library(data.table)
library(DBI)
library(RPostgreSQL)
library(plyr)
library(dplyr)

source("functions.R")

##----- 'Global' variables

dbname <- "trajectory"
host <- "localhost"
port <- "5432"
user <- Sys.getenv("LOGNAME")
drop_hours_pre <- 24 # number of hours dropped at the begenning of each trajectory
file_receptor <- "../data/receptor.csv" # receptor file (e.g., monitors, zip code centroids); 3 columns: "lat", "lng", "target" (= ID)
dir_trajectories <- "~/Dropbox/Facility_Attributes_2003_2006/parameters_2003/results_2003/" # HYSPLIT trajectory directory
dir_trajectories_with_ID <- "~/Dropbox/Facility_Attributes_2003_2006/trajID_2003/" # Add trajectory ID to data; must exist; can overwrite dir_trajectories
dir_linkage_results_as_list <- "~/Dropbox/Facility_Attributes_2003_2006/Linkage_list_2003" # must exist
dir_linkage_results_as_data_frame <- "~/Dropbox/Facility_Attributes_2003_2006/Linkage_2003" # must exist
buffer_size <- 10000 # buffer size in meters
max_height <- 1000
dismiss_above <- FALSE # FALSE: no height constraint, TRUE: keep as long as height < max_height

##----- Create spatial database

create_trajectory_db() # create 'trajectory' database
# system("dropdb trajectory") # drops 'trajectory' database

##----- Load receptors (e.g., monitors, zip code centroids)

receptor <- fread(file_receptor)
copy_to_db_points(receptor)
add_spatial_index("receptor")

##----- Looping over HYSPLIT trajectories

files_trajectories <- list.files(dir_trajectories)
# file_trajectory <- files_trajectories[1]

for (file_trajectory in files_trajectories[1:3]) { # for testing purposes
# for (file_trajectory in files_trajectories) {
  print(file_trajectory)
  
  p_trajectory <- preprocess_trajectory(file.path(dir_trajectories, file_trajectory),
                                        drop_hours_pre = drop_hours_pre,
                                        dismiss_above = FALSE)
  write.csv(p_trajectory, file.path(dir_trajectories_with_ID, file_trajectory))
  copy_to_db_points(p_trajectory)
  
  ##-- Intersect the dummy plume trajectory with the receptor buffer
  create_line_geom() # create table l_trajectory with timestamp information
  out <- intersection_point_linestring()
  
  ##-- Write linkage to files
  write.csv(out, file.path(dir_linkage_results_as_list, file_trajectory))
  df_out <- process_linkage_output(data.frame(out))
  write.csv(df_out, file.path(dir_linkage_results_as_data_frame, file_trajectory))
  
  ##-- Remove tables from database before next iteration
  remove_table("p_trajectory")
  remove_table("l_trajectory")
}

