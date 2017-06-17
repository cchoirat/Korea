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
library(dbplyr)

source("functions.R")

##----- 'Global' variables

dbname <- "pm"
host <- "localhost"
port <- "5432"
user <- Sys.getenv("LOGNAME")
dir_trajectories <- "~/Dropbox/Korea/Trajectories/" # HYSPLIT trajectory directory
dir_trajectories_with_ID <- "~/Dropbox/Korea/Trajectories_with_ID/" # Add trajectory ID to data; must exist; can overwrite dir_trajectories
dir_linkage_results <- "~/Dropbox/Korea/Linkage" # must exist
dir_world_shapefiles <- "/Users/cchoirat/Documents/LocalGit/Korea/data/countries/countries.shp" # WARNING: absolute path required

##----- Create spatial database.  YOU ONLY NEED TO DO IT ONCE.

create_trajectory_db() # create 'pm' database
# system("dropdb trajectory") # drops 'trajectory' database
add_asia_projection_to_db()
import_world_shapefiles()

##----- Pre-process HYSPLIT output to add trajectory ID's

hysplit_input <- paste0(dir_trajectories, list.files(dir_trajectories))

for (f in hysplit_input) {
  print(f)
  hysplit_output <- file.path(dir_trajectories_with_ID, basename(f))
  d <- preprocess_trajectory(f)
  fwrite(d, hysplit_output)
}

##----- Link one processed HYSPLIT output

f <- hysplit_processed <- paste0(dir_trajectories_with_ID, list.files(dir_trajectories_with_ID))[2]

traj1 <- fread(f)
copy_to_db_points(pmkorea, "traj1")
link <- percentage_trajectories(table_pm = "traj1", table_link = "traj1_link", table_lines = "traj1_lines")
link[, date := traj1$date[1]]
link[, receptor := traj1$receptor[1]]
fwrite(link, file.path(dir_linkage_results, basename(f)))
remove_table("traj1")
