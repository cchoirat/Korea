##----- Install Postgres and PostGIS with homebrew
# brew install postgres
# brew install postgis

##----- Start Postgres from R
system("pg_ctl -D /usr/local/var/postgres start &")
##----- Stop Postgres
# system("pg_ctl -D /usr/local/var/postgres stop &")

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

max_height <- 500
min_height <- 0

##------------------------------------
## BEGIN: YOU ONLY NEED TO DO IT ONCE
##------------------------------------

##----- Create spatial database.

create_trajectory_db() # create 'pm' database
# system("dropdb trajectory") # drops 'trajectory' database
add_asia_projection_to_db()
import_world_shapefiles()

##----- Pre-process HYSPLIT output to add trajectory ID's

hysplit_input <- paste0(dir_trajectories, list.files(dir_trajectories))

for (f in hysplit_input) {
  print(f)
  hysplit_output <- file.path(dir_trajectories_with_ID, basename(f))
  d <- preprocess_trajectory_korea(f, min_height, max_height)
  fwrite(d, hysplit_output)
}

##------------------------------------
## END: YOU ONLY NEED TO DO IT ONCE
##------------------------------------

##----------------------------------------------------------
## BEGIN: YOU COULD USE A LOOP (NO RENAMING NEEDED).
##        YOU CAN ALSO OPEN DIFFERENT R SESSIONS HERE
##        AS LONG AS YOU USE A DIFFERENT VARIABLE NAME
##        FOR EXAMPLE traj2, traj2_link, traj2_lines.
##----------------------------------------------------------

##----- Link one processed HYSPLIT output

hysplit_processed <- paste0(dir_trajectories_with_ID, list.files(dir_trajectories_with_ID))

f <- hysplit_processed[1]
traj1 <- fread(f)
copy_to_db_points(traj1, "traj1")
## linkage takes 1-2 minutes:
link <- percentage_trajectories(table_pm = "traj1", table_link = "traj1_link", table_lines = "traj1_lines")
M <- merge(unique(traj1, by = "tid"), link)
fwrite(M, file.path(dir_linkage_results, basename(f)))
remove_table("traj1")

##----------------------------------------------------------
## END: YOU COULD USE A LOOP OR DIFFERENT R SESSIONS HERE
##----------------------------------------------------------
