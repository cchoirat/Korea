library(data.table)

##----- Add identifier to trajectory

preprocess_trajectory <- function(file_trajectory = "from_monitor.csv", drop_hours_pre = 0, dismiss_above = FALSE) {
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
  d <- d[order > drop_hours_pre]
  if (dismiss_above) {
    d$above <- 0
    d[, above := as.numeric(height > max_height)]
    d <- d[, .SD[order < match(1, above)], by = tid]
    d[, above := NULL]
  }
  return(d)
}

##----- Example

d <- preprocess_trajectory()
fwrite(d, "from_monitor_with_id.csv")
