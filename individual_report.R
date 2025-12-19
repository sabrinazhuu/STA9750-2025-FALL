# ==== SQ: How does station infrastructure relate to e-bike vs classic bike usage? ====

# ==== Load packages ====
library(data.table)
library(sf)
library(ggplot2)
library(dplyr)
library(knitr)

# ==== Bike Trip Data ====
# Setup 
raw_dir <- "data/raw"
out_dir <- "data/processed"
gis_dir <- "data/gis"

dir.create(raw_dir, showWarnings = FALSE, recursive = TRUE)
dir.create(out_dir, showWarnings = FALSE, recursive = TRUE)
dir.create(gis_dir, showWarnings = FALSE, recursive = TRUE)

options(timeout = max(1200, getOption("timeout")))
options(download.file.method = "libcurl")

# Check if already processed
final_file <- file.path(out_dir, "citibike_manhattan_all_202410_202510.rds")

if (file.exists(final_file)) {
  message("\n Found existing data! Loading from cache...")
  manhattan_trips <- readRDS(final_file)
  message("Loaded ", format(nrow(manhattan_trips), big.mark = ","), " trips")
  message("Date range: ", min(manhattan_trips$date), " to ", max(manhattan_trips$date))
  
} else {
  # No cache found - Process everything
  message("\n No cache found...\n")
  
  months <- format(
    seq(as.Date("2024-10-01"), as.Date("2025-10-01"), by = "month"),
    "%Y%m"
  )
  
  base_url <- "https://s3.amazonaws.com/tripdata"
  
  ## 1) Get Manhattan polygon (cached separately)
  manhattan_poly_file <- file.path(gis_dir, "manhattan_polygon.rds")
  
  if (file.exists(manhattan_poly_file)) {
    message("Loading Manhattan polygon from cache")
    manhattan_poly <- readRDS(manhattan_poly_file)
  } else {
    message("Downloading NYC borough boundaries...")
    borough_url <- "https://www.nyc.gov/assets/planning/download/zip/data-maps/open-data/nybb_16a.zip"
    borough_zip <- file.path(gis_dir, "nyc_boroughs.zip")
    
    if (!file.exists(borough_zip)) {
      download.file(borough_url, borough_zip, mode = "wb")
    }
    
    unzip(borough_zip, exdir = gis_dir)
    shp_file <- list.files(gis_dir, pattern = "\\.shp$", recursive = TRUE, full.names = TRUE)[1]
    
    boroughs <- st_read(shp_file, quiet = TRUE)
    boroughs <- st_transform(boroughs, 4326)
    manhattan_poly <- boroughs[boroughs$BoroName == "Manhattan", ]
    
    saveRDS(manhattan_poly, manhattan_poly_file)
    message("Manhattan polygon cached")
  }
  
  ## 2) Download function (checks if zip exists)
  download_zip <- function(m) {
    zip_name <- sprintf("%s-citibike-tripdata.zip", m)
    zip_path <- file.path(raw_dir, zip_name)
    zip_url  <- paste0(base_url, "/", zip_name)
    
    if (file.exists(zip_path)) {
      message("Already have: ", zip_name)
      return(zip_path)
    }
    
    message("Downloading: ", zip_name, "...")
    download.file(zip_url, zip_path, mode = "wb", quiet = TRUE)
    message("Downloaded: ", zip_name)
    return(zip_path)
  }
  
  ## 3) Process month function (with per-month caching)
  process_month_manhattan <- function(m) {
    
    # Check if this month already processed
    month_cache <- file.path(out_dir, sprintf("month_%s.rds", m))
    if (file.exists(month_cache)) {
      message("Month ", m, " already processed, loading from cache")
      return(readRDS(month_cache))
    }
    
    message("\n=== Processing month: ", m, " ===")
    
    zip_path <- download_zip(m)
    
    tmpdir <- file.path(tempdir(), m)
    dir.create(tmpdir, showWarnings = FALSE)
    unzip(zip_path, exdir = tmpdir)
    
    csv_files <- list.files(tmpdir, pattern = "\\.csv$", full.names = TRUE)
    
    if (length(csv_files) == 0) {
      unlink(tmpdir, recursive = TRUE)
      return(NULL)
    }
    
    month_list <- vector("list", length(csv_files))
    
    for (i in seq_along(csv_files)) {
      csv <- csv_files[i]
      message(" Reading: ", basename(csv))
      
      dt <- fread(
        csv,
        select = c(
          "ride_id", "rideable_type", "started_at", "ended_at",
          "start_station_name", "start_station_id",
          "end_station_name", "end_station_id",
          "start_lat", "start_lng", "end_lat", "end_lng",
          "member_casual"
        )
      )
      
      dt[, started_at := as.POSIXct(started_at, tz = "America/New_York")]
      dt[, ended_at   := as.POSIXct(ended_at,   tz = "America/New_York")]
      dt[, date := as.Date(started_at)]
      dt[, hour := as.integer(format(started_at, "%H"))]
      
      # Filter to Manhattan
      dt <- dt[!is.na(start_lat) & !is.na(start_lng)]
      
      if (nrow(dt) == 0) {
        month_list[[i]] <- NULL
        next
      }
      
      dt_sf <- st_as_sf(dt, coords = c("start_lng", "start_lat"), 
                        crs = 4326, remove = FALSE)
      inside_manhattan <- st_within(dt_sf, manhattan_poly, sparse = FALSE)[, 1]
      dt_manhattan <- as.data.table(st_drop_geometry(dt_sf[inside_manhattan, ]))
      
      if (nrow(dt_manhattan) == 0) {
        month_list[[i]] <- NULL
        next
      }
      
      month_list[[i]] <- dt_manhattan
      rm(dt, dt_sf, dt_manhattan)
      gc()
    }
    
    month_all <- rbindlist(month_list, use.names = TRUE, fill = TRUE)
    unlink(tmpdir, recursive = TRUE)
    
    # Cache this month
    if (!is.null(month_all) && nrow(month_all) > 0) {
      saveRDS(month_all, month_cache)
      message("Cached month ", m)
    }
    
    return(month_all)
  }
  
  ## 4) Process all months
  all_months_list <- vector("list", length(months))
  
  for (i in seq_along(months)) {
    m <- months[i]
    month_all <- process_month_manhattan(m)
    all_months_list[[i]] <- month_all
    rm(month_all)
    gc()
  }
  
  manhattan_trips <- rbindlist(all_months_list, use.names = TRUE, fill = TRUE)
  
  # Save final combined dataset
  saveRDS(manhattan_trips, final_file)
  message("\ Saved final dataset to: ", final_file)
}

# Data is ready
citibike_manhattan_all <- manhattan_trips
message("\n Object 'citibike_manhattan_all' has ", 
        format(nrow(citibike_manhattan_all), big.mark = ","), " trips\n")

# ==== Bike Routes Data ====
download_bike_routes <- function() {
  # Check for required packages
  if (!require("sf", quietly = TRUE)) {
    stop("Package 'sf' is required. Install it with: install.packages('sf')")
  }
  if (!require("dplyr", quietly = TRUE)) {
    stop("Package 'dplyr' is required. Install it with: install.packages('dplyr')")
  }
  
  # Create directory if needed
  if (!dir.exists(file.path("data", "individual_report"))) {
    dir.create(file.path("data", "individual_report"), showWarnings = FALSE, recursive = TRUE)
  }
  
  # File paths
  cache_file <- file.path("data", "individual_report", "bike_routes.rds")
  
  # Check cache first
  if (file.exists(cache_file)) {
    cat("Loading bike routes from cache...\n")
    bike_routes <- readRDS(cache_file)
    cat("Loaded", nrow(bike_routes), "bike routes\n")
    return(bike_routes)
  }
  
  # Download from API using GeoJSON (better for spatial data)
  cat("Downloading bike route data...\n")
  base_url <- "https://data.cityofnewyork.us/resource/mzxg-pwib.geojson"
  all_routes <- list()
  offset <- 0
  limit <- 50000
  continue_download <- TRUE
  
  while (continue_download) {
    url <- paste0(base_url, "?$limit=", limit, "&$offset=", format(offset, scientific = FALSE))
    temp_file <- tempfile(fileext = ".geojson")
    
    tryCatch({
      download.file(url, temp_file, mode = "wb", quiet = TRUE)
      chunk <- sf::st_read(temp_file, quiet = TRUE)
      
      if (nrow(chunk) == 0) {
        continue_download <- FALSE
      } else {
        all_routes[[length(all_routes) + 1]] <- chunk
        cat("Downloaded", offset + nrow(chunk), "routes...\n")
        
        if (nrow(chunk) < limit) {
          continue_download <- FALSE
        } else {
          offset <- offset + limit
        }
      }
    }, error = function(e) {
      cat("Error encountered:", e$message, "\n")
      continue_download <<- FALSE
    })
    
    if (!continue_download) break
  }
  
  # Check if we got any data
  if (length(all_routes) == 0) {
    stop("Failed to download")
  }
  
  # Combine and process
  cat("Processing bike route data...\n")
  bike_routes <- dplyr::bind_rows(all_routes) |>
    sf::st_make_valid() |>
    sf::st_transform("WGS84") |>
    dplyr::mutate(
      # Convert factor columns if needed
      street = as.character(street),
      from_street = as.character(fromstreet),
      to_street = as.character(tostreet),
      lanecount = as.numeric(lanecount)
    )
  
  # Save to cache
  cat("Saving to cache...\n")
  saveRDS(bike_routes, cache_file)
  
  cat("Bike routes saved to: data/individual_report/bike_routes.rds\n")
  cat("Total routes:", nrow(bike_routes), "\n")
  
  return(bike_routes)
}

bike_routes <- download_bike_routes()

# Filter bike routes for Manhattan
manhattan_bbox <- st_bbox(c(xmin = -74.02, xmax = -73.90, ymin = 40.70, ymax = 40.88), 
                          crs = st_crs(4326)) |> st_as_sfc()

manhattan_routes <- bike_routes |>
  filter(st_intersects(geometry, manhattan_bbox, sparse = FALSE)[,1])


# ==== Create 5% Stratified Sample by Date ====
# Check if full dataset exists
if (!exists("citibike_manhattan_all")) {
  # Load it if you've already saved it
  full_path <- file.path(out_dir, "citibike_manhattan_all_202410_202510.rds")
  if (file.exists(full_path)) {
    message("Loading full dataset...")
    citibike_manhattan_all <- readRDS(full_path)
  } else {
    stop("Full dataset not found. Run the data collection code first.")
  }
}

# Ensure date column exists
if (!"date" %in% names(citibike_manhattan_all)) {
  citibike_manhattan_all[, date := as.Date(started_at)]
}

# Set seed for reproducibility
set.seed(123)

# Stratified sample: 5% from each date
citibike_sample <- citibike_manhattan_all[, .SD[sample(.N, ceiling(.N * 0.05))], by = date]

# Report
cat("Original dataset: ", format(nrow(citibike_manhattan_all), big.mark = ","), " trips")
cat("Sampled dataset:  ", format(nrow(citibike_sample), big.mark = ","), " trips")
cat("Sampling rate:    ", round(nrow(citibike_sample) / nrow(citibike_manhattan_all) * 100, 2), "%")
cat("Date range:       ", min(citibike_sample$date), " to ", max(citibike_sample$date))
cat("Days covered:     ", length(unique(citibike_sample$date)))

# Save sampled version
sample_path <- file.path(out_dir, "citibike_manhattan_sample_5pct.rds")
saveRDS(citibike_sample, sample_path)

# Clean up
rm(citibike_sample)
gc()

# ==== Manhattan polygon ====
borough_url <- "https://www.nyc.gov/assets/planning/download/zip/data-maps/open-data/nybb_16a.zip"
borough_zip <- "data/gis/nyc_boroughs.zip"

dir.create("data/gis", showWarnings = FALSE, recursive = TRUE)

download.file(borough_url, borough_zip, mode = "wb")
unzip(borough_zip, exdir = "data/gis")

shp_file <- list.files("data/gis", pattern = "\\.shp$", recursive = TRUE, full.names = TRUE)[1]

boroughs <- st_read(shp_file, quiet = TRUE)
boroughs <- st_transform(boroughs, 4326)
manhattan_poly <- boroughs[boroughs$BoroName == "Manhattan", ]

saveRDS(manhattan_poly, "data/gis/manhattan_polygon.rds")

# ==== Load Data ====
citibike_sample <- readRDS("data/processed/citibike_manhattan_sample_5pct.rds")
routes <- readRDS("data/individual_report/bike_routes.rds")
manhattan_poly <- readRDS("data/gis/manhattan_polygon.rds")

# ==== 1. Infrastructure Classification ====
# Get unique stations with coordinates
stations <- citibike_sample[!is.na(start_station_id) & 
                                     !is.na(start_lat) & 
                                     !is.na(start_lng), 
                                   .(lat = first(start_lat),
                                     lng = first(start_lng),
                                     station_name = first(start_station_name)),
                                   by = start_station_id]

cat("Found", nrow(stations), "unique stations\n")

# Convert to spatial object
stations_sf <- st_as_sf(stations, coords = c("lng", "lat"), crs = 4326, remove = FALSE)

# Metric 1: Station Density (count nearby stations within 500m)
stations_buffer <- st_buffer(stations_sf, dist = 500)
stations$nearby_count <- sapply(1:nrow(stations_sf), function(i) {
  sum(st_intersects(stations_buffer[i, ], stations_sf, sparse = FALSE)) - 1
})

# Metric 2: Distance to Bike Lanes
manhattan_bbox <- st_bbox(c(xmin = -74.02, xmax = -73.90, ymin = 40.70, ymax = 40.88), 
                          crs = st_crs(4326)) |> st_as_sfc()

manhattan_routes <- routes %>%
  filter(st_intersects(geometry, manhattan_bbox, sparse = FALSE)[,1]) %>%
  st_transform(4326)

stations$dist_to_bike_lane_m <- sapply(1:nrow(stations_sf), function(i) {
  distances <- st_distance(stations_sf[i, ], manhattan_routes)
  min(as.numeric(distances))
})

# Create Infrastructure Score (normalized 0-1)
stations$density_score <- (stations$nearby_count - min(stations$nearby_count)) / 
  (max(stations$nearby_count) - min(stations$nearby_count))

max_dist <- quantile(stations$dist_to_bike_lane_m, 0.95)
stations$lane_proximity_score <- 1 - pmin(stations$dist_to_bike_lane_m, max_dist) / max_dist

stations$infrastructure_score <- 0.5 * stations$density_score + 0.5 * stations$lane_proximity_score

# Classify into tertiles
tertiles <- quantile(stations$infrastructure_score, probs = c(0.33, 0.67))
stations$infrastructure_level <- cut(
  stations$infrastructure_score,
  breaks = c(-Inf, tertiles[1], tertiles[2], Inf),
  labels = c("Low", "Medium", "High"),
  include.lowest = TRUE
)

cat("\nInfrastructure Classification:\n")
print(table(stations$infrastructure_level))

# Save station classification
saveRDS(stations, "data/processed/stations_infrastructure.rds")


# ==== 2. Create Station-Level E-bike Data ====

station_variation <- citibike_sample[, .(
  ebike_pct = mean(rideable_type == "electric_bike") * 100,
  trips = .N,
  lat = mean(start_lat),
  lng = mean(start_lng)
), by = start_station_name]

# Merge with infrastructure
station_full <- merge(station_variation, 
                      stations[, .(station_name, infrastructure_level, infrastructure_score)],
                      by.x = "start_station_name", 
                      by.y = "station_name",
                      all.x = TRUE)

# Calculate bike balance (relative to average)
overall_ebike_share <- mean(citibike_sample$rideable_type == "electric_bike") * 100
station_full[, bike_balance := ebike_pct - overall_ebike_share]


# ==== 3. Create Heat map Data ====

# Define time periods
citibike_sample[, time_period := fcase(
  hour %in% c(6,7,8,9), "Morning Rush",
  hour %in% c(10,11,12,13,14,15), "Midday",
  hour %in% c(16,17,18,19), "Evening Rush",
  hour %in% c(20,21,22,23,0,1,2,3,4,5), "Night"
)]
citibike_sample[, time_period := factor(time_period, 
                                               levels = c("Morning Rush", "Midday", "Evening Rush", "Night"))]

# Define volume groups
citibike_sample[, station_volume := .N, by = start_station_name]
citibike_sample[, volume_group := cut(station_volume, 
                                             breaks = quantile(station_volume, probs = c(0, 0.33, 0.67, 1)),
                                             labels = c("Low Traffic", "Medium Traffic", "High Traffic"),
                                             include.lowest = TRUE)]

# Calculate e-bike % by rider type, volume, and time
ebike_full <- citibike_sample[!is.na(volume_group) & !is.na(time_period), .(
  ebike_pct = mean(rideable_type == "electric_bike") * 100,
  trips = .N
), by = .(member_casual, volume_group, time_period)]

ebike_full[, rider_label := ifelse(member_casual == "casual", "Casual Riders", "Members")]


# ==== VISUALIZATIONS ====

dir.create("outputs/figures", showWarnings = FALSE, recursive = TRUE)

# ==== Rider Summary ====
overall_summary <- citibike_manhattan_all[, .(
  Total        = .N,
  Electric     = sum(rideable_type == "electric_bike"),
  Classic      = sum(rideable_type == "classic_bike")
), by = member_casual][
  , `:=`(
    Pct_Electric = round(Electric / Total * 100, 1),
    Pct_Classic  = round(Classic  / Total * 100, 1)
  )
]

# Optional: order rider types nicely
overall_summary[, member_casual := factor(member_casual, levels = c("casual", "member"))]
setorder(overall_summary, member_casual)

display_table <- overall_summary[, .(
  `Rider Type`     = tools::toTitleCase(as.character(member_casual)),
  `Total Trips`    = format(Total, big.mark = ","),
  `Electric Bikes` = format(Electric, big.mark = ","),
  `% Electric`     = Pct_Electric,
  `Classic Bikes`  = format(Classic, big.mark = ","),
  `% Classic`      = Pct_Classic
)]

kable(display_table, align = c("l","r","r","r","r","r"))

# ==== V1: Infrastructure Map ====
infra_map <- ggplot() +
  # Manhattan boundary
  geom_sf(data = manhattan_poly, fill = "gray95", color = "gray60", size = 0.3) +
  
  # Add bike lanes
  geom_sf(data = manhattan_routes, color = "lightblue", alpha = 0.3, size = 0.5) +
  
  # Stations colored by infrastructure level
  geom_point(data = stations, 
             aes(x = lng, y = lat, color = infrastructure_level, size = nearby_count),
             alpha = 0.7) +
  
  scale_color_manual(
    values = c("Low" = "#F44336", "Medium" = "#FF9800", "High" = "#4CAF50"),
    name = "Infrastructure Level"
  ) +
  
  scale_size_continuous(name = "Station Density", range = c(1, 4)) +
  
  labs(
    title = "Citi Bike Station Infrastructure",
    subtitle = "Stations classified by bike lane proximity and station density",
    caption = "Size indicates number of nearby stations within 500m"
  ) +
  
  theme_minimal() +
  theme(
    panel.grid      = element_blank(),
    axis.text       = element_blank(),
    axis.title      = element_blank(),
    legend.position = "right",
    plot.title      = element_text(size = 14, face = "bold"),
    plot.subtitle   = element_text(size = 10, color = "gray40")
  )

print(infra_map)

# ==== V2: Scatter Plot - Infrastructure vs E-bike Usage ====
scatter <- ggplot(station_full[!is.na(infrastructure_score) & trips >= 50 & ebike_pct >= 40], 
                  aes(x = infrastructure_score, y = ebike_pct)) +
  geom_point(aes(color = infrastructure_level, size = trips), alpha = 0.6) +
  geom_smooth(method = "lm", color = "black", linetype = "dashed", se = TRUE) +
  scale_color_manual(values = c("Low" = "#F44336", "Medium" = "#FF9800", "High" = "#4CAF50"),
                     name = "Infrastructure Level") +
  scale_size_continuous(range = c(1, 6), name = "Total Trips") +
  labs(
    title = "Infrastructure Score vs. E-bike Usage",
    subtitle = "Each dot is a station | Dashed line shows overall trend",
    x = "Infrastructure Score (higher = better infrastructure)",
    y = "E-bike Usage (%)"
  ) +
  theme_minimal() +
  theme(plot.title = element_text(size = 14, face = "bold"))

print(scatter)


# ==== V3: E-Bike Map ====
ebike_map <- ggplot() +
  geom_sf(data = manhattan_poly, fill = "gray95", color = "gray60", linewidth = 0.3) +
  geom_point(data = station_full[trips >= 50],
             aes(x = lng, y = lat, color = bike_balance, size = trips),
             alpha = 0.9) +
  scale_color_gradientn(
    colors = c("#08306b", "#2171b5", "#6baed6", "#f7f7f7", "#fcbba1", "#fb6a4a", "#cb181d"),
    values = scales::rescale(c(-60, -30, -10, 0, 10, 25, 40)),
    limits = c(-60, 40),
    breaks = c(-30, 0, 20),
    labels = c("More classic", "Near avg", "More e-bikes"),
    name = "Relative E-bike Share"
  ) +
  scale_size_continuous(range = c(2, 7), name = "Station\nTrip Volume", labels = scales::comma) +
  labs(
    title = "Where Are Stations More or Less E-bike-Heavy?",
    subtitle = sprintf("Color shows difference from Manhattan average (~%.0f%% e-bikes): orange = higher, blue = lower", overall_ebike_share),
    caption = "Data: Citi Bike Manhattan trips | Stations with 50+ trips shown"
  ) +
  coord_sf(datum = NA) +
  theme_minimal() +
  theme(
    panel.grid = element_blank(),
    axis.text = element_blank(),
    axis.title = element_blank(),
    legend.position = "right",
    plot.title = element_text(size = 14, face = "bold"),
    plot.subtitle = element_text(size = 10, color = "gray40")
  )

print(ebike_map)

# ==== V4: Heatmap ====
heatmap <- ggplot(ebike_full, aes(x = time_period, y = volume_group, fill = ebike_pct)) +
  geom_tile(color = "white", size = 1.5) +
  geom_text(aes(label = paste0(round(ebike_pct, 1), "%")),
            size = 4.5, fontface = "bold", color = "white") +
  facet_wrap(~rider_label) +
  scale_fill_gradient2(
    low = "#3498db",
    mid = "#95a5a6",
    high = "#e74c3c",
    midpoint = 68,
    name = "E-bike\nUsage",
    limits = c(60, 80),
    breaks = seq(60, 80, 5),
    labels = function(x) paste0(x, "%")
  ) +
  labs(
    title = "E-bike Usage Patterns Across Multiple Dimensions",
    subtitle = "Blue = More classic bikes | Red = More e-bikes | Overall average = 68% e-bikes",
    x = "Time of Day",
    y = "Station Activity Level",
    caption = "Rider Type Distribution: Casual – 22% | Member – 78%"
  ) +
  theme_minimal(base_size = 13) +
  theme(
    axis.text.x = element_text(angle = 45, hjust = 1, size = 11),
    axis.text.y = element_text(size = 11),
    strip.text = element_text(size = 14, face = "bold"),
    plot.title = element_text(size = 16, face = "bold", hjust = 0),
    plot.subtitle = element_text(size = 11, color = "gray30"),
    plot.caption = element_text(hjust = 0, size = 9),
    panel.grid = element_blank(),
    legend.position = "right"
  )

print(heatmap)

