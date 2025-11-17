# ============================================================================
# NYC Tree Analysis - Complete R Script with Extra Credit Datasets
# Author: Sabrina Zhu
# ============================================================================

# Load libraries
library(sf)
library(tidyverse)
library(leaflet)
library(DT)
library(data.table)

# ============================================================================
# PART 1: DOWNLOAD FUNCTIONS
# ============================================================================

# Download NYC Council Districts
download_districts <- function() {
  dir.create("data/mp03", showWarnings = FALSE, recursive = TRUE)
  
  zip_file <- "data/mp03/nycc_25c.zip"
  shp_file <- "data/mp03/nycc_25c/nycc.shp"
  
  if (!file.exists(shp_file)) {
    if (!file.exists(zip_file)) {
      download.file(
        "https://s-media.nyc.gov/agencies/dcp/assets/files/zip/data-tools/bytes/city-council/nycc_25c.zip",
        zip_file, mode = "wb", quiet = TRUE
      )
    }
    unzip(zip_file, exdir = "data/mp03")
  }
  
  st_read(shp_file, quiet = TRUE) |>
    st_transform("WGS84") |>
    st_make_valid()
}

# Download NYC Trees
download_trees <- function() {
  cache_file <- "data/mp03/all_trees.rds"
  
  if (file.exists(cache_file)) {
    return(readRDS(cache_file))
  }
  
  base_url <- "https://data.cityofnewyork.us/resource/hn5i-inap.geojson"
  all_trees <- list()
  offset <- 0
  limit <- 50000
  
  repeat {
    url <- paste0(base_url, "?$limit=", limit, "&$offset=", format(offset, scientific = FALSE))
    temp_file <- tempfile(fileext = ".geojson")
    
    tryCatch({
      download.file(url, temp_file, mode = "wb", quiet = TRUE)
      chunk <- st_read(temp_file, quiet = TRUE)
      
      if (nrow(chunk) == 0) break
      all_trees[[length(all_trees) + 1]] <- chunk
      cat("Downloaded", format(offset + nrow(chunk), big.mark = ","), "trees...\n")
      
      if (nrow(chunk) < limit) break
      
      offset <- offset + limit
    }, error = function(e) {
      cat("Error downloading at offset", offset, ":", e$message, "\n")
      return(NULL)
    })
  }
  
  trees <- bind_rows(all_trees) |>
    st_make_valid() |>
    mutate(
      tpcondition = factor(
        ifelse(is.na(tpcondition), "Unknown", tpcondition),
        levels = c("Excellent", "Good", "Fair", "Poor", "Critical", "Dead", "Unknown")
      ),
      genusspecies = as.factor(genusspecies)
    )
  
  saveRDS(trees, cache_file)
  cat("Total trees downloaded:", format(nrow(trees), big.mark = ","), "\n")
  trees
}

# EXTRA CREDIT: Download Tree Safety Risk Data
download_tree_risk <- function() {
  cache_file <- "data/mp03/tree_risk.rds"
  
  if (file.exists(cache_file)) {
    cat("Loading cached tree risk data...\n")
    return(readRDS(cache_file))
  }
  
  base_url <- "https://data.cityofnewyork.us/resource/259a-b6s7.json"
  all_risk <- list()
  offset <- 0
  limit <- 50000
  
  repeat {
    url <- paste0(base_url, "?$limit=", limit, "&$offset=", format(offset, scientific = FALSE))
    
    tryCatch({
      temp_data <- jsonlite::fromJSON(url)
      
      if (nrow(temp_data) == 0) break
      all_risk[[length(all_risk) + 1]] <- temp_data
      cat("Downloaded", format(offset + nrow(temp_data), big.mark = ","), "risk records...\n")
      
      if (nrow(temp_data) < limit) break
      
      offset <- offset + limit
    }, error = function(e) {
      cat("Error downloading risk data at offset", offset, ":", e$message, "\n")
      break
    })
  }
  
  risk_data <- bind_rows(all_risk)
  saveRDS(risk_data, cache_file)
  cat("Total risk records downloaded:", format(nrow(risk_data), big.mark = ","), "\n")
  risk_data
}

# EXTRA CREDIT: Download Tree Maintenance Data
download_tree_maintenance <- function() {
  cache_file <- "data/mp03/tree_maintenance.rds"
  
  if (file.exists(cache_file)) {
    cat("Loading cached maintenance data...\n")
    return(readRDS(cache_file))
  }
  
  base_url <- "https://data.cityofnewyork.us/resource/bdjm-n7q4.json"
  all_maintenance <- list()
  offset <- 0
  limit <- 50000
  
  repeat {
    url <- paste0(base_url, "?$limit=", limit, "&$offset=", format(offset, scientific = FALSE))
    
    tryCatch({
      temp_data <- jsonlite::fromJSON(url)
      
      if (nrow(temp_data) == 0) break
      all_maintenance[[length(all_maintenance) + 1]] <- temp_data
      cat("Downloaded", format(offset + nrow(temp_data), big.mark = ","), "maintenance records...\n")
      
      if (nrow(temp_data) < limit) break
      
      offset <- offset + limit
    }, error = function(e) {
      cat("Error downloading maintenance data at offset", offset, ":", e$message, "\n")
      break
    })
  }
  
  maintenance_data <- bind_rows(all_maintenance)
  saveRDS(maintenance_data, cache_file)
  cat("Total maintenance records downloaded:", format(nrow(maintenance_data), big.mark = ","), "\n")
  maintenance_data
}

# Spatial join with chunking
spatial_join <- function(trees, districts) {
  cache_file <- "data/mp03/trees_joined.rds"
  
  if (file.exists(cache_file)) {
    return(readRDS(cache_file))
  }
  
  n <- nrow(trees)
  chunk_size <- 100000
  chunks <- split(seq_len(n), ceiling(seq_len(n) / chunk_size))
  
  result <- map_dfr(chunks, ~{
    st_join(trees[.x, ], districts, join = st_intersects, left = TRUE)
  })
  
  saveRDS(result, cache_file)
  result
}

# ============================================================================
# TREE SAFETY RISK ASSESSMENT DATA
# ============================================================================

download_risk_assessment <- function(district = NULL) {
  if (!is.null(district)) {
    cache_file <- paste0("data/mp03/risk_assessment_d", district, ".rds")
  } else {
    cache_file <- "data/mp03/risk_assessment_all.rds"
  }
  
  if (file.exists(cache_file)) {
    return(readRDS(cache_file))
  }
  
  base_url <- "https://data.cityofnewyork.us/resource/259a-b6s7.json"
  
  if (!is.null(district)) {
    url <- paste0(base_url, "?council_district=", district, "&$limit=50000")
    data <- jsonlite::fromJSON(url)
    saveRDS(data, cache_file)
    return(data)
  } else {
    all_data <- list()
    offset <- 0
    limit <- 50000
    
    repeat {
      url <- paste0(base_url, "?$limit=", limit, "&$offset=", format(offset, scientific = FALSE))
      
      tryCatch({
        chunk <- jsonlite::fromJSON(url)
        if (nrow(chunk) == 0) break
        all_data[[length(all_data) + 1]] <- chunk
        if (nrow(chunk) < limit) break
        offset <- offset + limit
      }, error = function(e) {
        break
      })
    }
    
    data <- bind_rows(all_data)
    saveRDS(data, cache_file)
    return(data)
  }
}

# ============================================================================
# TREE MAINTENANCE WORK ORDERS DATA
# ============================================================================

download_work_orders <- function(district = NULL) {
  if (!is.null(district)) {
    cache_file <- paste0("data/mp03/work_orders_d", district, ".rds")
  } else {
    cache_file <- "data/mp03/work_orders_all.rds"
  }
  
  if (file.exists(cache_file)) {
    return(readRDS(cache_file))
  }
  
  base_url <- "https://data.cityofnewyork.us/resource/bdjm-n7q4.json"
  
  if (!is.null(district)) {
    url <- paste0(base_url, "?council_district=", district, "&$limit=50000")
    data <- jsonlite::fromJSON(url)
    saveRDS(data, cache_file)
    return(data)
  } else {
    all_data <- list()
    offset <- 0
    limit <- 50000
    
    repeat {
      url <- paste0(base_url, "?$limit=", limit, "&$offset=", format(offset, scientific = FALSE))
      
      tryCatch({
        chunk <- jsonlite::fromJSON(url)
        if (nrow(chunk) == 0) break
        all_data[[length(all_data) + 1]] <- chunk
        if (nrow(chunk) < limit) break
        offset <- offset + limit
      }, error = function(e) {
        break
      })
    }
    
    data <- bind_rows(all_data)
    saveRDS(data, cache_file)
    return(data)
  }
}

# ============================================================================
# PART 2: LOAD DATA
# ============================================================================

districts <- download_districts()
trees <- download_trees()
trees_joined <- spatial_join(trees, districts)

# EXTRA CREDIT: Load additional datasets
tree_risk <- download_tree_risk()
tree_maintenance <- download_tree_maintenance()

# Convert to data.table for fast aggregations
trees_dt <- trees_joined |> st_drop_geometry() |> as.data.table()

# ============================================================================
# PART 2B: PROCESS EXTRA CREDIT DATA
# ============================================================================

# Process risk data - convert to spatial if it has coordinates
if ("location" %in% names(tree_risk) && is.data.frame(tree_risk$location)) {
  cat("\nProcessing tree risk data with spatial coordinates...\n")
  
  risk_with_coords <- tree_risk |>
    filter(!is.na(location$coordinates)) |>
    mutate(
      longitude = sapply(location$coordinates, function(x) if(length(x) >= 2) x[1] else NA),
      latitude = sapply(location$coordinates, function(x) if(length(x) >= 2) x[2] else NA)
    ) |>
    filter(!is.na(longitude) & !is.na(latitude)) |>
    st_as_sf(coords = c("longitude", "latitude"), crs = 4326)
  
  # Spatial join to districts
  risk_with_districts <- risk_with_coords |>
    st_join(districts, join = st_intersects)
  
  risk_dt <- risk_with_districts |> st_drop_geometry() |> as.data.table()
  
  cat("Risk records with coordinates:", format(nrow(risk_dt), big.mark = ","), "\n")
}

# Process maintenance data - convert to spatial if it has coordinates
if ("location" %in% names(tree_maintenance) && is.data.frame(tree_maintenance$location)) {
  cat("\nProcessing tree maintenance data with spatial coordinates...\n")
  
  maintenance_with_coords <- tree_maintenance |>
    filter(!is.na(location$coordinates)) |>
    mutate(
      longitude = sapply(location$coordinates, function(x) if(length(x) >= 2) x[1] else NA),
      latitude = sapply(location$coordinates, function(x) if(length(x) >= 2) x[2] else NA)
    ) |>
    filter(!is.na(longitude) & !is.na(latitude)) |>
    st_as_sf(coords = c("longitude", "latitude"), crs = 4326)
  
  # Spatial join to districts
  maintenance_with_districts <- maintenance_with_coords |>
    st_join(districts, join = st_intersects)
  
  maintenance_dt <- maintenance_with_districts |> st_drop_geometry() |> as.data.table()
  
  cat("Maintenance records with coordinates:", format(nrow(maintenance_dt), big.mark = ","), "\n")
}

# ============================================================================
# MAIN MAP - NYC TREE DISTRIBUTION
# ============================================================================

# District summary
district_summary <- trees_dt[!is.na(CounDist), .N, by = CounDist]
districts_map <- districts |> left_join(district_summary, by = "CounDist")

# Color palettes
pal_districts <- colorNumeric("Greens", district_summary$N)
pal_trees <- colorFactor(
  palette = c("#006400", "#228B22", "#FFD700", "#FF8C00", "#DC143C", "#8B0000", "#808080"),
  levels = c("Excellent", "Good", "Fair", "Poor", "Critical", "Dead", "Unknown")
)

# Create interactive map
main_map <- leaflet() |>
  addProviderTiles(providers$CartoDB.Positron) |>
  addPolygons(
    data = districts_map,
    fillColor = ~pal_districts(N),
    fillOpacity = 0.7,
    color = "white",
    weight = 2,
    label = ~paste0("District ", CounDist, ": ", format(N, big.mark = ","), " trees")
  ) |>
  addCircleMarkers(
    data = trees_joined,
    radius = 1,
    color = ~pal_trees(tpcondition),
    fillColor = ~pal_trees(tpcondition),
    fillOpacity = 0.8,
    weight = 1,
    popup = ~paste0("<b>", genusspecies, "</b><br>Condition: ", tpcondition),
    clusterOptions = markerClusterOptions(
      maxClusterRadius = 80,
      disableClusteringAtZoom = 14
    )
  ) |>
  addLegend("bottomright", pal_districts, district_summary$N, title = "Trees per District") |>
  addLegend("topleft", pal_trees, levels(trees_joined$tpcondition), title = "Tree Condition") |>
  addControl(paste0("<b>Total Trees: ", format(nrow(trees_joined), big.mark = ","), "</b>"), "topright") |>
  setView(-73.95, 40.7, 12)

print(main_map)

# ============================================================================
# DISTRICT-LEVEL ANALYSIS (Q1-Q5)
# ============================================================================

# Q1: Total Tree Count by Council District
q1 <- trees_dt[!is.na(CounDist), .N, by = CounDist][order(-N)] |>
  mutate(Rank = row_number()) |>
  select(Rank, CounDist, N) |>
  setnames(c("CounDist", "N"), c("Council District", "Number of Trees"))

q1_table <- datatable(q1, options = list(pageLength = 10, dom = 'tp'), rownames = FALSE) |>
  formatStyle('Rank', target = 'row', backgroundColor = styleEqual(1, '#dbe5ff')) |>
  formatCurrency('Number of Trees', currency = "", interval = 3, mark = ",", digits = 0)

print(q1_table)
cat("\nDistrict", q1$'Council District'[1], "leads with", format(q1$'Number of Trees'[1], big.mark = ","), "trees.\n\n")

# Q2: Tree Density Across Council Districts
q2 <- trees_dt[!is.na(CounDist), .N, by = CounDist] |>
  left_join(districts |> st_drop_geometry() |> select(CounDist, Shape_Area), by = "CounDist") |>
  mutate(tree_density = (N / Shape_Area) * 1000) |>
  arrange(desc(tree_density)) |>
  mutate(Rank = row_number()) |>
  select(Rank, CounDist, N, Shape_Area, tree_density) |>
  setnames(c("CounDist", "N", "Shape_Area", "tree_density"), 
           c("Council District", "Number of Trees", "Area (sq units)", "Tree Density"))

q2_table <- datatable(q2, options = list(pageLength = 10, dom = 'tp'), rownames = FALSE) |>
  formatStyle('Rank', target = 'row', backgroundColor = styleEqual(1, '#ffe4b3')) |>
  formatCurrency('Number of Trees', currency = "", interval = 3, mark = ",", digits = 0) |>
  formatRound(c('Area (sq units)', 'Tree Density'), c(2, 4))

print(q2_table)

# Q3: Districts with the Highest Share of Dead Trees
q3 <- trees_dt[!is.na(CounDist), .(
  total_trees = .N,
  dead_trees = sum(tpcondition == "Dead", na.rm = TRUE)
), by = CounDist][, percent_dead := (dead_trees / total_trees) * 100][order(-percent_dead)] |>
  mutate(Rank = row_number()) |>
  select(Rank, CounDist, total_trees, dead_trees, percent_dead) |>
  setnames(c("CounDist", "total_trees", "dead_trees", "percent_dead"),
           c("Council District", "Total Trees", "Dead Trees", "Percent Dead"))

q3_table <- datatable(q3, options = list(pageLength = 10, dom = 'tip'), rownames = FALSE) |>
  formatStyle('Rank', target = 'row', backgroundColor = styleEqual(1, '#ffcccc')) |>
  formatCurrency(c('Total Trees', 'Dead Trees'), currency = "", interval = 3, mark = ",", digits = 0) |>
  formatRound('Percent Dead', digits = 2)

print(q3_table)

# Q4: Most Common Tree Species in Manhattan
trees_dt[, Borough := fcase(
  between(CounDist, 1, 10), "Manhattan",
  between(CounDist, 11, 18), "Bronx",
  between(CounDist, 19, 32), "Queens",
  between(CounDist, 33, 48), "Brooklyn",
  between(CounDist, 49, 51), "Staten Island"
)]

q4 <- trees_dt[Borough == "Manhattan" & !is.na(genusspecies), .N, by = genusspecies][order(-N)] |>
  mutate(Rank = row_number(), percent = N / sum(N) * 100) |>
  select(Rank, genusspecies, N, percent) |>
  setnames(c("genusspecies", "N", "percent"), c("Species", "Count", "Percent of Total"))

q4_table <- datatable(q4, options = list(pageLength = 10, dom = 'tip'), rownames = FALSE) |>
  formatStyle('Rank', target = 'row', backgroundColor = styleEqual(1, '#c8e6c9')) |>
  formatCurrency('Count', currency = "", interval = 3, mark = ",", digits = 0) |>
  formatRound('Percent of Total', digits = 2)

print(q4_table)

# Q5: Tree Species Nearest to Baruch College
baruch <- st_sfc(st_point(c(-73.9834, 40.7403)), crs = "WGS84")

q5 <- trees_joined |>
  mutate(distance_ft = as.numeric(st_distance(geometry, baruch)) * 3.28084) |>
  arrange(distance_ft) |>
  slice(1:10) |>
  st_drop_geometry() |>
  mutate(Rank = row_number()) |>
  select(Rank, genusspecies, distance_ft, tpcondition, CounDist) |>
  setnames(c("genusspecies", "distance_ft", "tpcondition", "CounDist"),
           c("Species", "Distance (ft)", "Condition", "Council District"))

q5_table <- datatable(q5, options = list(pageLength = 10, dom = 't'), rownames = FALSE) |>
  formatStyle('Rank', target = 'row', backgroundColor = styleEqual(1, '#fff4cc')) |>
  formatRound('Distance (ft)', digits = 0)

print(q5_table)

# ============================================================================
# PART 5: GOVERNMENT PROJECT - DISTRICT 2 ANALYSIS 
# ============================================================================

# Calculate comprehensive district-level analysis
dead_analysis <- trees_dt[!is.na(CounDist), .(
  total_trees = .N,
  dead_trees = sum(tpcondition == "Dead", na.rm = TRUE),
  critical_trees = sum(tpcondition == "Critical", na.rm = TRUE),
  poor_trees = sum(tpcondition == "Poor", na.rm = TRUE),
  unhealthy_trees = sum(tpcondition %in% c("Dead", "Critical", "Poor"), na.rm = TRUE),
  excellent_trees = sum(tpcondition == "Excellent", na.rm = TRUE),
  good_trees = sum(tpcondition == "Good", na.rm = TRUE)
), by = CounDist][, `:=`(
  percent_dead = (dead_trees / total_trees) * 100,
  percent_unhealthy = (unhealthy_trees / total_trees) * 100
)][order(-percent_dead)]

# Add borough classification
dead_analysis[, Borough := fcase(
  between(CounDist, 1, 10), "Manhattan",
  between(CounDist, 11, 18), "Bronx",
  between(CounDist, 19, 32), "Queens",
  between(CounDist, 33, 48), "Brooklyn",
  between(CounDist, 49, 51), "Staten Island"
)]

# EXTRA CREDIT: Analyze safety risk data by district
if (exists("risk_dt")) {
  safety_analysis <- risk_dt[!is.na(CounDist), .(
    total_risk_assessments = .N
  ), by = CounDist]
  
  # Check what risk-related columns exist
  risk_columns <- names(risk_dt)
  cat("\nRisk data columns available:", paste(risk_columns, collapse = ", "), "\n")
  
  # Join to dead_analysis
  dead_analysis <- dead_analysis[safety_analysis, on = "CounDist"]
}

# EXTRA CREDIT: Analyze maintenance data by district
if (exists("maintenance_dt")) {
  maintenance_analysis <- maintenance_dt[!is.na(CounDist), .(
    pending_maintenance = .N
  ), by = CounDist]
  
  # Check what maintenance columns exist
  maint_columns <- names(maintenance_dt)
  cat("\nMaintenance data columns available:", paste(maint_columns, collapse = ", "), "\n")
  
  # Join to dead_analysis
  dead_analysis <- dead_analysis[maintenance_analysis, on = "CounDist"]
}

# Focus on District 2
d2 <- dead_analysis[CounDist == 2]
top5 <- dead_analysis[1:5]
queens_districts <- dead_analysis[Borough == "Queens"][order(-percent_dead)]
healthiest_queens <- queens_districts[CounDist != 2][order(percent_dead)][1]

cat("\n=== DISTRICT 2 (QUEENS) COMPREHENSIVE ANALYSIS ===\n")
cat("Total trees:", format(d2$total_trees, big.mark = ","), "\n")
cat("Dead trees:", format(d2$dead_trees, big.mark = ","), sprintf("(%.2f%%)\n", d2$percent_dead))
cat("Critical trees:", format(d2$critical_trees, big.mark = ","), "\n")
cat("Poor trees:", format(d2$poor_trees, big.mark = ","), "\n")
cat("Total unhealthy (Dead+Critical+Poor):", format(d2$unhealthy_trees, big.mark = ","), 
    sprintf("(%.2f%%)\n", d2$percent_unhealthy))

if ("total_risk_assessments" %in% names(d2)) {
  cat("\n=== EXTRA CREDIT: SAFETY RISK DATA ===\n")
  cat("Trees with risk assessments:", format(d2$total_risk_assessments, big.mark = ","), "\n")
}

if ("pending_maintenance" %in% names(d2)) {
  cat("\n=== EXTRA CREDIT: MAINTENANCE DATA ===\n")
  cat("Pending maintenance orders:", format(d2$pending_maintenance, big.mark = ","), "\n")
}

cat("\n")

# Enhanced comparison table with extra credit data
comparison_cols <- c("Rank", "CounDist", "total_trees", "dead_trees", "unhealthy_trees", 
                     "percent_dead", "percent_unhealthy")

if ("total_risk_assessments" %in% names(top5)) {
  comparison_cols <- c(comparison_cols, "total_risk_assessments")
}

if ("pending_maintenance" %in% names(top5)) {
  comparison_cols <- c(comparison_cols, "pending_maintenance")
}

comparison <- top5 |>
  mutate(Rank = row_number()) |>
  select(any_of(comparison_cols))

# Rename columns for display
col_names <- c("CounDist" = "Council District", 
               "total_trees" = "Total Trees", 
               "dead_trees" = "Dead Trees", 
               "unhealthy_trees" = "Unhealthy Trees", 
               "percent_dead" = "% Dead", 
               "percent_unhealthy" = "% Unhealthy")

if ("total_risk_assessments" %in% names(comparison)) {
  col_names <- c(col_names, "total_risk_assessments" = "Risk Assessments")
}

if ("pending_maintenance" %in% names(comparison)) {
  col_names <- c(col_names, "pending_maintenance" = "Pending Maintenance")
}

comparison <- comparison |> setnames(old = names(col_names), new = col_names, skip_absent = TRUE)

comparison_table <- datatable(comparison, options = list(pageLength = 5, dom = 't'), rownames = FALSE,
                              caption = "Top 5 Districts by Tree Mortality (with Extra Credit Data)") |>
  formatStyle('Rank', target = 'row', backgroundColor = styleEqual(1, '#ffcccc')) |>
  formatCurrency(c('Total Trees', 'Dead Trees', 'Unhealthy Trees'), currency = "", interval = 3, mark = ",", digits = 0) |>
  formatRound(c('% Dead', '% Unhealthy'), digits = 2)

if ("Risk Assessments" %in% names(comparison)) {
  comparison_table <- comparison_table |>
    formatCurrency('Risk Assessments', currency = "", interval = 3, mark = ",", digits = 0)
}

if ("Pending Maintenance" %in% names(comparison)) {
  comparison_table <- comparison_table |>
    formatCurrency('Pending Maintenance', currency = "", interval = 3, mark = ",", digits = 0)
}

print(comparison_table)

# ============================================================================
# PART 6: VISUALIZATIONS
# ============================================================================

# Viz 1: Manhattan Districts Mortality Comparison
manhattan_districts <- dead_analysis[Borough == "Manhattan"][order(-percent_dead)]

viz1 <- manhattan_districts |>
  mutate(
    District = factor(CounDist),
    Highlight = ifelse(CounDist == 2, "District 2", "Other Manhattan Districts")
  ) |>
  ggplot(aes(reorder(District, -percent_dead), percent_dead, fill = Highlight)) +
  geom_col(width = 0.7) +
  geom_text(aes(label = sprintf("%.2f%%", percent_dead)), vjust = -0.5, size = 4, fontface = "bold") +
  scale_fill_manual(values = c("District 2" = "#dc3545", "Other Manhattan Districts" = "#6c757d")) +
  labs(
    title = "Tree Mortality Rates Across Manhattan Council Districts",
    subtitle = sprintf("District 2: %s dead trees (%.2f%% mortality)", 
                       format(d2$dead_trees, big.mark = ","), d2$percent_dead),
    x = "Manhattan Council District", y = "Percent Dead Trees (%)", fill = ""
  ) +
  theme_minimal() +
  theme(
    plot.title = element_text(size = 16, face = "bold"),
    plot.subtitle = element_text(size = 12, color = "gray40"),
    legend.position = "bottom",
    panel.grid.major.x = element_blank()
  ) +
  ylim(0, max(manhattan_districts$percent_dead) * 1.15)

print(viz1)

# Viz 3: Enhanced District 2 Map with Better Styling
d2_boundary <- districts |> filter(CounDist == 2)
d2_trees <- trees_joined |> 
  filter(CounDist == 2) |>
  mutate(lon = st_coordinates(geometry)[,1], lat = st_coordinates(geometry)[,2]) |>
  st_drop_geometry()

# Focus on dead and unhealthy trees for your project
d2_trees_project <- d2_trees |>
  mutate(
    project_relevant = case_when(
      tpcondition %in% c("Dead", "Critical", "Poor") ~ "Needs Attention",
      tpcondition %in% c("Excellent", "Good") ~ "Healthy",
      TRUE ~ "Fair/Unknown"
    )
  )

# version showing ONLY trees needing attention
viz3_alt <- ggplot() +
  geom_sf(data = d2_boundary, fill = "#f8f9fa", color = "#2c3e50", linewidth = 2) +
  
  # Only show problematic trees
  geom_point(
    data = d2_trees |> filter(tpcondition %in% c("Dead", "Critical", "Poor")),
    aes(lon, lat, color = tpcondition), 
    size = 1.5, alpha = 0.7
  ) +
  
  scale_color_manual(
    values = c("Dead" = "#8B0000", "Critical" = "#dc3545", "Poor" = "#ff6b6b"),
    name = "Tree Condition"
  ) +
  
  geom_sf_text(
    data = d2_boundary,
    label = "District 2",
    size = 8,
    fontface = "bold",
    color = "#2c3e50",
    alpha = 0.2
  ) +
  
  labs(
    title = "District 2: Trees Requiring Immediate Attention",
    subtitle = sprintf(
      "%s trees in critical condition (Dead/Critical/Poor)",
      format(sum(d2_trees$tpcondition %in% c("Dead", "Critical", "Poor")), big.mark = ",")
    ),
    caption = "Project Focus Area: Replacement and Rehabilitation Program"
  ) +
  
  coord_sf(
    xlim = st_bbox(d2_boundary)[c(1, 3)] + c(-0.005, 0.005),
    ylim = st_bbox(d2_boundary)[c(2, 4)] + c(-0.005, 0.005),
    expand = FALSE
  ) +
  
  theme_minimal() +
  theme(
    plot.title = element_text(size = 18, face = "bold", hjust = 0.5),
    plot.subtitle = element_text(size = 12, color = "gray40", hjust = 0.5),
    plot.caption = element_text(size = 11, face = "italic", color = "gray50", hjust = 0.5, margin = margin(t = 10)),
    axis.title = element_blank(),
    axis.text = element_text(size = 8, color = "gray60"),
    panel.grid = element_line(color = "gray95", linewidth = 0.3),
    legend.position = "bottom",
    legend.title = element_text(face = "bold"),
    panel.background = element_rect(fill = "white", color = NA),
    plot.background = element_rect(fill = "white", color = NA),
    plot.margin = margin(20, 20, 20, 20)
  )

print(viz3_alt)




# Option 1: FIXED - Dot Plot with Context
healthiest_manhattan <- manhattan_districts[CounDist != 2][order(percent_dead)][1]

viz4 <- manhattan_districts |>
  mutate(
    highlight = case_when(
      CounDist == 2 ~ "District 2 (Focus)",
      CounDist == healthiest_manhattan$CounDist ~ "Healthiest District",
      TRUE ~ "Other Manhattan Districts"
    ),
    district_label = paste0("District ", CounDist)
  ) |>
  ggplot(aes(x = percent_dead, y = reorder(district_label, percent_dead))) +
  
  # Lines connecting to show range
  geom_segment(
    aes(x = 0, xend = percent_dead, yend = district_label),
    color = "gray80",
    linewidth = 2
  ) +
  
  # Points
  geom_point(aes(color = highlight, size = highlight), alpha = 0.9) +
  
  # Labels for key districts
  geom_text(
    aes(label = ifelse(CounDist %in% c(2, healthiest_manhattan$CounDist),
                       sprintf("%.1f%% (%s dead)", percent_dead, format(dead_trees, big.mark = ",")),
                       "")),
    hjust = -0.2,
    size = 5,
    fontface = "bold"
  ) +
  
  scale_color_manual(
    values = c("District 2 (Focus)" = "#e74c3c", 
               "Healthiest District" = "#2ecc71",
               "Other Manhattan Districts" = "gray60"),
    name = NULL
  ) +
  
  scale_size_manual(
    values = c("District 2 (Focus)" = 8, 
               "Healthiest District" = 8,
               "Other Manhattan Districts" = 5),
    guide = "none"
  ) +
  
  scale_x_continuous(
    limits = c(0, max(manhattan_districts$percent_dead) * 1.3),
    labels = function(x) paste0(x, "%")
  ) +
  
  labs(
    title = "District 2 Has the Highest Tree Mortality Rate in Manhattan",
    subtitle = "Dot plot showing all 10 Manhattan districts ranked by tree mortality",
    x = "Tree Mortality Rate",
    y = NULL
  ) +
  
  theme_minimal(base_size = 13) +
  theme(
    legend.position = "top",
    legend.text = element_text(size = 11),
    plot.title = element_text(hjust = 0.5, face = "bold", size = 16),
    plot.subtitle = element_text(hjust = 0.5, color = "gray40", size = 11),
    panel.grid.major.y = element_blank(),
    panel.grid.minor = element_blank(),
    axis.text.y = element_text(size = 11, face = "bold")
  )

print(viz4)