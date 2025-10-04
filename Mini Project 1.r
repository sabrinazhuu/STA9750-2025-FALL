# 1. Aquire Data
if (!dir.exists(file.path("data", "mp01"))) {
  dir.create(file.path("data", "mp01"), showWarnings = FALSE, recursive = TRUE)
}

GLOBAL_TOP_10_FILENAME <- file.path("data", "mp01", "global_top10_alltime.csv")

if (!file.exists(GLOBAL_TOP_10_FILENAME)) {
  download.file("https://www.netflix.com/tudum/top10/data/all-weeks-global.tsv",
    destfile = GLOBAL_TOP_10_FILENAME
  )
}

COUNTRY_TOP_10_FILENAME <- file.path("data", "mp01", "country_top10_alltime.csv")

if (!file.exists(COUNTRY_TOP_10_FILENAME)) {
  download.file("https://www.netflix.com/tudum/top10/data/all-weeks-countries.tsv",
    destfile = COUNTRY_TOP_10_FILENAME
  )
}

# 2. Data Import and Preparation
if (!require("tidyverse")) install.packages("tidyverse")
library(readr)
library(dplyr)
library(scales)

# GLOBAL TOP 10
GLOBAL_TOP_10 <- read_tsv(GLOBAL_TOP_10_FILENAME)

str(GLOBAL_TOP_10)

glimpse(GLOBAL_TOP_10)

# Data Cleaning
GLOBAL_TOP_10 <- GLOBAL_TOP_10 |>
  mutate(season_title = if_else(season_title == "N/A", NA_character_, season_title))

glimpse(GLOBAL_TOP_10)

# COUNTRY TOP 10
COUNTRY_TOP_10 <- read_tsv(COUNTRY_TOP_10_FILENAME)

str(COUNTRY_TOP_10)

glimpse(COUNTRY_TOP_10)

# Data Cleaning
COUNTRY_TOP_10 <- COUNTRY_TOP_10 |>
  mutate(season_title = if_else(season_title == "N/A", NA_character_, season_title))

glimpse(GLOBAL_TOP_10)

# 3. Initial Data Exploration
# Add DT package
if (!require("DT")) install.packages('DT')
xfun::session_info('DT')

# GLOBAL TOP 10
library(DT)
GLOBAL_TOP_10 |> 
  head(n=20) |>
  datatable(options=list(searching=FALSE, info=FALSE))

#Format Columns & Numbers
library(stringr)
format_titles <- function(df){
  colnames(df) <- str_replace_all(colnames(df), "_", " ") |> str_to_title()
  df
}

GLOBAL_TOP_10 |> 
  format_titles() |>
  head(n=20) |>
  datatable(options=list(searching=FALSE, info=FALSE)) |>
  formatRound(c('Weekly Hours Viewed', 'Weekly Views'))

#Drop "Season Title"
GLOBAL_TOP_10 |> 
  select(-season_title) |>
  format_titles() |>
  head(n=20) |>
  datatable(options=list(searching=FALSE, info=FALSE)) |>
  formatRound(c('Weekly Hours ViewedC', 'Weekly Views'))

#Convert Runtime to Minutes
GLOBAL_TOP_10 |> 
  mutate(`runtime_(minutes)` = round(60 * runtime)) |>
  select(-season_title, 
         -runtime) |>
  format_titles() |>
  head(n=20) |>
  datatable(options=list(searching=FALSE, info=FALSE)) |>
  formatRound(c('Weekly Hours Viewed', 'Weekly Views'))

# COUNTRY TOP 10
library(DT)
COUNTRY_TOP_10 |> 
  head(n=20) |>
  datatable(options=list(searching=FALSE, info=FALSE))

# Format Columns & Numbers
library(stringr)
format_titles <- function(df){
  colnames(df) <- str_replace_all(colnames(df), "_", " ") |> str_to_title()
  df
}

COUNTRY_TOP_10  |> 
  format_titles() |>
  head(n=20) |>
  datatable(options=list(searching=FALSE, info=FALSE))

#Drop "Country Iso2" and "Season Title
COUNTRY_TOP_10 |> 
  select(-country_iso2, -season_title) |>
  format_titles() |>
  head(n=20) |>
  datatable(options=list(searching=FALSE, info=FALSE))

# 4. Exploratory Questions

# 1) How many different countries does Netflix operate in?
COUNTRY_TOP_10 |> 
  summarise(num_countries = n_distinct(country_name))

# 2) Which non-English-language film has spent the most cumulative weeks in the global top 10? How many weeks did it spend?
GLOBAL_TOP_10 |>
  distinct(category)

GLOBAL_TOP_10 |>
  filter(category == "Films (Non-English)") |>
  slice_max(cumulative_weeks_in_top_10, n = 1, with_ties = TRUE) |>
  select(show_title, cumulative_weeks_in_top_10)

# 3) What is the longest film (English or non-English) to have ever appeared in the Netflix global Top 10? How long is it in minutes?
GLOBAL_TOP_10 |> 
  filter(category %in% c("Films (English)", "Films (Non-English)")) |> 
  filter(!is.na(runtime)) |> 
  mutate(`runtime_(minutes)` = round(60 * runtime)) |>     
  slice_max(`runtime_(minutes)`, n = 1, with_ties = TRUE) |>  
  select(show_title, `runtime_(minutes)`) |> 
  distinct(show_title, .keep_all = TRUE)              

# 4) For each of the four categories, what program has the most total hours of global viewership?
GLOBAL_TOP_10 |> 
  group_by(category) |>
  filter(!is.na(weekly_hours_viewed)) |> 
  slice_max(weekly_hours_viewed, n = 1, with_ties = TRUE) |> 
  select(category, show_title, weekly_hours_viewed)

# 5) Which TV show had the longest run in a country’s Top 10? How long was this run and in what country did it occur?
COUNTRY_TOP_10 |> 
  slice_max(cumulative_weeks_in_top_10, n = 1, with_ties = TRUE) |> 
  select(country_name, show_title, cumulative_weeks_in_top_10)

# 6) Netflix provides over 200 weeks of service history for all but one country in our data set. Which country is this and when did Netflix cease operations in that country?
COUNTRY_TOP_10 |> 
  group_by(country_name) |> 
  summarise(
    total_weeks = n_distinct(week),
    last_week   = max(week)
  ) |> 
  arrange(total_weeks) |> 
  slice_head(n = 1)

# 7) What is the total viewership of the TV show Squid Game? Note that there are three seasons total and we are looking for the total number of hours watched across all seasons.
GLOBAL_TOP_10 |> 
  filter(show_title == "Squid Game") |>
  summarise(total_hours = sum(weekly_hours_viewed, na.rm = TRUE))

# 8) The movie Red Notice has a runtime of 1 hour and 58 minutes. Approximately how many views did it receive in 2021? Note that Netflix does not provide the weekly_views values that far back in the past, but you can compute it yourself using the total view time and the runtime.
  #Runtime in minutes (1h 58m = 118 minutes)
GLOBAL_TOP_10 |> 
  filter(show_title == "Red Notice", format(week, "%Y") == "2021") |> 
  summarise(
    total_hours = sum(weekly_hours_viewed, na.rm = TRUE),
    est_views   = total_hours / (118/60)
  ) |> 
  mutate(est_views = comma(round(est_views)))

# 9) How many Films reached Number 1 in the US but did not originally debut there? 
# That is, find films that first appeared on the Top 10 chart at, e.g., Number 4 but then became more popular and eventually hit Number 1? 
# What is the most recent film to pull this off?
COUNTRY_TOP_10 |> 
  filter(country_name == "United States", category == "Films") |> 
  group_by(show_title) |> 
  mutate(
    debuted_rank = weekly_rank[which.min(week)],      
    hit_number1  = any(weekly_rank == 1)            
  ) |> 
  summarise(
    debuted_rank = first(debuted_rank),       
    hit_number1 = first(hit_number1),
    most_recent_week = max(week)                  
  ) |> 
  filter(hit_number1, debuted_rank > 1) |>    
  slice_max(most_recent_week, n = 1, with_ties = FALSE)

# 10) Which TV show/season hit the top 10 in the most countries in its debut week? 
# In how many countries did it chart?
COUNTRY_TOP_10 |> 
  filter(category == "TV") |>
  group_by(show_title) |> 
  mutate(debut_week = min(week)) |>            # first week each show appeared
  ungroup() |> 
  group_by(show_title, debut_week) |> 
  summarise(countries = n_distinct(country_name), .groups = "drop") |> 
  slice_max(countries, n = 1)
  




# Total unique films and shows in India
india_content_totals <- COUNTRY_TOP_10 |>
  filter(country_name == "India") |>
  mutate(content_type = if_else(grepl("Films", category), "Films", "TV Shows")) |>
  group_by(content_type) |>
  summarise(unique_titles = n_distinct(show_title))

india_total_films <- india_content_totals |> filter(content_type == "Films") |> pull(unique_titles)
india_total_shows <- india_content_totals |> filter(content_type == "TV Shows") |> pull(unique_titles)
india_total_all <- sum(india_content_totals$unique_titles)

# Top films by weeks in Top 10
top_india_films_weeks <- COUNTRY_TOP_10 |>
  filter(country_name == "India", grepl("Films", category)) |>
  group_by(show_title) |>
  summarise(
    total_weeks = n_distinct(week),
    peak_rank = min(weekly_rank),
    max_cumulative = max(cumulative_weeks_in_top_10)
  ) |>
  arrange(desc(max_cumulative)) |>
  slice_head(n = 10)

# Top shows by weeks in Top 10
top_india_shows_weeks <- COUNTRY_TOP_10 |>
  filter(country_name == "India", grepl("TV", category)) |>
  group_by(show_title) |>
  summarise(
    total_weeks = n_distinct(week),
    peak_rank = min(weekly_rank),
    max_cumulative = max(cumulative_weeks_in_top_10)
  ) |>
  arrange(desc(max_cumulative)) |>
  slice_head(n = 10)

