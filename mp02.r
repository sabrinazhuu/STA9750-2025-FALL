# Task 1: Data Import
# Incorporate the instructor provided code below into your report. 
# As you do so, make sure to look for possible “keys” on which you will be able to join datasets together.

## 1.1 Use tidycensus package
if(!dir.exists(file.path("data", "mp02"))){
  dir.create(file.path("data", "mp02"), showWarnings=FALSE, recursive=TRUE)
}

library <- function(pkg){
  ## Mask base::library() to automatically install packages if needed
  ## Masking is important here so downlit picks up packages and links
  ## to documentation
  pkg <- as.character(substitute(pkg))
  options(repos = c(CRAN = "https://cloud.r-project.org"))
  if(!require(pkg, character.only=TRUE, quietly=TRUE)) install.packages(pkg)
  stopifnot(require(pkg, character.only=TRUE, quietly=TRUE))
}

library(tidyverse)
library(glue)
library(readxl)
library(tidycensus)

get_acs_all_years <- function(variable, geography="cbsa",
                              start_year=2009, end_year=2023){
  fname <- glue("{variable}_{geography}_{start_year}_{end_year}.csv")
  fname <- file.path("data", "mp02", fname)
  
  if(!file.exists(fname)){
    YEARS <- seq(start_year, end_year)
    YEARS <- YEARS[YEARS != 2020] # Drop 2020 - No survey (covid)
    
    ALL_DATA <- map(YEARS, function(yy){
      tidycensus::get_acs(geography, variable, year=yy, survey="acs1") |>
        mutate(year=yy) |>
        select(-moe, -variable) |>
        rename(!!variable := estimate)
    }) |> bind_rows()
    
    write_csv(ALL_DATA, fname)
  }
  
  read_csv(fname, show_col_types=FALSE)
}

# Household income (12 month)
INCOME <- get_acs_all_years("B19013_001") |>
  rename(household_income = B19013_001)

# Monthly rent
RENT <- get_acs_all_years("B25064_001") |>
  rename(monthly_rent = B25064_001)

# Total population
POPULATION <- get_acs_all_years("B01003_001") |>
  rename(population = B01003_001)

# Total number of households
HOUSEHOLDS <- get_acs_all_years("B11001_001") |>
  rename(households = B11001_001)




## 1.2 We will also need the number of new housing units built each year.
get_building_permits <- function(start_year = 2009, end_year = 2023){
  fname <- glue("housing_units_{start_year}_{end_year}.csv")
  fname <- file.path("data", "mp02", fname)
  
  if(!file.exists(fname)){
    HISTORICAL_YEARS <- seq(start_year, 2018)
    
    HISTORICAL_DATA <- map(HISTORICAL_YEARS, function(yy){
      historical_url <- glue("https://www.census.gov/construction/bps/txt/tb3u{yy}.txt")
      
      LINES <- readLines(historical_url)[-c(1:11)]
      
      CBSA_LINES <- str_detect(LINES, "^[[:digit:]]")
      CBSA <- as.integer(str_sub(LINES[CBSA_LINES], 5, 10))
      
      PERMIT_LINES <- str_detect(str_sub(LINES, 48, 53), "[[:digit:]]")
      PERMITS <- as.integer(str_sub(LINES[PERMIT_LINES], 48, 53))
      
      data_frame(CBSA = CBSA,
                 new_housing_units_permitted = PERMITS, 
                 year = yy)
    }) |> bind_rows()
    
    CURRENT_YEARS <- seq(2019, end_year)
    
    CURRENT_DATA <- map(CURRENT_YEARS, function(yy){
      current_url <- glue("https://www.census.gov/construction/bps/xls/msaannual_{yy}99.xls")
      
      temp <- tempfile()
      
      download.file(current_url, destfile = temp, mode="wb")
      
      fallback <- function(.f1, .f2){
        function(...){
          tryCatch(.f1(...), 
                   error=function(e) .f2(...))
        }
      }
      
      reader <- fallback(read_xlsx, read_xls)
      
      reader(temp, skip=5) |>
        na.omit() |>
        select(CBSA, Total) |>
        mutate(year = yy) |>
        rename(new_housing_units_permitted = Total)
    }) |> bind_rows()
    
    ALL_DATA <- rbind(HISTORICAL_DATA, CURRENT_DATA)
    
    write_csv(ALL_DATA, fname)
    
  }
  
  read_csv(fname, show_col_types=FALSE)
}

PERMITS <- get_building_permits()

## 1.3 North American Industry Classification System (NAICS) coding system
library(httr2)
library(rvest)
get_bls_industry_codes <- function(){
  fname <- fname <- file.path("data", "mp02", "bls_industry_codes.csv")
  
  if(!file.exists(fname)){
    
    resp <- request("https://www.bls.gov") |> 
      req_url_path("cew", "classifications", "industry", "industry-titles.htm") |>
      req_headers(`User-Agent` = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:143.0) Gecko/20100101 Firefox/143.0") |> 
      req_error(is_error = \(resp) FALSE) |>
      req_perform()
    
    resp_check_status(resp)
    
    naics_table <- resp_body_html(resp) |>
      html_element("#naics_titles") |> 
      html_table() |>
      mutate(title = str_trim(str_remove(str_remove(`Industry Title`, Code), "NAICS"))) |>
      select(-`Industry Title`) |>
      mutate(depth = if_else(nchar(Code) <= 5, nchar(Code) - 1, NA)) |>
      filter(!is.na(depth))
    
    naics_table <- naics_table |> 
      filter(depth == 4) |> 
      rename(level4_title=title) |> 
      mutate(level1_code = str_sub(Code, end=2), 
             level2_code = str_sub(Code, end=3), 
             level3_code = str_sub(Code, end=4)) |>
      left_join(naics_table, join_by(level1_code == Code)) |>
      rename(level1_title=title) |>
      left_join(naics_table, join_by(level2_code == Code)) |>
      rename(level2_title=title) |>
      left_join(naics_table, join_by(level3_code == Code)) |>
      rename(level3_title=title) |>
      select(-starts_with("depth")) |>
      rename(level4_code = Code) |>
      select(level1_title, level2_title, level3_title, level4_title, 
             level1_code,  level2_code,  level3_code,  level4_code)
    
    write_csv(naics_table, fname)
  }
  
  read_csv(fname, show_col_types=FALSE)
  
}

INDUSTRY_CODES <- get_bls_industry_codes()

## 1.4 BLS Quarterly Census of Employment and Wages

library(httr2)
library(rvest)
get_bls_qcew_annual_averages <- function(start_year=2009, end_year=2023){
  fname <- glue("bls_qcew_{start_year}_{end_year}.csv.gz")
  fname <- file.path("data", "mp02", fname)
  
  YEARS <- seq(start_year, end_year)
  YEARS <- YEARS[YEARS != 2020] # Drop Covid year to match ACS
  
  if(!file.exists(fname)){
    ALL_DATA <- map(YEARS, .progress=TRUE, possibly(function(yy){
      fname_inner <- file.path("data", "mp02", glue("{yy}_qcew_annual_singlefile.zip"))
      
      if(!file.exists(fname_inner)){
        request("https://www.bls.gov") |> 
          req_url_path("cew", "data", "files", yy, "csv",
                       glue("{yy}_annual_singlefile.zip")) |>
          req_headers(`User-Agent` = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:143.0) Gecko/20100101 Firefox/143.0") |> 
          req_retry(max_tries=5) |>
          req_perform(fname_inner)
      }
      
      if(file.info(fname_inner)$size < 755e5){
        warning(sQuote(fname_inner), "appears corrupted. Please delete and retry this step.")
      }
      
      read_csv(fname_inner, 
               show_col_types=FALSE) |> 
        mutate(YEAR = yy) |>
        select(area_fips, 
               industry_code, 
               annual_avg_emplvl, 
               total_annual_wages, 
               YEAR) |>
        filter(nchar(industry_code) <= 5, 
               str_starts(area_fips, "C")) |>
        filter(str_detect(industry_code, "-", negate=TRUE)) |>
        mutate(FIPS = area_fips, 
               INDUSTRY = as.integer(industry_code), 
               EMPLOYMENT = as.integer(annual_avg_emplvl), 
               TOTAL_WAGES = total_annual_wages) |>
        select(-area_fips, 
               -industry_code, 
               -annual_avg_emplvl, 
               -total_annual_wages) |>
        # 10 is a special value: "all industries" , so omit
        filter(INDUSTRY != 10) |> 
        mutate(AVG_WAGE = TOTAL_WAGES / EMPLOYMENT)
    })) |> bind_rows()
    
    write_csv(ALL_DATA, fname)
  }
  
  ALL_DATA <- read_csv(fname, show_col_types=FALSE)
  
  ALL_DATA_YEARS <- unique(ALL_DATA$YEAR)
  
  YEARS_DIFF <- setdiff(YEARS, ALL_DATA_YEARS)
  
  if(length(YEARS_DIFF) > 0){
    stop("Download failed for the following years: ", YEARS_DIFF, 
         ". Please delete intermediate files and try again.")
  }
  
  ALL_DATA
}

WAGES <- get_bls_qcew_annual_averages()

# Data acquisition end.


# View Info
#glimpse (INCOME)
#glimpse (RENT)
#glimpse (POPULATION)
#glimpse(HOUSEHOLDS)
#glimpse(PERMITS)
#glimpse(INDUSTRY_CODES)
#glimpse(WAGES)


# 2. Multi-Table Questions

# 2.1 Which CBSA (by name) permitted the largest number of new housing units in the decade from 2010 to 2019 (inclusive)?
library(dplyr)

# Filter for 2010-2019 and sum permits by CBSA
permits_2010_2019 <- PERMITS |>
  filter(year >= 2010 & year <= 2019) |>
  group_by(CBSA) |>
  summarise(total_permits = sum(new_housing_units_permitted, na.rm = TRUE)) |>
  arrange(desc(total_permits))

# Get the top CBSA with name (taking just one name per CBSA)
top_cbsa_with_name <- permits_2010_2019 |>
  slice(1) |>
  left_join(INCOME |> 
              select(GEOID, NAME, year) |> 
              arrange(desc(year)) |>  # Get most recent year's name
              distinct(GEOID, .keep_all = TRUE), 
            by = c("CBSA" = "GEOID"))

# Store the answer as a variable
answer_top_cbsa_name <- top_cbsa_with_name$NAME
answer_top_cbsa_permits <- top_cbsa_with_name$total_permits

# Print to verify
print(top_cbsa_with_name)


# 2, 2 In what year did Albuquerque, NM (CBSA Number 10740) permit the most new housing units?
# Hint: There is a Covid-19 data artifact here that may trip you up if you do not look at your answer closely.

# Filter for Albuquerque (CBSA 10740) and find the year with most permits
albuquerque_permits <- PERMITS |>
  filter(CBSA == 10740) |>
  arrange(desc(new_housing_units_permitted))

# Get the top year
top_year_abq <- albuquerque_permits |>
  slice(1)

# Store the answer
answer_abq_year <- top_year_abq$year
answer_abq_permits <- top_year_abq$new_housing_units_permitted

# You might also want to visualize to see the COVID artifact
library(ggplot2)

# Filter for Albuquerque (CBSA 10740)
albuquerque_permits <- PERMITS |>
  filter(CBSA == 10740) |>
  arrange(year)

# Create the visualization
ggplot(albuquerque_permits, aes(x = year, y = new_housing_units_permitted)) +
  geom_line(linewidth = 1, color = "steelblue") +
  geom_point(size = 3, color = "steelblue") +
  geom_point(data = albuquerque_permits |> filter(year == 2021), 
             size = 5, color = "red") +  # Highlight 2021
  geom_vline(xintercept = 2020, linetype = "dashed", color = "gray50") +
  annotate("text", x = 2020, y = 3800, 
           label = "COVID-19", angle = 90, vjust = -0.5, size = 3.5) +
  labs(title = "Housing Permits in Albuquerque, NM (CBSA 10740)",
       subtitle = "2021 spike likely reflects pent-up demand from COVID-19 disruptions",
       x = "Year",
       y = "New Housing Units Permitted") +
  scale_y_continuous(labels = scales::comma) +
  theme_minimal() +
  theme(plot.title = element_text(face = "bold"))


# 2.3 Which state (not CBSA) had the highest average individual income in 2015? 
# To answer this question, you will need to first compute the total income per CBSA by multiplying the average household income by the number of households, 
# and then sum total income and total population across all CBSAs in a state. With these numbers, you can answer this question.

library(dplyr)
library(stringr)

# Create state lookup dataframe
state_df <- data.frame(abb  = c(state.abb, "DC", "PR"),
                       name = c(state.name, "District of Columbia", "Puerto Rico"))

# Compute total income by CBSA for 2015 and extract state
cbsa_income_2015 <- INCOME |>
  filter(year == 2015) |>
  left_join(HOUSEHOLDS |> filter(year == 2015), 
            by = c("GEOID", "NAME", "year")) |>
  left_join(POPULATION |> filter(year == 2015),
            by = c("GEOID", "NAME", "year")) |>
  mutate(total_income = household_income * households) |>
  mutate(state = str_extract(NAME, ", (.{2})", group = 1))

# Group by state and calculate average individual income
state_income_2015 <- cbsa_income_2015 |>
  group_by(state) |>
  summarise(
    total_income = sum(total_income, na.rm = TRUE),
    total_population = sum(population, na.rm = TRUE)
  ) |>
  mutate(avg_individual_income = total_income / total_population) |>
  arrange(desc(avg_individual_income))

# Get the top state
top_state_2015 <- state_income_2015 |>
  slice(1)



# Join with full state names
top_state_with_name <- top_state_2015 |>
  left_join(state_df, by = c("state" = "abb"))

# Store the answer
answer_top_state_abbr <- top_state_with_name$state
answer_top_state_name <- top_state_with_name$name
answer_top_state_income <- top_state_with_name$avg_individual_income

# Join state names for all states
state_income_2015_with_names <- state_income_2015 |>
  left_join(state_df, by = c("state" = "abb")) |>
  filter(!is.na(name))

# Get top 10 and bottom 10 states
top_bottom_states <- bind_rows(
  state_income_2015_with_names |> slice_head(n = 10) |> mutate(category = "Top 10"),
  state_income_2015_with_names |> slice_tail(n = 10) |> mutate(category = "Bottom 10")
) |>
  mutate(name = factor(name, levels = name[order(avg_individual_income)]))

# Create bar chart with pastel colors and labels
ggplot(top_cbsa_by_year, aes(x = EMPLOYMENT, y = factor(YEAR), fill = city)) +
  geom_col(width = 0.7) +
  geom_text(aes(label = city), 
            hjust = -0.1, size = 3.5, fontface = "bold") +
  scale_x_continuous(labels = scales::comma_format(), 
                     expand = expansion(mult = c(0.05, 0.2))) +
  scale_y_discrete(breaks = seq(min(top_cbsa_by_year$YEAR), 
                                max(top_cbsa_by_year$YEAR), by = 1)) +
  scale_fill_manual(values = c("New York" = "#FF6B6B", 
                                "San Francisco" = "#4ECDC4",
                                "San Jose" = "#95E1D3",
                                "Washington DC" = "#F38181",
                                "Seattle" = "#AA96DA",
                                "Boston" = "#FCBAD3")) +
  labs(
    title = "Leading Metropolitan Area for Data Scientists Over Time",
    subtitle = "CBSA with highest data scientist employment each year (NAICS 5182)",
    x = "Number of Data Scientists",
    y = "Year",
    caption = "Source: BLS QCEW. New York was last #1 in 2015, San Francisco has dominated since."
  ) +
  theme_minimal(base_size = 12) +
  theme(
    plot.title = element_text(face = "bold", size = 16, margin = margin(b = 5)),
    plot.subtitle = element_text(size = 12, color = "gray40", margin = margin(b = 15)),
    axis.title = element_text(size = 12, face = "bold"),
    axis.text = element_text(size = 11),
    plot.caption = element_text(hjust = 0, size = 10, color = "gray50"),
    legend.position = "none",
    panel.grid.minor = element_blank(),
    panel.grid.major.y = element_blank()
  )


# 2.4 Data scientists and business analysts are recorded under NAICS code 5182. 
# What is the last year in which the NYC CBSA had the most data scientists in the country? 
# In recent, the San Francisco CBSA has had the most data scientists.

library(dplyr)
library(stringr)

# Get distinct CBSA names (only one row per CBSA, not per year)
# We just need the name lookup, not year-specific data
t1 <- POPULATION |>
  select(GEOID, NAME) |>
  distinct(GEOID, .keep_all = TRUE) |>  
  mutate(std_cbsa = paste0("C", GEOID))

# Filter wages data for data scientists (NAICS 5182) and standardize CBSA ID
# BLS uses FIPS codes like "C1018", so we add "0" suffix to match Census format
t2 <- WAGES |>
  filter(INDUSTRY == 5182) |>
  mutate(std_cbsa = paste0(FIPS, "0"))

# Join the tables to attach CBSA names to employment data
data_scientists_with_names <- inner_join(t1, t2, join_by(std_cbsa == std_cbsa))

# Identify which CBSA had the highest data scientist employment each year
top_cbsa_by_year <- data_scientists_with_names |>
  group_by(YEAR) |>
  slice_max(order_by = EMPLOYMENT, n = 1, with_ties = FALSE) |>  # Added with_ties = FALSE
  ungroup() |>  # Remove grouping
  select(YEAR, NAME, EMPLOYMENT) |>
  arrange(YEAR)

# Find the most recent year when NYC had the top data scientist employment
last_nyc_year <- top_cbsa_by_year |>
  filter(str_detect(NAME, "New York")) |>
  arrange(desc(YEAR)) |>  
  slice(1)

# Store answer for inline reporting
answer_last_nyc_year <- last_nyc_year$YEAR


# 2.5 What fraction of total wages in the NYC CBSA was earned by people employed in the finance and insurance industries (NAICS code 52)? 
# In what year did this fraction peak?

library(dplyr)
library(stringr)

# Find NYC's CBSA code
nyc_cbsa <- POPULATION |>
  filter(str_detect(NAME, "New York.*NY.*Metro")) |>
  distinct(GEOID, NAME)

# Convert GEOID to BLS format
nyc_fips <- "C3562" 

# Calculate total wages and finance wages by year for NYC
nyc_wages_by_year <- WAGES |>
  filter(FIPS == nyc_fips) |>
  group_by(YEAR) |>
  summarise(
    total_wages = sum(TOTAL_WAGES, na.rm = TRUE),
    finance_wages = sum(TOTAL_WAGES[INDUSTRY == 52], na.rm = TRUE)
  ) |>
  mutate(finance_fraction = finance_wages / total_wages) |>
  arrange(YEAR)

# Find the year with the highest finance fraction
peak_year <- nyc_wages_by_year |>
  slice_max(order_by = finance_fraction, n = 1, with_ties = FALSE)


# Store answers for inline reporting
answer_finance_fraction <- peak_year$finance_fraction
answer_peak_year <- peak_year$YEAR


# 3.1 The relationship between monthly rent and average household income per CBSA in 2009.
library(dplyr)
library(ggplot2)

# Prepare the data: join rent and income for 2009
rent_income_2009 <- RENT |>
  filter(year == 2009) |>
  inner_join(INCOME |> filter(year == 2009), 
             by = c("GEOID", "NAME", "year"))

# Create publication-ready scatterplot
ggplot(rent_income_2009, aes(x = household_income, y = monthly_rent)) +
  geom_point(alpha = 0.6, color = "steelblue", size = 2) +
  geom_smooth(method = "lm", se = FALSE, color = "darkred", linewidth = 1.2) +
  labs(
    title = "Monthly Rent vs. Household Income Across U.S. Metropolitan Areas",
    subtitle = "Data from 2009",
    x = "Average Household Income (USD)",
    y = "Monthly Rent (USD)",
    caption = "Note: Each point represents one Core Based Statistical Area (CBSA)"
  ) +
  scale_x_continuous(labels = scales::dollar_format(prefix = "$", suffix = "", big.mark = ",")) +
  scale_y_continuous(labels = scales::dollar_format(prefix = "$", suffix = "", big.mark = ",")) +
  theme_minimal(base_size = 12) +
  theme(
    plot.title = element_text(face = "bold", size = 16, hjust = 0),
    plot.subtitle = element_text(size = 13, hjust = 0, color = "gray30"),
    axis.title.x = element_text(size = 13, face = "bold", margin = margin(t = 10)),
    axis.title.y = element_text(size = 13, face = "bold", margin = margin(r = 10)),
    axis.text = element_text(size = 11),
    plot.caption = element_text(size = 10, hjust = 0, color = "gray50"),
    panel.grid.minor = element_blank()
  )

#  3.2 The relationship between total employment and total employment in the health care and social services sector (NAICS 62) across different CBSAs. 
#  Design your visualization so that it is possible to see the evolution of this relationship over time.

library(dplyr)
library(ggplot2)

# Calculate healthcare as percentage of total employment for each CBSA and year
healthcare_share <- WAGES |>
  group_by(FIPS, YEAR) |>
  summarise(
    total_employment = sum(EMPLOYMENT, na.rm = TRUE),
    healthcare_employment = sum(EMPLOYMENT[INDUSTRY == 62], na.rm = TRUE),
    .groups = "drop"
  ) |>
  filter(total_employment > 0 & healthcare_employment > 0) |>
  mutate(healthcare_pct = (healthcare_employment / total_employment) * 100)

# Calculate national average by year
national_avg <- healthcare_share |>
  group_by(YEAR) |>
  summarise(
    avg_healthcare_pct = mean(healthcare_pct, na.rm = TRUE),
    .groups = "drop"
  )

# Create clean, modern plot with trend line
ggplot(national_avg, aes(x = YEAR, y = avg_healthcare_pct)) +
  geom_smooth(method = "lm", se = FALSE, color = "gray60", 
              linetype = "dashed", linewidth = 0.8) +
  geom_line(linewidth = 1.2, color = "#0066CC") +
  geom_point(size = 2.5, color = "#0066CC") +
  labs(
    title = "Healthcare Share of U.S. Employment",
    x = "Year",
    y = "Share of Total Employment (%)",
    caption = "Source: Bureau of Labor Statistics. Linear trend shown in gray."
  ) +
  scale_y_continuous(
    labels = function(x) paste0(x, "%"),
    breaks = seq(3.5, 3.9, by = 0.1)
  ) +
  scale_x_continuous(breaks = seq(2009, 2023, by = 2)) +
  theme_minimal(base_size = 12) +
  theme(
    plot.title = element_text(face = "bold", size = 16, margin = margin(b = 15)),
    axis.title = element_text(size = 12, face = "bold"),
    axis.text = element_text(size = 10),
    plot.caption = element_text(size = 9, color = "gray50", hjust = 0),
    panel.grid.minor = element_blank(),
    panel.grid.major = element_line(color = "gray92", linewidth = 0.3),
    plot.margin = margin(15, 15, 15, 15)
  )

# 3.3 The evolution of average household size over time. Use different lines to represent different CBSAs.

library(dplyr)
library(ggplot2)
library(gghighlight)

# Calculate household size for all CBSAs
household_size <- POPULATION |>
  inner_join(HOUSEHOLDS, by = c("GEOID", "NAME", "year")) |>
  mutate(avg_household_size = population / households) |>
  filter(!is.na(avg_household_size) & avg_household_size > 0)

# Create a clean city name variable for highlighting
household_size <- household_size |>
  mutate(city_label = case_when(
    GEOID == 35620 ~ "New York",
    GEOID == 31080 ~ "Los Angeles",
    TRUE ~ NA_character_
  ))

# Create plot with gghighlight
ggplot(household_size, aes(x = year, y = avg_household_size, 
                           group = GEOID, color = city_label)) +
  geom_line(linewidth = 1) +
  gghighlight(GEOID %in% c(35620, 31080),  # NYC and LA
              label_key = city_label,
              use_direct_label = TRUE,
              label_params = list(size = 4, fontface = "bold")) +
  labs(
    title = "Evolution of Average Household Size Across U.S. Metropolitan Areas",
    subtitle = "New York and Los Angeles highlighted; all other CBSAs shown in gray",
    x = "Year",
    y = "Average Household Size (persons)",
    caption = "Source: U.S. Census Bureau"
  ) +
  scale_x_continuous(breaks = seq(2009, 2023, by = 2)) +
  scale_color_manual(values = c("New York" = "#0066CC", "Los Angeles" = "#E63946"),
                     na.value = "gray70") +
  theme_minimal(base_size = 12) +
  theme(
    plot.title = element_text(face = "bold", size = 16, margin = margin(b = 5)),
    plot.subtitle = element_text(size = 12, color = "gray40", margin = margin(b = 15)),
    axis.title = element_text(size = 12, face = "bold"),
    axis.text = element_text(size = 10),
    plot.caption = element_text(size = 9, color = "gray50", hjust = 0),
    panel.grid.minor = element_blank(),
    panel.grid.major = element_line(color = "gray92", linewidth = 0.3),
    plot.margin = margin(15, 15, 15, 15),
    legend.position = "none"
  )


# Table 1: NYC rent burden over time
nyc_burden_table <- rent_burden_data |>
  filter(GEOID == 35620) |>
  select(Year = year, 
         `Monthly Rent` = monthly_rent, 
         `Monthly Income` = monthly_household_income,
         `Rent/Income (%)` = raw_rent_burden,
         `Burden Index` = rent_burden_index,
         `Burden Score (0-100)` = rent_burden_score) |>
  arrange(Year) |>
  mutate(
    `Monthly Rent` = paste0("$", format(round(`Monthly Rent`), big.mark = ",")),
    `Monthly Income` = paste0("$", format(round(`Monthly Income`), big.mark = ",")),
    `Rent/Income (%)` = round(`Rent/Income (%)`, 1),
    `Burden Index` = round(`Burden Index`, 1),
    `Burden Score (0-100)` = round(`Burden Score (0-100)`, 1)
  )

datatable(nyc_burden_table,
          caption = "Table 1: Rent Burden Evolution in New York Metro Area",
          options = list(pageLength = 15, dom = 't', scrollX = TRUE),
          rownames = FALSE) |>
  formatStyle('Burden Score (0-100)',
              background = styleColorBar(c(0, 100), '#87CEEB'),
              backgroundSize = '100% 90%',
              backgroundRepeat = 'no-repeat',
              backgroundPosition = 'center')

# Table 2: Top 10 highest and lowest burden metros
recent_year <- max(rent_burden_data$year)

top_10_highest <- rent_burden_data |>
  filter(year == recent_year) |>
  arrange(desc(rent_burden_score)) |>
  head(10) |>
  mutate(Rank = row_number(), Category = "Highest Burden")

top_10_lowest <- rent_burden_data |>
  filter(year == recent_year) |>
  arrange(rent_burden_score) |>
  head(10) |>
  mutate(Rank = row_number(), Category = "Lowest Burden")

extreme_burden_table <- bind_rows(top_10_highest, top_10_lowest) |>
  select(Category,
         Rank,
         `Metropolitan Area` = NAME,
         `Monthly Rent` = monthly_rent,
         `Monthly Income` = monthly_household_income,
         `Rent/Income (%)` = raw_rent_burden,
         `Burden Index` = rent_burden_index,
         `Burden Score` = rent_burden_score) |>
  mutate(
    `Monthly Rent` = paste0("$", format(round(`Monthly Rent`), big.mark = ",")),
    `Monthly Income` = paste0("$", format(round(`Monthly Income`), big.mark = ",")),
    `Rent/Income (%)` = round(`Rent/Income (%)`, 1),
    `Burden Index` = round(`Burden Index`, 1),
    `Burden Score` = round(`Burden Score`, 1)
  ) |>
  arrange(desc(Category), Rank)

datatable(extreme_burden_table,
          caption = paste0("Table 2: Metro Areas with Highest and Lowest Rent Burden (", recent_year, ")"),
          options = list(pageLength = 20, scrollX = TRUE),
          rownames = FALSE) |>
  formatStyle('Category',
              target = 'row',
              backgroundColor = styleEqual(c('Highest Burden', 'Lowest Burden'),
                                           c('#ffcccc', '#ccffcc'))) |>
  formatStyle('Burden Score',
              fontWeight = 'bold',
              color = styleInterval(50, c('green', 'red')))
              
# 5. Housing Growth

library(dplyr)
library(DT)

# Join POPULATION and PERMITS tables
# Note: PERMITS uses CBSA, POPULATION uses GEOID (same values, different names)
housing_data <- POPULATION |>
  inner_join(PERMITS, by = c("GEOID" = "CBSA", "year")) |>
  arrange(GEOID, year)

# Step 1: Calculate 5-year rolling population growth
# For each CBSA, calculate population change from 5 years ago
housing_data <- housing_data |>
  group_by(GEOID) |>
  mutate(
    population_5yr_ago = lag(population, n = 5),
    population_growth_5yr = population - population_5yr_ago,
    population_growth_rate_5yr = (population_growth_5yr / population_5yr_ago) * 100
  ) |>
  ungroup()

# Filter to years where we have 5-year lookback (2014 onwards)
housing_data_with_growth <- housing_data |>
  filter(year >= 2014, !is.na(population_growth_5yr))

# METRIC 1: INSTANTANEOUS HOUSING GROWTH
# Measures: New housing units per 1,000 residents (permits relative to current population)
# STANDARDIZATION: Set 100 = national average in 2014 (first year with 5yr data)
# SCALING: Divide by baseline (times baseline)

instantaneous_baseline <- housing_data_with_growth |>
  filter(year == 2014) |>
  summarise(baseline = mean(new_housing_units_permitted / population * 1000, na.rm = TRUE)) |>
  pull(baseline)

housing_data_with_growth <- housing_data_with_growth |>
  mutate(
    permits_per_1000_pop = (new_housing_units_permitted / population) * 1000,
    instantaneous_index = (permits_per_1000_pop / instantaneous_baseline) * 100
  )

# METRIC 2: RATE-BASED HOUSING GROWTH
# Measures: Ratio of new permits to population growth (are we building faster than growing?)
# High values = permits exceed population growth (YIMBY-friendly)
# STANDARDIZATION: Set 100 = national average in 2014
# SCALING: Divide by baseline (times baseline)

# Calculate permits-to-growth ratio (how many units per new resident)
# Typical rule: need ~1 housing unit per 2.5-3 new residents
housing_data_with_growth <- housing_data_with_growth |>
  mutate(
    permits_per_new_resident = ifelse(population_growth_5yr > 0,
                                      new_housing_units_permitted / population_growth_5yr,
                                      NA_real_)
  )

rate_baseline <- housing_data_with_growth |>
  filter(year == 2014, !is.na(permits_per_new_resident)) |>
  summarise(baseline = mean(permits_per_new_resident, na.rm = TRUE)) |>
  pull(baseline)

housing_data_with_growth <- housing_data_with_growth |>
  mutate(
    rate_based_index = (permits_per_new_resident / rate_baseline) * 100
  )

# Table 1: Top/Bottom 10 CBSAs on INSTANTANEOUS metric (most recent year)
recent_year <- max(housing_data_with_growth$year)

instant_top10 <- housing_data_with_growth |>
  filter(year == recent_year) |>
  arrange(desc(instantaneous_index)) |>
  head(10) |>
  mutate(Rank = row_number(), Category = "Highest Growth")

instant_bottom10 <- housing_data_with_growth |>
  filter(year == recent_year) |>
  arrange(instantaneous_index) |>
  head(10) |>
  mutate(Rank = row_number(), Category = "Lowest Growth")

instant_table <- bind_rows(instant_top10, instant_bottom10) |>
  select(Category, Rank, `Metro Area` = NAME, Population = population,
         `Permits Issued` = new_housing_units_permitted,
         `Permits per 1,000` = permits_per_1000_pop,
         `Growth Index` = instantaneous_index) |>
  mutate(
    Population = format(Population, big.mark = ","),
    `Permits Issued` = format(`Permits Issued`, big.mark = ","),
    `Permits per 1,000` = round(`Permits per 1,000`, 2),
    `Growth Index` = round(`Growth Index`, 1)
  ) |>
  arrange(desc(Category), Rank)

datatable(instant_table,
          caption = paste0("Table 1: Instantaneous Housing Growth (", recent_year, ")"),
          options = list(pageLength = 20, scrollX = TRUE),
          rownames = FALSE) |>
  formatStyle('Category',
              target = 'row',
              backgroundColor = styleEqual(c('Highest Growth', 'Lowest Growth'),
                                           c('#ccffcc', '#ffcccc')))

# Table 2: Top/Bottom 10 CBSAs on RATE-BASED metric (most recent year)
rate_top10 <- housing_data_with_growth |>
  filter(year == recent_year, !is.na(rate_based_index)) |>
  arrange(desc(rate_based_index)) |>
  head(10) |>
  mutate(Rank = row_number(), Category = "Building Faster")

rate_bottom10 <- housing_data_with_growth |>
  filter(year == recent_year, !is.na(rate_based_index)) |>
  arrange(rate_based_index) |>
  head(10) |>
  mutate(Rank = row_number(), Category = "Building Slower")

rate_table <- bind_rows(rate_top10, rate_bottom10) |>
  select(Category, Rank, `Metro Area` = NAME,
         `5yr Pop Growth` = population_growth_5yr,
         `Permits Issued` = new_housing_units_permitted,
         `Permits per New Resident` = permits_per_new_resident,
         `Growth Index` = rate_based_index) |>
  mutate(
    `5yr Pop Growth` = format(`5yr Pop Growth`, big.mark = ","),
    `Permits Issued` = format(`Permits Issued`, big.mark = ","),
    `Permits per New Resident` = round(`Permits per New Resident`, 2),
    `Growth Index` = round(`Growth Index`, 1)
  ) |>
  arrange(desc(Category), Rank)

datatable(rate_table,
          caption = paste0("Table 2: Rate-Based Housing Growth vs Population Growth (", recent_year, ")"),
          options = list(pageLength = 20, scrollX = TRUE),
          rownames = FALSE) |>
  formatStyle('Category',
              target = 'row',
              backgroundColor = styleEqual(c('Building Faster', 'Building Slower'),
                                           c('#ccffcc', '#ffcccc')))

# COMPOSITE METRIC: Combine both metrics
# Using weighted average: 40% instantaneous + 60% rate-based
# (Rate-based is more important for YIMBY analysis - shows housing keeping up with growth)
# STANDARDIZATION: Composite score where 100 = average performance on both metrics
# SCALING: 0-100 linear scale for interpretability

housing_data_with_growth <- housing_data_with_growth |>
  mutate(
    composite_raw = (0.4 * instantaneous_index) + (0.6 * rate_based_index)
  )

# Scale composite to 0-100
composite_min <- min(housing_data_with_growth$composite_raw, na.rm = TRUE)
composite_max <- max(housing_data_with_growth$composite_raw, na.rm = TRUE)

housing_data_with_growth <- housing_data_with_growth |>
  mutate(
    yimby_score = ((composite_raw - composite_min) / (composite_max - composite_min)) * 100
  )

# Top/Bottom 10 CBSAs on COMPOSITE YIMBY SCORE
composite_top10 <- housing_data_with_growth |>
  filter(year == recent_year, !is.na(yimby_score)) |>
  arrange(desc(yimby_score)) |>
  head(10) |>
  mutate(Rank = row_number(), Category = "Most YIMBY")

composite_bottom10 <- housing_data_with_growth |>
  filter(year == recent_year, !is.na(yimby_score)) |>
  arrange(yimby_score) |>
  head(10) |>
  mutate(Rank = row_number(), Category = "Least YIMBY")

composite_table <- bind_rows(composite_top10, composite_bottom10) |>
  select(Category, Rank, `Metro Area` = NAME,
         `Instantaneous Index` = instantaneous_index,
         `Rate-Based Index` = rate_based_index,
         `YIMBY Score` = yimby_score) |>
  mutate(
    `Instantaneous Index` = round(`Instantaneous Index`, 1),
    `Rate-Based Index` = round(`Rate-Based Index`, 1),
    `YIMBY Score` = round(`YIMBY Score`, 1)
  ) |>
  arrange(desc(Category), Rank)

datatable(composite_table,
          caption = paste0("Table 3: Composite YIMBY Score (", recent_year, ")"),
          options = list(pageLength = 20, scrollX = TRUE),
          rownames = FALSE) |>
  formatStyle('Category',
              target = 'row',
              backgroundColor = styleEqual(c('Most YIMBY', 'Least YIMBY'),
                                           c('#ccffcc', '#ffcccc'))) |>
  formatStyle('YIMBY Score',
              fontWeight = 'bold',
              background = styleColorBar(c(0, 100), '#87CEEB'),
              backgroundSize = '98% 88%',
              backgroundRepeat = 'no-repeat',
              backgroundPosition = 'center')


# 6. Vizualization

library(dplyr)
library(ggplot2)
library(DT)

# Combine rent burden and housing growth data
yimby_analysis <- housing_data_with_growth |>
  left_join(rent_burden_data |> select(GEOID, year, rent_burden_index), 
            by = c("GEOID", "year"))

# Calculate key metrics for YIMBY identification
recent_year <- max(yimby_analysis$year, na.rm = TRUE)

yimby_metrics <- yimby_analysis |>
  group_by(GEOID, NAME) |>
  summarise(
    # Criterion 1: High early rent burden
    early_rent_burden = rent_burden_index[year == 2014][1],
    recent_rent_burden = rent_burden_index[year == recent_year][1],
    
    # Criterion 2: Decrease in rent burden
    rent_burden_change = recent_rent_burden - early_rent_burden,
    
    # Criterion 3: Population growth
    population_2014 = population[year == 2014][1],
    population_recent = population[year == recent_year][1],
    population_growth_pct = ((population_recent - population_2014) / population_2014) * 100,
    
    # Criterion 4: Above-average housing growth
    avg_yimby_score = mean(yimby_score, na.rm = TRUE),
    
    .groups = "drop"
  ) |>
  filter(!is.na(early_rent_burden) & !is.na(recent_rent_burden) & 
           !is.na(population_growth_pct) & !is.na(avg_yimby_score))

# Define YIMBY success criteria (using medians for realistic thresholds)
yimby_metrics <- yimby_metrics |>
  mutate(
    criterion_1 = early_rent_burden > median(early_rent_burden, na.rm = TRUE),
    criterion_2 = rent_burden_change < 0,
    criterion_3 = population_growth_pct > 0,
    criterion_4 = avg_yimby_score > median(avg_yimby_score, na.rm = TRUE),
    
    yimby_success = criterion_1 & criterion_2 & criterion_3 & criterion_4,
    criteria_met = criterion_1 + criterion_2 + criterion_3 + criterion_4
  )

# VISUALIZATION 1: Rent Burden Change vs Housing Growth
viz1_data <- yimby_metrics |>
  mutate(category = case_when(
    yimby_success ~ "YIMBY Success (All 4 Criteria)",
    criteria_met == 3 ~ "3 of 4 Criteria",
    TRUE ~ "Other"
  ))

ggplot(viz1_data, aes(x = avg_yimby_score, y = rent_burden_change, 
                      color = category, size = population_recent)) +
  geom_hline(yintercept = 0, linetype = "dashed", color = "gray50", linewidth = 0.8) +
  geom_vline(xintercept = median(viz1_data$avg_yimby_score, na.rm = TRUE), 
             linetype = "dashed", color = "gray50", linewidth = 0.8) +
  geom_point(alpha = 0.6) +
  scale_color_manual(values = c("YIMBY Success (All 4 Criteria)" = "#00AA00",
                                "3 of 4 Criteria" = "#FFA500",
                                "Other" = "gray60")) +
  scale_size_continuous(range = c(1, 8), labels = scales::comma) +
  labs(
    title = "Housing Growth vs. Change in Rent Burden",
    subtitle = "YIMBY Success = Above-median early rent + Falling rents + Population growth + Above-median housing",
    x = "Average YIMBY Score (0-100)",
    y = "Change in Rent Burden Index (2014 to 2023)",
    color = "YIMBY Status",
    size = "Population",
    caption = "Top-right quadrant: declining rent burden with high housing growth"
  ) +
  theme_minimal(base_size = 12) +
  theme(
    plot.title = element_text(face = "bold", size = 16, margin = margin(b = 5)),
    plot.subtitle = element_text(size = 11, color = "gray40", margin = margin(b = 15)),
    axis.title = element_text(size = 12, face = "bold"),
    legend.position = "bottom",
    legend.box = "vertical",
    panel.grid.minor = element_blank()
  ) +
  guides(size = guide_legend(order = 2),
         color = guide_legend(order = 1, override.aes = list(size = 4)))

# VISUALIZATION 2: Population Growth vs Early Rent Burden
ggplot(viz1_data, aes(x = early_rent_burden, y = population_growth_pct, 
                      color = avg_yimby_score, shape = yimby_success)) +
  geom_hline(yintercept = 0, linetype = "dashed", color = "gray50", linewidth = 0.8) +
  geom_vline(xintercept = median(viz1_data$early_rent_burden, na.rm = TRUE), 
             linetype = "dashed", color = "gray50", linewidth = 0.8) +
  geom_point(size = 3, alpha = 0.7) +
  scale_color_gradient2(low = "#d73027", mid = "#ffffbf", high = "#1a9850",
                        midpoint = 50, name = "Avg YIMBY\nScore") +
  scale_shape_manual(values = c("TRUE" = 17, "FALSE" = 16),
                     labels = c("TRUE" = "YIMBY Success", "FALSE" = "Other"),
                     name = "") +
  labs(
    title = "Population Growth in Higher-Rent Cities",
    subtitle = "Green triangles = Cities that succeeded with all 4 YIMBY criteria",
    x = "Early Rent Burden Index (2014)",
    y = "Population Growth 2014-2023 (%)",
    caption = "Top-right quadrant: expensive cities that grew (not declining)"
  ) +
  theme_minimal(base_size = 12) +
  theme(
    plot.title = element_text(face = "bold", size = 16, margin = margin(b = 5)),
    plot.subtitle = element_text(size = 11, color = "gray40", margin = margin(b = 15)),
    axis.title = element_text(size = 12, face = "bold"),
    legend.position = "right",
    panel.grid.minor = element_blank()
  )

# Table 1: Top YIMBY Success Stories
yimby_success_table <- yimby_metrics |>
  filter(yimby_success) |>
  arrange(desc(avg_yimby_score)) |>
  head(20) |>
  select(`Metro Area` = NAME,
         `Early Rent Burden` = early_rent_burden,
         `Rent Change` = rent_burden_change,
         `Pop Growth (%)` = population_growth_pct,
         `YIMBY Score` = avg_yimby_score) |>
  mutate(
    `Early Rent Burden` = round(`Early Rent Burden`, 1),
    `Rent Change` = round(`Rent Change`, 1),
    `Pop Growth (%)` = round(`Pop Growth (%)`, 1),
    `YIMBY Score` = round(`YIMBY Score`, 1)
  )

datatable(yimby_success_table,
          caption = "Top YIMBY Success Stories: Cities Meeting All 4 Criteria",
          options = list(pageLength = 20, scrollX = TRUE),
          rownames = FALSE) |>
  formatStyle('YIMBY Score',
              fontWeight = 'bold',
              background = styleColorBar(c(0, 100), '#00AA00'),
              backgroundSize = '98% 88%',
              backgroundRepeat = 'no-repeat',
              backgroundPosition = 'center') |>
  formatStyle('Rent Change',
              color = styleInterval(0, c('green', 'red')))

# Table 2: Strong YIMBY Performers
strong_performers_table <- yimby_metrics |>
  filter(criteria_met == 3) |>
  arrange(desc(avg_yimby_score)) |>
  head(20) |>
  select(`Metro Area` = NAME,
         `Early Rent` = early_rent_burden,
         `Rent Change` = rent_burden_change,
         `Pop Growth (%)` = population_growth_pct,
         `YIMBY Score` = avg_yimby_score,
         `Criteria Met` = criteria_met) |>
  mutate(
    `Early Rent` = round(`Early Rent`, 1),
    `Rent Change` = round(`Rent Change`, 1),
    `Pop Growth (%)` = round(`Pop Growth (%)`, 1),
    `YIMBY Score` = round(`YIMBY Score`, 1)
  )

datatable(strong_performers_table,
          caption = "Strong YIMBY Performers: Cities Meeting 3 of 4 Criteria",
          options = list(pageLength = 20, scrollX = TRUE),
          rownames = FALSE) |>
  formatStyle('YIMBY Score',
              background = styleColorBar(c(0, 100), '#FFA500'),
              backgroundSize = '98% 88%',
              backgroundRepeat = 'no-repeat',
              backgroundPosition = 'center')



# compute_permits_population_growth.R
# Minimal, readable pipeline using |> and no explicit is.na checks.
# Expects POPULATION(GEOID, NAME, year, population) and PERMITS(GEOID, NAME, year, permits_total).

library(dplyr)
library(DT)

growth_metrics <- POPULATION |>
  inner_join(PERMITS, by = c("GEOID", "NAME", "year")) |>
  mutate(
    year = as.integer(year),
    population = as.numeric(population),
    permits_total = as.numeric(permits_total)
  ) |>
  filter(population > 0, permits_total >= 0) |>
  arrange(GEOID, NAME, year) |>
  group_by(GEOID, NAME) |>
  mutate(
    pop_lag5 = lag(population, 5),
    pop_change_5yr = population - pop_lag5,
    avg_annual_pop_change = pop_change_5yr / 5
  ) |>
  ungroup() |>
  mutate(
    # instantaneous measure: permits per 1,000 residents in the year
    permits_per_1k = permits_total / population * 1000,
    # rate-based measure: permits this year divided by avg annual pop growth over prior 5 years
    permits_per_avg_annual_growth = case_when(
      avg_annual_pop_change > 0 ~ permits_total / avg_annual_pop_change,
      TRUE ~ NA_real_
    )
  )

# Baselines: national means (instantaneous baseline = 2009, rate baseline = 2014)
baseline_inst <- growth_metrics |>
  filter(year == 2009) |>
  summarise(b = mean(permits_per_1k, na.rm = TRUE)) |>
  pull(b)

baseline_rate <- growth_metrics |>
  filter(year == 2014) |>
  summarise(b = mean(permits_per_avg_annual_growth, na.rm = TRUE)) |>
  pull(b)

# Indices scaled so baseline national mean = 100; use case_when to avoid explicit is.na checks
growth_metrics <- growth_metrics |>
  mutate(
    instantaneous_index = case_when(
      is.finite(baseline_inst) & baseline_inst > 0 ~ permits_per_1k / baseline_inst * 100,
      TRUE ~ NA_real_
    ),
    rate_index = case_when(
      is.finite(baseline_rate) & baseline_rate > 0 ~ permits_per_avg_annual_growth / baseline_rate * 100,
      TRUE ~ NA_real_
    ),
    # composite: average of available indices (prefer both when present)
    composite_index = coalesce(
      (instantaneous_index + rate_index) / 2,
      instantaneous_index,
      rate_index
    )
  ) |>
  group_by(year) |>
  mutate(
    inst_rank = dense_rank(desc(instantaneous_index)),
    rate_rank = dense_rank(desc(rate_index)),
    comp_rank = dense_rank(desc(composite_index))
  ) |>
  ungroup()

# Recent year top/bottom lists (most recent year in data)
recent_year <- max(growth_metrics$year, na.rm = TRUE)

inst_top <- growth_metrics |>
  filter(year == recent_year) |>
  arrange(desc(instantaneous_index)) |>
  slice_head(n = 10) |>
  transmute(
    Category = "Highest",
    Rank = row_number(),
    `Metropolitan Area` = NAME,
    GEOID,
    population = population,
    permits = permits_total,
    permits_per_1k = round(permits_per_1k, 2),
    instantaneous_index = round(instantaneous_index, 1)
  )

inst_low <- growth_metrics |>
  filter(year == recent_year) |>
  arrange(instantaneous_index) |>
  slice_head(n = 10) |>
  transmute(
    Category = "Lowest",
    Rank = row_number(),
    `Metropolitan Area` = NAME,
    GEOID,
    population = population,
    permits = permits_total,
    permits_per_1k = round(permits_per_1k, 2),
    instantaneous_index = round(instantaneous_index, 1)
  )

rate_top <- growth_metrics |>
  filter(year == recent_year) |>
  arrange(desc(rate_index)) |>
  slice_head(n = 10) |>
  transmute(
    Category = "Highest",
    Rank = row_number(),
    `Metropolitan Area` = NAME,
    GEOID,
    avg_annual_pop_change = round(avg_annual_pop_change, 2),
    permits = permits_total,
    permits_per_avg_annual_growth = round(permits_per_avg_annual_growth, 2),
    rate_index = round(rate_index, 1)
  )

rate_low <- growth_metrics |>
  filter(year == recent_year) |>
  arrange(rate_index) |>
  slice_head(n = 10) |>
  transmute(
    Category = "Lowest",
    Rank = row_number(),
    `Metropolitan Area` = NAME,
    GEOID,
    avg_annual_pop_change = round(avg_annual_pop_change, 2),
    permits = permits_total,
    permits_per_avg_annual_growth = round(permits_per_avg_annual_growth, 2),
    rate_index = round(rate_index, 1)
  )

comp_top <- growth_metrics |>
  filter(year == recent_year) |>
  arrange(desc(composite_index)) |>
  slice_head(n = 10) |>
  transmute(
    Category = "Highest",
    Rank = row_number(),
    `Metropolitan Area` = NAME,
    GEOID,
    instantaneous_index = round(instantaneous_index, 1),
    rate_index = round(rate_index, 1),
    composite_index = round(composite_index, 1)
  )

comp_low <- growth_metrics |>
  filter(year == recent_year) |>
  arrange(composite_index) |>
  slice_head(n = 10) |>
  transmute(
    Category = "Lowest",
    Rank = row_number(),
    `Metropolitan Area` = NAME,
    GEOID,
    instantaneous_index = round(instantaneous_index, 1),
    rate_index = round(rate_index, 1),
    composite_index = round(composite_index, 1)
  )

inst_table <- bind_rows(inst_top, inst_low)
rate_table <- bind_rows(rate_top, rate_low)
comp_table <- bind_rows(comp_top, comp_low)

# Optional: view interactive tables in RMarkdown / Shiny
datatable(inst_table, caption = paste0("Instantaneous index top/bottom (", recent_year, ")"), rownames = FALSE, options = list(dom = 't'))
datatable(rate_table, caption = paste0("Rate-based index top/bottom (", recent_year, ")"), rownames = FALSE, options = list(dom = 't'))
datatable(comp_table, caption = paste0("Composite index top/bottom (", recent_year, ")"), rownames = FALSE, options = list(dom = 't'))