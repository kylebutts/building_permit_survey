# %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
library(tidyverse)
library(here)
library(fs)
library(arrow)

# %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
# County Monthly (2000 -- 2025) ----
# "These are the County totals for all counties in which permit offices are requested to report"
county_download_dir <- here("data/raw/bps/county_monthly")
counties_files <- tibble(
  date = seq(ymd("2000-01-01"), ymd("2025-02-01"), by = "month")
) |>
  mutate(
    year = year(date),
    month = month(date),
    file = here(
      county_download_dir,
      sprintf("co%02d%02dc.txt", year %% 100, month)
    )
  )

county_col_names <- c(
  'survey_date',
  'fips_state',
  'fips_county',
  'region_code',
  'division_code',
  'name',
  # Imputed values
  'bps_buildings_1',
  'bps_units_1',
  'bps_value_1',
  'bps_buildings_2',
  'bps_units_2',
  'bps_value_2',
  'bps_buildings_3_4',
  'bps_units_3_4',
  'bps_value_3_4',
  'bps_buildings_5_more',
  'bps_units_5_more',
  'bps_value_5_more',
  # Reported values
  'bps_buildings_reported_1',
  'bps_units_reported_1',
  'bps_value_reported_1',
  'bps_buildings_reported_2',
  'bps_units_reported_2',
  'bps_value_reported_2',
  'bps_buildings_reported_3_4',
  'bps_units_reported_3_4',
  'bps_value_reported_3_4',
  'bps_buildings_reported_5_more',
  'bps_units_reported_5_more',
  'bps_value_reported_5_more'
)

bps_county <- list_rbind(map(counties_files$file, function(file) {
  raw <- read_csv(
    file,
    skip = 2,
    show_col_types = F,
    col_names = county_col_names
  )

  clean <- raw |>
    mutate(
      fips = paste0(fips_state, fips_county),
      year = as.numeric(str_sub(survey_date, start = 1L, end = 4L)),
      month = as.numeric(str_sub(survey_date, start = 5L, end = 6L)),
      .keep = "unused",
      .before = 1
    )

  return(clean)
}))

bps_county <- bps_county |>
  mutate(
    region_code = case_when(
      region_code == 1 ~ "Northeast",
      region_code == 2 ~ "Midwest",
      region_code == 3 ~ "South",
      region_code == 4 ~ "West"
    ),
    division_code = case_when(
      division_code == 1 ~ "New England",
      division_code == 2 ~ "Middle Atlantic",
      division_code == 3 ~ "East North Central",
      division_code == 4 ~ "West North Central",
      division_code == 5 ~ "South Atlantic",
      division_code == 6 ~ "East South Central",
      division_code == 7 ~ "West South Central",
      division_code == 8 ~ "Mountain",
      division_code == 9 ~ "Pacific"
    )
  ) |>
  select(-name)

bps_county_quarterly <- bps_county |>
  mutate(quarter = ceiling(month / 3)) |>
  summarize(
    .by = c(fips, year, quarter), 
    division_code = first(division_code),
    region_code = first(region_code),
    across(starts_with("bps"), sum)
  )

bps_annual_from_monthly_counties <- bps_county |>
    summarize(
      .by = c(fips, year), 
      division_code = first(division_code),
      region_code = first(region_code),
      across(starts_with("bps"), sum)
    )



arrow::write_parquet(
  bps_county,
  here("data/base/bps/bps_county_monthly.parquet")
)
arrow::write_parquet(
  bps_county_quarterly,
  here("data/base/bps/bps_county_quarterly.parquet")
)
arrow::write_parquet(
  bps_annual_from_monthly_counties,
  here("data/base/bps/bps_annual_from_monthly_counties.parquet")
)

# %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
# County Annual (1990 -- 2025) ----
annual_files <- fs::dir_ls(here("data/raw/bps/county_annual"), regexp = "a.txt")
county_col_names <- c(
  'survey_date',
  'fips_state',
  'fips_county',
  'region_code',
  'division_code',
  'name',
  # Imputed values
  'bps_buildings_1',
  'bps_units_1',
  'bps_value_1',
  'bps_buildings_2',
  'bps_units_2',
  'bps_value_2',
  'bps_buildings_3_4',
  'bps_units_3_4',
  'bps_value_3_4',
  'bps_buildings_5_more',
  'bps_units_5_more',
  'bps_value_5_more',
  # Reported values
  'bps_buildings_reported_1',
  'bps_units_reported_1',
  'bps_value_reported_1',
  'bps_buildings_reported_2',
  'bps_units_reported_2',
  'bps_value_reported_2',
  'bps_buildings_reported_3_4',
  'bps_units_reported_3_4',
  'bps_value_reported_3_4',
  'bps_buildings_reported_5_more',
  'bps_units_reported_5_more',
  'bps_value_reported_5_more'
)

bps_county_annual <- list_rbind(map(annual_files, function(file) {
  raw <- read_csv(
    file,
    skip = 2,
    show_col_types = F,
    ,
    col_types = cols(region_code = "c", division_code = "c"),
    col_names = county_col_names
  )

  year <- as.numeric(str_extract(file, "(\\d{4})a.txt$", group = 1))

  clean <- raw |>
    mutate(
      fips = paste0(fips_state, fips_county),
      year = .env$year,
      .keep = "unused",
      .before = 1
    )

  return(clean)
}))

bps_county_annual <- bps_county_annual |>
  mutate(
    region_code = case_when(
      region_code == 1 ~ "Northeast",
      region_code == 2 ~ "Midwest",
      region_code == 3 ~ "South",
      region_code == 4 ~ "West"
    ),
    division_code = case_when(
      division_code == 1 ~ "New England",
      division_code == 2 ~ "Middle Atlantic",
      division_code == 3 ~ "East North Central",
      division_code == 4 ~ "West North Central",
      division_code == 5 ~ "South Atlantic",
      division_code == 6 ~ "East South Central",
      division_code == 7 ~ "West South Central",
      division_code == 8 ~ "Mountain",
      division_code == 9 ~ "Pacific"
    )
  ) |>
  select(-name)

bps_county_annual |> glimpse()

arrow::write_parquet(
  bps_county_annual,
  here("data/base/bps/bps_county_annual.parquet")
)


# %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
# MSA Monthly (1988 -- 2023) ----
msa_download_dir <- here("data/raw/bps/msa_monthly")
msa_files <- tibble(
  date = seq(ymd("1988-01-01"), ymd("2023-12-01"), by = "month")
) |>
  mutate(
    year = year(date),
    month = month(date),
    # https://www2.census.gov/econ/bps/Metro%20(ending%202023)/ma8801c.txt
    file = here(
      msa_download_dir,
      sprintf("ma%02d%02dc.txt", year %% 100, month)
    )
  )

msa_col_names <- c(
  'survey_date',
  'csa_code',
  'cbsa_code',
  'alpha_code', # Alpha Code - Header Coverage Code - Blank or “C”, if CSA/CBSA is completely covered by monthly permit issuing places
  'alpha_name', # Alpha Geographic (CBSA) Name
  # Imputed values
  'bps_buildings_1',
  'bps_units_1',
  'bps_value_1',
  'bps_buildings_2',
  'bps_units_2',
  'bps_value_2',
  'bps_buildings_3_4',
  'bps_units_3_4',
  'bps_value_3_4',
  'bps_buildings_5_more',
  'bps_units_5_more',
  'bps_value_5_more',
  # Reported values
  'bps_buildings_reported_1',
  'bps_units_reported_1',
  'bps_value_reported_1',
  'bps_buildings_reported_2',
  'bps_units_reported_2',
  'bps_value_reported_2',
  'bps_buildings_reported_3_4',
  'bps_units_reported_3_4',
  'bps_value_reported_3_4',
  'bps_buildings_reported_5_more',
  'bps_units_reported_5_more',
  'bps_value_reported_5_more'
)

# TODO:
bps_msa <- list_rbind(map(msa_files$file, function(file) {
  raw <- read_csv(
    file,
    skip = 2,
    show_col_types = F,
    col_names = msa_col_names
  )

  clean <- raw |>
    mutate(
      # Works with 8801 and 200001
      year = as.numeric(str_sub(survey_date, start = 1L, end = -3L)),
      year = if_else(year <= 99, year + 1900, year),
      month = as.numeric(str_sub(survey_date, start = -2L, end = -1L)),
      .keep = "unused",
      .before = 1
    ) |>
    select(csa_code, year, month, everything())

  if (is.numeric(clean$csa_code)) {
    clean$csa_code <- sprintf("%04d", clean$csa_code)
  }
  if (is.numeric(clean$cbsa_code)) {
    clean$cbsa_code <- sprintf("%05d", clean$cbsa_code)
  }

  return(clean)
}))

bps_msa

arrow::write_parquet(
  bps_msa,
  here("data/base/bps/bps_county_monthly.parquet")
)
