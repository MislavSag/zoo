library(finutils)
library(fastverse)
library(finfeatures)
library(janitor)


# Globals
PATH_PRICES = "/home/sn/data/strategies/zoo"
if (!dir.exists(PATH_PRICES)) dir.create(PATH_PRICES)

# Import daily data
prices = qc_daily(
  file_path   = "/home/sn/lean/data/stocks_daily.csv",
  min_obs     = 1001,
  duplicates  = "fast",
  add_dv_rank = TRUE
)

# Add month column and EOM indecies
prices[, month := yearmon(date)]
prices[, pbs_i := date == data.table::last(date), by = .(symbol, month)]
prices[pbs_i == 1, .N] / nrow(prices) * 100

# Create target variables
targets = prices[, .(open = data.table::first(open), close = data.table::last(close)), by = .(symbol, month)] |>
  _[order(symbol, month)] |>
  _[, let(
    target_m = shift(close, 1, type = "lead") / shift(open, 1, type = "lead") - 1,
    target_q = shift(close, 3, type = "lead") / shift(open, 1, type = "lead") - 1,
    target_h = shift(close, 6, type = "lead") / shift(open, 1, type = "lead") - 1,
    target_y = shift(close, 12, type = "lead") / shift(open, 1, type = "lead") - 1
  ), by = symbol]
prices = targets[, .SD, .SDcols = -c("open", "close")][prices, on = c("symbol", "month")]
prices[pbs_i == FALSE, let(
  target_m = NA, target_q = NA, target_h = NA, target_y = NA
)]
prices[pbs_i == TRUE]

# Save every symbol separately
prices_dir = file.path(PATH_PRICES, "prices_zoo_month")
if (!dir.exists(prices_dir)) {
  dir.create(prices_dir)
}
for (s in prices[, unique(symbol)]) {
  if (s == "prn") next()
  prices_ = prices[symbol == s]
  if (nrow(prices_) == 0) next
  file_name = file.path(prices_dir, paste0(s, ".csv"))
  fwrite(prices_, file_name)
}

# Create sh file for predictors
cont = sprintf(
  "#!/bin/bash

#PBS -N pead_predictions
#PBS -l ncpus=1
#PBS -l mem=4GB
#PBS -J 1-%d
#PBS -o logs
#PBS -j e

cd ${PBS_O_WORKDIR}

apptainer run image_predictors.sif predictors_padobran_zoo_month.R",
  length(list.files(prices_dir)))
writeLines(cont, "predictors_padobran_zoo_month.sh")

# Add to padobran
# scp -r /home/sn/data/strategies/pead/prices padobran:/home/jmaric/peadml/prices
