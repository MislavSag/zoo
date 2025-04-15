library(finutils)
library(fastverse)
library(finfeatures)
library(janitor)


# Globals
PATH_PRICES = "D:/strategies/zoo"

# Import daily data
prices = qc_daily(
  file_path   = "F:/lean/data/stocks_daily.csv",
  min_obs     = 1001,
  duplicates  = "fast",
  add_dv_rank = TRUE
)

# Add month column and EOM indecies
prices[, month := yearmon(date)]
prices[, pbs_i := date == data.table::last(date), by = .(symbol, month)]
prices[pbs_i == 1, .N] / nrow(prices) * 100

# Save every symbol separately
prices_dir = file.path(PATH_PRICES, "prices")
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
#PBS -j oe

cd ${PBS_O_WORKDIR}

apptainer run image_predictors.sif predictors_padobran.R",
  length(list.files(prices_dir)))
writeLines(cont, "predictors_padobran.sh")

# Add to padobran
# scp -r /home/sn/data/strategies/pead/prices padobran:/home/jmaric/peadml/prices
