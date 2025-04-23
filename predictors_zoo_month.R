options(progress_enabled = FALSE)

# remotes::install_github("MislavSag/finfeatures")
library(data.table)
library(fs)
library(finfeatures)
library(glue)
library(reticulate)
library(janitor)
library(arrow)
library(AzureStor)
library(qlcal)


# SETUP -------------------------------------------------------------------
# Paths
PATH                  = "/home/sn/data/strategies/zoo"
PATH_ROLLING          = path(PATH, "predictors")
PATH_ROLLING_PADOBRAN = path(PATH, "predictors_zoo_month")
PATH_PREDICTORS       = "/home/sn/data/equity/us/predictors_daily/zoo_predictors/"

# Create directories
if (!dir_exists(PATH_ROLLING)) dir_create(PATH_ROLLING)

# Python environment
# reticulate::use_python("/home/sn/quant/bin/python3", required = TRUE)
# builtins = import_builtins(convert = FALSE)
# main = import_main(convert = FALSE)
# tsfel = reticulate::import("tsfel", convert = FALSE)
# warnigns = reticulate::import("warnings", convert = FALSE)
# warnigns$filterwarnings('ignore')

# Utils
# Move this to finfeatures
clean_col_names = function(names) {
  names = gsub(" |-|\\.|\"", "_", names)
  names = gsub("_{2,}", "_", names)
  names
} 

# PRICES AND EVENTS -------------------------------------------------------
# Get data
ohlcv = lapply(list.files(file.path(PATH, "prices_zoo_month"), full.names = TRUE), fread)
ohlcv = rbindlist(ohlcv, fill = TRUE)
ohlcv = Ohlcv$new(ohlcv[, .(symbol, date, open, high, low, close, volume, pbs_i)], 
                  date_col = "date")


# ROLLING PREDICTORS ------------------------------------------------------
# Command to get data from padobran. This is necessary only first time
# scp -r padobran:/home/jmaric/zoo/predictors_zoo_month/ /home/sn/data/strategies/zoo/

# Clean padobran data
fnames = unique(gsub("-.*", "", path_file(dir_ls(PATH_ROLLING_PADOBRAN))))
fnames_exist = path_ext_remove(path_file(dir_ls(PATH_ROLLING)))  
fnames_miss = setdiff(fnames, fnames_exist)
if (length(fnames_miss) > 0) {
  lapply(fnames_miss, function(x) {
    print(x)
    # x = "ohlcv"
    dt_ = lapply(dir_ls(PATH_ROLLING_PADOBRAN, regexp = x), fread)
    if (x %in% c("theftpy", "theftpyr")) {
      # dt_[[1]][, 1:10]
      dt_ = lapply(dt_, function(d) {
        cols_remove = which(duplicated(colnames(d)))
        d[, .SD, .SDcols = -cols_remove]
      })
      dt_ = lapply(dt_, function(y) {
        cols = colnames(y)[3:ncol(y)]
        y[, (cols) := lapply(.SD, as.numeric), .SDcols = cols]
        y
      })
      
    }
    dt_ = rbindlist(dt_, fill = TRUE)
    # dt_[, 1:10]
    fwrite(dt_, path(PATH_ROLLING, paste0(x, ".csv")))
  })
}

# Parameters
workers = 2L
windows = c(66, 252) # day and 2H;  cca 10 days

# Define at parameter
ohlcv_max_date = ohlcv$X[, max(date)]
get_at = function(n = "exuber.csv", remove_old = TRUE) {
  # n = "backcusum.csv"
  if (is.null(n)) {
    new_dataset = dataset[, .(symbol, date = as.IDate(date))]
  } else {
    predictors_ = fread(path(PATH_ROLLING, n))
    new_dataset = fsetdiff(dataset[, .(symbol, date = as.IDate(date))],
                           predictors_[, .(symbol, date)])
  }
  new_data = merge(
    ohlcv$X[, .(symbol, date, date_ohlcv = date)],
    new_dataset[, .(symbol, date, date_event = date, index = TRUE)],
    by = c("symbol", "date"),
    all.x = TRUE,
    all.y = FALSE
  )
  new_data[, n := 1:.N]
  new_data = new_data[index == TRUE, .(symbol, date_ohlcv, date_event, n)]
  new_data[, lags := -1L]
  
  # Skip everything before padobran
  if (remove_old) {
    new_data = new_data[date_ohlcv > as.Date("2024-12-01")]
  }
  return(new_data)
}

# Exuber
n_ = "exuber.csv"
meta = get_at(n_)
windows_ = c(windows, 504)
if (meta[, any(lags == -1L)]) {
  predictors_init = RollingExuber$new(
    windows = windows_,
    workers = if (nrow(meta) < 10) 1L else workers,
    at = meta[lags == -1L, n],
    lag = -1L,
    exuber_lag = 1L
  )
  new = predictors_init$get_rolling_features(ohlcv, TRUE)
  path_ = path(PATH_ROLLING, n_)
  old = fread(path(PATH_ROLLING, n_))
  new = rbind(old, new)
  new = unique(new, by = c("symbol", "date"))
  fwrite(new, path_)
}
if (meta[, any(lags == 0L)]) {
  predictors_init = RollingExuber$new(
    windows = windows_,
    workers = if (nrow(meta) < 10) 1L else workers,
    at = meta[lags == 0L, n],
    lag = 0L,
    exuber_lag = 1L
  )
  new = predictors_init$get_rolling_features(ohlcv, TRUE)
  path_ = path(PATH_ROLLING, glue("live_{n_}"))
  fwrite(new, path_)
}

# Backcusum
n_ = "backcusum.csv"
meta = get_at(n_)
windows_ = c(windows, 504)
if (meta[, any(lags == -1L)]) {
  predictors_init = RollingBackcusum$new(
    windows = windows_,
    workers = if (nrow(meta) < 10) 1L else workers,
    at = meta[lags == -1L, n],
    lag = -1L,
    alternative = c("greater", "two.sided"),
    return_power = c(1, 2)
  )
  new = predictors_init$get_rolling_features(ohlcv)
  path_ = path(PATH_ROLLING, n_)
  old = fread(path(PATH_ROLLING, n_))
  new = rbind(old, new)
  new = unique(new, by = c("symbol", "date"))
  fwrite(new, path_)
}
if (meta[, any(lags == 0L)]) {
  predictors_init = RollingBackcusum$new(
    windows = windows_,
    workers = if (nrow(meta) < 10) 1L else workers,
    at = meta[lags == 0L, n],
    lag = 0L,
    alternative = c("greater", "two.sided"),
    return_power = c(1, 2)
  )
  new = predictors_init$get_rolling_features(ohlcv)
  path_ = path(PATH_ROLLING, glue("live_{n_}"))
  fwrite(new, path_)
}

# Forecasts
n_ = "forecasts.csv"
meta = get_at(n_)
windows_ = c(252, 252 * 2)
if (meta[, any(lags == -1L)]) {
  predictors_init = RollingForecats$new(
    windows = windows_,
    workers = workers,
    at = meta[lags == -1L, n],
    lag = -1L,
    forecast_type = c("autoarima", "nnetar", "ets"),
    h = 22
  )
  new = predictors_init$get_rolling_features(ohlcv)
  path_ = path(PATH_ROLLING, n_)
  old = fread(path(PATH_ROLLING, n_))
  new = rbind(old, new)
  new = unique(new, by = c("symbol", "date"))
  fwrite(new, path_)
}
if (meta[, any(lags == 0L)]) {
  predictors_init = RollingForecats$new(
    windows = windows_,
    workers = if (nrow(meta) < 10) 1L else workers,
    at = meta[lags == 0L, n],
    lag = 0L,
    forecast_type = c("autoarima", "nnetar", "ets"),
    h = 22
  )
  new = predictors_init$get_rolling_features(ohlcv)
  path_ = path(PATH_ROLLING, glue("live_{n_}"))
  fwrite(new, path_)
}

# Theft r
n_ = "theftr.csv"
meta = get_at(n_)
windows_ = c(5, 22, windows)
if (meta[, any(lags == -1L)]) {
  predictors_init = RollingTheft$new(
    windows = windows_,
    workers = workers,
    at = meta[lags == -1L, n],
    lag = -1L,
    features_set = c("catch22", "feasts")
  )
  new = predictors_init$get_rolling_features(ohlcv)
  path_ = path(PATH_ROLLING, n_)
  old = fread(path(PATH_ROLLING, n_))
  new = rbind(old, new, fill = TRUE)
  new[, c("feasts____22_5", "feasts____25_22") := NULL]
  new = unique(new, by = c("symbol", "date"))
  fwrite(new, path_)
}
if (meta[, any(lags == 0L)]) {
  predictors_init = RollingTheft$new(
    windows = windows_,
    workers = if (nrow(meta) < 10) 1L else workers,
    at = meta[lags == 0L, n],
    lag = 0L,
    features_set = c("catch22", "feasts")
  )
  new = predictors_init$get_rolling_features(ohlcv)
  path_ = path(PATH_ROLLING, glue("live_{n_}"))
  fwrite(new, path_)
}

# Theft r with returns
n_ = "theftrr.csv"
meta = get_at(n_)
windows_ = c(5, 22, windows)
if (meta[, any(lags == -1L)]) {
  predictors_init = RollingTheft$new(
    windows = windows_,
    workers = workers,
    at = meta[lags == -1L, n],
    lag = -1L,
    features_set = c("catch22", "feasts")
  )
  new = predictors_init$get_rolling_features(ohlcv)
  path_ = path(PATH_ROLLING, n_)
  old = fread(path(PATH_ROLLING, n_))
  new = rbind(old, new, fill = TRUE)
  new[, c("feasts____22_5", "feasts____25_22") := NULL]
  new = unique(new, by = c("symbol", "date"))
  fwrite(new, path_)
}
if (meta[, any(lags == 0L)]) {
  predictors_init = RollingTheft$new(
    windows = windows_,
    workers = workers,
    at = meta[lags == 0L, n],
    lag = 0L,
    features_set = c("catch22", "feasts")
  )
  new = predictors_init$get_rolling_features(ohlcv)
  path_ = path(PATH_ROLLING, glue("live_{n_}"))
  fwrite(new, path_)
}

# Tsfeatures
n_ = "tsfeatures.csv"
meta = get_at(n_)
if (meta[, any(lags == -1L)]) {
  predictors_init = RollingTsfeatures$new(
    windows = windows,
    workers = workers,
    at = meta[lags == -1L, n],
    lag = -1L,
    scale = TRUE
  )
  new = predictors_init$get_rolling_features(ohlcv)
  path_ = path(PATH_ROLLING, n_)
  old = fread(path(PATH_ROLLING, n_))
  new = rbind(old, new)
  new = unique(new, by = c("symbol", "date"))
  fwrite(new, path_)
}
if (meta[, any(lags == 0L)]) {
  predictors_init = RollingTsfeatures$new(
    windows = windows,
    workers = workers,
    at = meta[lags == 0L, n],
    lag = 0L,
    scale = TRUE
  )
  new = predictors_init$get_rolling_features(ohlcv)
  path_ = path(PATH_ROLLING, glue("live_{n_}"))
  fwrite(new, path_)
}

# WaveletArima
n_ = "waveletarima.csv"
meta = get_at(n_)
if (meta[, any(lags == -1L)]) {
  predictors_init = RollingWaveletArima$new(
    windows = windows,
    workers = workers,
    at = meta[lags == -1L, n],
    lag = -1L,
    filter = "haar"
  )
  new = predictors_init$get_rolling_features(ohlcv)
  path_ = path(PATH_ROLLING, n_)
  old = fread(path(PATH_ROLLING, n_))
  new = rbind(old, new)
  new = unique(new, by = c("symbol", "date"))
  fwrite(new, path_)
}
if (meta[, any(lags == 0L)]) {
  predictors_init = RollingWaveletArima$new(
    windows = windows,
    workers = workers,
    at = meta[lags == 0L, n],
    lag = 0L,
    filter = "haar"
  )
  new = predictors_init$get_rolling_features(ohlcv)
  path_ = path(PATH_ROLLING, glue("live_{n_}"))
  fwrite(new, path_)
}

# FracDiff
n_ = "fracdiff.csv"
meta = get_at(n_)
if (meta[, any(lags == -1L)]) {
  predictors_init = RollingFracdiff$new(
    windows = windows,
    workers = workers,
    at = meta[lags == -1L, n],
    lag = -1L,
    nar = c(1), 
    nma = c(1),
    bandw_exp = c(0.1, 0.5, 0.9)
  )
  new = predictors_init$get_rolling_features(ohlcv)
  path_ = path(PATH_ROLLING, n_)
  old = fread(path(PATH_ROLLING, n_))
  new = rbind(old, new)
  new = unique(new, by = c("symbol", "date"))
  fwrite(new, path_)
}
if (meta[, any(lags == 0L)]) {
  predictors_init = RollingFracdiff$new(
    windows = windows,
    workers = workers,
    at = meta[lags == 0L, n],
    lag = 0L,
    nar = c(1), 
    nma = c(1),
    bandw_exp = c(0.1, 0.5, 0.9)
  )
  new = predictors_init$get_rolling_features(ohlcv)
  path_ = path(PATH_ROLLING, glue("live_{n_}"))
  fwrite(new, path_)
}

# Theft py
n_ = "theftpy.csv"
meta = get_at(n_)
windows_ = c(22, windows)
if (meta[, any(lags == -1L)]) {
  predictors_init = RollingTheft$new(
    windows = windows_,
    workers = workers,
    at = meta[lags == -1L, n],
    lag = -1L,
    features_set = c("tsfel", "tsfresh")
  )
  new = suppressMessages({predictors_init$get_rolling_features(ohlcv)})
  path_ = path(PATH_ROLLING, n_)
  old = fread(path(PATH_ROLLING, n_))
  colnames(old) = clean_col_names(colnames(old))
  colnames(new) = clean_col_names(colnames(new))
  new = rbind(old, new, fill = TRUE)
  new = unique(new, by = c("symbol", "date"))
  # new[, colnames(new)[duplicated(colnames(new))]]
  new = new[, .SD, .SDcols = -new[, which(duplicated(colnames(new)))]]
  fwrite(new, path_)
}
if (meta[, any(lags == 0L)]) {
  predictors_init = RollingTheft$new(
    windows = windows_,
    workers = workers,
    at = meta[lags == 0L, n],
    lag = 0L,
    features_set = c("tsfel", "tsfresh")
  )
  new = predictors_init$get_rolling_features(ohlcv)
  colnames(new) = clean_col_names(colnames(new))
  # new[, colnames(new)[duplicated(colnames(new))]]
  new = new[, .SD, .SDcols = -new[, which(duplicated(colnames(new)))]]
  path_ = path(PATH_ROLLING, glue("live_{n_}"))
  fwrite(new, path_)
}

# Theft py with returns
n_ = "theftpyr.csv"
meta = get_at(n_)
windows_ = c(22, windows)
if (meta[, any(lags == -1L)]) {
  predictors_init = RollingTheft$new(
    windows = windows_,
    workers = workers,
    at = meta[lags == -1L, n],
    lag = -1L,
    features_set = c("tsfel", "tsfresh")
  )
  new = suppressMessages({predictors_init$get_rolling_features(ohlcv)})
  path_ = path(PATH_ROLLING, n_)
  old = fread(path(PATH_ROLLING, n_))
  colnames(old) = clean_col_names(colnames(old))
  colnames(new) = clean_col_names(colnames(new))
  new = rbind(old, new, fill = TRUE)
  new = unique(new, by = c("symbol", "date"))
  # new[, colnames(new)[duplicated(colnames(new))]]
  new = new[, .SD, .SDcols = -new[, which(duplicated(colnames(new)))]]
  fwrite(new, path_)
}
if (meta[, any(lags == 0L)]) {
  predictors_init = RollingTheft$new(
    windows = windows_,
    workers = workers,
    at = meta[lags == 0L, n],
    lag = 0L,
    features_set = c("tsfel", "tsfresh")
  )
  new = predictors_init$get_rolling_features(ohlcv)
  colnames(new) = clean_col_names(colnames(new))
  # new[, colnames(new)[duplicated(colnames(new))]]
  new = new[, .SD, .SDcols = -new[, which(duplicated(colnames(new)))]]
  path_ = path(PATH_ROLLING, glue("live_{n_}"))
  fwrite(new, path_)
}

# Vse
n_ = "vse.csv"
meta = get_at(n_)
windows_ = c(22, 44, 150, windows, 504)
if (meta[, any(lags == -1L)]) {
  predictors_init = RollingVse$new(
    windows = windows_,
    workers = workers,
    at = meta[lags == -1L, n],
    lag = -1L,
    m = c(0.4, 0.5)
  )
  new = suppressMessages({predictors_init$get_rolling_features(ohlcv)})
  path_ = path(PATH_ROLLING, n_)
  old = fread(path(PATH_ROLLING, n_))
  colnames(old) = clean_col_names(colnames(old))
  colnames(new) = clean_col_names(colnames(new))
  new = rbind(old, new, fill = TRUE)
  new = unique(new, by = c("symbol", "date"))
  # new[, colnames(new)[duplicated(colnames(new))]]
  new = new[, .SD, .SDcols = -new[, which(duplicated(colnames(new)))]]
  fwrite(new, path_)
}
if (meta[, any(lags == 0L)]) {
  predictors_init = RollingVse$new(
    windows = windows_,
    workers = workers,
    at = meta[lags == 0L, n],
    lag = 0L,
    m = c(0.4, 0.5)
  )
  new = predictors_init$get_rolling_features(ohlcv)
  colnames(new) = clean_col_names(colnames(new))
  # new[, colnames(new)[duplicated(colnames(new))]]
  new = new[, .SD, .SDcols = -new[, which(duplicated(colnames(new)))]]
  path_ = path(PATH_ROLLING, glue("live_{n_}"))
  fwrite(new, path_)
}

# Prepare features for merge
fnames = dir_ls(PATH_ROLLING)
rolling_predictors = lapply(fnames[!grepl("live", fnames)], fread)
names(rolling_predictors) = gsub("\\.csv", "", basename(fnames[!grepl("live", fnames)]))
colnames(rolling_predictors[["theftrr"]])[-(1:2)] = paste0(colnames(rolling_predictors[["theftrr"]])[-(1:2)], 
                                                           "_returns")
colnames(rolling_predictors[["theftpyr"]])[-(1:2)] = paste0(colnames(rolling_predictors[["theftpyr"]])[-(1:2)], 
                                                            "_returns") 
rolling_predictors = lapply(rolling_predictors, function(dt_) {
  dt_[, !duplicated(names(dt_)), with = FALSE]
})

# Merge signals
rolling_predictors = Reduce(
  function(x, y) merge( x, y, by = c("symbol", "date"), all.x = TRUE, all.y = FALSE),
  rolling_predictors
)
if (length(fnames[grepl("live", fnames)]) > 0) {
  rolling_predictors_new =  Reduce(
    function(x, y) merge( x, y, by = c("symbol", "date"), all.x = TRUE, all.y = FALSE),
    lapply(fnames[grepl("live", fnames)], fread)
  )
} else {
  rolling_predictors_new =  NULL
}
rolling_predictors = rbind(rolling_predictors, rolling_predictors_new, fill = TRUE)
dim(rolling_predictors)

# Fix column names
rolling_predictors = clean_names(rolling_predictors)


# OHLCV PREDICTORS --------------------------------------------------------
# Define at parameter
at_meta = get_at(NULL, FALSE)
keep_ohlcv = at_meta[, n] - at_meta[, lags]

# Features from OHLLCV
print("Calculate Ohlcv features.")
ohlcv_init = OhlcvFeaturesDaily$new(
  at = keep_ohlcv,
  windows = c(5, 10, 22, 22 * 3, 22 * 6, 22 * 12, 22 * 12 * 2),
  quantile_divergence_window =  c(22, 22 * 3, 22 * 6, 22 * 12, 22 * 12 * 2)
)
ohlcv_features = ohlcv_init$get_ohlcv_features(copy(ohlcv$X))
setorderv(ohlcv_features, c("symbol", "date"))

# CHECKS ------------------------------------------------------------------
### Importmant notes:
# 1. ohlcv_features_sample has date 2 trading days before event.
# 2. Rolling predictors have date column that is the same as event date, but
# the predictor is calculated for 2 trading days before
# 3. So, we have to merge ohlcv_features_sample with roling from xxx.

# Check if dates corresponds to above notes
symbol_ = "MSFT"
dataset[symbol == symbol_, head(.SD), .SDcols = c("symbol", "date", "date_event")]
ohlcv_features[symbol == symbol_, head(.SD), .SDcols = c("symbol", "date")]
rolling_predictors[symbol == symbol_, head(.SD), .SDcols = c("symbol", "date")]
# We can see that rolling predictors have the same date as dataset. That is date
# column in rolling predictors is the event date, that is date when earnings
# are released. But the ohlcv have date columns that is 1 trading days after
# This doesn't mean that ohlcv and rolling predictors are calculated on different dates.
# They are bot calculated 1 day after an event.

# Check dates for new data
dataset[, max(date)]
ohlcv$X[, max(date)]
ohlcv_features[, max(date)]
rolling_predictors[, max(date)]
dataset[date == max(date), .(symbol, date)]

# Check dates before merge
colnames(rolling_predictors)[grep("date", colnames(rolling_predictors))]
colnames(dataset)[grep("date", colnames(dataset))]
colnames(ohlcv_features)[grep("date", colnames(ohlcv_features))]
dataset[symbol == symbol_, .(date, date_prices, date_event)]
rolling_predictors[symbol == symbol_, .(date)]
ohlcv_features[symbol == symbol_, .(date)]
ohlcv_features[, max(date)]

# MERGE PREDICTORS --------------------------------------------------------
# Merge OHLCV predictors and rolling predictors
rolling_predictors[, date_rolling := date]
ohlcv_features[, date_ohlcv := date]
features = rolling_predictors[ohlcv_features, on = c("symbol", "date"), roll = Inf]

# Remove rows with missing dates
features = na.omit(features, cols = c("date", "date_rolling", "date_ohlcv"))

# Check again merging dates
features[symbol == symbol_, .(symbol, date_rolling, date_ohlcv, date)]
features[, max(date)]
features[date == max(date), .(symbol, date_rolling, date_ohlcv, date)]

# Remove observations awhere diff between date_ohlcv and date_rolling is greater than 1 business day
nrow(features[businessDaysBetween(date_rolling, date_ohlcv) > 1]) # We remove those
features = features[businessDaysBetween(date_rolling, date_ohlcv) <= 1]

# Check for duplicates
anyDuplicated(features, by = c("symbol", "date"))
anyDuplicated(features, by = c("symbol", "date_ohlcv"))
anyDuplicated(features, by = c("symbol", "date_rolling"))
features[duplicated(as.data.frame(features[, .SD, .SDcols = c("symbol", "date_rolling")])) |
           duplicated(as.data.frame(features[, .SD, .SDcols = c("symbol", "date_rolling")]), fromLast = TRUE), 
         .SD,
         .SDcols = c("symbol", "date", "date_rolling", "date_ohlcv")]
if (anyDuplicated(features, by = c("symbol", "date_rolling"))) {
  features = unique(features, by = c("symbol", "date_rolling"))
}

# Check merge features and events
features[symbol == symbol_, .(symbol, date, date_rolling, date_ohlcv)]
features[, all(date == date_ohlcv)] # date is date_ohlcv
dataset[symbol == symbol_, .(symbol, date)]
dataset[,  max(date)]
dataset[,  max(date_prices, na.rm = TRUE)]
features[, max(date)]
features[date == (max(date) - 1), .(date, date_ohlcv, date_rolling)]
features[date == max(date), .(date, date_ohlcv, date_rolling)]
dataset[date == max(date), .(date)]

# Merge features and events
features = merge(features, dataset,
                 by.x = c("symbol", "date_rolling"), 
                 by.y = c("symbol", "date"),
                 all.x = TRUE, all.y = FALSE)

# Remove same dates
colnames(features)[grepl("date", colnames(features))]
features[, .(date, date_event, date_ohlcv, date_prices, date_rolling)]
features[, all(date_prices == date_rolling)]
features[, all(date_event == date_rolling)]
features[, all(date_event == date_prices)]
features[, all(date == date_ohlcv)]
features[, c("date_prices", "date_rolling", "date_ohlcv") := NULL]
# We keep only:
# 1) date - this is one trading date AFTER the event date
# 2) date_event - this is event date, that is date of earning announcement

# Check all those dates
features[is.na(date), 1:5]
features[is.na(date_event), 1:5]


# FUNDAMENTALS ------------------------------------------------------------
# import fundamental factors
fundamentals = read_parquet(path(
  "/home/sn/data/equity/us",
  "predictors_daily",
  "factors",
  "fundamental_factors",
  ext = "parquet"
))

# Clean fundamentals
fundamentals = fundamentals[date > as.Date("2008-01-01")]
fundamentals[, acceptedDateTime := as.POSIXct(acceptedDate, tz = "America/New_York")]
fundamentals[, acceptedDate := as.Date(acceptedDateTime)]
fundamentals[, acceptedDateFundamentals := acceptedDate]
data.table::setnames(fundamentals, "date", "fundamental_date")
fundamentals = unique(fundamentals, by = c("symbol", "acceptedDate"))

# Check last date for fundamentals
fundamentals[, max(acceptedDate, na.rm = TRUE)]

# Merge features and fundamental data
features[, date_features := date]
features = fundamentals[features, on = c("symbol", "acceptedDate" = "date_features"), roll = Inf]
features[, .(symbol, date, date_event, acceptedDate, acceptedDateTime, receivablesGrowth)]

# remove unnecessary columns
features[, `:=`(period = NULL, link = NULL, finalLink = NULL, acceptedDate = NULL,
                reportedCurrency = NULL, cik = NULL, calendarYear = NULL, 
                fillingDate = NULL, fundamental_date = NULL)]

# convert char features to numeric features
char_cols = features[, colnames(.SD), .SDcols = is.character]
char_cols = setdiff(char_cols, c("symbol", "time", "right_time", "industry", "sector"))
features[, (char_cols) := lapply(.SD, as.numeric), .SDcols = char_cols]


# TRANSCRIPTS -------------------------------------------------------------
# Import transcripts for all stocks
path_ = "/home/sn/data/equity/us/fundamentals/transcripts_sentiment_finbert"
files = list.files(path_, full.names = TRUE)
symbols_ = gsub("\\.parquet", "", basename(files))
files = files[symbols_ %in% features[, unique(symbol)]]
print(paste0("We removed ", features[, length(unique(symbol))] - length(files), " symbols"))
transcirpts = rbindlist(lapply(files, read_parquet))
transcirpts[, date_transcript := as.Date(date)]
transcirpts = transcirpts[, -c("quarter", "year", "date")]

# Merge transcripts and features
features[, date_features := date]
features = transcirpts[features, on = c("symbol", "date_transcript" = "date_features"), roll = Inf]
features[, .(symbol, date, date_event, date_transcript, prob_negative)]

# remove unnecessary columns
features[, `:=`(date_transcript = NULL)]


# # MACRO -------------------------------------------------------------------
# # Fred meta
# fred_meta = fread("/home/sn/data/macro/fred_meta.csv")
# fred_meta = fred_meta[last_updated > (Sys.Date() - 60)]
# fred_meta = fred_meta[popularity == 1]
# 
# # Import FRED data
# files = list.files("/home/sn/data/macro/fred", full.names = TRUE)
# ids_ = gsub("\\.csv", "", basename(files))
# files = files[ids_ %in% fred_meta[, id]]
# fred_dt = lapply(files, fread)
# fred_dt = rbindlist(fred_dt)
# fred_dt[, date_real := date]
# fred_dt[vintage == 1, date_real := realtime_start]
# fred_dt = fred_dt[, .(id = series_id, date_real, value)]
# fred_dt = fred_dt[date_real > as.Date("2015-01-01")]
# 
# # Clean fredt_dt
# any(fred_dt[, sd(value) == 0, by = id][, V1])
# if (anyDuplicated(fred_dt, by = c("id", "date_real"))) {
#   fred_dt = unique(fred_dt, by = c("id", "date_real"))
# }
# fred_dt = dcast(fred_dt, date_real ~ id, value.var = "value")
# setorder(fred_dt, date_real)
# setnafill(fred_dt, type = "locf", cols = colnames(fred_dt)[3:ncol(fred_dt)])
# remove_cols = fred_dt[, colSums(is.na(fred_dt)) / nrow(fred_dt) > 0.3]
# remove_cols = names(remove_cols[remove_cols == TRUE])
# fred_dt = fred_dt[, .SD, .SDcols= -remove_cols]
# tmp = cor(fred_dt[, -c("date_real")])
# tmp[upper.tri(tmp)] = 0
# diag(tmp) = 0
# tmp = abs(tmp)
# to_remove = c()  # Will store column indices to remove
# for (col_i in seq_len(ncol(tmp))) {
#   # If col_i is already marked for removal, skip it
#   if (col_i %in% to_remove) next
#   
#   # Which columns are highly correlated with col_i?
#   high_cor_with_i = which(tmp[, col_i] > 0.95)
#   
#   # Remove the "extra" columns from those found
#   # Typically we keep the first column we encounter and remove the rest,
#   # or you might decide to keep the one with fewer missing values, etc.
#   # Here, we remove all 'high cor' columns except 'col_i' itself.
#   for (col_j in high_cor_with_i) {
#     if (!(col_j %in% to_remove) && col_j != col_i) {
#       to_remove <- c(to_remove, col_j)
#     }
#   }
# }
# to_remove = unique(to_remove)  # make sure it’s unique
# fred_dt = fred_dt[, .SD, .SDcols = -colnames(tmp)[to_remove]]
# 
# # Merge fred and features
# features[, date_features := date]
# features = fred_dt[features, on = c("date_real" = "date_features"), roll = Inf]
# features[, .(symbol, date, date_event, date_real, TMBSCBW027NBOG)]
# 
# # remove unnecessary columns
# features[, `:=`(date_real = NULL)]


# FEATURES SPACE ----------------------------------------------------------
# Remove events columns
# > colnames(events)
# [1] "symbol"                        "date"                          "eps"                          
# [4] "epsEstimated"                  "time"                          "revenue"                      
# [7] "revenueEstimated"              "fiscalDateEnding"              "updatedFromDate"              
# [10] "time_investingcom"             "eps_investingcom"              "eps_forecast_investingcom"    
# [13] "revenue_investingcom"          "revenue_forecast_investingcom" "right_time"                   
# [16] "actualEarningResult"           "estimatedEarning"              "same_announce_time" 

# Features space from features raw
colnames(features)[grepl("date", colnames(features))]
cols_remove = c("trading_date_after_event", "time", "datetime_investingcom",
                 "eps_investingcom", "eps_forecast_investingcom", "revenue_investingcom",
                 "revenue_forecast_investingcom", "time_dummy",
                 "trading_date_after_event", "fundamental_date", "cik", "link", "finalLink",
                 "fillingDate", "calendarYear", "eps.y", "revenue.y", "period.x", "period.y",
                 "acceptedDateTime", "acceptedDateFundamentals", "reportedCurrency",
                 "fundamental_acceptedDate", "period", "right_time",
                 "updatedFromDate", "fiscalDateEnding", "time_investingcom",
                 "eps", "epsEstimated", "revenue", "revenueEstimated",
                 "same_announce_time", "time_transcript",
                 # remove dates we don't need
                 setdiff(colnames(features)[grep("date", colnames(features), ignore.case = TRUE)], c("date", "date_event")),
                 # remove columns with i - possible duplicates
                 colnames(features)[grep("i\\.|\\.y$", colnames(features))],
                 colnames(features)[grep("^open\\.|^high\\.|^low\\.|^close\\.|^volume\\.|^returns\\.", 
                                         colnames(features))],
                 "actualEarningResult", "estimatedEarning"
)
cols_non_features <- c("symbol", "date", "time", "right_time", "date_event",
                       "open", "high", "low", "close", "volume", "returns",
                       target_variables)
cols_features = setdiff(colnames(features), c(cols_remove, cols_non_features))
head(cols_features, 100)
tail(cols_features, 500)
cols_features[grepl("evenu", cols_features)]
length(cols_features)
cols = c(cols_non_features, cols_features)
cols_remove = setdiff(cols, colnames(features))
cols = cols[!(cols %in% cols_remove)]
features = features[, .SD, .SDcols = cols]

# Checks
features[, max(date)]
# features[, .(symbol, date, date_rolling)]


# CLEAN DATA --------------------------------------------------------------
# Convert columns to numeric. This is important only if we import existing features
chr_to_num_cols = setdiff(colnames(features[, .SD, .SDcols = is.character]),
                          c("symbol", "time", "right_time", "industry", "sector"))
if (length(chr_to_num_cols) > 0) {
  features = features[, (chr_to_num_cols) := lapply(.SD, as.numeric), .SDcols = chr_to_num_cols] 
}
log_to_num_cols = colnames(features[, .SD, .SDcols = is.logical])
features = features[, (log_to_num_cols) := lapply(.SD, as.numeric), .SDcols = log_to_num_cols]

# Remove duplicates
any_duplicates = any(duplicated(features[, .(symbol, date)]))
if (any_duplicates) features = unique(features, by = c("symbol", "date"))

# Remove columns with many NA
cols_remove = names(which(colMeans(!is.na(features)) < 0.6))
cols_remove = setdiff(cols_remove, cols_remove[grepl("^bin_", cols_remove)])
length(cols_remove)
length(cols_remove[grepl("tsfresh", cols_remove)])
length(cols_remove[grepl("tsfel", cols_remove)])
# TODO: Check all this missing values
features = features[, .SD, .SDcols = !cols_remove]

# Remove Inf and Nan values if they exists
is.infinite.data.frame <- function(x) do.call(cbind, lapply(x, is.infinite))
keep_cols = names(which(colMeans(!is.infinite(as.data.frame(features))) > 0.98))
print(paste0("Removing columns with Inf values: ", setdiff(colnames(features), keep_cols)))
features = features[, .SD, .SDcols = keep_cols]

# Remove inf values
n_0 = nrow(features)
features = features[is.finite(rowSums(features[, .SD, .SDcols = is.numeric], na.rm = TRUE))]
n_1 = nrow(features)
print(paste0("Removing ", n_0 - n_1, " rows because of Inf values"))

# Final checks
# clf_data[, .(symbol, date, date_rolling)]
# features[, .(symbol, date, date_rolling)]
features[, max(date)]
features[date == max(date), 1:15]
features[date == max(date), 1500:1525]

# Save features
last_pead_date = strftime(features[, max(date)], "%Y%m%d")
file_name = paste0("pead-predictors-", last_pead_date, ".csv")  
file_name_local = fs::path(PATH_PREDICTORS, file_name)
fwrite(features, file_name_local)

# Add to padobran
# scp /home/sn/data/equity/us/predictors_daily/pead_predictors/pead-predictors-20250203.csv padobran:/home/jmaric/peadml/predictors.csv





# Calculate features
# prices_sample = prices[symbol %in% prices[, sample(unique(symbol), 50)]]
ohlcv_features = OhlcvFeaturesDaily$new(
  at = prices[, which(eom == 1)],
  windows = c(5, 10, 22, 44, 66, 125, 252, 500, 1000),
  quantile_divergence_window = c(22, 44, 66, 125, 252, 500, 1000)
)
ohlcv_predictors = ohlcv_features$get_ohlcv_features(prices)

# Define predictors
predictors = setdiff(colnames(ohlcv_predictors), colnames(prices))

# convert variables with low number of unique values to factors
int_numbers = na.omit(ohlcv_predictors[, ..predictors])[, lapply(.SD, function(x) all(floor(x) == x))]
int_cols = colnames(ohlcv_predictors[, ..predictors])[as.matrix(int_numbers)[1,]]
factor_cols = ohlcv_predictors[, ..int_cols][, lapply(.SD, function(x) length(unique(x)))]
factor_cols = as.matrix(factor_cols)[1, ]
factor_cols = factor_cols[factor_cols <= 100]
ohlcv_predictors = ohlcv_predictors[, (names(factor_cols)) := lapply(.SD, as.factor), .SD = names(factor_cols)]

# Create target variable
ohlcv_predictors[, target_1m := shift(close, 1L, type = "lead") / close - 1, by = symbol]

# Convert types
ohlcv_predictors = ohlcv_predictors[, names(.SD) := lapply(.SD, as.numeric), .SDcols = bit64::is.integer64]
ohlcv_predictors[, date := as.POSIXct(date, tz = "UTC")]

# Remove missing targets
ohlcv_predictors = na.omit(ohlcv_predictors, cols = c("target_1m"))

# Remove Inf values
check_inf_col_proportion = function(dt, predictors) {
  inf_cols = dt[, sapply(.SD, function(x) sum(is.infinite(x))), .SDcols = predictors]
  inf_cols = inf_cols[inf_cols > 0]
  if (length(inf_cols) == 0) {
    return(0)
  }
  inf_cols = inf_cols / nrow(dt) 
  inf_cols = as.data.table(inf_cols, keep.rownames = "vars")
  setnames(inf_cols, c("vars", "inf_proportion"))
  return(inf_cols)
}
inf_cols = check_inf_col_proportion(ohlcv_predictors)
remove_cols = inf_cols[inf_proportion > 0.001]
change_cols = inf_cols[inf_proportion <= 0.001]
ohlcv_predictors[, (remove_cols[, vars]) := NULL]
ohlcv_predictors[, (change_cols[, vars]) := lapply(.SD, function(x) ifelse(is.infinite(x), NA, x)), 
                 .SDcols = change_cols[, vars]]
setnafill(ohlcv_predictors, type = "locf", cols = change_cols[, vars])
predictors = setdiff(predictors, remove_cols[, vars])

# Basic cleaning with janitor
ohlcv_predictors = remove_constant(ohlcv_predictors, quiet = FALSE)
ohlcv_predictors = remove_empty(ohlcv_predictors, "cols", 0.33, FALSE)
ohlcv_predictors = remove_empty(ohlcv_predictors, "rows", 0.33, FALSE)
# get_one_to_one(teest_) # cant figure put why this doesnt work
# teest_ = na.omit(ohlcv_predictors[, ..predictors][1:2000, .SD, .SDcols = 1:2]) 
# fwrite(teest_, "test.csv")
# dt = fread("https://snpmarketdata.blob.core.windows.net/qanda/test.csv")
# get_one_to_one(dt)

# Add predictors to atributes
data.table::setattr(ohlcv_predictors, "predictors", predictors)
attributes(ohlcv_predictors)

# Save and mannually upload to padobran
saveRDS(ohlcv_predictors, "D:/strategies/zoo/ohlcv_predictors.rds")
