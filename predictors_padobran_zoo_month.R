options(progress_enabled = FALSE)

library(data.table)
library(finfeatures)
library(reticulate)


# paths
if (interactive()) {
  PATH_PREDICTORS = file.path("predictors_zoo_month")
  PATH_DATASET    = file.path("/home/sn/data/strategies/zoo/prices_zoo_month")
} else {
  PATH_PREDICTORS = file.path("predictors_zoo_month")
  PATH_DATASET    = file.path("prices_zoo_month")
}

# Python environment
if (interactive()) {
  reticulate::use_virtualenv("/home/sn/projects_py/pyquant", required = TRUE)
  tsfel = reticulate::import("tsfel")
  tsfresh = reticulate::import("tsfresh", convert = FALSE)
  warnigns = reticulate::import("warnings", convert = FALSE)
  warnigns$filterwarnings('ignore')  
} else {
  reticulate::use_virtualenv("/opt/venv")
  tsfel = reticulate::import("tsfel")
  tsfresh = reticulate::import("tsfresh", convert = FALSE)
  warnigns = reticulate::import("warnings", convert = FALSE)
  warnigns$filterwarnings('ignore')
}

# Create directory if it doesnt exists
if (!dir.exists(PATH_PREDICTORS)) {
  dir.create(PATH_PREDICTORS)
}

# Get index
if (interactive()) {
  i = 1L
} else {
  i = as.integer(Sys.getenv('PBS_ARRAY_INDEX')) 
}

# Get symbol
symbols = gsub("\\.csv", "", list.files(PATH_DATASET))
symbol_i = symbols[i]

# Import Ohlcv data
ohlcv = fread(file.path(PATH_DATASET, paste0(symbol_i, ".csv")))
ohlcv = Ohlcv$new(ohlcv[, .(symbol, date, open, high, low, close, volume, pbs_i)], 
                  date_col = "date")

# Parameters
lag_ = 0L
workers = 1L
windows = c(66, 252)
at_ = ohlcv$X[, which(pbs_i == TRUE)]

# Utils
wide_to_long = function(dt, label) {
  print(label)
  dt[, feature_set := label]
  melt(dt, id.vars = c("symbol", "date", "feature_set"))
}

# Daily Ohlcv data
ohlcv_features = OhlcvFeaturesDaily$new(
  at = at_,
  windows = c(5, 10, 22, 44, 66, 125, 252, 500, 1000),
  quantile_divergence_window = c(22, 44, 66, 125, 252, 500, 1000)
)
ohlcv_predictors = ohlcv_features$get_ohlcv_features(ohlcv$X)
ohlcv_predictors = wide_to_long(ohlcv_predictors, "ohlcv")

# Exuber
windows_ = c(windows, 504)
if (max(at_) > min(windows)) {
  exuber_init = RollingExuber$new(
    windows = windows_,
    workers = workers,
    at = at_,
    lag = lag_,
    exuber_lag = c(1L)
  )
  exuber <<- exuber_init$get_rolling_features(ohlcv, log_prices = TRUE)
}
exuber = wide_to_long(exuber, "exuber")

# Backcusum
if (max(at_) > min(windows)) {
  backcusum_init = RollingBackcusum$new(
    windows = windows_,
    workers = workers,
    at = at_,
    lag = lag_,
    alternative = c("greater", "two.sided"),
    return_power = c(1, 2))
  backcusum <<- backcusum_init$get_rolling_features(ohlcv)
}
backcusum = wide_to_long(backcusum, "backcusum")

# Forecasts
windows_ = c(252, 252 * 2)
if (max(at_) > min(windows)) {
  forecasts_init = RollingForecats$new(
    windows = windows_,
    workers = workers,
    at = at_,
    lag = lag_,
    forecast_type = c("autoarima", "nnetar", "ets"),
    h = 22)
  forecastsr <<- suppressMessages(forecasts_init$get_rolling_features(ohlcv, price_col = "returns"))
}
forecastsr = wide_to_long(forecastsr, "forecastsr")

# Forecasts with prices
windows_ = c(252, 252 * 2)
if (max(at_) > min(windows)) {
  forecasts_init = RollingForecats$new(
    windows = windows_,
    workers = workers,
    at = at_,
    lag = lag_,
    forecast_type = c("autoarima"),
    h = 22)
  forecasts <<- suppressMessages(forecasts_init$get_rolling_features(ohlcv))
}
forecasts = wide_to_long(forecasts, "forecasts")

# Theft r
windows_ = c(5, 22, windows)
if (max(at_) > min(windows)) {
  theft_init = RollingTheft$new(
    windows = windows_,
    workers = workers,
    at = at_,
    lag = lag_,
    features_set = c("catch22", "feasts"))
  theftr <<- theft_init$get_rolling_features(ohlcv)
}
theftr = wide_to_long(theftr, "theftr")

# Theft r with returns
windows_ = c(5, 22, windows)
if (max(at_) > min(windows)) {
  theft_init = RollingTheft$new(
    windows = windows_,
    workers = workers,
    at = at_,
    lag = lag_,
    features_set = c("catch22", "feasts"))
  theftrr <<- theft_init$get_rolling_features(ohlcv, price_col = "returns")
}
theftrr = wide_to_long(theftrr, "theftrr")

# Theft py with returns
windows_ = c(22, windows)
if (max(at_) > min(windows)) {
  theft_init = RollingTheft$new(
    windows = windows_,
    workers = 1L,
    at = at_,
    lag = lag_,
    features_set = "tsfresh") # c("tsfel", "tsfresh"))
  theftpyr <<- suppressMessages(theft_init$get_rolling_features(ohlcv, price_col = "returns"))
}
theftpyr = wide_to_long(theftpyr, "theftpyr")

# Theft py
windows_ = c(22, windows)
if (max(at_) > min(windows)) {
  theft_init = RollingTheft$new(
    windows = windows_,
    workers = 1L,
    at = at_,
    lag = lag_,
    features_set = "tsfresh") # c("tsfel", "tsfresh"))
  theftpy <<- suppressMessages(theft_init$get_rolling_features(ohlcv))
}
theftpy = wide_to_long(theftpy, "theftpy")

# Tsfeatures
if (max(at_) > min(windows)) {
  tsfeatures_init = RollingTsfeatures$new(
    windows = windows,
    workers = workers,
    at = at_,
    lag = lag_,
    scale = TRUE)
  tsfeatures <<- suppressMessages(tsfeatures_init$get_rolling_features(ohlcv))
}
tsfeatures = wide_to_long(tsfeatures, "tsfeatures")

# WaveletArima
if (max(at_) > min(windows)) {
  waveletarima_init = RollingWaveletArima$new(
    windows = windows,
    workers = workers,
    at = at_,
    lag = lag_,
    filter = "haar")
  waveletarima <<- suppressMessages(waveletarima_init$get_rolling_features(ohlcv))
}
waveletarima = wide_to_long(waveletarima, "waveletarima")

# FracDiff
if (max(at_) > min(windows)) {
  fracdiff_init = RollingFracdiff$new(
    windows = windows,
    workers = workers,
    at = at_,
    lag = lag_,
    nar = c(1), 
    nma = c(1),
    bandw_exp = c(0.1, 0.5, 0.9))
  fracdiff <<- suppressMessages(fracdiff_init$get_rolling_features(ohlcv))
}
fracdiff = wide_to_long(fracdiff, "fracdiff")

# VSE
windows_ = c(22, 44, 150, windows, 504)
if (max(at_) > min(windows)) {
  vse_init = RollingVse$new(
    windows = windows_,
    workers = workers,
    at = at_,
    lag = lag_,
    m = c(0.4, 0.5))
  vse <<- vse_init$get_rolling_features(ohlcv)
}
vse = wide_to_long(vse, "vse")

# Merge all indicators
predictors = rbindlist(list(ohlcv_predictors, exuber, backcusum, forecasts, 
                            forecastsr, theftr, theftrr, theftpyr, theftpy,
                            tsfeatures, waveletarima, fracdiff, vse))

# Save
fwrite(predictors[, .SD, .SDcols = -"symbol"], paste0(predictors[1, symbol], ".csv"))
