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
PATH            = "/home/sn/data/strategies/zoo"
PATH_PREDICTORS = path(PATH, "predictors_zoo_month")


# PRICES AND EVENTS -------------------------------------------------------
# Import prices only for symbols we have data for
predfiles = list.files(PATH_PREDICTORS, full.names = TRUE)
files = file.path(PATH, "prices_zoo_month", basename(predfiles))
files = files[file.exists(files)]
ohlcv = lapply(files, fread)
ohlcv = rbindlist(ohlcv, fill = TRUE)
setkey(ohlcv, "symbol")
setorder(ohlcv, symbol, date)

# Remove stocks with raw prices lower than 2$
ohlcv[, average_mon_raw_price := mean(close_raw), by = .(symbol, month)]
ohlcv[, length(unique(symbol))]
ohlcv = ohlcv[average_mon_raw_price >= 2]
ohlcv[, length(unique(symbol))]
ohlcv[, average_mon_raw_price := NULL]

# Remove stocks with volume lower than 100.000
ohlcv[, average_volume := sum(volume), by = .(symbol, month)]
ohlcv[, length(unique(symbol))]
ohlcv = ohlcv[average_volume >= 100000]
ohlcv[, length(unique(symbol))]
ohlcv[, average_volume := NULL]

# Extract 2000 most liquid by dollar volume
dol_vol_month = ohlcv[, .(value = sum(close_raw * volume)), by = .(symbol, month)]
dol_vol_month[, dolvol_rank := frankv(value, order = -1L, ties.method = "first"), by = month]
dol_vol_month = dol_vol_month[, .SD[dolvol_rank <= 1200], by = month]
ohlcv = dol_vol_month[, .SD, .SDcols = -"value"][ohlcv, on = c("symbol", "month")]
ohlcv = na.omit(ohlcv, cols = "dolvol_rank")
ohlcv[pbs_i == TRUE, .N, by = month][order(month)]
ohlcv[pbs_i == TRUE, .N, by = month][, all(N == 1200)]

# Summary statistics
ohlcv[, .(
 n_symbols = length(unique(symbol)) 
)]


# ROLLING PREDICTORS ------------------------------------------------------
# Command to get data from padobran. This is necessary only first time
# fs::dir_delete("/home/sn/data/strategies/zoo/predictors_zoo_month")
# scp -r padobran:/home/jmaric/zoo/predictors_zoo_month/ /home/sn/data/strategies/zoo/

# Import all predictors
filesp = file.path(PATH_PREDICTORS, ohlcv[, paste0(unique(symbol), ".csv")])
system.time({
  rolling_predictors = lapply(filesp[1:100], fread)  
  names(rolling_predictors) = gsub("\\.csv", "", basename(filesp[1:100]))
  rolling_predictors = rbindlist(rolling_predictors, idcol = "symbol")
  rolling_predictors[, variable := paste0(feature_set, "_", variable)]
})

# Remove duplicates
duplicates = rolling_predictors[symbol == "a" & date == as.Date("2020-01-31")] |>
  _[duplicated(variable), variable]
rolling_predictors = rolling_predictors[variable %notin% duplicates]

# Long to wide
rolling_predictors = dcast(rolling_predictors, symbol + date ~ variable, value.var = "value")
setorderv(rolling_predictors, c("symbol", "date"))

# Fix column names
rolling_predictors = clean_names(rolling_predictors)

# Convert string columns to numeric
string_cols = rolling_predictors[, setdiff(colnames(.SD), "symbol"), 
                                 .SDcols = is.character]
rolling_predictors[, (string_cols) := lapply(.SD, as.numeric), .SDcols = string_cols]

# Checks
anyDuplicated(rolling_predictors, by = c("symbol", "date"))
head(colnames(rolling_predictors), 10)
tail(colnames(rolling_predictors), 10)


# # FUNDAMENTALS ------------------------------------------------------------
# # import fundamental factors
# fundamentals = read_parquet(path(
#   "/home/sn/data/equity/us",
#   "predictors_daily",
#   "factors",
#   "fundamental_factors",
#   ext = "parquet"
# ))
# 
# # Clean fundamentals
# fundamentals = fundamentals[date > as.Date("2008-01-01")]
# fundamentals[, acceptedDateTime := as.POSIXct(acceptedDate, tz = "America/New_York")]
# fundamentals[, acceptedDate := as.Date(acceptedDateTime)]
# fundamentals[, acceptedDateFundamentals := acceptedDate]
# data.table::setnames(fundamentals, "date", "fundamental_date")
# fundamentals = unique(fundamentals, by = c("symbol", "acceptedDate"))
# 
# # Check last date for fundamentals
# fundamentals[, max(acceptedDate, na.rm = TRUE)]
# 
# # Merge features and fundamental data
# features[, date_features := date]
# features = fundamentals[features, on = c("symbol", "acceptedDate" = "date_features"), roll = Inf]
# features[, .(symbol, date, date_event, acceptedDate, acceptedDateTime, receivablesGrowth)]
# 
# # remove unnecessary columns
# features[, `:=`(period = NULL, link = NULL, finalLink = NULL, acceptedDate = NULL,
#                 reportedCurrency = NULL, cik = NULL, calendarYear = NULL, 
#                 fillingDate = NULL, fundamental_date = NULL)]
# 
# # convert char features to numeric features
# char_cols = features[, colnames(.SD), .SDcols = is.character]
# char_cols = setdiff(char_cols, c("symbol", "time", "right_time", "industry", "sector"))
# features[, (char_cols) := lapply(.SD, as.numeric), .SDcols = char_cols]


# # TRANSCRIPTS -------------------------------------------------------------
# # Import transcripts for all stocks
# path_ = "/home/sn/data/equity/us/fundamentals/transcripts_sentiment_finbert"
# files = list.files(path_, full.names = TRUE)
# symbols_ = gsub("\\.parquet", "", basename(files))
# files = files[symbols_ %in% features[, unique(symbol)]]
# print(paste0("We removed ", features[, length(unique(symbol))] - length(files), " symbols"))
# transcirpts = rbindlist(lapply(files, read_parquet))
# transcirpts[, date_transcript := as.Date(date)]
# transcirpts = transcirpts[, -c("quarter", "year", "date")]
# 
# # Merge transcripts and features
# features[, date_features := date]
# features = transcirpts[features, on = c("symbol", "date_transcript" = "date_features"), roll = Inf]
# features[, .(symbol, date, date_event, date_transcript, prob_negative)]
# 
# # remove unnecessary columns
# features[, `:=`(date_transcript = NULL)]


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
# Features space from features raw
colnames(rolling_predictors)[grepl("ohlcv_.*[a-z]$", colnames(rolling_predictors))]
cols_non_features = c(
  "symbol", "date", "ohlcv_open", "ohlcv_high", "ohlcv_low", "ohlcv_close", 
  "ohlcv_volume", "ohlcv_returns", "ohlcv_pbs_i", "ohlcv_dolvolm")
cols_features = setdiff(colnames(rolling_predictors), cols_non_features)

# Check predictors
head(cols_features, 100)
tail(cols_features, 500)
length(cols_features)
cols = c(cols_non_features, cols_features)


# CLEAN DATA --------------------------------------------------------------
# Convert columns to numeric. This is important only if we import existing features
chr_to_num_cols = setdiff(colnames(rolling_predictors[, .SD, .SDcols = is.character]),
                          c("symbol", "time", "right_time", "industry", "sector"))
if (length(chr_to_num_cols) > 0) {
  features = features[, (chr_to_num_cols) := lapply(.SD, as.numeric), .SDcols = chr_to_num_cols] 
}
log_to_num_cols = colnames(rolling_predictors[, .SD, .SDcols = is.logical])
if (length(log_to_num_cols) > 0) {
  rolling_predictors = rolling_predictors[, (log_to_num_cols) := lapply(.SD, as.numeric), .SDcols = log_to_num_cols]  
}

# Remove duplicates
any_duplicates = any(duplicated(rolling_predictors[, .(symbol, date)]))
if (any_duplicates) rolling_predictors = unique(rolling_predictors, by = c("symbol", "date"))

# Basic cleaning with janitor
rolling_predictors = remove_constant(rolling_predictors, quiet = FALSE)
rolling_predictors = remove_empty(rolling_predictors, "cols", 0.33, FALSE)
rolling_predictors = remove_empty(rolling_predictors, "rows", 0.33, FALSE)
cols_features = setdiff(colnames(rolling_predictors), cols_non_features) 
# get_one_to_one(teest_) # cant figure put why this doesnt work
# teest_ = na.omit(ohlcv_predictors[, ..predictors][1:2000, .SD, .SDcols = 1:2]) 
# fwrite(teest_, "test.csv")
# dt = fread("https://snpmarketdata.blob.core.windows.net/qanda/test.csv")
# get_one_to_one(dt)

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
inf_cols = check_inf_col_proportion(rolling_predictors)
remove_cols = inf_cols[inf_proportion > 0.001]
change_cols = inf_cols[inf_proportion <= 0.001]
cols_features = setdiff(cols_features, remove_cols)
rolling_predictors[, (remove_cols[, vars]) := NULL]
rolling_predictors[, (change_cols[, vars]) := lapply(.SD, function(x) ifelse(is.infinite(x), NA, x)), 
                   .SDcols = change_cols[, vars]]
setnafill(rolling_predictors, type = "locf", cols = change_cols[, vars])

# Convert variables with low number of unique values to factors
int_numbers = na.omit(rolling_predictors[, ..cols_features])[, lapply(.SD, function(x) all(floor(x) == x))]
int_cols = colnames(rolling_predictors[, ..predictors])[as.matrix(int_numbers)[1,]]
factor_cols = rolling_predictors[, ..int_cols][, lapply(.SD, function(x) length(unique(x)))]
factor_cols = as.matrix(factor_cols)[1, ]
factor_cols = factor_cols[factor_cols <= 100]
rolling_predictors = rolling_predictors[, (names(factor_cols)) := lapply(.SD, as.factor), .SD = names(factor_cols)]

# Convert types
rolling_predictors = rolling_predictors[, names(.SD) := lapply(.SD, as.numeric), .SDcols = bit64::is.integer64]
rolling_predictors[, date := as.POSIXct(date, tz = "UTC")]


# Add predictors to atributes
data.table::setattr(rolling_predictors, "predictors", predictors)
attributes(rolling_predictors)

# Save and mannually upload to padobran
saveRDS(ohlcv_predictors, "D:/strategies/zoo/ohlcv_predictors.rds")

# Add to padobran
# scp /home/sn/data/equity/us/predictors_daily/pead_predictors/pead-predictors-20250203.csv padobran:/home/jmaric/peadml/predictors.csv





# Create target variable
ohlcv_predictors[, target_1m := shift(close, 1L, type = "lead") / close - 1, by = symbol]

# Remove missing targets
ohlcv_predictors = na.omit(ohlcv_predictors, cols = c("target_1m"))
