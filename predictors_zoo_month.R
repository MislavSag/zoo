
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
