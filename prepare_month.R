library(finutils)
library(fastverse)
library(finfeatures)
library(mlr3)
library(paradox)
library(mlr3filters)
library(mlr3learners)
library(mlr3tuning)
library(mlr3extralearners)
library(mlr3torch)
library(mlr3pipelines)
library(mlr3finance)
library(janitor)
library(mlr3batchmark)
library(batchtools)


# Import daily data
prices = qc_daily(
  file_path   = "F:/lean/data/stocks_daily.csv",
  min_obs     = 1001,
  duplicates  = "fast",
  add_dv_rank = TRUE
)

# Add month column and EOM indecies
prices[, month := yearmon(date)]
prices[, eom := date == data.table::last(date), by = .(symbol, month)]
prices[eom == 1, .N] / nrow(prices) * 100

# Calculate features
prices_sample = prices[symbol %in% prices[, sample(unique(symbol), 50)]]
ohlcv_features = OhlcvFeaturesDaily$new(
  at = prices_sample[, which(eom == 1)],
  windows = c(5, 10, 22, 44, 66, 125, 252, 500, 1000),
  quantile_divergence_window = c(22, 44, 66, 125, 252, 500, 1000)
)
ohlcv_predictors = ohlcv_features$get_ohlcv_features(prices_sample)

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

# Create task
ids = c("symbol", "date", "month")
cols_ = c(ids, "target_1m", predictors)
task_zoo = as_task_regr(ohlcv_predictors[, ..cols_], id = "zoo_month", target = "target_1m")
task_zoo$set_col_roles("month", "group")
task_zoo$set_col_roles("date", "order")
task_zoo$col_roles$feature = setdiff(task_zoo$col_roles$feature, "symbol")


# MODEL -------------------------------------------------------------------
# Features filtering
filters_ = list(
  po("filter", flt("disr"), filter.nfeat = 5),
  po("filter", flt("jmim"), filter.nfeat = 5),
  po("filter", flt("jmi"), filter.nfeat = 5),
  po("filter", flt("mim"), filter.nfeat = 5),
  po("filter", flt("mrmr"), filter.nfeat = 5),
  po("filter", flt("njmim"), filter.nfeat = 5),
  po("filter", flt("cmim"), filter.nfeat = 5),
  if (!interactive()) po("filter", flt("carscore"), filter.nfeat = 5),
  if (!interactive()) po("filter", flt("information_gain"), filter.nfeat = 5),
  if (!interactive()) po("filter", filter = flt("relief"), filter.nfeat = 5)
  # po("filter", filter = flt("gausscov_f1st"), p0 = 0.01, step = 0.01, save = TRUE, filter.nfeat = 5)
  # po("filter", mlr3filters::flt("importance", learner = mlr3::lrn("classif.rpart")), filter.nfeat = 5, id = "importance_1"),
  # po("filter", mlr3filters::flt("importance", learner = lrn), filter.nfeat = 10, id = "importance_2")
)
filters_ = filters_[sapply(filters_, function(x) !is.null(x))]
graph_filters = gunion(filters_) %>>%
  po("featureunion", length(filters_), id = "feature_union_filters")
# test MLP learner
mlp_graph = po("torch_ingress_num") %>>%
  po("nn_linear", out_features = 20) %>>%
  po("nn_relu") %>>%
  po("nn_head") %>>%
  po("torch_loss", loss = t_loss("mse")) %>>%
  po("torch_optimizer", optimizer = t_opt("adam", lr = 0.1)) %>>%
  po("torch_callbacks", callbacks = t_clbk("history")) %>>%
  po("torch_model_regr", batch_size = 16, epochs = 50, device = "cpu")

# Main graph
graph =
  po("branch",
     options = c("nop_filter_target", "filter_target_select"),
     id = "filter_target_branch") %>>%
  gunion(list(
    po("nop", id = "nop_filter_target"),
    po("filterregrtarget")
  )) %>>%
  po("unbranch", id = "filter_target_unbranch") %>>% 
  # po("subsample") %>>% # uncomment this for hyperparameter tuning
  po("dropnacol", id = "dropnacol", cutoff = 0.05) %>>%
  po("dropna", id = "dropna") %>>%
  po("removeconstants", id = "removeconstants_1", ratio = 0)  %>>%
  po("fixfactors", id = "fixfactors") %>>%
  po("winsorize", lower = 0.01, upper = 0.99, na.rm = TRUE) %>>%
  po("removeconstants", id = "removeconstants_2", ratio = 0) %>>%
  po("dropcor") %>>%
  po("branch", options = c("uniformization", "scale"), id = "scale_branch") %>>%
  gunion(list(po("uniform"),
              po("scale")
  )) %>>%
  po("unbranch", id = "scale_unbranch") %>>%
  po("dropna", id = "dropna_v2") %>>%
  gunion(list(
    po("nop", id = "nop_union_pca"),
    po("pca", center = FALSE, rank. = 10),
    po("ica", n.comp = 10)
  )) %>>% po("featureunion", id = "feature_union_pca") %>>%
  graph_filters %>>%
  # po("branch", options = c("nop_interaction", "modelmatrix"), id = "interaction_branch") %>>%
  # gunion(list(
  #   po("nop", id = "nop_interaction"),
  #   po("modelmatrix", formula = ~ . ^ 2))) %>>%
  # po("unbranch", id = "interaction_unbranch") %>>%
  po("removeconstants", id = "removeconstants_3", ratio = 0) %>>%
  gunion(
    list(
      ranger  = lrn("regr.ranger", id = "ranger"),
      xgboost = lrn("regr.xgboost", id = "xgboost"),
      # bart    = lrn("regr.bart", id = "bart", sigest = 1),
      nnet    = lrn("regr.nnet", id = "nnet", MaxNWts = 50000),
      mlp     = mlp_graph
      # resnet  = lrn("regr.tab_resnet")
      # resnet  = lrn("regr.tab_resnet", epochs = 100, batch_size = 64,
      #               dropout1 = 0.1, dropout2 = 0.1, n_blocks = 2, d_block = 32),
      # Error in .__ParamSet__get_values(self = self, private = private, super = super,  : 
      # Missing required parameters: resnet.regr.tab_resnet.epochs, resnet.regr.tab_resnet.batch_size, resnet.regr.tab_resnet.dropout1, resnet.regr.tab_resnet.dropout2, resnet.regr.tab_resnet.n_blocks, resnet.regr.tab_resnet.d_block
    )
  )%>>%
  po("regravg")
plot(graph)
graph_learner = as_learner(graph)
graph_learner$impute_selected_features = TRUE

# Search space
gp = as.data.table(graph$param_set)
gausscov_sp = as.character(seq(0.2, 0.8, by = 0.1))
winsorize_sp =  c(0.999, 0.99, 0.98, 0.97, 0.90, 0.8)
gp[grepl("wins", id)]
search_space = ps(
  # subsample for hyperband
  # subsample.frac = p_dbl(0.7, 1, tags = "budget"), # commencement this if we want to use hyperband optimization
  
  # preprocessing
  filter_target_branch.selection = p_fct(levels = c("nop_filter_target", "filter_target_select")),
  filter_target.lower_percentile = p_dbl(0.1, 0.3),
  filter_target.upper_percentile = p_dbl(0.70, 0.90),
  dropcor.method = p_fct(c("pearson", "spearman", "kendall")),
  dropcor.cutoff = p_fct(c("0.80", "0.90", "0.95", "0.99"),
                          trafo = function(x, param_set) return(as.double(x))),
  winsorize.upper        = p_fct(as.character(winsorize_sp),
                                       trafo = function(x, param_set) return(as.double(x))),
  winsorize.lower        = p_fct(as.character(1-winsorize_sp),
                                       trafo = function(x, param_set) return(as.double(x))),
  scale_branch.selection = p_fct(c("uniformization", "scale")),
  # interaction_branch.selection = p_fct(levels = c("nop_interaction", "modelmatrix")),
  
  # models
  ranger.ranger.max.depth  = p_int(1, 15),
  ranger.ranger.replace    = p_lgl(),
  ranger.ranger.mtry.ratio = p_dbl(0.3, 1),
  ranger.ranger.num.trees  = p_int(10, 2000),
  ranger.ranger.splitrule  = p_fct(levels = c("variance", "extratrees")),
  xgboost.xgboost.alpha     = p_dbl(0.001, 100, logscale = TRUE),
  xgboost.xgboost.max_depth = p_int(1, 20),
  xgboost.xgboost.eta       = p_dbl(0.0001, 1, logscale = TRUE),
  xgboost.xgboost.nrounds   = p_int(1, 5000),
  xgboost.xgboost.subsample = p_dbl(0.1, 1),
  nnet.nnet.size  = p_int(lower = 2, upper = 15),
  nnet.nnet.decay = p_dbl(lower = 0.0001, upper = 0.1),
  nnet.nnet.maxit = p_int(lower = 50, upper = 500),
  mlp.torch_model_regr.batch_size = p_int(lower = 8, upper = 128),
  mlp.torch_optimizer.lr = p_dbl(lower = 1e-4, upper = 1e-1, logscale = TRUE),
  mlp.torch_model_regr.epochs = p_int(lower = 20, upper = 200)
  # resnet.regr.tab_resnet.batch_size = p_int(lower = 16, upper = 512),
  # resnet.regr.tab_resnet.opt.lr     = p_dbl(lower = 1e-5, upper = 1e-1, logscale = TRUE),
  # resnet.regr.tab_resnet.n_blocks   = p_int(lower = 1, upper = 5),
  # resnet.regr.tab_resnet.d_block    = p_int(lower = 32, upper = 128),
  # resnet.regr.tab_resnet.dropout1   = p_dbl(lower = 0.0, upper = 0.5),
  # resnet.regr.tab_resnet.dropout2   = p_dbl(lower = 0.0, upper = 0.5),
  # resnet.regr.tab_resnet.d_hidden   = p_int(lower = 2, upper = 64),
  # resnet.regr.tab_resnet.epochs     = p_int(lower = 20, upper = 200)
  # bart.bart.k      = p_dbl(lower = 1, upper = 10),
  # bart.bart.numcut = p_int(lower = 10, upper = 200),
  # bart.bart.ntree  = p_int(lower = 50, upper = 500)
  # Error in makeModelMatrixFromDataFrame(x.test, if (!is.null(drop)) drop else TRUE) : 
  #   when list, drop must have length equal to x
  # This happened PipeOp bart.bart's $predict()
)

# # Test graph
# if (interactive()) {
#   # show all combinations from search space, like in grid
#   sp_grid = generate_design_grid(search_space, 1)
#   sp_grid = sp_grid$data
#   sp_grid
# 
#   # help graph for testing preprocessing
#   task_ = task_zoo$clone()
#   nr = task_$nrow
#   rows_ = (nr-1000):nr
#   task_$filter(rows_)
#   gr_test = graph$clone()
#   gr_test$param_set$values$dropcor.cutoff = 0.8
#   gr_test$param_set$values$resnet.regr.tab_resnet.batch_size = 32
#   gr_test$param_set$values$resnet.regr.tab_resnet.epochs = 20
#   gr_test$param_set$values$resnet.regr.tab_resnet.dropout1 = 0.1
#   gr_test$param_set$values$resnet.regr.tab_resnet.dropout2 = 0.1
#   gr_test$param_set$values$resnet.regr.tab_resnet.n_blocks = 2
#   gr_test$param_set$values$resnet.regr.tab_resnet.d_block = 3
#   gr_test$param_set$values$resnet.regr.tab_resnet.d_hidden = 3
#   gr_test$param_set$values$interaction_branch.selection = "modelmatrix"
#   gr_test = as_learner(gr_test)
#   system.time({test_default = gr_test$train(task_)})
#   test_default$predict(task_)
#   
# }

# auto tuner rf
mlr_resamplings
at_graph = auto_tuner(
  tuner        = tnr("random_search"), # tnr("hyperband", eta = 3); terminator = trm("none")
  learner      = graph_learner,
  resampling   = rsmp("holdout_gap_ratio", gap = 1, ratio = 0.8),
  measure      = msr("regr.mse"),
  search_space = search_space,
  term_evals   = 1
  # callbacks    = 
)
at_graph$id = "finautoml"

# My callbacks for saving data
myclb = callback_resample(
  id = "capture_selected_features",
  on_resample_end = function(callback, context) {
    learner = context$learner
    
    # Get selected features if available
    sel_feats = NULL
    if ("selected_features" %in% learner$properties) {
      sel_feats = learner$selected_features()
    }
    
    # Store in data_extra
    context$data_extra = list(
      features = sel_feats
    )
  }
)

# For test, smaller sample
task_zoo_sample = task_zoo$clone()
task_zoo_sample = task_zoo_sample$filter(1:1000)

# Create design
mlr3::mlr_resamplings
design = benchmark_grid(
  tasks = task_zoo_sample,
  learners = list(at_graph),
  resamplings = rsmp("holdout_gap_ratio", gap = 1, ratio = 0.8),
)

# Benchmark
benchmark_res = benchmark(design, store_models = FALSE, callbacks = list(myclb))
