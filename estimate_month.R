library(data.table)
library(mlr3)
library(paradox)
library(mlr3filters)
library(mlr3learners)
library(mlr3tuning)
library(mlr3extralearners)
library(mlr3torch)
library(mlr3pipelines)
library(mlr3finance)
library(mlr3batchmark)
library(batchtools)



# DATA --------------------------------------------------------------------
# Import data
if (interactive()) {
  dt = readRDS("D:/strategies/zoo/ohlcv_predictors.rds")  
} else {
  dt = fread("ohlcv_predictors.csv")  
}

# Extract predictors
predictors = attr(dt, "predictors")

# Create task
ids = c("symbol", "date", "month")
cols_ = c(ids, "target_1m", predictors)
task_zoo = as_task_regr(dt[, ..cols_], id = "zoo_month", target = "target_1m")
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
)
at_graph$id = "finautoml"

# For test, smaller sample
task_zoo_sample = task_zoo$clone()
task_zoo_sample = task_zoo_sample$filter(1:1000)

# Create design
mlr3::mlr_resamplings
design = benchmark_grid(
  tasks = task_zoo_sample,
  learners = list(at_graph),
  resamplings = rsmp("gap_cv", initial_window = 322, horizon = 1, gap = 0, step = 1, rolling = FALSE),
)

# exp dir
if (interactive()) {
  dirname_ = "experiments_pead_test"
  if (dir.exists(dirname_)) system(paste0("rm -r ", dirname_))
} else {
  dirname_ = "experiments_pead"
  if (dir.exists(dirname_)) system(paste0("rm -r ", dirname_))
}

# Create registry
packages = c("data.table", "gausscov", "paradox", "mlr3", "mlr3pipelines",
             "mlr3tuning", "mlr3misc", "future", "future.apply",
             "mlr3extralearners", "stats", "mlr3finance")
reg = makeExperimentRegistry(file.dir = dirname_, seed = 1, packages = packages)

# Populate registry with problems and algorithms to form the jobs
batchmark(design, reg = reg)

# Save registry
saveRegistry(reg = reg)

# If localy, try to execute one job. If not create file
if (interactive()) {
  ids = findNotDone(reg = reg)
  cf = makeClusterFunctionsSocket(ncpus = 2L)
  reg$cluster.functions = cf
  saveRegistry(reg = reg)
  
  # define resources and submit jobs
  resources = list(ncpus = 2, memory = 8000)
  submitJobs(ids = ids$job.id[1], resources = resources, reg = reg)
  
  # Check result
  res_ = reduceResultsBatchmark(ids = 1:2, store_backends = FALSE, reg = reg)
  res_dt = as.data.table(res_)
  res_dt$prediction
  predictions = lapply(res_dt$prediction, as.data.table)
  predictions = rbindlist(predictions)
  predictions[, sum(sign(truth) == sign(response)) / nrow(predictions)]
  predictions[response > 0, mean(truth)]
  predictions[response < 0.03, mean(truth)]
  res_$score(msr("mean_absolute_directional_loss"))
  res_$score(msr("regr.mse"))
  res_$score(msr("regr.mae"))
  
  # Important features
  files = list.files("gausscov_f1", full.names = TRUE)
  tasks_ = gsub("gausscov_f1-|-\\d+\\.rds", "", basename(files))
  tasks_unique = unique(tasks_)
  gausscov_tasks = list()
  for (i in seq_along(tasks_unique)) {
    task_ = tasks_unique[i]
    print(task_)
    files_ = files[which(tasks_ == task_)]
    gausscov_tasks[[i]] = unlist(lapply(files, function(x) {
      names(head(sort(readRDS(x), decreasing = TRUE), 5))
    }))
  }
  names(gausscov_tasks) = tasks_unique
  best_vars = lapply(seq_along(gausscov_tasks), function(i) {
    x = gausscov_tasks[[i]]
    cbind.data.frame(task = names(gausscov_tasks)[i], 
                     as.data.table(head(sort(table(x), decreasing = TRUE), 10)))
  })
  best_vars = rbindlist(best_vars)
} else {
  # create sh file
  sh_file = sprintf("
#!/bin/bash

#PBS -N PEAD
#PBS -l ncpus=4
#PBS -l mem=30GB
#PBS -J 1-%d
#PBS -o %s/logs
#PBS -j oe

cd ${PBS_O_WORKDIR}
apptainer run image.sif run_job.R 0 %s
", nrow(design), dirname_, dirname_)
  sh_file_name = "padobran.sh"
  file.create(sh_file_name)
  writeLines(sh_file, sh_file_name)
}
