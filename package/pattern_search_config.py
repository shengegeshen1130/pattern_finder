class PatternSearchConfig:
    def __init__(self, ftr_config_file, eval_metric, min_support, eval_cutoff, dedup_cutoff, rank_priority, metric_config_file, gini_cutoff = 0.3, lvl_limit = 4, min_block_size = 0.01, overlap = 0.5,trunc_cutoff=1000000,trunc_cp_size=25):
        self.ftr_config_file = ftr_config_file
        self.eval_metric = eval_metric
        self.min_support = min_support
        self.eval_cutoff = eval_cutoff
        self.dedup_cutoff = dedup_cutoff
        self.rank_priority = rank_priority
        self.metric_config_file = metric_config_file
        self.gini_cutoff = gini_cutoff
        self.lvl_limit = lvl_limit
        self.min_block_size = min_block_size
        self.overlap = overlap
        self.trunc_cutoff = trunc_cutoff #the activity size cutoff to trigger truncat process
        self.trunc_cp_size = trunc_cp_size #the maximum activity size per cp to truncat
