# Databricks notebook source
# MAGIC %pip install --force-reinstall /dbfs/FileStore/odap_widgets-1.0.0-py3-none-any.whl

# COMMAND ----------

token = dbutils.secrets.get(scope="key-vault", key="dbx-token")

# COMMAND ----------

from odap.main import show

# COMMAND ----------

res = show(
    show_display=False,
    features_table="sb_odap_feature_store.upsell_leads_data",
    segments_table="hive_metastore.odap_app_main.segments",
    destinations_table="hive_metastore.odap_app_main.destinations",
    #data_path='["dbfs:/user/hive/warehouse/sb_odap_feature_store.db/union_data_non_delta"]',
    #data_path='["abfss://featurestore@odapczlakeg2dev.dfs.core.windows.net/latest/client.delta"]',
    data_path='["dbfs:/tmp/sb/sb_odap_feature_store/datasets/data_for_monitoring.delta"]',
    databricks_host="https://adb-8406481409439422.2.azuredatabricks.net",
    databrics_token=str(token),
    cluster_id="0511-092604-a9jb3hrb",
    notebook_path=(
        "/Repos/persona/skeleton-databricks/src/__myproject__/_export/p360_export"
    ),
    lib_version="0.1.3",
    stats_table="sb_odap_feature_store.stats_with_label",
)

# COMMAND ----------

# MAGIC %md 
# MAGIC # How to use Quick Insights

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Display saved segments

# COMMAND ----------

conditions = spark.sql("select * from hive_metastore.odap_app_main.segments")
display(conditions)

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Select target group and base

# COMMAND ----------

import pyspark.sql.functions as F
base_id = "139cb633-fce9-4398-bd08-fee0d6db3bc5" # "5c062c64-9e86-40ee-be30-b9856d123b12"
target_id = "5909a9c9-5b78-4e83-941f-e2bbc8db4c03" # "96e7e123-0a2f-4c0a-87cc-c480d6cf2e12"
base_conds_ = (conditions
               .filter(F.col("id")==base_id)
              ).collect()
target_conds_ = (conditions
                 .filter(F.col("id")==target_id)
                ).collect()

base_conds = [item.conditions for item in base_conds_]
target_conds = [item.conditions for item in target_conds_]

conds = [base_conds[0], target_conds[0]]

# COMMAND ----------

conds = ['(label == 1)', '(label == 0)']

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run quick isnights

# COMMAND ----------

res.set_condition(obs_cond=conds[0],base_cond=conds[1])
res.get_qi()

# COMMAND ----------

# MAGIC %md
# MAGIC # deprecated

# COMMAND ----------

stats = spark.sql('select * from feature_store_stats').collect()
stats = [item.asDict() for item in stats]
stats = {item['feature']:item for item in stats}

# COMMAND ----------

vars_ = spark.sql(
    "describe table hive_metastore.dev_odap_feature_store_small.features_client_latest"
).collect()

numerical_vars = [
    var.col_name
    for var in vars_
    if var.data_type == 'double' or var.data_type == 'bigint'
]
# Integer vars are all variables which have integer values. Howwever, we need to split them into interval integer variables
# and not integer interval variables. The reason behind this logic is that we can consider the integer variables with the lot
# of distinct values as numerical variables. But not in the all cases. For example, computing the counts in the some intervals, etc.
integer_vars = [
    var.col_name
    for var in vars_
    if var.data_type == 'int'
]

# Categorical variables are all variables which have string values.
categorical_vars = [
    var.col_name
    for var in vars_
    if var.data_type == 'string'
]

categorical_vars = [item for item in categorical_vars if not 'id' in item.lower()]

# Binary variables are all variables which has 0-1 integer values.
binary_vars = [
    var.col_name
    for var in vars_
    if var.data_type == 'boolean']


# COMMAND ----------

import pandas as pd
def compute_statistics(path_to_feature_store:str, save_table_name: str):
    vars_ = spark.sql(
    f"describe table {path_to_feature_store}"
    ).collect()

    df = spark.sql("select * from hive_metastore.dev_odap_feature_store_small.features_client_latest")

    numerical_vars = [
        var.col_name
        for var in vars_
        if var.data_type == 'double'
    ]
    # Integer vars are all variables which have integer values. Howwever, we need to split them into interval integer variables
    # and not integer interval variables. The reason behind this logic is that we can consider the integer variables with the lot
    # of distinct values as numerical variables. But not in the all cases. For example, computing the counts in the some intervals, etc.
    integer_vars = [
        var.col_name
        for var in vars_
        if var.data_type == 'int' or var.data_type == 'bigint'
    ]

    # Categorical variables are all variables which have string values.
    categorical_vars = [
        var.col_name
        for var in vars_
        if var.data_type == 'string'
    ]

    # Binary variables are all variables which has 0-1 integer values.
    binary_vars = [
        var.col_name
        for var in vars_
        if var.data_type == 'boolean']


    numerical_features_statistics = df.select(
        *[f.max(col).alias(f"{col}_max") for col in numerical_vars],
        *[f.min(col).alias(f"{col}_min") for col in numerical_vars],
        *[f.avg(col).alias(f"{col}_avg") for col in numerical_vars],
        *[f.count(f.when(f.col(col).isNull(), col)).alias(f"{col}_cnt_nulls") for col in numerical_vars],
        *[f.count(f.when(f.col(col).isNotNull(), col)).alias(f"{col}_cnt_not_nulls") for col in numerical_vars],
    )

    integer_features_statistics = df.select(
        *[f.max(col).alias(f"{col}_max") for col in integer_vars],
        *[f.min(col).alias(f"{col}_min") for col in integer_vars],
        *[f.avg(col).alias(f"{col}_avg") for col in integer_vars],
        *[f.count(f.when(f.col(col).isNull(), col)).alias(f"{col}_cnt_nulls") for col in integer_vars],
        *[f.count(f.when(f.col(col).isNotNull(), col)).alias(f"{col}_cnt_not_nulls") for col in integer_vars],
    )

    binary_features_statistics = df.select(
        *[f.max(f.col(col).cast("byte")).alias(f"{col}_max") for col in binary_vars],
        *[f.min(f.col(col).cast("byte")).alias(f"{col}_min") for col in binary_vars],
        *[f.avg(f.col(col).cast("byte")).alias(f"{col}_avg") for col in binary_vars],
        *[f.count(f.when(f.col(col).isNull(), col)).alias(f"{col}_cnt_nulls") for col in binary_vars],
        *[f.count(f.when(f.col(col).isNotNull(), col)).alias(f"{col}_cnt_not_nulls") for col in binary_vars],
    )

    categorical_features_statistics = df.select(
        *[f.countDistinct(col).alias(f"{col}_distinct_count") for col in categorical_vars],
        *[f.collect_set(col).alias(f"{col}_distinct_values") for col in categorical_vars],
        *[f.count(f.when(f.col(col).isNull(), col)).alias(f"{col}_cnt_nulls") for col in categorical_vars],
        *[f.count(f.when(f.col(col).isNotNull(), col)).alias(f"{col}_cnt_not_nulls") for col in categorical_vars],
    )

    numerical_statistics_rows = numerical_features_statistics.collect()
    integer_statistics_rows = integer_features_statistics.collect()
    binary_statistics_rows = binary_features_statistics.collect()
    categorical_statistics_rows = categorical_features_statistics.collect()

    numerical_statistics_dict = numerical_statistics_rows[0].asDict() if numerical_statistics_rows else {}
    integer_statistics_dict = integer_statistics_rows[0].asDict() if integer_statistics_rows else {}
    binary_statistics_dict = binary_statistics_rows[0].asDict() if binary_statistics_rows else {}
    categorical_statistics_dict = categorical_statistics_rows[0].asDict() if categorical_statistics_rows else {}

    statistics = {}

    for col in numerical_vars:
        statistics[col] = {
            "min_value": numerical_statistics_dict[f"{col}_min"],
            "max_value": numerical_statistics_dict[f"{col}_max"],
            "avg_value": numerical_statistics_dict[f"{col}_avg"],
            "null_count": numerical_statistics_dict[f"{col}_cnt_nulls"],
            "not_null_count": numerical_statistics_dict[f"{col}_cnt_not_nulls"],
        }

    for col in integer_vars:
        statistics[col] = {
            "min_value": integer_statistics_dict[f"{col}_min"],
            "max_value": integer_statistics_dict[f"{col}_max"],
            "avg_value": integer_statistics_dict[f"{col}_avg"],
            "null_count": integer_statistics_dict[f"{col}_cnt_nulls"],
            "not_null_count": integer_statistics_dict[f"{col}_cnt_not_nulls"],
        }

    for col in binary_vars:
        statistics[col] = {
            "min_value": binary_statistics_dict[f"{col}_min"],
            "max_value": binary_statistics_dict[f"{col}_max"],
            "avg_value": binary_statistics_dict[f"{col}_avg"],
            "null_count": binary_statistics_dict[f"{col}_cnt_nulls"],
            "not_null_count": binary_statistics_dict[f"{col}_cnt_not_nulls"],
        }

    for col in categorical_vars:
        statistics[col] = {
            "distinct_values_count": categorical_statistics_dict[f"{col}_distinct_count"],
            "distinct_values": categorical_statistics_dict[f"{col}_distinct_values"],
            "null_count": categorical_statistics_dict[f"{col}_cnt_nulls"],
            "not_null_count": categorical_statistics_dict[f"{col}_cnt_not_nulls"],
        }
    
    temp = pd.DataFrame.from_records(statistics).T
    temp['feature'] = temp.index.values
    temp = temp.reset_index(drop=True)
    temp = temp[['feature','min_value','max_value','avg_value','null_count','not_null_count','distinct_values_count','distinct_values']]
    df = spark.createDataFrame(temp)
    
    df.write.saveAsTable(save_table_name)

# COMMAND ----------

# DBTITLE 1,QiApi class
from abc import get_cache_token
from odap.api.functions import check_active
from typing import Tuple, List
from scipy.stats import f, chi2_contingency
import numpy as np
import pyspark.sql.functions as F

# 

class QiApi:

    def __init__(self, features_table,spark,stats) -> None:
        self.features_table = features_table
        self.base_cond = ''
        self.obs_cond = ''
        self.active=True
        self.spark = spark
        self.stats_table = stats

        (
            self.numerical_vars,
            self.integer_vars,
            self.categorical_vars,
            self.binary_vars
        ) = self.get_variables()

    @check_active
    def set_condition(self, base_cond: str='', obs_cond: str='') -> None:

            self.base_cond = f' WHERE {base_cond}' if base_cond else ''
            self.obs_cond = f'WHERE {obs_cond}' if obs_cond else ''
            
            print("Condition for the base group is:",self.base_cond)
            print("Condition for the target group is:",self.obs_cond)

    @check_active
    def get_counts(self) -> dict:

        """
        Compute number of samples for target and base group.
        """
        cts = {}
        if not self.obs_cond and not self.base_cond:
            cts = self.spark.sql(
                f"select count(*) as base_cts, count(*) as obs_cts, count(*) as n from {self.features_table}"
            ).collect()[0].asDict()
        
        else:
            
            cts['base_cts'] = self.spark.sql(
                    f"select count(*) as cts from {self.features_table} {self.base_cond}"
                ).collect()[0].cts
            cts['obs_cts'] = self.spark.sql(
                    f"select count(*) as cts from {self.features_table} {self.obs_cond}"
                ).collect()[0].cts
            cts['n'] = cts['base_cts'] + cts['obs_cts']
        
        return cts


    @check_active
    def get_variables(self) -> Tuple[list,list,list,list]:

        """
        Get a list variables per data type.
        """

        vars_ = self.spark.sql(
            f"describe table {self.features_table}"
        ).collect()

        numerical_vars = [
            var.col_name
            for var in vars_
            if var.data_type == 'double' or var.data_type == 'bigint'
        ]

        # Integer vars are all variables which have integer values. Howwever, we need to split them into interval integer variables
        # and not integer interval variables. The reason behind this logic is that we can consider the integer variables with the lot
        # of distinct values as numerical variables. But not in the all cases. For example, computing the counts in the some intervals, etc.
        
        # TODO: 
        integer_vars = [
            var.col_name
            for var in vars_
            if var.data_type == 'int' and 'flag' not in var.col_name
        ]

        # Categorical variables are all variables which have string values.
        categorical_vars = [
            var.col_name
            for var in vars_
            if var.data_type == 'string'
        ]

        # Binary variables are all variables which has 0-1 integer values.
        binary_vars = [
            var.col_name
            for var in vars_
            if var.data_type == 'boolean' or 'flag' in var.col_name
        ]

        return numerical_vars, integer_vars, categorical_vars,binary_vars
    

    @check_active
    def get_numerical_total_averages(self) -> dict:

        """
        Compute a mean for all numerical variables and for base and target groups.
        """

        numerical_vars = self.numerical_vars.copy()
        numerical_vars.extend(self.integer_vars)
        
        numerical_vars = ','.join([f"avg({numeric}) as avg_{numeric}" for numeric in numerical_vars])
        return self.spark.sql(
            f"select {numerical_vars} from {self.features_table}"
        ).collect()[0].asDict()
    
    @check_active
    def get_group_averages(self) -> Tuple[dict,dict]:

        """
        Compute a mean for all numerical variables and for base and target groups.
        """
        
        numerical_vars = self.numerical_vars.copy()
        numerical_vars.extend(self.integer_vars)

        numerical_vars_base = ','.join([f"avg({numeric}) as avg_base_{numeric}" for numeric in numerical_vars])
        numerical_vars_obs = ','.join([f"avg({numeric}) as avg_obs_{numeric}" for numeric in numerical_vars])
        
        res_base = self.spark.sql(
            f"select {numerical_vars_base} from {self.features_table} {self.base_cond}"
        ).collect()[0].asDict()

        res_obs = self.spark.sql(
            f"select {numerical_vars_obs} from {self.features_table} {self.obs_cond}"
        ).collect()[0].asDict()

        return res_base,res_obs
    
    @check_active
    def get_group_sample_variances(self) -> Tuple[dict,dict]:

        """
        Calculate an overal means for each numerical variable in the chosen base and target groups.
        """
        numerical_vars = self.numerical_vars.copy()
        numerical_vars.extend(self.integer_vars)

        numerical_vars_base = ','.join([f"var_samp({numeric}) as var_base_{numeric}" for numeric in numerical_vars])
        numerical_vars_obs = ','.join([f"var_samp({numeric}) as var_obs_{numeric}" for numeric in numerical_vars])
        
        
        group_vars_base = self.spark.sql(
            f"select {numerical_vars_base} from {self.features_table} {self.base_cond}"
        ).collect()[0].asDict()

        group_vars_obs = self.spark.sql(
            f"select {numerical_vars_obs} from {self.features_table} {self.obs_cond}"
        ).collect()[0].asDict()

        return group_vars_base,group_vars_obs

    @check_active
    def anova(self) -> Tuple[dict,dict,dict]:

        """
        This functions calculate a one-way anova for each variable and for base and target group.
        Then, the result F statistics for each variable is sorted by the significance and the p-value.
        Finally, based on the mentioned order, the distances between base group mean and target group mean are
        calculated for the measure how the distribution is different between two groups. The almost same groups
        will be always at the end.
        """
    
        cts = self.get_counts()

        (
            group_vars_base,
            group_vars_obs,
        ) = self.get_group_sample_variances()

        (
            group_means_base,
            group_means_obs
        ) = self.get_group_averages()

        numerical_vars = self.numerical_vars.copy()
        numerical_vars.extend(self.integer_vars)

        #numerical_vars_stat = ','.join([f"min({numeric}) as min_{numeric}, max({numeric}) as max_{numeric}" for numeric in numerical_vars])
        
        # TODO: Zmenit tuto kokotinku
        stats = spark.sql(f'select * from {self.stats_table}').collect()
        stats = [item.asDict() for item in stats]
        stats = {item['feature']:item for item in stats}
        

        f_stats_aux = {}
        p_vals = {}

        for var in numerical_vars:
            if var in stats.keys(): 
                stats_aux = stats[var]

                group_base_var = (
                    group_vars_base[f"var_base_{var}"]
                    if group_vars_base.get(f"var_base_{var}", 0)
                    else 0
                )
                group_obs_var = (
                    group_vars_obs[f"var_obs_{var}"]
                    if group_vars_obs.get(f"var_obs_{var}")
                    else 0
                )

                gr_mean_base = (
                    group_means_base[f"avg_base_{var}"] if group_means_base.get(f"avg_base_{var}") else 0
                )
                gr_mean_obs = (
                    group_means_obs[f"avg_obs_{var}"] if group_means_obs.get(f"avg_obs_{var}") else 0
                )
                group_base_overall_diff = (
                    cts["base_cts"]
                    * (gr_mean_base - (stats_aux["avg_value"] if stats_aux["avg_value"] else 0)) ** 2
                )
                group_obs_overall_diff = (
                    cts["obs_cts"]
                    * (gr_mean_obs - (stats_aux["avg_value"] if stats_aux["avg_value"] else 0)) ** 2
                )

                sse = (cts["base_cts"] - 1) * group_base_var + (cts["obs_cts"] - 1) * group_obs_var
                sse /= cts["n"] - 2 
                ssr = group_base_overall_diff + group_obs_overall_diff
                # TODO: premysliet to delenie 0.1 ci to dava zmysel
                F = ssr / (sse if sse else 0.1)

                f_stats_aux[var] = F

                p_vals[var] = 1 - f.cdf(F, 1, cts["n"] - 2)

                sorted_by_p = dict(sorted(p_vals.items(), key=lambda item: item[1]))

                f_stat = {}
                distances = {}

                for k, v in sorted_by_p.items():

                    stats_aux = stats[k]

                    gr_mean_base = (
                        group_means_base[f"avg_base_{k}"] if group_means_base.get(f"avg_base_{k}") else 0
                    )
                    gr_mean_obs = (
                        group_means_obs[f"avg_obs_{k}"] if group_means_obs.get(f"avg_obs_{k}") else 0
                    )

                    mean_scaled_base = (gr_mean_base - stats_aux['min_value']) / (
                        stats_aux['max_value'] - stats_aux['min_value'] 
                        if stats_aux['max_value'] - stats_aux['min_value'] > 0
                        else 1
                    )

                    mean_scaled_obs = (gr_mean_obs - stats_aux['min_value']) / (
                        stats_aux['max_value'] - stats_aux['min_value'] 
                        if stats_aux['max_value'] - stats_aux['min_value']  > 0
                        else 1
                    )

                    distances[k] = (
                        mean_scaled_obs - mean_scaled_base
    #                     if sorted_by_p[k] <= 0.05
    #                     else sorted_by_p[k]
                    )

                    f_stat[k] = f_stats_aux[k]

            return distances, f_stat, sorted_by_p
    
    @check_active
    def get_contigency_tables(self) -> dict:

        cat_bin_vars = self.categorical_vars.copy()
        cat_bin_vars.extend(self.binary_vars)

        cont_tables = {}
        
        # TODO: backlog znova
        # vybrat kategoricke ktore maju menej ako 1000 kategorii
        for var in cat_bin_vars:
            if not self.base_cond and not self.obs_cond:
                res = self.spark.sql(
                    f"""
                    (select case when 1 == 1 then 1 end as cat_target_base, {var}, count(*) from {self.features_table} sfm group by cat_target_base, {var} order by {var}) 
                    union 
                    (select case when 0 == 0 then 0 end as cat_target_base, {var}, count(*)  from {self.features_table} sfm group by cat_target_base, {var} order by {var})
                    """
                ).collect()
            else:
                res = self.spark.sql(
                    f"""
                    (select 1 as cat_target_base, {var}, count(*) from {self.features_table} {self.base_cond} group by cat_target_base, {var} order by {var}) 
                    union 
                    (select 0 as cat_target_base, {var}, count(*) from {self.features_table} {self.obs_cond} group by cat_target_base, {var} order by {var})
                    """
                ).collect()
            
            cont_tables[f"cont_{var}"] = [item.asDict() for item in res]
            for i in range(len(cont_tables[f'cont_{var}'])):
                if cont_tables[f"cont_{var}"][i][var] is None:
                    cont_tables[f"cont_{var}"][i][var] = ""
            
        return cont_tables
    
    @check_active
    def sanitize_counts(self) -> dict:

        """
        This function makes an equal length rows in the contigency table. More preciselly,
        if there are some missing categories in contigency table for base or target group,
        these missing counts are replaced with zeros. Furthermore, we need ensure, that
        there are no zero columns, i.e there are no zeros counts for both variables - in this
        case the test will fail.
        """
        
        # TODO: pridat unikatne hodnoty

        cont_tables = self.get_contigency_tables()
        cont_cts = {}
        
        for k in cont_tables.keys():

            temp = []
            # get the unique values in categorical variable
            unique_vals = [
                item[k.replace('cont_','')] for item in cont_tables[f'{k}'] if item[k.replace('cont_','')] != ""
            ] if not k.replace('cont_','') in self.binary_vars else [0,1]

            unique_vals = np.unique(unique_vals).tolist()
            # iterate over base (0) and target (1) group and get counts for each category.
            for i in [0, 1]:
                temp.append(
                    [
                        item.get("count(1)", 0)
                        for item in cont_tables[k]
                        if item["cat_target_base"] == i
                    ]
                )

            # check if there is any blank values
            check_empty = sum(
                [
                    True if item[k.replace("cont_", "")] == "" else False
                    for item in cont_tables[k]
                ]
            )

            # add the blank values into the unique vals.
            n_unique_vals = len(unique_vals) + 1 if check_empty > 0 else len(unique_vals)

            # If there are some missing category value, add 0
            cont_cts_aux = [
                np.pad(item, (0, abs(n_unique_vals - len(item)))).tolist()
                if len(item) < n_unique_vals
                else item
                for item in temp
            ]

            # ad hoc post processing - ensures that there will be same number of columns for base and target group.
            if len(cont_cts_aux[0]) > len(cont_cts_aux[1]):
                cont_cts_aux[1] = np.pad(
                    cont_cts_aux[1], (0, len(cont_cts_aux[0]) - len(cont_cts_aux[1]))
                ).tolist()
            elif len(cont_cts_aux[0]) < len(cont_cts_aux[1]):
                cont_cts_aux[0] = np.pad(
                    cont_cts_aux[0], (0, len(cont_cts_aux[1]) - len(cont_cts_aux[0]))
                ).tolist()

            cont_cts[k] = cont_cts_aux
        
        return cont_cts

    @check_active
    def apply_cat_variable_test(self) -> Tuple[dict,dict,dict]:

        """
        Calculate Chi square test on the contigency tables.
        """

        # compute chi square test and distances
        
        # TODO: zamysliet sa ci vobec toto chainovanie ma takto zmysel
        cont_cts = self.sanitize_counts()
        
        p_vals_cat = {}
        chi_stat = {}

        for k in cont_cts.keys():
            # TODO: vymazat tuto blbost
            cells_sum = [x + y for x, y in zip(cont_cts[k][0], cont_cts[k][1])]
            idx = np.where(np.array(cells_sum) == 0)

            # We need remove the columns which includes only zeros for both base and target group

            #self.removed_idx[k + "_removed_cats"] = idx[0]
            if len(idx[0] > 0):
                for _ in range(len(idx[0])):
                    # [item.pop(idx[0][0]) for item in cont_cts[k]]
                    [item.remove(0) for item in cont_cts[k]]
            
            
            chi, p, dof, exp = chi2_contingency(cont_cts[k])
            p_vals_cat[k] = p
            chi_stat[k] = chi
        sorted_p = dict(sorted(p_vals_cat.items(), key=lambda item: item[1]))

        distances_cat = {}
        chi = {}

        for k in sorted_p.keys():
            n_row = np.sum(cont_cts[k], axis=0)
            print("n_row",n_row)
            props = (np.array(cont_cts[k]) / n_row).tolist()
            distances_cat[k.replace("cont_", "")] = sum(
                [abs(x - y) for x, y in zip(props[0], props[1])]
            )
            chi[k.replace("cont_", "")] = chi_stat[k]

        return distances_cat, chi, sorted_p
    
    @check_active
    def get_qi(self) -> List[dict]:

#         (
#             group_vars_base,
#             group_vars_obs,
#         ) = self.get_group_sample_variances()

#         (
#             group_means_base,
#             group_means_obs
#         ) = self.get_group_averages()
        
        (
            distances, 
            f_stat, 
            sorted_by_p,
        ) = self.anova()
        
        
        (
            distances_cat, 
            chi, 
            sorted_by_p_cat   
        ) = self.apply_cat_variable_test()

        distances.update(distances_cat)
        stat = f_stat.copy()
        stat.update(chi)
        sorted_by_p.update(sorted_by_p_cat)

        qi = []

        numerical_vars = self.numerical_vars.copy()
        numerical_vars.extend(self.integer_vars)

        for var in numerical_vars:

            group_means_obs[f"avg_obs_{var}"] = float(
                group_means_obs[f"avg_obs_{var}"] if group_means_obs[f"avg_obs_{var}"] else 0
            )
            group_means_base[f"avg_base_{var}"] = float(
                group_means_base[f"avg_base_{var}"] if group_means_base[f"avg_base_{var}"] else 0
            )
            qi.append(
                {
                    "id": var,
                    "valueObs": round(group_means_obs[f"avg_obs_{var}"], 2),
                    "valueBase": round(group_means_base[f"avg_base_{var}"], 2),
                    # "true_rate": float(
                    #     round(
                    #         self.set_rate(
                    #             [whole[f"obs_{i}_mean"], whole[f"base_{i}_mean"]]
                    #         ),
                    #         ROUND_DIGITS,
                    #     )
                    # ),
                    "rate": stat[var] if stat.get(var) and not np.isnan(stat[var]) else -1000,
                    "distances": distances[var]
                    if distances.get(var) and not np.isnan(distances[var])
                    else -1000,
                    "obvious": False,
                    "significant": bool(sorted_by_p[var] <= 0.05) if sorted_by_p.get(var) else False,
                }
            )


        cat_bin_vars = self.categorical_vars.copy()
        cat_bin_vars.extend(self.binary_vars)
        
        for var in cat_bin_vars:
            qi.append(
                {
                    "id": var,
                    "valueObs": None,
                    "valueBase": None,
                    "true_rate": 0,
                    "rate": stat[var] if stat.get(var) and not np.isnan(stat[var]) else -1000,
                    "distances": distances[var]
                    if distances.get(var) and not np.isnan(distances[var])
                    else -1000,
                    "obvious": False,
                    "significant": bool(sorted_by_p[var] <= 0.05) if sorted_by_p.get(var) else False,
                }
            )
        
        qi = sorted(
                    qi, key=lambda d: (d["significant"], d["distances"]), reverse=True
                )

        return qi

# COMMAND ----------

# df = spark.sql('select * from hive_metastore.dev_odap_feature_store_small.features_client_latest')
# conditions = spark.sql("select * from hive_metastore.odap_app_main.segments")

# qi = QiApi(features_table="hive_metastore.dev_odap_feature_store_small.features_client_latest", spark=spark, stats="feature_store_stats")

res.set_condition(obs_cond=conds[0],base_cond=conds[1])
res.get_qi()

# COMMAND ----------

len(stats.keys())

# COMMAND ----------

qi.set_condition(obs_cond=target_conds[0], base_cond=base_conds[0])
qi.get_qi()

# COMMAND ----------



# COMMAND ----------

spark.sql(
                f"select 1 as cts,Profile_Demographic_Gender, count(*) as cts  from man_city_feature_store where 1 < 0 group by Profile_Demographic_Gender"
            ).collect()

# COMMAND ----------

from odap.main import show

# COMMAND ----------

vars_ = spark.sql(
    "describe table man_city_feature_store"
)

# COMMAND ----------

vars_[0].data_type

# COMMAND ----------

vars_ = spark.sql(
    "describe table man_city_feature_store"
)

numerical_vars = [
    var.col_name
    for var in vars_
    if var.data_type == 'double'
]
ca
# Integer vars are all variables which have integer values. Howwever, we need to split them into interval integer variables
# and not integer interval variables. The reason behind this logic is that we can consider the integer variables with the lot
# of distinct values as numerical variables. But not in the all cases. For example, computing the counts in the some intervals, etc.
integer_vars = [
    var.col_name
    for var in vars_
    if var.data_type == 'int'
]

# Categorical variables are all variables which have string values.
categorical_vars = [
    var.col_name
    for var in vars_
    if var.data_type == 'string'
]

# Binary variables are all variables which has 0-1 integer values.
binary_vars = [
    var.col_name
    for var in vars_
    if var.data_type == 'boolean']

# Identifying intervanul variables. We can identify the interval variables with help of its bins. If the difference
# between bins is more than 1, we can assume that this variable is interval variable.
# interval_vars = []
# for int_var in integer_vars:
#     cum_diff = []
#     for i in range(1, len(self.bins[int_var])):
#         res = float(self.bins[int_var][i]) - float(self.bins[int_var][i - 1])
#         cum_diff.append(res)

#     if abs(len(self.bins[int_var]) - sum(cum_diff)) > 1:
#         self.interval_vars.append(int_var)


# COMMAND ----------

cts = spark.sql(
    "select count(*) as base_cts, count(*) as obs_cts, count(*) as n from man_city_feature_store"
).collect()[0].asDict()
cts

# COMMAND ----------

numerical_vars.extend(integer_vars)
numerical_vars = ','.join([f"min({numeric}) as min_{numeric}, max({numeric}) as max_{numeric}" for numeric in numerical_vars])
stats = spark.sql(
    f"select {numerical_vars} from man_city_feature_store"
    ).collect()[0].asDict()
#res = {f"{k.replace('(','').replace(')','').replace('avg','avg_')}":v for k,v in res.items()}
stats

# COMMAND ----------

numerical_vars.extend(integer_vars)
numerical_vars = ','.join([f"avg({numeric}) as avg_{numeric}" for numeric in numerical_vars])
means = spark.sql(
    f"select {numerical_vars} from man_city_feature_store"
    ).collect()[0].asDict()
#res = {f"{k.replace('(','').replace(')','').replace('avg','avg_')}":v for k,v in res.items()}
means

# COMMAND ----------

numerical_vars.extend(integer_vars)
numerical_vars_base = ','.join([f"avg({numeric}) as avg_base_{numeric}" for numeric in numerical_vars])
numerical_vars_obs = ','.join([f"avg({numeric}) as avg_obs_{numeric}" for numeric in numerical_vars])

group_means_base = spark.sql(
    f"select {numerical_vars_base} from man_city_feature_store"
).collect()[0].asDict()

group_means_obs = spark.sql(
    f"select {numerical_vars_obs} from man_city_feature_store"
).collect()[0].asDict()


# COMMAND ----------

numerical_vars_base

# COMMAND ----------

conds[0]

# COMMAND ----------

numerical_vars.extend(integer_vars)
numerical_vars_base = ','.join([f"var_samp({numeric}) as var_base_{numeric}" for numeric in numerical_vars])
numerical_vars_obs = ','.join([f"var_samp({numeric}) as var_obs_{numeric}" for numeric in numerical_vars])

group_vars_base = spark.sql(
    f"select {numerical_vars_base} from hive_metastore.dev_odap_feature_store_small.features_client_latest where {conds[0]}"
).collect()[0].asDict()

group_vars_obs = spark.sql(
    f"select {numerical_vars_obs} from hive_metastore.dev_odap_feature_store_small.features_client_latest"
).collect()[0].asDict()

# COMMAND ----------

from scipy.stats import f, chi2_contingency

#numerical_vars.extend(integer_vars)

f_stats_aux = {}
p_vals = {}

for var in numerical_vars:
    group_base_var = (
        group_vars_base[f"var_base_{var}"]
        if group_vars_base.get(f"var_base_{var}", 0)
        else 0
    )
    group_obs_var = (
        group_vars_obs[f"var_obs_{var}"]
        if group_vars_obs.get(f"var_obs_{var}")
        else 0
    )

    gr_mean_base = (
        group_means_base[f"avg_base_{var}"] if group_means_base.get(f"avg_base_{var}") else 0
    )
    gr_mean_obs = (
        group_means_obs[f"avg_obs_{var}"] if group_means_obs.get(f"avg_obs_{var}") else 0
    )
    group_base_overall_diff = (
        cts["base_cts"]
        * (gr_mean_base - (means[f"avg_{var}"] if means[f"avg_{var}"] else 0)) ** 2
    )
    group_obs_overall_diff = (
        cts["obs_cts"]
        * (gr_mean_obs - (means[f"avg_{var}"] if means[f"avg_{var}"] else 0)) ** 2
    )

    sse = (cts["base_cts"] - 1) * group_base_var + (cts["obs_cts"] - 1) * group_obs_var
    sse /= cts["n"] - 2
    ssr = group_base_overall_diff + group_obs_overall_diff
    F = ssr / (sse if sse else 0.1)
    f_stats_aux[var] = F

    p_vals[var] = 1 - f.cdf(F, 1, cts["n"] - 2)

    sorted_by_p = dict(sorted(p_vals.items(), key=lambda item: item[1]))
f_stat = {}
distances = {}

for k, v in sorted_by_p.items():

    gr_mean_base = (
        group_means_base[f"avg_base_{k}"] if group_means_base.get(f"avg_base_{k}") else 0
    )
    gr_mean_obs = (
        group_means_obs[f"avg_obs_{k}"] if group_means_obs.get(f"avg_obs_{k}") else 0
    )

    mean_scaled_base = (gr_mean_base - stats[f"min_{k}"]) / (
        stats[f"max_{k}"] - stats[f"min_{k}"]
    )
    mean_scaled_obs = (gr_mean_obs - stats[f"min_{k}"]) / (
        stats[f"max_{k}"] - stats[f"min_{k}"]
    )
    distances[k] = (
        mean_scaled_obs - mean_scaled_base
        if sorted_by_p[k] <= 0.05
        else sorted_by_p[k]
    )

    f_stat[k] = f_stats_aux[k]


# COMMAND ----------

cat_bin_vars = categorical_vars.copy()
binary_vars = binary_vars.copy()

cat_bin_vars.extend(binary_vars)

cont_tables = {}
for var in cat_bin_vars:
    print(var)
    res = spark.sql(
        f"""
        (select case when 1 == 1 then 1 end as cat_target_base, {var}, count(*) from man_city_feature_store sfm group by cat_target_base, {var} order by {var}) 
        union 
        (select case when 0 == 0 then 0 end as cat_target_base, {var}, count(*)  from man_city_feature_store sfm group by cat_target_base, {var} order by {var})
        """
    ).collect()
    cont_tables[f"cont_{var}"] = [item.asDict() for item in res]
    for i in range(len(cont_tables[f'cont_{var}'])):
        if cont_tables[f"cont_{var}"][i][var] is None:
            cont_tables[f"cont_{var}"][i][var] = ""

# COMMAND ----------

temp = list(cont_tables.keys())
temp

# COMMAND ----------

res = spark.sql(
    "select distinct(Profile_Demographic_Gender) from man_city_feature_store"
).collect()

# COMMAND ----------

cont_tables['cont_Profile_Demographic_AgeGroup']

# COMMAND ----------

import numpy as np
cont_cts = {}
keys = ['cont_Profile_Demographic_AgeGroup','cont_Profile_Demographic_Gender']
for k in keys:

    temp = []
    # get the unique values in categorical variable
    unique_vals = [
        item[k.replace('cont_','')] for item in cont_tables[f'{k}'] if item[k.replace('cont_','')] != ""
    ] if not k.replace('cont_','') in binary_vars else [0,1]

    unique_vals = np.unique(unique_vals).tolist()
    # iterate over base (0) and target (1) group and get counts for each category.
    for i in [0, 1]:
        temp.append(
            [
                item.get("count(1)", 0)
                for item in cont_tables[k]
                if item["cat_target_base"] == i
            ]
        )

    # check if there is any blank values
    check_empty = sum(
        [
            True if item[k.replace("cont_", "")] == "" else False
            for item in cont_tables[k]
        ]
    )

    # add the blank values into the unique vals.
    n_unique_vals = len(unique_vals) + 1 if check_empty > 0 else len(unique_vals)

    # If there are some missing category value, add 0
    cont_cts_aux = [
        np.pad(item, (0, abs(n_unique_vals - len(item)))).tolist()
        if len(item) < n_unique_vals
        else item
        for item in temp
    ]

    # ad hoc post processing - ensures that there will be same number of columns for base and target group.
    if len(cont_cts_aux[0]) > len(cont_cts_aux[1]):
        cont_cts_aux[1] = np.pad(
            cont_cts_aux[1], (0, len(cont_cts_aux[0]) - len(cont_cts_aux[1]))
        ).tolist()
    elif len(cont_cts_aux[0]) < len(cont_cts_aux[1]):
        cont_cts_aux[0] = np.pad(
            cont_cts_aux[0], (0, len(cont_cts_aux[1]) - len(cont_cts_aux[0]))
        ).tolist()

    cont_cts[k] = cont_cts_aux

# COMMAND ----------

# compute chi square test and distances
p_vals_cat = {}
chi_stat = {}
removed_idx = {}
for k in cont_cts.keys():
    cells_sum = [x + y for x, y in zip(cont_cts[k][0], cont_cts[k][1])]
    idx = np.where(np.array(cells_sum) == 0)

    # We need remove the columns which includes only zeros for both base and target group

    removed_idx[k + "_removed_cats"] = idx[0]
    if len(idx[0] > 0):
        for _ in range(len(idx[0])):
            # [item.pop(idx[0][0]) for item in cont_cts[k]]
            [item.remove(0) for item in cont_cts[k]]
    chi, p, dof, exp = chi2_contingency(cont_cts[k])
    p_vals_cat[k] = p
    chi_stat[k] = chi
sorted_p = dict(sorted(p_vals_cat.items(), key=lambda item: item[1]))

distances_cat = {}
chi = {}

for k in sorted_p.keys():
    N = np.sum(cont_cts[k], axis=0)
    props = (np.array(cont_cts[k]) / N).tolist()
    distances_cat[k.replace("cont_", "")] = sum(
        [abs(x - y) for x, y in zip(props[0], props[1])]
    )
    chi[k.replace("cont_", "")] = chi_stat[k]


# COMMAND ----------

distances.update(distances_cat)
stat = f_stat.copy()
stat.update(chi)
sorted_by_p.update(sorted_p)

qi = []

numerical_vars.extend(integer_vars)

for var in numerical_vars:

    group_means_obs[f"avg_obs_{var}"] = float(
        group_means_obs[f"avg_obs_{var}"] if group_means_obs[f"avg_obs_{var}"] else 0
    )
    group_means_base[f"avg_base_{var}"] = float(
        group_means_base[f"avg_base_{var}"] if group_means_base[f"avg_base_{var}"] else 0
    )
    qi.append(
        {
            "id": var,
            "valueObs": round(group_means_obs[f"avg_obs_{var}"], 2),
            "valueBase": round(group_means_base[f"avg_base_{var}"], 2),
            # "true_rate": float(
            #     round(
            #         self.set_rate(
            #             [whole[f"obs_{i}_mean"], whole[f"base_{i}_mean"]]
            #         ),
            #         ROUND_DIGITS,
            #     )
            # ),
            "rate": stat[var] if stat.get(var) and not np.isnan(stat[var]) else -1000,
            "distances": distances[var]
            if distances.get(var) and not np.isnan(distances[var])
            else -1000,
            "obvious": False,
            "significant": bool(sorted_by_p[var] <= 0.05) if sorted_by_p.get(var) else False,
        }
    )


cat_bin_vars = categorical_vars.copy()
binary_vars = binary_vars.copy()

cat_bin_vars.extend(binary_vars)

for var in cat_bin_vars:
    qi.append(
        {
            "id": var,
            "valueObs": None,
            "valueBase": None,
            "true_rate": 0,
            "rate": stat[i] if stat.get(i) and not np.isnan(stat[i]) else -1000,
            "distances": distances[i]
            if distances.get(i) and not np.isnan(distances[i])
            else -1000,
            "obvious": False,
            "significant": bool(p_vals[i] <= 0.05) if p_vals.get(i) else False,
        }
    )

qi = sorted(
            qi, key=lambda d: (d["significant"], d["distances"]), reverse=True
        )


# COMMAND ----------

qi

# COMMAND ----------

numerical_vars = [
    var.col_name
    for var in vars_
    if var.data_type == 'double'
]

# Integer vars are all variables which have integer values. Howwever, we need to split them into interval integer variables
# and not integer interval variables. The reason behind this logic is that we can consider the integer variables with the lot
# of distinct values as numerical variables. But not in the all cases. For example, computing the counts in the some intervals, etc.
integer_vars = [
    var.col_name
    for var in vars_
    if var.data_type == 'int'
]

# Categorical variables are all variables which have string values.
categorical_vars = [
    var.col_name
    for var in vars_
    if var.data_type == 'string'
]

# Binary variables are all variables which has 0-1 integer values.
binary_vars = [
    var.col_name
    for var in vars_
    if var.data_type == 'boolean']

# Identifying intervanul variables. We can identify the interval variables with help of its bins. If the difference
# between bins is more than 1, we can assume that this variable is interval variable.
# interval_vars = []
# for int_var in integer_vars:
#     cum_diff = []
#     for i in range(1, len(self.bins[int_var])):
#         res = float(self.bins[int_var][i]) - float(self.bins[int_var][i - 1])
#         cum_diff.append(res)

#     if abs(len(self.bins[int_var]) - sum(cum_diff)) > 1:
#         self.interval_vars.append(int_var)


# COMMAND ----------

vars_ = spark.sql(
    "describe table man_city_feature_store"
).collect()

# COMMAND ----------

cat_variables = [cat.col_name for cat in vars_ if cat.data_type == 'string']

# COMMAND ----------

cat_variables

# COMMAND ----------

# Graph generation - set following objects
# monitoring_data_df 
# hardcoded feature - web_analytics_loan_visits_count_14d, watch out for bins, plt.xlim and description
import matplotlib.pyplot as plt

plt.hist(monitoring_data_df.filter(f.col("label") == 1).toPandas()['web_analytics_loan_visits_count_14d'], 
         alpha=0.5, # the transaparency parameter
         label='target',
         bins = 10,
         density = True)
  
plt.hist(monitoring_data_df.filter(f.col("label") == 0).toPandas()['web_analytics_loan_visits_count_14d'],
         alpha=0.5,
         label='non_target',
         bins = 10,
         density = True)

plt.xlim([0, 10])
plt.legend(loc='upper right')
plt.title('Number of visits to the main loan page seen in last 14 days between target and non-target group')
plt.show()
