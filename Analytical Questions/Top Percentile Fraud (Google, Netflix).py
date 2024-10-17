import pyspark.sql.functions as F
from pyspark.sql import Window


result = fraud_score \
    .withColumn('pcnt_rank', F.percent_rank().over(Window.partitionBy('state').orderBy(F.col('fraud_score').desc()))) \
    .where(F.col('pcnt_rank') < 0.05) \
    .select('policy_num', 'state', 'claim_cost', 'fraud_score') \
    .toPandas()