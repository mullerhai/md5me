import pyspark
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
import numpy as np
import pandas as pd
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.recommendation import ALS
from pyspark.sql import Row
import pyspark.sql.functions as F
# import seaborn as sns
# import matplotlib.pyplot as plt
import sys
import numpy as np

from pyspark.ml import Pipeline
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.feature import HashingTF, Tokenizer
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
# import alert_program as ap

#spark = SparkSession.builder.getOrCreate()
spark=SparkSession.builder.master("spark://cdhnode1:7077").appName("my app").config(conf=SparkConf()).getOrCreate()
sc=SparkSession.builder.config(conf=SparkConf())


