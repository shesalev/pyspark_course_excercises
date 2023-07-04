from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import StringType, DoubleType, ArrayType
import pandas as pd

@pandas_udf(DoubleType())
def calc_score(views: pd.Series, likes: pd.Series, dislikes: pd.Series, commnet_likes: pd.Series, commnet_replies: pd.Series) -> pd.Series:
    return (views + commnet_likes + commnet_replies) / dislikes * likes

@pandas_udf(ArrayType(StringType()))
def split_string(column: pd.Series) -> pd.Series:
    return column.str.split("|")
