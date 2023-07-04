import pytest


from video_analytics.functions import *
# from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, ArrayType,StructType,StructField
from chispa import assert_column_equality


# @pytest.fixture(scope='session')
# def spark():
#     return SparkSession.builder \
#       .master("local") \
#       .appName("chispa") \
#       .getOrCreate()

# def getVideos(spark):
#     return spark.read.option('header', 'true').option("inferSchema", "true").csv('datasets/USvideos.csv')
    

def test_split_string(spark):

    # from pyspark.sql.types import StructType,StructField, StringType, IntegerType
    data = [("James|cat|feat",['James', 'cat', 'feat']),
        ("Apple|iPhone 10|iPhone Ten|iPhone|Portrait Lighting|A11 Bionic|augmented reality|emoji|animoji|Face ID|Apple Pay|camera|smartphone",['Apple', 'iPhone 10', 'iPhone Ten', 'iPhone', 'Portrait Lighting', 'A11 Bionic', 'augmented reality', 'emoji', 'animoji', 'Face ID', 'Apple Pay', 'camera', 'smartphone']),
        ("americasnews|Cuba|natural disasters|Irma tropical storm|latin america|package|Julia Galiano",['americasnews', 'Cuba', 'natural disasters', 'Irma tropical storm', 'latin america', 'package', 'Julia Galiano'])
    ]
    
    schema = StructType([ 
        StructField("tags",StringType(),True), 
        StructField("expected_tags",ArrayType(StringType()),True)
    ])

    df = spark.createDataFrame(data=data, schema = schema)
    
    df_tags = df.select(split_string("tags").alias("tags"),"expected_tags")

    # assert df_result.count() > 0
    assert_column_equality(df_tags,"tags","expected_tags")


def test_calc_score(spark):
    data = [
        (1,9,5,3,4,14.4),
        (0,4,4,3,1,4.0),
        (7,5,3,3,2,20.0),
        (8,0,6,3,3,0.0),
        (6,7,2,3,1,35.0)
    ]

    df = spark.createDataFrame(data=data, schema = ["views"
                                                , "likes"
                                                , "dislikes"
                                                , "commnet_likes"
                                                , "commnet_replies"
                                                , "expected_score"])

    df_score = df.select(calc_score("views"
                                                , "likes"
                                                , "dislikes"
                                                , "commnet_likes"
                                                , "commnet_replies"
                                           ).alias("score"),"expected_score")

    # assert df_result.count() > 0

    assert_column_equality(df_score,"score","expected_score")
