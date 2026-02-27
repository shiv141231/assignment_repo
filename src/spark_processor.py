
from __future__ import annotations
import logging

import pandas as pd
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, StringType, StructField, StructType

from src.processor import BaseProcessor
from src.parsers import parse_referrer, parse_revenue
from src.config import PURCHASE_EVENT, TSV_DELIMITER

logger = logging.getLogger(__name__)


HIT_SCHEMA = StructType([
    StructField("hit_time_gmt",  StringType(), True),
    StructField("date_time",     StringType(), True),
    StructField("user_agent",    StringType(), True),
    StructField("ip",            StringType(), True),
    StructField("event_list",    StringType(), True),
    StructField("geo_city",      StringType(), True),
    StructField("geo_region",    StringType(), True),
    StructField("geo_country",   StringType(), True),
    StructField("pagename",      StringType(), True),
    StructField("page_url",      StringType(), True),
    StructField("product_list",  StringType(), True),
    StructField("referrer",      StringType(), True),
])

@F.pandas_udf(StructType([
    StructField("domain",  StringType(), True),
    StructField("keyword", StringType(), True),
]))
def _udf_parse_referrer(referrer_series: pd.Series) -> pd.DataFrame:
    results = referrer_series.map(parse_referrer)
    return pd.DataFrame(results.tolist(), columns=["domain", "keyword"])


@F.pandas_udf(DoubleType())
def _udf_parse_revenue(
    product_series: pd.Series,
    event_series: pd.Series,
) -> pd.Series:
    return pd.Series([
        parse_revenue(p, e)
        for p, e in zip(product_series, event_series)
    ])



class SparkProcessor(BaseProcessor):

    def describe(self) -> str:
        return f"SparkProcessor | file={self.input_path}"

    def process(self) -> dict[tuple[str, str], float]:
        spark = self._get_session()

        df = self._read(spark)
        df = self._enrich(df)
        df = self._attribute(df)
        df = self._aggregate(df)

        rows = df.collect()
        logger.info("Spark pipeline complete | unique (engine,keyword): %d", len(rows))
        return {(r["domain"], r["keyword"]): r["revenue"] for r in rows}


    def _read(self, spark: SparkSession):
        logger.info("Reading: %s", self.input_path)
        return (
            spark.read
            .option("header",    "true")
            .option("delimiter", TSV_DELIMITER)
            .option("quote",     '"')
            .option("escape",    '"')
            .option("multiLine", "false")
            .option("mode",      "PERMISSIVE") 
            .schema(HIT_SCHEMA)
            .csv(self.input_path)
        )

    def _enrich(self, df):
        parsed = _udf_parse_referrer(F.col("referrer"))
        return (
            df
            .withColumn("_ref",       parsed)
            .withColumn("se_domain",  F.col("_ref.domain"))
            .withColumn("se_keyword", F.col("_ref.keyword"))
            .drop("_ref")
            .withColumn("revenue", _udf_parse_revenue(
                F.col("product_list"), F.col("event_list")
            ))
            .withColumn("hit_time_gmt", F.col("hit_time_gmt").cast("long"))
        )

    def _attribute(self, df):
        w = (
            Window
            .partitionBy("ip")
            .orderBy("hit_time_gmt")
            .rowsBetween(Window.unboundedPreceding, Window.currentRow)
        )
        return (
            df
            .withColumn("domain",  F.last("se_domain",  ignorenulls=True).over(w))
            .withColumn("keyword", F.last("se_keyword", ignorenulls=True).over(w))
            .filter(F.array_contains(F.split("event_list", ","), PURCHASE_EVENT))
            .filter(F.col("domain").isNotNull())
            .filter(F.col("revenue") > 0)
            .select("domain", "keyword", "revenue")
        )

    def _aggregate(self, df):
        return (
            df
            .groupBy("domain", "keyword")
            .agg(F.round(F.sum("revenue"), 2).alias("revenue"))
            .orderBy(F.col("revenue").desc())
        )


    @staticmethod
    def _get_session() -> SparkSession:

        return (
            SparkSession.builder
            .appName("SearchKeywordPerformance")
            .config("spark.sql.adaptive.enabled",                            "true")
            .config("spark.sql.adaptive.coalescePartitions.enabled",         "true")
            .config("spark.sql.adaptive.coalescePartitions.minPartitionNum", "1")
            .config("spark.sql.adaptive.advisoryPartitionSizeInBytes",       "134217728")
            .config("spark.sql.adaptive.skewJoin.enabled",                   "true")
            .config("spark.sql.adaptive.skewJoin.skewedPartitionFactor",     "5")
            .config("spark.sql.adaptive.localShuffleReader.enabled",         "true")
            .config("spark.sql.autoBroadcastJoinThreshold",                  "20971520")
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
            .getOrCreate()
        )
