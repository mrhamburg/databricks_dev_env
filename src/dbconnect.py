from typing import Callable
from pyspark.sql import SparkSession, DataFrame
import logging
from pyspark.sql.types import StructType, ArrayType
import pyspark.sql.functions as f

clusters = {
    "dev": {
        "id": "cluster id",
        "port": "port"
    },
    "prod": {
        "id": "cluster id",
        "port": "port"
    }
}

# Logging


class SilenceFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        return False


logging.basicConfig(format="%(asctime)s|%(levelname)s|%(name)s|%(message)s",
                    level=logging.INFO)
logging.getLogger("py4j.java_gateway").addFilter(SilenceFilter())
log = logging.getLogger("dbconnect")


def _check_is_databricks() -> bool:
    return _get_spark().conf.get(
        "spark.databricks.service.client.enabled") == "true"


def _get_spark() -> SparkSession:
    if _check_is_databricks:
        user_ns = SparkSession.builder.getOrCreate()
        return user_ns
    else:
        user_ns = get_ipython().user_ns
        return user_ns


def _display(df: DataFrame) -> None:
    df.show(truncate=False)


def _display_with_json(df: DataFrame) -> None:
    for column in df.schema:
        t = type(column.dataType)
        if t == StructType or t == ArrayType:
            df = df.withColumn(column.name, f.to_json(column.name))
    df.show(truncate=False)


def _get_display() -> Callable[[DataFrame], None]:
    if not _check_is_databricks:
        return get_ipython().user_ns.get("display")
    else:
        return _display_with_json


def _get_dbutils(spark: SparkSession):
    if _check_is_databricks:
        from pyspark.dbutils import DBUtils
        return DBUtils(spark)
    else:
        import IPython
        return IPython.get_ipython().user_ns["dbutils"]


# initialise Spark variables
is_databricks: bool = _check_is_databricks()
spark: SparkSession = _get_spark()
display = _get_display()
dbutils = _get_dbutils(spark)


def use_cluster(cluster_name: str):
    """
    When running via Databricks Connect, specify to which cluster to connect instead of the default cluster.
    This call is ignored when running in Databricks environment.
    :param cluster_name: Name of the cluster as defined in the beginning of this file.
    """
    real_cluster_name = spark.conf.get(
        "spark.databricks.clusterUsageTags.clusterName", None)

    # do not configure if we are already running in Databricks
    if not real_cluster_name:
        cluster_config = clusters.get(cluster_name)
        log.info(
            f"attaching to cluster '{cluster_name}' (id: {cluster_config['id']}, port: {cluster_config['port']})"
        )

        spark.conf.set("spark.driver.host", "127.0.0.1")
        spark.conf.set("spark.databricks.service.clusterId",
                       cluster_config["id"])
        spark.conf.set("spark.databricks.service.port", cluster_config["port"])
