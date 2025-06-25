from dagster import asset, AssetKey, MetadataValue, Output
from ..spark_utils import get_spark_session

@asset(
    key=AssetKey(["workspace", "default", "us_customers"]),
    deps=[AssetKey(["samples", "bakehouse", "sales_customers"])],
)
def us_customers():
    spark = get_spark_session()

    df = spark.read.table("samples.bakehouse.sales_customers")
    us_df = df.filter(df["country"] == "USA")

    us_df.write.format("delta").mode("overwrite").saveAsTable("workspace.default.us_customers")

    # ✅ Collect metadata BEFORE stopping session
    row_count = us_df.count()
    preview_md = us_df.limit(5).toPandas().to_markdown(index=False)

    spark.stop()

    return Output(
        value=None,
        metadata={
            "row_count": MetadataValue.int(row_count),
            "preview": MetadataValue.md(preview_md),
        }
    )