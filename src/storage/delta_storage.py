import os
import logging
from delta.tables import DeltaTable
from pyspark.sql.functions import col

logger = logging.getLogger(__name__)

class DeltaStorage:
    def __init__(self, spark_session):
        self.spark = spark_session

    def merge_or_insert(self, df, target_path, merge_keys):
        """
        Idempotent write using Delta MERGE.
        If table doesn't exist, create it (save as append/overwrite).
        If exists, merge based on merge_keys.
        """
        logger.info(f"Writing to Delta at {target_path} with keys {merge_keys}")
        
        if not DeltaTable.isDeltaTable(self.spark, target_path):
            logger.info("Target Delta table does not exist. Creating new table via append.")
            df.write.format("delta").mode("append").save(target_path)
            return {"inserted": df.count(), "updated": 0}

        # Build merge condition
        condition = " AND ".join([f"target.{k} = source.{k}" for k in merge_keys])
        
        target_table = DeltaTable.forPath(self.spark, target_path)
        
        # Deduplicate source by merge_keys before merging to avoid ambiguous updates
        df_dedup = df.dropDuplicates(merge_keys)
        
        try:
            target_table.alias("target") \
                .merge(df_dedup.alias("source"), condition) \
                .whenMatchedUpdateAll() \
                .whenNotMatchedInsertAll() \
                .execute()
            
            # Since Spark 3.x Delta open source metrics are sometimes limited without history parsing,
            # we just report a successful merge completion.
            return {"status": "MERGED_SUCCESSFULLY"}
        except Exception as e:
            logger.error(f"Delta merge failed at {target_path}: {e}")
            raise e
