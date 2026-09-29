import logging
from pyspark.sql.functions import col, lit, array, struct, to_json, expr

logger = logging.getLogger(__name__)

class DataQualityFramework:
    def __init__(self, spark_session):
        self.spark = spark_session

    def enforce_rules(self, df, rules):
        """
        Takes a DataFrame and a list of rules (SQL expressions).
        Returns (valid_df, invalid_df).
        Invalid DF will have a 'rejection_reason' column appended.
        """
        logger.info(f"Applying {len(rules)} quality rules.")
        
        # Build the combined validity expression
        valid_condition = " AND ".join([f"({rule['expr']})" for rule in rules])
        
        valid_df = df.filter(expr(valid_condition))
        
        # Build invalid condition (NOT valid)
        invalid_df = df.filter(expr(f"NOT ({valid_condition})"))
        
        # Optionally, figure out exactly which rule failed (simplified to a generic string for now)
        rejection_reason = " | ".join([f"Violated: {rule['name']}" for rule in rules])
        invalid_df = invalid_df.withColumn("rejection_reason", lit(rejection_reason)) \
                               .withColumn("raw_payload", to_json(struct([df[x] for x in df.columns])))
                               
        valid_count = valid_df.count()
        invalid_count = invalid_df.count()
        logger.info(f"Data Quality: Valid={valid_count}, Invalid={invalid_count}")
        
        return valid_df, invalid_df
