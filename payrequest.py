for entry in schema_data:
    if stage_error == False:
        for key in entry.keys():  
            schema = entry[key]["schema"]
            table = entry[key]["table_name"] 
            if key == "read":
                view_df = postgres_query(host,mtf_db,schema,table,action="read")
            elif key == "insert":
                today = F.current_date()
                df_with_flag = view_df.withColumn("expired_flag",F.when(
                        ((F.col("release_dt").isNull()) & (F.col("hold_dt") > today))
                        | ((F.col("release_dt").isNotNull()) & (F.col("release_dt") > today)),
                        F.lit("No")).otherwise(F.lit("Yes"))
                        )
                df_filtered = df_with_flag.filter(F.col("expired_flag") == "No")
                df_final = (df_filtered
                        .withColumn("claim_msg_cd", F.lit(155))
                        .withColumn("claim_loctn_cd", F.lit(301))
                        .withColumn("insert_user_id", F.lit(-1))
                    )
                df_final = (df_final.withColumn("received_dt", F.col("received_dt").cast(DateType())))
                df_final = df_final.select(
                    "received_dt",
                    "received_id",
                    "transactionid",
                    "claim_msg_cd",
                    "claim_loctn_cd",
                    "insert_user_id"
                )
                df_final = df_final.withColumnRenamed("transactionid", "internal_claim_num")
                # postgres_query(host,mtf_db,schema,table,action="insert", data=df_final)

            elif key == "insert2":
                df_msg = df_final.select(
                    "received_dt",
                    "received_id",
                    "internal_claim_num",
                    "claim_msg_cd",
                    "insert_user_id"
                )
                postgres_query(host,mtf_db,schema,table,action="insert", data=df_msg)
