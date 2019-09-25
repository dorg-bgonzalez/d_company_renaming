#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import snowflake.connector
import pandas as pd
import numpy as np
from creds import *


def get_query(snowflake_query):
    # connecting to snowflake db
    con = snowflake.connector.connect(
        user=user_name,
        password=user_passwd,
        account=user_acc,
        role=user_role,
        warehouse=user_wh,
        database=user_db,
        schema=user_schema
    )
    # writing data to pandas df
    data = pd.read_sql(query, con)
    # closing connection
    con.close()
    # returns df
    return data


query = """
SELECT *
FROM "USER"."VSATHER"."URL_IN_CO_NAME";
"""

results = get_query(query)

# makes a csv copy of the data being queried
results.to_csv("stored_snowflake_query_test_new_creds.csv", index=False)


# flatten df
# clean_df = expand_list(all_fields_df, 'company', 'flat_company')
# join people records to clean df
# clean_df = pd.merge(all_fields_df, domain_df[['c_id', 'number_of_silver_records_in_person_index', 'number_of_records_in_person_index']], on='c_id', how='left')
# sort new df by people records by people records
# clean_df.sort_values(by=['number_of_silver_records_in_person_index', 'number_of_records_in_person_index'], ascending=False, inplace=True)