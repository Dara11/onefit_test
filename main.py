import os
import json
import pandas as pd
from sqlalchemy import create_engine

##### STAGE 1 AND 3
try:
    # connect to the PostgreSQL server
    print('connecting to the PostgreSQL database...')

    engine = create_engine('postgresql://test_user:mZGCb|8n@onefit-dwh.cwdxvaoqegwr.eu-central-1.rds.amazonaws.com:5432/postgres')
    connection = engine.connect()
    # Using pandas library, get the data from the table as a dataFrame
    sql_query = """
                CREATE OR REPLACE VIEW v_amount_and_paid AS
                SELECT main_user.id AS user_id, 
                       user_membership.activated_at AS activation_date,
                       CASE WHEN user_membership.status = 'ACTIVE' THEN date(user_membership.monthly_limit_refresh_datetime) + user_membership.days_left 
                            END AS current_membership_end,
                       payment_order.total_amount AS total_amount,
                       main_visits.threshold_price AS paids_for_visits,
                       (payment_order.total_amount-main_visits.threshold_price) AS diff_btw_amount_and_paid
                FROM public.authe_mainuser AS main_user
                LEFT JOIN public.membership_usermembership AS user_membership
                ON main_user.id = user_membership.user_id
                LEFT JOIN public.payment_order AS payment_order
                ON main_user.id = payment_order.user_id
                LEFT JOIN public.main_visit AS main_visits
                ON main_user.id = main_visits.user_id
                WHERE date(user_membership.activated_at) BETWEEN '2022-07-01' AND '2022-08-01';


                CREATE OR REPLACE PROCEDURE p_amount_and_paid()
                LANGUAGE plpgsql
                AS $$
                BEGIN
                    CREATE TABLE t_amount_and_paid AS
                    SELECT user_id,
                           activation_date,
                           current_membership_end,
                           total_amount,
                           paids_for_visits,
                           diff_btw_amount_and_paid
                    FROM public.v_amount_and_paid
                    COMMIT;
                END; $$;

                CALL p_amount_and_paid(); 
                
                SELECT * FROM t_amount_and_paid;
                """
    df = pd.read_sql(sql_query, connection)
    print('successfully read in data')
except Exception as error:
    print(error)
finally:
    if 'connection' in locals():
        connection.close()
        print('database connection closed')

##### STAGE 2 AND 4
max_diff_value = df['diff_btw_amount_and_paid'].max()
df_profit = df[df['diff_btw_amount_and_paid']==max_diff_value][['activation_date','diff_btw_amount_and_paid']]
df_profit.to_json('max_profit.json', orient = 'split', index = False)