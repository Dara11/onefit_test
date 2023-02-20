# 1FIT_test
Take Home problem from the company 1fit. It has several stages:

Stage 1: Used SQLalchemy library to connect to the database. Created view, procedure and table. Table loaded to public schema (could be loaded to another schema, but left a public schema). Used Pandas library to get data from the table. Applied try, except, finally approach.

View: v_amount_and_paid.  Procedure: p_amount_and_paid.  Table: t_amount_and_paid

Stage 2: Used Pandas library to find an activation date with maximum profit.

Stage 3: Table was loaded to database. (could be implemented another method using df.to_sql())

Stage 4: Result from Stage 2 was loaded to json file.

Stage 5: Created repository and python file of code was committed. 