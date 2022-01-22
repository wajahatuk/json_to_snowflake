import snowflake.connector

sql = "USE DATABASE SPARK"

sql_1 = """
create table if not exists subscription_statging
( createdAt varchar(50),
  updatedAt varchar(50),
  startDate varchar(150),
  startDate_updated varchar(150),
  endDate varchar(150),
  endDate_updated varchar(150),
  status varchar(150),
  status_updated varchar(150),
  amount varchar(50),
  amount_updated varchar(50),
  user_id int
) """
sql_2 = """
Insert into SPARK.PUBLIC.subscription_statging
select replace(replace(split_part("subscription.createdAt", ',', 1), '[', ''),']','') as createdat
, replace(replace(replace(split_part("subscription.createdAt", ',', 2), ']',''),'[',''), ' ','') as updatedat
, replace(replace(replace(split_part("subscription.startDate", ',', 1), ']',''),'[',''), ' ','') as startDate
, replace(replace(replace(split_part("subscription.startDate", ',', 2), ']',''),'[',''), ' ','') as startDate_updated
, replace(replace(replace(split_part("subscription.endDate", ',', 1), ']',''),'[',''), ' ','') as endDate
, replace(replace(replace(split_part("subscription.endDate", ',', 2), ']',''),'[',''), ' ','') as endDate_updated
, replace(replace(replace(split_part("subscription.status", ',', 1), '[', ''),']',''), ' ','') as status
, replace(replace(replace(split_part("subscription.status", ',', 2), '[', ''),']',''), ' ','') as status_updated
, replace(replace(replace(split_part("subscription.amount", ',', 1), '[', ''),']',''), ' ','') as amount
, replace(replace(replace(split_part("subscription.amount", ',', 2), '[', ''),']',''), ' ','') as amount_updated
, ID as user_id 
from SPARK.PUBLIC.users 
where ID not in (select user_id from SPARK.PUBLIC.subscription_statging)
"""
sql_3 = """
create table if not exists subscription
( createdAt varchar(150),
  updatedAt varchar(150),
  startDate varchar(150),
  endDate varchar(150),
  status varchar(150),
  amount varchar(50),
  user_id int
)
"""

sql_4 = """
Insert into SPARK.PUBLIC.subscription
select createdat,
updatedat
, case when updatedat = '' then startdate else startdate_updated end as startdate
, case when updatedat = '' then enddate else enddate_updated end as enddate
, case when updatedat = '' then status else status_updated end as status
, case when updatedat = '' then amount else amount_updated end as amount
, user_id
from spark.public.subscription_statging
where user_id not in (select user_id from SPARK.PUBLIC.subscription);
"""

def connector():
    con = snowflake.connector.connect(
        user='Wajahat007',
        account='JR20984.eu-central-1',
        password='Wajahat123!@#',
        Database='SPARK',
        Schema='PUBLIC',
        Warehouse='COMPUTE_WH'
    )
    try:
        con.cursor().execute(sql)
        con.cursor().execute(sql_1)
        con.cursor().execute(sql_2)
        con.cursor().execute(sql_3)
        con.cursor().execute(sql_4)
    finally:
        con.close()
