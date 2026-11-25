use role sysadmin;
use database sandbox;
use schema stage_sch;
use warehouse adhoc_wh;

create or replace table stage_sch.orders (
    orderid text comment 'Primary Key (Source System)',                  -- primary key as text
    customerid text comment 'Customer FK(Source System)',               -- foreign key reference as text (no constraint in snowflake)
    restaurantid text comment 'Restaurant FK(Source System)',             -- foreign key reference as text (no constraint in snowflake)
    orderdate text,                -- order date as text
    totalamount text,              -- total amount as text (no decimal constraint)
    status text,                   -- status as text
    paymentmethod text,            -- payment method as text
    createddate text,              -- created date as text
    modifieddate text,             -- modified date as text

    -- audit columns with appropriate data types
    _stg_file_name text,
    _stg_file_load_ts timestamp,
    _stg_file_md5 text,
    _copy_data_ts timestamp default current_timestamp
)
comment = 'This is the order stage/raw table where data will be copied from internal stage using copy command. This is as-is data represetation from the source location. All the columns are text data type except the audit columns that are added for traceability.';


create or replace stream stage_sch.orders_stm 
on table stage_sch.orders
append_only = true
comment = 'This is the append-only stream object on orders entity that only gets delta data';

list  @stage_sch.csv_stg/initial/orders/orders-initial.csv;


copy into stage_sch.orders (orderid, customerid, restaurantid, orderdate, totalamount, 
                  status, paymentmethod, createddate, modifieddate,
                  _stg_file_name, _stg_file_load_ts, _stg_file_md5, _copy_data_ts)
from (
    select 
        t.$1::text as orderid,
        t.$2::text as customerid,
        t.$3::text as restaurantid,
        t.$4::text as orderdate,
        t.$5::text as totalamount,
        t.$6::text as status,
        t.$7::text as paymentmethod,
        t.$8::text as createddate,
        t.$9::text as modifieddate,
        metadata$filename as _stg_file_name,
        metadata$file_last_modified as _stg_file_load_ts,
        metadata$file_content_key as _stg_file_md5,
        current_timestamp as _copy_data_ts
    from @stage_sch.csv_stg/initial/orders/orders-initial.csv t
)
file_format = (format_name = 'stage_sch.csv_file_format')
on_error = abort_statement;


CREATE OR REPLACE TABLE CLEAN_SCH.ORDERS (
    ORDER_SK NUMBER AUTOINCREMENT PRIMARY KEY comment 'Surrogate Key (EDW)',                -- Auto-incremented primary key
    ORDER_ID BIGINT UNIQUE comment 'Primary Key (Source System)',                      -- Primary key inferred as BIGINT
    CUSTOMER_ID_FK BIGINT comment 'Customer FK(Source System)',                   -- Foreign key inferred as BIGINT
    RESTAURANT_ID_FK BIGINT comment 'Restaurant FK(Source System)',                 -- Foreign key inferred as BIGINT
    ORDER_DATE TIMESTAMP,                 -- Order date inferred as TIMESTAMP
    TOTAL_AMOUNT DECIMAL(10, 2),          -- Total amount inferred as DECIMAL with two decimal places
    STATUS STRING,                        -- Status as STRING
    PAYMENT_METHOD STRING,                -- Payment method as STRING
    created_dt timestamp_tz,                                     -- record creation date
    modified_dt timestamp_tz,                                    -- last modified date, allows null if not modified

    -- additional audit columns
    _stg_file_name string,                                       -- file name for audit
    _stg_file_load_ts timestamp_ntz,                             -- file load timestamp for audit
    _stg_file_md5 string,                                        -- md5 hash for file content for audit
    _copy_data_ts timestamp_ntz default current_timestamp        -- timestamp when data is copied, defaults to current timestamp
)
comment = 'Order entity under clean schema with appropriate data type under clean schema layer, data is populated using merge statement from the stage layer location table. This table does not support SCD2';

-- Stream object to capture the changes. 
create or replace stream CLEAN_SCH.ORDERS_stm 
on table CLEAN_SCH.ORDERS
comment = 'This is the stream object on ORDERS table table to track insert, update, and delete changes';


MERGE INTO CLEAN_SCH.ORDERS AS target
USING STAGE_SCH.ORDERS_STM AS source
    ON target.ORDER_ID = TRY_TO_NUMBER(source.ORDERID) -- Match based on ORDER_ID
WHEN MATCHED THEN
    -- Update existing records
    UPDATE SET
        TOTAL_AMOUNT = TRY_TO_DECIMAL(source.TOTALAMOUNT),
        STATUS = source.STATUS,
        PAYMENT_METHOD = source.PAYMENTMETHOD,
        MODIFIED_DT = TRY_TO_TIMESTAMP_TZ(source.MODIFIEDDATE),
        _STG_FILE_NAME = source._STG_FILE_NAME,
        _STG_FILE_LOAD_TS = source._STG_FILE_LOAD_TS,
        _STG_FILE_MD5 = source._STG_FILE_MD5,
        _COPY_DATA_TS = CURRENT_TIMESTAMP
WHEN NOT MATCHED THEN
    -- Insert new records
    INSERT (
        ORDER_ID,
        CUSTOMER_ID_FK,
        RESTAURANT_ID_FK,
        ORDER_DATE,
        TOTAL_AMOUNT,
        STATUS,
        PAYMENT_METHOD,
        CREATED_DT,
        MODIFIED_DT,
        _STG_FILE_NAME,
        _STG_FILE_LOAD_TS,
        _STG_FILE_MD5,
        _COPY_DATA_TS
    )
    VALUES (
        TRY_TO_NUMBER(source.ORDERID),
        TRY_TO_NUMBER(source.CUSTOMERID),
        TRY_TO_NUMBER(source.RESTAURANTID),
        TRY_TO_TIMESTAMP(source.ORDERDATE),
        TRY_TO_DECIMAL(source.TOTALAMOUNT),
        source.STATUS,
        source.PAYMENTMETHOD,
        TRY_TO_TIMESTAMP_TZ(source.CREATEDDATE),
        TRY_TO_TIMESTAMP_TZ(source.MODIFIEDDATE),
        source._STG_FILE_NAME,
        source._STG_FILE_LOAD_TS,
        source._STG_FILE_MD5,
        CURRENT_TIMESTAMP
    );


-- part-2
list @stage_sch.csv_stg/delta/orders/;
copy into stage_sch.orders (orderid, customerid, restaurantid, orderdate, totalamount, 
                  status, paymentmethod, createddate, modifieddate,
                  _stg_file_name, _stg_file_load_ts, _stg_file_md5, _copy_data_ts)
from (
    select 
        t.$1::text as orderid,
        t.$2::text as customerid,
        t.$3::text as restaurantid,
        t.$4::text as orderdate,
        t.$5::text as totalamount,
        t.$6::text as status,
        t.$7::text as paymentmethod,
        t.$8::text as createddate,
        t.$9::text as modifieddate,
        metadata$filename as _stg_file_name,
        metadata$file_last_modified as _stg_file_load_ts,
        metadata$file_content_key as _stg_file_md5,
        current_timestamp as _copy_data_ts
    from @stage_sch.csv_stg/delta/orders/day-02-orders.csv t
)
file_format = (format_name = 'stage_sch.csv_file_format')
on_error = abort_statement;


SELECT COUNT(*) AS STREAM_ROWS
FROM SANDBOX.STAGE_SCH.ORDERITEM_STM;






USE ROLE SYSADMIN;
USE WAREHOUSE ADHOC_WH;
USE DATABASE SANDBOX;

MERGE INTO CLEAN_SCH.ORDER_ITEM AS TARGET

USING SANDBOX.STAGE_SCH.ORDERITEM AS SOURCE

ON TARGET.ORDER_ITEM_ID = TRY_TO_NUMBER(SOURCE.ORDERITEMID)
AND TARGET.ORDER_ID_FK = TRY_TO_NUMBER(SOURCE.ORDERID)
AND TARGET.MENU_ID_FK = TRY_TO_NUMBER(SOURCE.MENUID)

WHEN MATCHED THEN
UPDATE SET
    TARGET.QUANTITY = TRY_TO_NUMBER(SOURCE.QUANTITY),
    TARGET.PRICE = TRY_TO_NUMBER(SOURCE.PRICE),
    TARGET.SUBTOTAL = TRY_TO_NUMBER(SOURCE.SUBTOTAL),
    TARGET.CREATED_DT = TRY_TO_TIMESTAMP_NTZ(SOURCE.CREATEDDATE),
    TARGET.MODIFIED_DT = TRY_TO_TIMESTAMP_NTZ(SOURCE.MODIFIEDDATE),
    TARGET._STG_FILE_NAME = SOURCE._STG_FILE_NAME,
    TARGET._STG_FILE_LOAD_TS = SOURCE._STG_FILE_LOAD_TS,
    TARGET._STG_FILE_MD5 = SOURCE._STG_FILE_MD5,
    TARGET._COPY_DATA_TS = SOURCE._COPY_DATA_TS

WHEN NOT MATCHED
AND TRY_TO_NUMBER(SOURCE.ORDERITEMID) IS NOT NULL
AND TRY_TO_NUMBER(SOURCE.ORDERID) IS NOT NULL
AND TRY_TO_NUMBER(SOURCE.MENUID) IS NOT NULL

THEN INSERT (
    ORDER_ITEM_ID,
    ORDER_ID_FK,
    MENU_ID_FK,
    QUANTITY,
    PRICE,
    SUBTOTAL,
    CREATED_DT,
    MODIFIED_DT,
    _STG_FILE_NAME,
    _STG_FILE_LOAD_TS,
    _STG_FILE_MD5,
    _COPY_DATA_TS
)
VALUES (
    TRY_TO_NUMBER(SOURCE.ORDERITEMID),
    TRY_TO_NUMBER(SOURCE.ORDERID),
    TRY_TO_NUMBER(SOURCE.MENUID),
    TRY_TO_NUMBER(SOURCE.QUANTITY),
    TRY_TO_NUMBER(SOURCE.PRICE),
    TRY_TO_NUMBER(SOURCE.SUBTOTAL),
    TRY_TO_TIMESTAMP_NTZ(SOURCE.CREATEDDATE),
    TRY_TO_TIMESTAMP_NTZ(SOURCE.MODIFIEDDATE),
    SOURCE._STG_FILE_NAME,
    SOURCE._STG_FILE_LOAD_TS,
    SOURCE._STG_FILE_MD5,
    SOURCE._COPY_DATA_TS
);



USE ROLE SYSADMIN;
USE WAREHOUSE ADHOC_WH;
USE DATABASE SANDBOX;

CREATE TABLE IF NOT EXISTS CLEAN_SCH.ORDER_ITEM (
    ORDER_ITEM_SK NUMBER AUTOINCREMENT PRIMARY KEY,
    ORDER_ITEM_ID NUMBER NOT NULL UNIQUE,
    ORDER_ID_FK NUMBER NOT NULL,
    MENU_ID_FK NUMBER NOT NULL,
    QUANTITY NUMBER(10,2),
    PRICE NUMBER(10,2),
    SUBTOTAL NUMBER(10,2),
    CREATED_DT TIMESTAMP,
    MODIFIED_DT TIMESTAMP,
    _STG_FILE_NAME VARCHAR(255),
    _STG_FILE_LOAD_TS TIMESTAMP,
    _STG_FILE_MD5 VARCHAR(255),
    _COPY_DATA_TS TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

INSERT INTO SANDBOX.CLEAN_SCH.ORDER_ITEM (
    ORDER_ITEM_ID,
    ORDER_ID_FK,
    MENU_ID_FK,
    QUANTITY,
    PRICE,
    SUBTOTAL,
    CREATED_DT,
    MODIFIED_DT,
    _STG_FILE_NAME,
    _STG_FILE_LOAD_TS,
    _STG_FILE_MD5,
    _COPY_DATA_TS
)

SELECT
    TRY_TO_NUMBER(S.ORDERITEMID),
    TRY_TO_NUMBER(S.ORDERID),
    TRY_TO_NUMBER(S.MENUID),
    TRY_TO_NUMBER(S.QUANTITY),
    TRY_TO_NUMBER(S.PRICE),
    TRY_TO_NUMBER(S.SUBTOTAL),
    TRY_TO_TIMESTAMP_NTZ(S.CREATEDDATE),
    TRY_TO_TIMESTAMP_NTZ(S.MODIFIEDDATE),
    S._STG_FILE_NAME,
    S._STG_FILE_LOAD_TS,
    S._STG_FILE_MD5,
    S._COPY_DATA_TS

FROM SANDBOX.STAGE_SCH.ORDERITEM S

WHERE TRY_TO_NUMBER(S.ORDERITEMID) IS NOT NULL
  AND TRY_TO_NUMBER(S.ORDERID) IS NOT NULL
  AND TRY_TO_NUMBER(S.MENUID) IS NOT NULL

  AND NOT EXISTS (
      SELECT 1
      FROM SANDBOX.CLEAN_SCH.ORDER_ITEM T
      WHERE T.ORDER_ITEM_ID = TRY_TO_NUMBER(S.ORDERITEMID)
  );








USE ROLE SYSADMIN;
USE WAREHOUSE ADHOC_WH;
USE DATABASE SANDBOX;

SELECT 'STAGE_ORDERS' AS TABLE_NAME, COUNT(*) AS ROW_COUNT
FROM STAGE_SCH.ORDERS

UNION ALL

SELECT 'STAGE_ORDERS_STM', COUNT(*)
FROM STAGE_SCH.ORDERS_STM

UNION ALL

SELECT 'CLEAN_ORDERS', COUNT(*)
FROM CLEAN_SCH.ORDERS;






USE ROLE SYSADMIN;
USE WAREHOUSE ADHOC_WH;
USE DATABASE SANDBOX;

MERGE INTO CLEAN_SCH.ORDERS AS TARGET

USING (
    SELECT
        TRY_TO_NUMBER(NULLIF(TRIM(ORDERID), '')) AS ORDER_ID,
        TRY_TO_NUMBER(NULLIF(TRIM(CUSTOMERID), '')) AS CUSTOMER_ID_FK,
        TRY_TO_NUMBER(NULLIF(TRIM(RESTAURANTID), '')) AS RESTAURANT_ID_FK,
        TRY_TO_TIMESTAMP_NTZ(NULLIF(TRIM(ORDERDATE), '')) AS ORDER_DATE,
        TRY_TO_DECIMAL(NULLIF(TRIM(TOTALAMOUNT), ''), 10, 2) AS TOTAL_AMOUNT,
        STATUS,
        PAYMENTMETHOD AS PAYMENT_METHOD,
        TRY_TO_TIMESTAMP_TZ(NULLIF(TRIM(CREATEDDATE), '')) AS CREATED_DT,
        TRY_TO_TIMESTAMP_TZ(NULLIF(TRIM(MODIFIEDDATE), '')) AS MODIFIED_DT,
        _STG_FILE_NAME,
        _STG_FILE_LOAD_TS,
        _STG_FILE_MD5
    FROM SANDBOX.STAGE_SCH.ORDERS
    WHERE TRY_TO_NUMBER(NULLIF(TRIM(ORDERID), '')) IS NOT NULL
) AS SOURCE

ON TARGET.ORDER_ID = SOURCE.ORDER_ID

WHEN MATCHED THEN UPDATE SET
    TARGET.CUSTOMER_ID_FK = SOURCE.CUSTOMER_ID_FK,
    TARGET.RESTAURANT_ID_FK = SOURCE.RESTAURANT_ID_FK,
    TARGET.ORDER_DATE = SOURCE.ORDER_DATE,
    TARGET.TOTAL_AMOUNT = SOURCE.TOTAL_AMOUNT,
    TARGET.STATUS = SOURCE.STATUS,
    TARGET.PAYMENT_METHOD = SOURCE.PAYMENT_METHOD,
    TARGET.CREATED_DT = SOURCE.CREATED_DT,
    TARGET.MODIFIED_DT = SOURCE.MODIFIED_DT,
    TARGET._STG_FILE_NAME = SOURCE._STG_FILE_NAME,
    TARGET._STG_FILE_LOAD_TS = SOURCE._STG_FILE_LOAD_TS,
    TARGET._STG_FILE_MD5 = SOURCE._STG_FILE_MD5,
    TARGET._COPY_DATA_TS = CURRENT_TIMESTAMP()

WHEN NOT MATCHED THEN INSERT (
    ORDER_ID,
    CUSTOMER_ID_FK,
    RESTAURANT_ID_FK,
    ORDER_DATE,
    TOTAL_AMOUNT,
    STATUS,
    PAYMENT_METHOD,
    CREATED_DT,
    MODIFIED_DT,
    _STG_FILE_NAME,
    _STG_FILE_LOAD_TS,
    _STG_FILE_MD5
)
VALUES (
    SOURCE.ORDER_ID,
    SOURCE.CUSTOMER_ID_FK,
    SOURCE.RESTAURANT_ID_FK,
    SOURCE.ORDER_DATE,
    SOURCE.TOTAL_AMOUNT,
    SOURCE.STATUS,
    SOURCE.PAYMENT_METHOD,
    SOURCE.CREATED_DT,
    SOURCE.MODIFIED_DT,
    SOURCE._STG_FILE_NAME,
    SOURCE._STG_FILE_LOAD_TS,
    SOURCE._STG_FILE_MD5
);

SELECT COUNT(*)
FROM SANDBOX.CLEAN_SCH.ORDERS;

SELECT COUNT(*) AS TOTAL_ORDERS
FROM SANDBOX.CLEAN_SCH.ORDERS;