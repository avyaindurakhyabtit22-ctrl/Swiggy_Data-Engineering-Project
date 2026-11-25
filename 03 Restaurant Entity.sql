-- change context
use role sysadmin;
use database sandbox;
use schema stage_sch;
use warehouse adhoc_wh;

-- create restaurant table under stage location, with all text value + audit column for copy command
create or replace table stage_sch.restaurant (
    restaurantid text,      
    name text ,                                         -- restaurant name, required field
    cuisinetype text,                                    -- type of cuisine offered
    pricing_for_2 text,                                  -- pricing for two people as text
    restaurant_phone text WITH TAG (common.pii_policy_tag = 'SENSITIVE'),                               -- phone number as text
    operatinghours text,                                 -- restaurant operating hours
    locationid text ,                                    -- location id, default as text
    activeflag text ,                                    -- active status
    openstatus text ,                                    -- open status
    locality text,                                       -- locality as text
    restaurant_address text,                             -- address as text
    latitude text,                                       -- latitude as text for precision
    longitude text,                                      -- longitude as text for precision
    createddate text,                                    -- record creation date
    modifieddate text,                                   -- last modified date

    -- audit columns for debugging
    _stg_file_name text,
    _stg_file_load_ts timestamp,
    _stg_file_md5 text,
    _copy_data_ts timestamp default current_timestamp
)
comment = 'This is the restaurant stage/raw table where data will be copied from internal stage using copy command. This is as-is data represetation from the source location. All the columns are text data type except the audit columns that are added for traceability.'
;

-- Stream object to capture the changes. 
create or replace stream stage_sch.restaurant_stm 
on table stage_sch.restaurant
append_only = true
comment = 'This is the append-only stream object on restaurant table that only gets delta data';


-- run copy command to load the data into stage-restaurant table.
copy into stage_sch.restaurant (restaurantid, name, cuisinetype, pricing_for_2, restaurant_phone, 
                      operatinghours, locationid, activeflag, openstatus, 
                      locality, restaurant_address, latitude, longitude, 
                      createddate, modifieddate, 
                      _stg_file_name, _stg_file_load_ts, _stg_file_md5, _copy_data_ts)
from (
    select 
        t.$1::text as restaurantid,        -- restaurantid as the first column
        t.$2::text as name,
        t.$3::text as cuisinetype,
        t.$4::text as pricing_for_2,
        t.$5::text as restaurant_phone,
        t.$6::text as operatinghours,
        t.$7::text as locationid,
        t.$8::text as activeflag,
        t.$9::text as openstatus,
        t.$10::text as locality,
        t.$11::text as restaurant_address,
        t.$12::text as latitude,
        t.$13::text as longitude,
        t.$14::text as createddate,
        t.$15::text as modifieddate,
        -- audit columns for tracking & debugging
        metadata$filename as _stg_file_name,
        metadata$file_last_modified as _stg_file_load_ts,
        metadata$file_content_key as _stg_file_md5,
        current_timestamp() as _copy_data_ts
     from @stage_sch.csv_stg/initial/restaurant/restaurant-delhi+NCR.csv t
)
file_format = (format_name = 'stage_sch.csv_file_format')
on_error = abort_statement;


-- the restaurant table where data types are defined. 
create or replace table clean_sch.restaurant (
    restaurant_sk number autoincrement primary key,              -- primary key with auto-increment
    restaurant_id number unique,                                        -- restaurant id without auto-increment
    name string(100) not null,                                   -- restaurant name, required field
    cuisine_type string,                                         -- type of cuisine offered
    pricing_for_two number(10, 2),                               -- pricing for two people, up to 10 digits with 2 decimal places
    restaurant_phone string(15) WITH TAG (common.pii_policy_tag = 'SENSITIVE'),                                 -- phone number, supports 10-digit or international format
    operating_hours string(100),                                  -- restaurant operating hours
    location_id_fk number,                                       -- reference id for location, defaulted to 1
    active_flag string(10),                                      -- indicates if the restaurant is active
    open_status string(10),                                      -- indicates if the restaurant is currently open
    locality string(100),                                        -- locality of the restaurant
    restaurant_address string,                                   -- address of the restaurant, supports longer text
    latitude number(9, 6),                                       -- latitude with 6 decimal places for precision
    longitude number(9, 6),                                      -- longitude with 6 decimal places for precision
    created_dt timestamp_tz,                                     -- record creation date
    modified_dt timestamp_tz,                                    -- last modified date, allows null if not modified

    -- additional audit columns
    _stg_file_name string,                                       -- file name for audit
    _stg_file_load_ts timestamp_ntz,                             -- file load timestamp for audit
    _stg_file_md5 string,                                        -- md5 hash for file content for audit
    _copy_data_ts timestamp_ntz default current_timestamp        -- timestamp when data is copied, defaults to current timestamp
)
comment = 'Restaurant entity under clean schema with appropriate data type under clean schema layer, data is populated using merge statement from the stage layer location table. This table does not support SCD2';

create or replace stream clean_sch.restaurant_stm 
on table clean_sch.restaurant
comment = 'This is a standard stream object on the clean restaurant table to track insert, update, and delete changes';

-- following is the insert statement..
insert into clean_sch.restaurant (
    restaurant_id,
    name,
    cuisine_type,
    pricing_for_two,
    restaurant_phone,
    operating_hours,
    location_id_fk,
    active_flag,
    open_status,
    locality,
    restaurant_address,
    latitude,
    longitude,
    created_dt,
    modified_dt,
    _stg_file_name,
    _stg_file_load_ts,
    _stg_file_md5
)
select 
    try_cast(restaurantid as number) as restaurant_id,
    try_cast(name as string) as name,
    try_cast(cuisinetype as string) as cuisine_type,
    try_cast(pricing_for_2 as number(10, 2)) as pricing_for_two,
    try_cast(restaurant_phone as string) as restaurant_phone,
    try_cast(operatinghours as string) as operating_hours,
    try_cast(locationid as number) as location_id_fk,
    try_cast(activeflag as string) as active_flag,
    try_cast(openstatus as string) as open_status,
    try_cast(locality as string) as locality,
    try_cast(restaurant_address as string) as restaurant_address,
    try_cast(latitude as number(9, 6)) as latitude,
    try_cast(longitude as number(9, 6)) as longitude,
    try_to_timestamp_ntz(createddate, 'YYYY-MM-DD HH24:MI:SS.FF9') as created_dt,
    try_to_timestamp_ntz(modifieddate, 'YYYY-MM-DD HH24:MI:SS.FF9') as modified_dt,
    _stg_file_name,
    _stg_file_load_ts,
    _stg_file_md5
from 
    stage_sch.restaurant;
        
-- here is the merge statement
MERGE INTO clean_sch.restaurant AS target
USING (
    SELECT 
        try_cast(restaurantid AS number) AS restaurant_id,
        try_cast(name AS string) AS name,
        try_cast(cuisinetype AS string) AS cuisine_type,
        try_cast(pricing_for_2 AS number(10, 2)) AS pricing_for_two,
        try_cast(restaurant_phone AS string) AS restaurant_phone,
        try_cast(operatinghours AS string) AS operating_hours,
        try_cast(locationid AS number) AS location_id_fk,
        try_cast(activeflag AS string) AS active_flag,
        try_cast(openstatus AS string) AS open_status,
        try_cast(locality AS string) AS locality,
        try_cast(restaurant_address AS string) AS restaurant_address,
        try_cast(latitude AS number(9, 6)) AS latitude,
        try_cast(longitude AS number(9, 6)) AS longitude,
        try_to_timestamp_ntz(createddate, 'YYYY-MM-DD HH24:MI:SS.FF9') AS created_dt,
        try_to_timestamp_ntz(modifieddate, 'YYYY-MM-DD HH24:MI:SS.FF9') AS modified_dt,
        _stg_file_name,
        _stg_file_load_ts,
        _stg_file_md5
    FROM 
        stage_sch.restaurant_stm
) AS source
ON target.restaurant_id = source.restaurant_id
WHEN MATCHED THEN 
    UPDATE SET 
        target.name = source.name,
        target.cuisine_type = source.cuisine_type,
        target.pricing_for_two = source.pricing_for_two,
        target.restaurant_phone = source.restaurant_phone,
        target.operating_hours = source.operating_hours,
        target.location_id_fk = source.location_id_fk,
        target.active_flag = source.active_flag,
        target.open_status = source.open_status,
        target.locality = source.locality,
        target.restaurant_address = source.restaurant_address,
        target.latitude = source.latitude,
        target.longitude = source.longitude,
        target.created_dt = source.created_dt,
        target.modified_dt = source.modified_dt,
        target._stg_file_name = source._stg_file_name,
        target._stg_file_load_ts = source._stg_file_load_ts,
        target._stg_file_md5 = source._stg_file_md5
WHEN NOT MATCHED THEN 
    INSERT (
        restaurant_id,
        name,
        cuisine_type,
        pricing_for_two,
        restaurant_phone,
        operating_hours,
        location_id_fk,
        active_flag,
        open_status,
        locality,
        restaurant_address,
        latitude,
        longitude,
        created_dt,
        modified_dt,
        _stg_file_name,
        _stg_file_load_ts,
        _stg_file_md5
    )
    VALUES (
        source.restaurant_id,
        source.name,
        source.cuisine_type,
        source.pricing_for_two,
        source.restaurant_phone,
        source.operating_hours,
        source.location_id_fk,
        source.active_flag,
        source.open_status,
        source.locality,
        source.restaurant_address,
        source.latitude,
        source.longitude,
        source.created_dt,
        source.modified_dt,
        source._stg_file_name,
        source._stg_file_load_ts,
        source._stg_file_md5
    );

-- now define dim table for restaurant.
CREATE OR REPLACE TABLE CONSUMPTION_SCH.RESTAURANT_DIM (
    RESTAURANT_HK NUMBER primary key,                   -- Hash key for the restaurant location
    RESTAURANT_ID NUMBER,                   -- Restaurant ID without auto-increment
    NAME STRING(100),                       -- Restaurant name
    CUISINE_TYPE STRING,                    -- Type of cuisine offered
    PRICING_FOR_TWO NUMBER(10, 2),          -- Pricing for two people
    RESTAURANT_PHONE STRING(15) WITH TAG (common.pii_policy_tag = 'SENSITIVE'),            -- Restaurant phone number
    OPERATING_HOURS STRING(100),            -- Restaurant operating hours
    LOCATION_ID_FK NUMBER,                  -- Foreign key reference to location
    ACTIVE_FLAG STRING(10),                 -- Indicates if the restaurant is active
    OPEN_STATUS STRING(10),                 -- Indicates if the restaurant is currently open
    LOCALITY STRING(100),                   -- Locality of the restaurant
    RESTAURANT_ADDRESS STRING,              -- Full address of the restaurant
    LATITUDE NUMBER(9, 6),                  -- Latitude for the restaurant's location
    LONGITUDE NUMBER(9, 6),                 -- Longitude for the restaurant's location
    EFF_START_DATE TIMESTAMP_TZ,            -- Effective start date for the record
    EFF_END_DATE TIMESTAMP_TZ,              -- Effective end date for the record (NULL if active)
    IS_CURRENT BOOLEAN                     -- Indicates whether the record is the current version
)
COMMENT = 'Dimensional table for Restaurant entity with hash keys and SCD enabled.';

-- how many changes are available.
select count(*) from CLEAN_SCH.RESTAURANT_STM;

-- merge statement
MERGE INTO 
    CONSUMPTION_SCH.RESTAURANT_DIM AS target
USING 
    CLEAN_SCH.RESTAURANT_STM AS source
ON 
    target.RESTAURANT_ID = source.RESTAURANT_ID AND 
    target.NAME = source.NAME AND 
    target.CUISINE_TYPE = source.CUISINE_TYPE AND 
    target.PRICING_FOR_TWO = source.PRICING_FOR_TWO AND 
    target.RESTAURANT_PHONE = source.RESTAURANT_PHONE AND 
    target.OPERATING_HOURS = source.OPERATING_HOURS AND 
    target.LOCATION_ID_FK = source.LOCATION_ID_FK AND 
    target.ACTIVE_FLAG = source.ACTIVE_FLAG AND 
    target.OPEN_STATUS = source.OPEN_STATUS AND 
    target.LOCALITY = source.LOCALITY AND 
    target.RESTAURANT_ADDRESS = source.RESTAURANT_ADDRESS AND 
    target.LATITUDE = source.LATITUDE AND 
    target.LONGITUDE = source.LONGITUDE
WHEN MATCHED 
    AND source.METADATA$ACTION = 'DELETE' AND source.METADATA$ISUPDATE = 'TRUE' THEN
    -- Update the existing record to close its validity period
    UPDATE SET 
        target.EFF_END_DATE = CURRENT_TIMESTAMP(),
        target.IS_CURRENT = FALSE
WHEN NOT MATCHED 
    AND source.METADATA$ACTION = 'INSERT' AND source.METADATA$ISUPDATE = 'TRUE' THEN
    -- Insert new record with current data and new effective start date
    INSERT (
        RESTAURANT_HK,
        RESTAURANT_ID,
        NAME,
        CUISINE_TYPE,
        PRICING_FOR_TWO,
        RESTAURANT_PHONE,
        OPERATING_HOURS,
        LOCATION_ID_FK,
        ACTIVE_FLAG,
        OPEN_STATUS,
        LOCALITY,
        RESTAURANT_ADDRESS,
        LATITUDE,
        LONGITUDE,
        EFF_START_DATE,
        EFF_END_DATE,
        IS_CURRENT
    )
    VALUES (
        hash(SHA1_hex(CONCAT(source.RESTAURANT_ID, source.NAME, source.CUISINE_TYPE, 
            source.PRICING_FOR_TWO, source.RESTAURANT_PHONE, source.OPERATING_HOURS, 
            source.LOCATION_ID_FK, source.ACTIVE_FLAG, source.OPEN_STATUS, source.LOCALITY, 
            source.RESTAURANT_ADDRESS, source.LATITUDE, source.LONGITUDE))),
        source.RESTAURANT_ID,
        source.NAME,
        source.CUISINE_TYPE,
        source.PRICING_FOR_TWO,
        source.RESTAURANT_PHONE,
        source.OPERATING_HOURS,
        source.LOCATION_ID_FK,
        source.ACTIVE_FLAG,
        source.OPEN_STATUS,
        source.LOCALITY,
        source.RESTAURANT_ADDRESS,
        source.LATITUDE,
        source.LONGITUDE,
        CURRENT_TIMESTAMP(),
        NULL,
        TRUE
    )
WHEN NOT MATCHED 
    AND source.METADATA$ACTION = 'INSERT' AND source.METADATA$ISUPDATE = 'FALSE' THEN
    -- Insert new record with current data and new effective start date
    INSERT (
        RESTAURANT_HK,
        RESTAURANT_ID,
        NAME,
        CUISINE_TYPE,
        PRICING_FOR_TWO,
        RESTAURANT_PHONE,
        OPERATING_HOURS,
        LOCATION_ID_FK,
        ACTIVE_FLAG,
        OPEN_STATUS,
        LOCALITY,
        RESTAURANT_ADDRESS,
        LATITUDE,
        LONGITUDE,
        EFF_START_DATE,
        EFF_END_DATE,
        IS_CURRENT
    )
    VALUES (
        hash(SHA1_hex(CONCAT(source.RESTAURANT_ID, source.NAME, source.CUISINE_TYPE, 
            source.PRICING_FOR_TWO, source.RESTAURANT_PHONE, source.OPERATING_HOURS, 
            source.LOCATION_ID_FK, source.ACTIVE_FLAG, source.OPEN_STATUS, source.LOCALITY, 
            source.RESTAURANT_ADDRESS, source.LATITUDE, source.LONGITUDE))),
        source.RESTAURANT_ID,
        source.NAME,
        source.CUISINE_TYPE,
        source.PRICING_FOR_TWO,
        source.RESTAURANT_PHONE,
        source.OPERATING_HOURS,
        source.LOCATION_ID_FK,
        source.ACTIVE_FLAG,
        source.OPEN_STATUS,
        source.LOCALITY,
        source.RESTAURANT_ADDRESS,
        source.LATITUDE,
        source.LONGITUDE,
        CURRENT_TIMESTAMP(),
        NULL,
        TRUE
    );


-- load the delta data

list @stage_sch.csv_stg/daily/restaurant/;

copy into stage_sch.restaurant (restaurantid, name, cuisinetype, pricing_for_2, restaurant_phone, 
                      operatinghours, locationid, activeflag, openstatus, 
                      locality, restaurant_address, latitude, longitude, 
                      createddate, modifieddate, 
                      _stg_file_name, _stg_file_load_ts, _stg_file_md5, _copy_data_ts)
from (
    select 
        t.$1::text as restaurantid,        -- restaurantid as the first column
        t.$2::text as name,
        t.$3::text as cuisinetype,
        t.$4::text as pricing_for_2,
        t.$5::text as restaurant_phone,
        t.$6::text as operatinghours,
        t.$7::text as locationid,
        t.$8::text as activeflag,
        t.$9::text as openstatus,
        t.$10::text as locality,
        t.$11::text as restaurant_address,
        t.$12::text as latitude,
        t.$13::text as longitude,
        t.$14::text as createddate,
        t.$15::text as modifieddate,
        metadata$filename as _stg_file_name,
        metadata$file_last_modified as _stg_file_load_ts,
        metadata$file_content_key as _stg_file_md5,
        current_timestamp() as _copy_data_ts
     from @stage_sch.csv_stg/daily/restaurant/day-02-upsert-restaurant-delhi+NCR.csv t
)
file_format = (format_name = 'stage_sch.csv_file_format')
on_error = abort_statement;


// ------------------------------------------------
// Part -2 loading the delta data

list @stage_sch.csv_stg/delta/restaurant/;
copy into stage_sch.restaurant (restaurantid, name, cuisinetype, pricing_for_2, restaurant_phone, 
                      operatinghours, locationid, activeflag, openstatus, 
                      locality, restaurant_address, latitude, longitude, 
                      createddate, modifieddate, 
                      _stg_file_name, _stg_file_load_ts, _stg_file_md5, _copy_data_ts)
from (
    select 
        t.$1::text as restaurantid,        -- restaurantid as the first column
        t.$2::text as name,
        t.$3::text as cuisinetype,
        t.$4::text as pricing_for_2,
        t.$5::text as restaurant_phone,
        t.$6::text as operatinghours,
        t.$7::text as locationid,
        t.$8::text as activeflag,
        t.$9::text as openstatus,
        t.$10::text as locality,
        t.$11::text as restaurant_address,
        t.$12::text as latitude,
        t.$13::text as longitude,
        t.$14::text as createddate,
        t.$15::text as modifieddate,
        metadata$filename as _stg_file_name,
        metadata$file_last_modified as _stg_file_load_ts,
        metadata$file_content_key as _stg_file_md5,
        current_timestamp() as _copy_data_ts
     from @stage_sch.csv_stg/delta/restaurant/day-02-upsert-restaurant-delhi+NCR.csv t
)
file_format = (format_name = 'stage_sch.csv_file_format')
on_error = abort_statement;

select * from table(information_schema.copy_history(table_name=>'RESTAURANT', start_time=> dateadd(hours, -1, current_timestamp())));



SELECT 'STAGE_RESTAURANT' AS TABLE_NAME, COUNT(*) AS ROW_COUNT
FROM SANDBOX.STAGE_SCH.RESTAURANT

UNION ALL

SELECT 'CLEAN_RESTAURANT', COUNT(*)
FROM SANDBOX.CLEAN_SCH.RESTAURANT

UNION ALL

SELECT 'RESTAURANT_DIM', COUNT(*)
FROM SANDBOX.CONSUMPTION_SCH.RESTAURANT_DIM;





USE ROLE SYSADMIN;
USE DATABASE SANDBOX;

SELECT
    FILE_NAME,
    STATUS,
    ROW_COUNT,
    ROW_PARSED,
    ERROR_COUNT,
    FIRST_ERROR_MESSAGE
FROM TABLE(
    INFORMATION_SCHEMA.COPY_HISTORY(
        TABLE_NAME => 'RESTAURANT',
        START_TIME => DATEADD(HOUR, -24, CURRENT_TIMESTAMP())
    )
);



USE ROLE SYSADMIN;
USE WAREHOUSE ADHOC_WH;
USE DATABASE SANDBOX;

SELECT
    FILE_NAME,
    STATUS,
    ROW_COUNT,
    ROW_PARSED,
    ERROR_COUNT,
    FIRST_ERROR_MESSAGE
FROM TABLE(
    SANDBOX.INFORMATION_SCHEMA.COPY_HISTORY(
        TABLE_NAME => 'STAGE_SCH.RESTAURANT',
        START_TIME => DATEADD(HOUR, -24, CURRENT_TIMESTAMP())
    )
)
ORDER BY LAST_LOAD_TIME DESC;



SELECT 'STAGE_RESTAURANT' AS TABLE_NAME,
       COUNT(*) AS ROW_COUNT
FROM SANDBOX.STAGE_SCH.RESTAURANT

UNION ALL

SELECT 'CLEAN_RESTAURANT',
       COUNT(*)
FROM SANDBOX.CLEAN_SCH.RESTAURANT

UNION ALL

SELECT 'RESTAURANT_DIM',
       COUNT(*)
FROM SANDBOX.CONSUMPTION_SCH.RESTAURANT_DIM;




USE ROLE SYSADMIN;
USE WAREHOUSE ADHOC_WH;
USE DATABASE SANDBOX;

INSERT INTO CLEAN_SCH.RESTAURANT (
    RESTAURANT_ID,
    NAME,
    CUISINE_TYPE,
    PRICING_FOR_TWO,
    RESTAURANT_PHONE,
    OPERATING_HOURS,
    LOCATION_ID_FK,
    ACTIVE_FLAG,
    OPEN_STATUS,
    LOCALITY,
    RESTAURANT_ADDRESS,
    LATITUDE,
    LONGITUDE,
    CREATED_DT,
    MODIFIED_DT,
    _STG_FILE_NAME,
    _STG_FILE_LOAD_TS,
    _STG_FILE_MD5
)

SELECT
    TRY_CAST(RESTAURANTID AS NUMBER),
    TRY_CAST(NAME AS STRING),
    TRY_CAST(CUISINETYPE AS STRING),
    TRY_CAST(PRICING_FOR_2 AS NUMBER(10,2)),
    TRY_CAST(RESTAURANT_PHONE AS STRING),
    TRY_CAST(OPERATINGHOURS AS STRING),
    TRY_CAST(LOCATIONID AS NUMBER),
    TRY_CAST(ACTIVEFLAG AS STRING),
    TRY_CAST(OPENSTATUS AS STRING),
    TRY_CAST(LOCALITY AS STRING),
    TRY_CAST(RESTAURANT_ADDRESS AS STRING),
    TRY_CAST(LATITUDE AS NUMBER(9,6)),
    TRY_CAST(LONGITUDE AS NUMBER(9,6)),
    TRY_TO_TIMESTAMP_NTZ(
        CREATEDDATE, 'YYYY-MM-DD HH24:MI:SS.FF9'
    ),
    TRY_TO_TIMESTAMP_NTZ(
        MODIFIEDDATE, 'YYYY-MM-DD HH24:MI:SS.FF9'
    ),
    _STG_FILE_NAME,
    _STG_FILE_LOAD_TS,
    _STG_FILE_MD5

FROM STAGE_SCH.RESTAURANT

WHERE TRY_CAST(RESTAURANTID AS NUMBER) IS NOT NULL
  AND NAME IS NOT NULL;


  SELECT COUNT(*) AS TOTAL_ROWS
FROM SANDBOX.CLEAN_SCH.RESTAURANT;

SELECT COUNT(*) AS TOTAL_ROWS
FROM SANDBOX.CLEAN_SCH.RESTAURANT;


SELECT COUNT(*) AS STREAM_ROWS
FROM SANDBOX.CLEAN_SCH.RESTAURANT_STM;



USE ROLE SYSADMIN;
USE WAREHOUSE ADHOC_WH;
USE DATABASE SANDBOX;

MERGE INTO CONSUMPTION_SCH.RESTAURANT_DIM AS TARGET

USING CLEAN_SCH.RESTAURANT_STM AS SOURCE

ON
    TARGET.RESTAURANT_ID = SOURCE.RESTAURANT_ID
    AND TARGET.NAME = SOURCE.NAME
    AND TARGET.CUISINE_TYPE = SOURCE.CUISINE_TYPE
    AND TARGET.PRICING_FOR_TWO = SOURCE.PRICING_FOR_TWO
    AND TARGET.RESTAURANT_PHONE = SOURCE.RESTAURANT_PHONE
    AND TARGET.OPERATING_HOURS = SOURCE.OPERATING_HOURS
    AND TARGET.LOCATION_ID_FK = SOURCE.LOCATION_ID_FK
    AND TARGET.ACTIVE_FLAG = SOURCE.ACTIVE_FLAG
    AND TARGET.OPEN_STATUS = SOURCE.OPEN_STATUS
    AND TARGET.LOCALITY = SOURCE.LOCALITY
    AND TARGET.RESTAURANT_ADDRESS = SOURCE.RESTAURANT_ADDRESS
    AND TARGET.LATITUDE = SOURCE.LATITUDE
    AND TARGET.LONGITUDE = SOURCE.LONGITUDE

WHEN MATCHED
    AND SOURCE.METADATA$ACTION = 'DELETE'
    AND SOURCE.METADATA$ISUPDATE = TRUE
THEN UPDATE SET
    TARGET.EFF_END_DATE = CURRENT_TIMESTAMP(),
    TARGET.IS_CURRENT = FALSE

WHEN NOT MATCHED
    AND SOURCE.METADATA$ACTION = 'INSERT'
    AND SOURCE.METADATA$ISUPDATE = TRUE
THEN INSERT (
    RESTAURANT_HK,
    RESTAURANT_ID,
    NAME,
    CUISINE_TYPE,
    PRICING_FOR_TWO,
    RESTAURANT_PHONE,
    OPERATING_HOURS,
    LOCATION_ID_FK,
    ACTIVE_FLAG,
    OPEN_STATUS,
    LOCALITY,
    RESTAURANT_ADDRESS,
    LATITUDE,
    LONGITUDE,
    EFF_START_DATE,
    EFF_END_DATE,
    IS_CURRENT
)
VALUES (
    HASH(SHA1_HEX(CONCAT(
        SOURCE.RESTAURANT_ID, SOURCE.NAME,
        SOURCE.CUISINE_TYPE, SOURCE.PRICING_FOR_TWO,
        SOURCE.RESTAURANT_PHONE, SOURCE.OPERATING_HOURS,
        SOURCE.LOCATION_ID_FK, SOURCE.ACTIVE_FLAG,
        SOURCE.OPEN_STATUS, SOURCE.LOCALITY,
        SOURCE.RESTAURANT_ADDRESS, SOURCE.LATITUDE,
        SOURCE.LONGITUDE
    ))),
    SOURCE.RESTAURANT_ID,
    SOURCE.NAME,
    SOURCE.CUISINE_TYPE,
    SOURCE.PRICING_FOR_TWO,
    SOURCE.RESTAURANT_PHONE,
    SOURCE.OPERATING_HOURS,
    SOURCE.LOCATION_ID_FK,
    SOURCE.ACTIVE_FLAG,
    SOURCE.OPEN_STATUS,
    SOURCE.LOCALITY,
    SOURCE.RESTAURANT_ADDRESS,
    SOURCE.LATITUDE,
    SOURCE.LONGITUDE,
    CURRENT_TIMESTAMP(),
    NULL,
    TRUE
)

WHEN NOT MATCHED
    AND SOURCE.METADATA$ACTION = 'INSERT'
    AND SOURCE.METADATA$ISUPDATE = FALSE
THEN INSERT (
    RESTAURANT_HK,
    RESTAURANT_ID,
    NAME,
    CUISINE_TYPE,
    PRICING_FOR_TWO,
    RESTAURANT_PHONE,
    OPERATING_HOURS,
    LOCATION_ID_FK,
    ACTIVE_FLAG,
    OPEN_STATUS,
    LOCALITY,
    RESTAURANT_ADDRESS,
    LATITUDE,
    LONGITUDE,
    EFF_START_DATE,
    EFF_END_DATE,
    IS_CURRENT
)
VALUES (
    HASH(SHA1_HEX(CONCAT(
        SOURCE.RESTAURANT_ID, SOURCE.NAME,
        SOURCE.CUISINE_TYPE, SOURCE.PRICING_FOR_TWO,
        SOURCE.RESTAURANT_PHONE, SOURCE.OPERATING_HOURS,
        SOURCE.LOCATION_ID_FK, SOURCE.ACTIVE_FLAG,
        SOURCE.OPEN_STATUS, SOURCE.LOCALITY,
        SOURCE.RESTAURANT_ADDRESS, SOURCE.LATITUDE,
        SOURCE.LONGITUDE
    ))),
    SOURCE.RESTAURANT_ID,
    SOURCE.NAME,
    SOURCE.CUISINE_TYPE,
    SOURCE.PRICING_FOR_TWO,
    SOURCE.RESTAURANT_PHONE,
    SOURCE.OPERATING_HOURS,
    SOURCE.LOCATION_ID_FK,
    SOURCE.ACTIVE_FLAG,
    SOURCE.OPEN_STATUS,
    SOURCE.LOCALITY,
    SOURCE.RESTAURANT_ADDRESS,
    SOURCE.LATITUDE,
    SOURCE.LONGITUDE,
    CURRENT_TIMESTAMP(),
    NULL,
    TRUE
);

SELECT COUNT(*) AS RESTAURANT_DIM_COUNT
FROM SANDBOX.CONSUMPTION_SCH.RESTAURANT_DIM;

SELECT *
FROM SANDBOX.CONSUMPTION_SCH.RESTAURANT_DIM;



USE ROLE SYSADMIN;
USE WAREHOUSE ADHOC_WH;
USE DATABASE SANDBOX;

SELECT COUNT(*) AS MATCHED_FACT_ROWS
FROM CLEAN_SCH.ORDER_ITEM oi

JOIN CLEAN_SCH.ORDERS o
    ON oi.ORDER_ID_FK = o.ORDER_ID

JOIN CLEAN_SCH.DELIVERY d
    ON o.ORDER_ID = d.ORDER_ID_FK

JOIN CONSUMPTION_SCH.CUSTOMER_DIM c
    ON o.CUSTOMER_ID_FK = c.CUSTOMER_ID

JOIN CONSUMPTION_SCH.CUSTOMER_ADDRESS_DIM ca
    ON c.CUSTOMER_ID = ca.CUSTOMER_ID_FK

JOIN CONSUMPTION_SCH.RESTAURANT_DIM r
    ON o.RESTAURANT_ID_FK = r.RESTAURANT_ID

JOIN CONSUMPTION_SCH.RESTAURANT_LOCATION_DIM rl
    ON r.LOCATION_ID_FK = rl.LOCATION_ID

JOIN CONSUMPTION_SCH.MENU_DIM m
    ON oi.MENU_ID_FK = m.MENU_ID

JOIN CONSUMPTION_SCH.DELIVERY_AGENT_DIM da
    ON d.DELIVERY_AGENT_ID_FK = da.DELIVERY_AGENT_ID

JOIN CONSUMPTION_SCH.DATE_DIM dd
    ON dd.CALENDAR_DATE = TO_DATE(o.ORDER_DATE);







USE ROLE SYSADMIN;
USE WAREHOUSE ADHOC_WH;
USE DATABASE SANDBOX;

SELECT '1. ORDER ITEM + ORDERS' AS CHECKPOINT,
       COUNT(*) AS MATCHED_ROWS
FROM CLEAN_SCH.ORDER_ITEM oi
JOIN CLEAN_SCH.ORDERS o
    ON oi.ORDER_ID_FK = o.ORDER_ID

UNION ALL

SELECT '2. + DELIVERY', COUNT(*)
FROM CLEAN_SCH.ORDER_ITEM oi
JOIN CLEAN_SCH.ORDERS o
    ON oi.ORDER_ID_FK = o.ORDER_ID
JOIN CLEAN_SCH.DELIVERY d
    ON o.ORDER_ID = d.ORDER_ID_FK

UNION ALL

SELECT '3. + CUSTOMER DIM', COUNT(*)
FROM CLEAN_SCH.ORDER_ITEM oi
JOIN CLEAN_SCH.ORDERS o
    ON oi.ORDER_ID_FK = o.ORDER_ID
JOIN CLEAN_SCH.DELIVERY d
    ON o.ORDER_ID = d.ORDER_ID_FK
JOIN CONSUMPTION_SCH.CUSTOMER_DIM c
    ON o.CUSTOMER_ID_FK = c.CUSTOMER_ID

UNION ALL

SELECT '4. + RESTAURANT DIM', COUNT(*)
FROM CLEAN_SCH.ORDER_ITEM oi
JOIN CLEAN_SCH.ORDERS o
    ON oi.ORDER_ID_FK = o.ORDER_ID
JOIN CLEAN_SCH.RESTAURANT r
    ON o.RESTAURANT_ID_FK = r.RESTAURANT_ID;



SELECT
o.ORDER_ID,
d.ORDER_ID_FK
FROM CLEAN_SCH.ORDERS o
LEFT JOIN CLEAN_SCH.DELIVERY d
    ON o.ORDER_ID = d.ORDER_ID_FK
ORDER BY o.ORDER_ID;




SELECT
    ORDER_ID_FK,
    DELIVERY_AGENT_ID_FK,
    DELIVERY_STATUS,
    ESTIMATED_TIME
FROM CLEAN_SCH.DELIVERY
ORDER BY ORDER_ID_FK;



SELECT COUNT(*) AS DELIVERY_MATCH_COUNT
FROM CLEAN_SCH.ORDERS o
INNER JOIN CLEAN_SCH.DELIVERY d
    ON o.ORDER_ID = d.ORDER_ID_FK;


SELECT
    d.DELIVERY_AGENT_ID_FK,
    da.DELIVERY_AGENT_ID
FROM CLEAN_SCH.DELIVERY d
LEFT JOIN CONSUMPTION_SCH.DELIVERY_AGENT_DIM da
    ON d.DELIVERY_AGENT_ID_FK = da.DELIVERY_AGENT_ID
ORDER BY d.DELIVERY_AGENT_ID_FK;

SELECT
    DELIVERY_AGENT_ID,
    DELIVERY_AGENT_NAME
FROM CONSUMPTION_SCH.DELIVERY_AGENT_DIM
ORDER BY DELIVERY_AGENT_ID;


DESC TABLE CONSUMPTION_SCH.DELIVERY_AGENT_DIM;



SELECT
    DELIVERY_AGENT_ID,
    NAME,
    LOCATION_ID_FK,
    IS_CURRENT
FROM CONSUMPTION_SCH.DELIVERY_AGENT_DIM
ORDER BY DELIVERY_AGENT_ID;


SELECT *
FROM CLEAN_SCH.DELIVERY_AGENT
WHERE DELIVERY_AGENT_ID = 6;    

SELECT *
FROM STAGE_SCH.DELIVERY_AGENT
WHERE DELIVERYAGENTID = 6;

SHOW TABLES IN SCHEMA SANDBOX.STAGE_SCH;


SELECT *
FROM SANDBOX.STAGE_SCH.DELIVERYAGENT
WHERE DELIVERYAGENTID = '6';


SELECT DELIVERYAGENTID
FROM SANDBOX.STAGE_SCH.DELIVERYAGENT
ORDER BY DELIVERYAGENTID;

SELECT
    o.CUSTOMER_ID_FK,
    c.CUSTOMER_ID
FROM CLEAN_SCH.ORDERS o
LEFT JOIN CONSUMPTION_SCH.CUSTOMER_DIM c
    ON o.CUSTOMER_ID_FK = c.CUSTOMER_ID
ORDER BY o.ORDER_ID;

SELECT *
FROM CLEAN_SCH.CUSTOMER
WHERE CUSTOMER_ID IN (9, 10);

SELECT *
FROM SANDBOX.STAGE_SCH.CUSTOMER
WHERE CUSTOMERID IN ('9', '10');

DESC TABLE SANDBOX.CLEAN_SCH.CUSTOMER;

DESC TABLE SANDBOX.STAGE_SCH.CUSTOMER;

USE ROLE SYSADMIN;
USE WAREHOUSE ADHOC_WH;
USE DATABASE SANDBOX;

MERGE INTO CLEAN_SCH.CUSTOMER AS target
USING (
    SELECT
        CUSTOMERID,
        NAME,
        MOBILE,
        EMAIL,
        LOGINBYUSING,
        GENDER,
        TRY_TO_DATE(DOB) AS DOB,
        TRY_TO_DATE(ANNIVERSARY) AS ANNIVERSARY,
        PREFERENCES,
        TRY_TO_TIMESTAMP_TZ(CREATEDDATE) AS CREATED_DT,
        TRY_TO_TIMESTAMP_TZ(MODIFIEDDATE) AS MODIFIED_DT,
        _STG_FILE_NAME,
        _STG_FILE_LOAD_TS,
        _STG_FILE_MD5,
        _COPY_DATA_TS
    FROM STAGE_SCH.CUSTOMER
    WHERE CUSTOMERID IN ('9', '10')
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY CUSTOMERID
        ORDER BY _COPY_DATA_TS DESC
    ) = 1
) AS source
ON target.CUSTOMER_ID = source.CUSTOMERID

WHEN MATCHED THEN UPDATE SET
    target.NAME = source.NAME,
    target.MOBILE = source.MOBILE,
    target.EMAIL = source.EMAIL,
    target.LOGIN_BY_USING = source.LOGINBYUSING,
    target.GENDER = source.GENDER,
    target.DOB = source.DOB,
    target.ANNIVERSARY = source.ANNIVERSARY,
    target.PREFERENCES = source.PREFERENCES,
    target.MODIFIED_DT = source.MODIFIED_DT,
    target._STG_FILE_NAME = source._STG_FILE_NAME,
    target._STG_FILE_LOAD_TS = source._STG_FILE_LOAD_TS,
    target._STG_FILE_MD5 = source._STG_FILE_MD5

WHEN NOT MATCHED THEN INSERT (
    CUSTOMER_ID, NAME, MOBILE, EMAIL, LOGIN_BY_USING,
    GENDER, DOB, ANNIVERSARY, PREFERENCES,
    CREATED_DT, MODIFIED_DT,
    _STG_FILE_NAME, _STG_FILE_LOAD_TS, _STG_FILE_MD5
)
VALUES (
    source.CUSTOMERID, source.NAME, source.MOBILE,
    source.EMAIL, source.LOGINBYUSING, source.GENDER,
    source.DOB, source.ANNIVERSARY, source.PREFERENCES,
    source.CREATED_DT, source.MODIFIED_DT,
    source._STG_FILE_NAME, source._STG_FILE_LOAD_TS,
    source._STG_FILE_MD5
);

SELECT CUSTOMER_ID, NAME
FROM CLEAN_SCH.CUSTOMER
WHERE CUSTOMER_ID IN ('9', '10');


USE ROLE SYSADMIN;
USE WAREHOUSE ADHOC_WH;
USE DATABASE SANDBOX;

SELECT
    CUSTOMER_ID,
    CUSTOMER_NAME
FROM CONSUMPTION_SCH.CUSTOMER_DIM
WHERE CUSTOMER_ID IN (9, 10);


DESC TABLE CONSUMPTION_SCH.CUSTOMER_DIM;

USE ROLE SYSADMIN;
USE WAREHOUSE ADHOC_WH;
USE DATABASE SANDBOX;

SELECT
    CUSTOMER_HK,
    CUSTOMER_ID,
    NAME,
    IS_CURRENT,
    EFF_START_DATE,
    EFF_END_DATE
FROM CONSUMPTION_SCH.CUSTOMER_DIM
WHERE CUSTOMER_ID IN ('9', '10')
ORDER BY CUSTOMER_ID, EFF_START_DATE;


SELECT *
FROM CONSUMPTION_SCH.CUSTOMER_DIM
LIMIT 5;

SELECT
    COUNT(*) AS TOTAL_RECORDS,
    COUNT_IF(IS_CURRENT = TRUE) AS CURRENT_RECORDS
FROM CONSUMPTION_SCH.CUSTOMER_DIM;

SELECT
    CUSTOMER_ID,
    HASH(CUSTOMER_ID) AS GENERATED_HK,
    CUSTOMER_HK AS EXISTING_HK
FROM CONSUMPTION_SCH.CUSTOMER_DIM
ORDER BY CUSTOMER_ID;

SELECT GET_DDL(
    'TABLE',
    'SANDBOX.CONSUMPTION_SCH.CUSTOMER_DIM'
);


SELECT
    CUSTOMER_ID,
    HASH(
        CUSTOMER_ID,
        NAME,
        MOBILE,
        EMAIL,
        LOGIN_BY_USING,
        GENDER,
        DOB,
        ANNIVERSARY,
        PREFERENCES
    ) AS FULL_HASH,
    CUSTOMER_HK
FROM CONSUMPTION_SCH.CUSTOMER_DIM
ORDER BY CUSTOMER_ID;

USE DATABASE SANDBOX;

SELECT
    START_TIME,
    QUERY_TYPE,
    QUERY_TEXT
FROM TABLE(
    INFORMATION_SCHEMA.QUERY_HISTORY(
        RESULT_LIMIT => 1000
    )
)
WHERE QUERY_TEXT ILIKE '%CUSTOMER_DIM%'
  AND (
      QUERY_TEXT ILIKE '%MERGE%'
      OR QUERY_TEXT ILIKE '%INSERT%'
  )
ORDER BY START_TIME DESC;


USE ROLE SYSADMIN;
USE WAREHOUSE ADHOC_WH;
USE DATABASE SANDBOX;

SELECT
    HASH(SHA1_HEX(CONCAT(
        CUSTOMER_ID,
        NAME,
        MOBILE,
        EMAIL,
        LOGIN_BY_USING,
        GENDER,
        DOB,
        ANNIVERSARY,
        PREFERENCES
    ))) AS CUSTOMER_HK,

    CUSTOMER_ID,
    NAME,
    MOBILE,
    EMAIL,
    LOGIN_BY_USING,
    GENDER,
    DOB,
    ANNIVERSARY,
    PREFERENCES

FROM CLEAN_SCH.CUSTOMER
WHERE CUSTOMER_ID IN ('9', '10')
ORDER BY CUSTOMER_ID;


USE ROLE SYSADMIN;
USE WAREHOUSE ADHOC_WH;
USE DATABASE SANDBOX;

INSERT INTO CONSUMPTION_SCH.CUSTOMER_DIM (
    CUSTOMER_HK,
    CUSTOMER_ID,
    NAME,
    MOBILE,
    EMAIL,
    LOGIN_BY_USING,
    GENDER,
    DOB,
    ANNIVERSARY,
    PREFERENCES,
    EFF_START_DATE,
    EFF_END_DATE,
    IS_CURRENT
)
SELECT
    HASH(SHA1_HEX(CONCAT(
        source.CUSTOMER_ID,
        source.NAME,
        source.MOBILE,
        source.EMAIL,
        source.LOGIN_BY_USING,
        source.GENDER,
        source.DOB,
        source.ANNIVERSARY,
        source.PREFERENCES
    ))),
    source.CUSTOMER_ID,
    source.NAME,
    source.MOBILE,
    source.EMAIL,
    source.LOGIN_BY_USING,
    source.GENDER,
    source.DOB,
    source.ANNIVERSARY,
    source.PREFERENCES,
    CURRENT_TIMESTAMP(),
    NULL,
    TRUE
FROM CLEAN_SCH.CUSTOMER AS source
WHERE source.CUSTOMER_ID IN ('9', '10')
  AND NOT EXISTS (
      SELECT 1
      FROM CONSUMPTION_SCH.CUSTOMER_DIM AS target
      WHERE target.CUSTOMER_ID = source.CUSTOMER_ID
        AND target.IS_CURRENT = TRUE
  );



  SELECT
    CUSTOMER_HK,
    CUSTOMER_ID,
    NAME,
    IS_CURRENT,
    EFF_START_DATE,
    EFF_END_DATE
FROM CONSUMPTION_SCH.CUSTOMER_DIM
WHERE CUSTOMER_ID IN ('9', '10')
ORDER BY CUSTOMER_ID;



USE ROLE SYSADMIN;
USE WAREHOUSE ADHOC_WH;
USE DATABASE SANDBOX;

SELECT DISTINCT
    d.DELIVERY_AGENT_ID_FK,
    da.DELIVERY_AGENT_ID
FROM CLEAN_SCH.DELIVERY d
LEFT JOIN CONSUMPTION_SCH.DELIVERY_AGENT_DIM da
    ON d.DELIVERY_AGENT_ID_FK = da.DELIVERY_AGENT_ID
WHERE da.DELIVERY_AGENT_ID IS NULL
ORDER BY d.DELIVERY_AGENT_ID_FK;

USE ROLE SYSADMIN;
USE WAREHOUSE ADHOC_WH;
USE DATABASE SANDBOX;

SELECT
    'STAGE' AS SOURCE,
    DELIVERYAGENTID,
    NAME
FROM STAGE_SCH.DELIVERY_AGENT
WHERE DELIVERYAGENTID = '6'

UNION ALL

SELECT
    'CLEAN' AS SOURCE,
    DELIVERY_AGENT_ID,
    NAME
FROM CLEAN_SCH.DELIVERY_AGENT
WHERE DELIVERY_AGENT_ID = '6';


USE DATABASE SANDBOX;

SHOW TABLES IN SCHEMA STAGE_SCH;


USE ROLE SYSADMIN;
USE WAREHOUSE ADHOC_WH;
USE DATABASE SANDBOX;

DESC TABLE STAGE_SCH.DELIVERYAGENT;



USE ROLE SYSADMIN;
USE WAREHOUSE ADHOC_WH;
USE DATABASE SANDBOX;

SELECT
    DELIVERYAGENTID,
    NAME,
    PHONE,
    VEHICLETYPE,
    LOCATIONID,
    STATUS,
    GENDER,
    RATING,
    CREATEDDATE,
    MODIFIEDDATE
FROM STAGE_SCH.DELIVERYAGENT
WHERE DELIVERYAGENTID = '6';



USE ROLE SYSADMIN;
USE WAREHOUSE ADHOC_WH;
USE DATABASE SANDBOX;

SELECT DISTINCT
    DELIVERY_AGENT_ID_FK
FROM CLEAN_SCH.DELIVERY
ORDER BY DELIVERY_AGENT_ID_FK;


SELECT DISTINCT
    DELIVERYAGENTID
FROM STAGE_SCH.DELIVERYAGENT
ORDER BY DELIVERYAGENTID;


USE ROLE SYSADMIN;
USE WAREHOUSE ADHOC_WH;
USE DATABASE SANDBOX;

SELECT
    s.DELIVERYAGENTID AS STAGE_AGENT_ID,
    c.DELIVERY_AGENT_ID AS CLEAN_AGENT_ID,
    d.DELIVERY_AGENT_ID AS DIM_AGENT_ID,
    d.DELIVERY_AGENT_HK
FROM STAGE_SCH.DELIVERYAGENT s
LEFT JOIN CLEAN_SCH.DELIVERY_AGENT c
    ON s.DELIVERYAGENTID = c.DELIVERY_AGENT_ID
LEFT JOIN CONSUMPTION_SCH.DELIVERY_AGENT_DIM d
    ON s.DELIVERYAGENTID = d.DELIVERY_AGENT_ID
ORDER BY s.DELIVERYAGENTID;


SELECT *
FROM STAGE_SCH.DELIVERYAGENT
WHERE DELIVERYAGENTID = '10';


SELECT *
FROM CLEAN_SCH.DELIVERY_AGENT
WHERE DELIVERY_AGENT_ID = '10';

DESC TABLE CLEAN_SCH.DELIVERY_AGENT;




USE ROLE SYSADMIN;
USE WAREHOUSE ADHOC_WH;
USE DATABASE SANDBOX;

INSERT INTO CLEAN_SCH.DELIVERY_AGENT (
    DELIVERY_AGENT_ID,
    NAME,
    PHONE,
    VEHICLE_TYPE,
    LOCATION_ID_FK,
    STATUS,
    GENDER,
    RATING,
    CREATED_DT,
    MODIFIED_DT,
    _STG_FILE_NAME,
    _STG_FILE_LOAD_TS,
    _STG_FILE_MD5,
    _COPY_DATA_TS
)
SELECT
    TRY_TO_NUMBER(DELIVERYAGENTID),
    NAME,
    PHONE,
    VEHICLETYPE,
    TRY_TO_NUMBER(LOCATIONID),
    STATUS,
    GENDER,
    TRY_TO_NUMBER(RATING),
    TRY_TO_TIMESTAMP_NTZ(CREATEDDATE),
    TRY_TO_TIMESTAMP_NTZ(MODIFIEDDATE),
    _STG_FILE_NAME,
    _STG_FILE_LOAD_TS,
    _STG_FILE_MD5,
    _COPY_DATA_TS
FROM STAGE_SCH.DELIVERYAGENT
WHERE DELIVERYAGENTID = '10'
  AND NOT EXISTS (
      SELECT 1
      FROM CLEAN_SCH.DELIVERY_AGENT target
      WHERE target.DELIVERY_AGENT_ID = 10
  );



SELECT
    DELIVERY_AGENT_SK,
    DELIVERY_AGENT_ID,
    NAME,
    PHONE,
    VEHICLE_TYPE,
    LOCATION_ID_FK,
    STATUS,
    GENDER,
    RATING
FROM CLEAN_SCH.DELIVERY_AGENT
WHERE DELIVERY_AGENT_ID = 10;


SELECT
    START_TIME,
    QUERY_TEXT
FROM TABLE(
    INFORMATION_SCHEMA.QUERY_HISTORY(RESULT_LIMIT => 1000)
)
WHERE QUERY_TEXT ILIKE '%DELIVERY_AGENT_DIM%'
  AND QUERY_TEXT ILIKE '%MERGE%'
ORDER BY START_TIME DESC;


USE ROLE SYSADMIN;
USE WAREHOUSE ADHOC_WH;
USE DATABASE SANDBOX;

INSERT INTO CONSUMPTION_SCH.DELIVERY_AGENT_DIM (
    DELIVERY_AGENT_HK,
    DELIVERY_AGENT_ID,
    NAME,
    PHONE,
    VEHICLE_TYPE,
    LOCATION_ID_FK,
    STATUS,
    GENDER,
    RATING,
    EFF_START_DATE,
    EFF_END_DATE,
    IS_CURRENT
)
SELECT
    HASH(SHA1_HEX(CONCAT(
        source.DELIVERY_AGENT_ID,
        source.NAME,
        source.PHONE,
        source.VEHICLE_TYPE,
        source.LOCATION_ID_FK,
        source.STATUS,
        source.GENDER,
        source.RATING
    ))),
    source.DELIVERY_AGENT_ID,
    source.NAME,
    source.PHONE,
    source.VEHICLE_TYPE,
    source.LOCATION_ID_FK,
    source.STATUS,
    source.GENDER,
    source.RATING,
    CURRENT_TIMESTAMP(),
    NULL,
    TRUE
FROM CLEAN_SCH.DELIVERY_AGENT source
WHERE source.DELIVERY_AGENT_ID = 10
AND NOT EXISTS (
    SELECT 1
    FROM CONSUMPTION_SCH.DELIVERY_AGENT_DIM target
    WHERE target.DELIVERY_AGENT_ID = source.DELIVERY_AGENT_ID
      AND target.IS_CURRENT = TRUE
);



SELECT
    DELIVERY_AGENT_HK,
    DELIVERY_AGENT_ID,
    NAME,
    IS_CURRENT,
    EFF_START_DATE,
    EFF_END_DATE
FROM CONSUMPTION_SCH.DELIVERY_AGENT_DIM
WHERE DELIVERY_AGENT_ID = 10;




USE ROLE SYSADMIN;
USE WAREHOUSE ADHOC_WH;
USE DATABASE SANDBOX;

SELECT
    COUNT(*) AS TOTAL_RECORDS,
    COUNT(CUSTOMER_DIM_KEY) AS CUSTOMER_KEYS,
    COUNT(DELIVERY_AGENT_DIM_KEY) AS DELIVERY_AGENT_KEYS,
    COUNT(*) - COUNT(CUSTOMER_DIM_KEY) AS MISSING_CUSTOMER_KEYS,
    COUNT(*) - COUNT(DELIVERY_AGENT_DIM_KEY) AS MISSING_DELIVERY_AGENT_KEYS
FROM CONSUMPTION_SCH.ORDER_ITEM_FACT;



USE ROLE SYSADMIN;
USE WAREHOUSE ADHOC_WH;
USE DATABASE SANDBOX;

SELECT 'ORDER_ITEM_STM' AS SOURCE, COUNT(*) AS RECORDS
FROM CLEAN_SCH.ORDER_ITEM_STM

UNION ALL

SELECT 'ORDERS_STM', COUNT(*)
FROM CLEAN_SCH.ORDERS_STM

UNION ALL

SELECT 'DELIVERY_STM', COUNT(*)
FROM CLEAN_SCH.DELIVERY_STM;


USE ROLE SYSADMIN;
USE WAREHOUSE ADHOC_WH;
USE DATABASE SANDBOX;

SELECT COUNT(*) AS DELIVERY_RECORDS
FROM CLEAN_SCH.DELIVERY;

SELECT *
FROM CLEAN_SCH.DELIVERY
ORDER BY ORDER_ID_FK;


SELECT 'ORDER_ITEM' AS TABLE_NAME, COUNT(*) AS TOTAL
FROM CLEAN_SCH.ORDER_ITEM

UNION ALL

SELECT 'ORDERS', COUNT(*)
FROM CLEAN_SCH.ORDERS

UNION ALL

SELECT 'DELIVERY', COUNT(*)
FROM CLEAN_SCH.DELIVERY;




SELECT
    COUNT(*) AS DELIVERY_RECORDS,
    COUNT(o.ORDER_ID) AS MATCHED_ORDERS,
    COUNT(oi.ORDER_ID_FK) AS MATCHED_ORDER_ITEMS
FROM CLEAN_SCH.DELIVERY d
LEFT JOIN CLEAN_SCH.ORDERS o
    ON d.ORDER_ID_FK = o.ORDER_ID
LEFT JOIN CLEAN_SCH.ORDER_ITEM oi
    ON o.ORDER_ID = oi.ORDER_ID_FK;




SELECT
    d.ORDER_ID_FK AS DELIVERY_ORDER_ID,
    o.ORDER_ID AS MATCHED_ORDER_ID,
    oi.ORDER_ID_FK AS ORDER_ITEM_ORDER_ID
FROM CLEAN_SCH.DELIVERY d
LEFT JOIN CLEAN_SCH.ORDERS o
    ON d.ORDER_ID_FK = o.ORDER_ID
LEFT JOIN CLEAN_SCH.ORDER_ITEM oi
    ON o.ORDER_ID = oi.ORDER_ID_FK
ORDER BY d.ORDER_ID_FK;    



SELECT
    ORDER_ITEM_ID,
    ORDER_ID_FK
FROM CLEAN_SCH.ORDER_ITEM
ORDER BY ORDER_ID_FK;



SELECT ORDER_ID
FROM CLEAN_SCH.ORDERS
ORDER BY ORDER_ID;



SELECT
    oi.ORDER_ID_FK,
    COUNT(*) AS ORDER_ITEM_COUNT
FROM CLEAN_SCH.ORDER_ITEM oi
LEFT JOIN CLEAN_SCH.ORDERS o
    ON oi.ORDER_ID_FK = o.ORDER_ID
WHERE o.ORDER_ID IS NULL
GROUP BY oi.ORDER_ID_FK
ORDER BY oi.ORDER_ID_FK;




SELECT
    o.ORDER_ID,
    COUNT(oi.ORDER_ITEM_ID) AS ORDER_ITEM_COUNT,
    COUNT(d.ORDER_ID_FK) AS DELIVERY_COUNT
FROM CLEAN_SCH.ORDERS o
LEFT JOIN CLEAN_SCH.ORDER_ITEM oi
    ON o.ORDER_ID = oi.ORDER_ID_FK
LEFT JOIN CLEAN_SCH.DELIVERY d
    ON o.ORDER_ID = d.ORDER_ID_FK
GROUP BY o.ORDER_ID
ORDER BY o.ORDER_ID;





SELECT
    COUNT(*) AS ORDER_ITEM_RECORDS,
    COUNT(c.CUSTOMER_ID) AS CUSTOMER_MATCHES,
    COUNT(ca.CUSTOMER_ID_FK) AS ADDRESS_MATCHES,
    COUNT(r.RESTAURANT_ID) AS RESTAURANT_MATCHES,
    COUNT(m.MENU_ID) AS MENU_MATCHES,
    COUNT(rl.LOCATION_ID) AS RESTAURANT_LOCATION_MATCHES,
    COUNT(dd.CALENDAR_DATE) AS DATE_MATCHES
FROM CLEAN_SCH.ORDER_ITEM oi
JOIN CLEAN_SCH.ORDERS o
    ON oi.ORDER_ID_FK = o.ORDER_ID
LEFT JOIN CONSUMPTION_SCH.CUSTOMER_DIM c
    ON o.CUSTOMER_ID_FK = c.CUSTOMER_ID
   AND c.IS_CURRENT = TRUE
LEFT JOIN CONSUMPTION_SCH.CUSTOMER_ADDRESS_DIM ca
    ON c.CUSTOMER_ID = ca.CUSTOMER_ID_FK
LEFT JOIN CONSUMPTION_SCH.RESTAURANT_DIM r
    ON o.RESTAURANT_ID_FK = r.RESTAURANT_ID
LEFT JOIN CONSUMPTION_SCH.MENU_DIM m
    ON oi.MENU_ID_FK = m.MENU_ID
LEFT JOIN CONSUMPTION_SCH.RESTAURANT_LOCATION_DIM rl
    ON r.LOCATION_ID_FK = rl.LOCATION_ID
LEFT JOIN CONSUMPTION_SCH.DATE_DIM dd
    ON dd.CALENDAR_DATE = DATE(o.ORDER_DATE);



    SELECT DISTINCT
    c.CUSTOMER_ID
FROM CLEAN_SCH.ORDERS o
JOIN CONSUMPTION_SCH.CUSTOMER_DIM c
    ON o.CUSTOMER_ID_FK = c.CUSTOMER_ID
   AND c.IS_CURRENT = TRUE
LEFT JOIN CONSUMPTION_SCH.CUSTOMER_ADDRESS_DIM ca
    ON c.CUSTOMER_ID = ca.CUSTOMER_ID_FK
WHERE ca.CUSTOMER_ID_FK IS NULL;


SELECT DISTINCT
    o.RESTAURANT_ID_FK
FROM CLEAN_SCH.ORDER_ITEM oi
JOIN CLEAN_SCH.ORDERS o
    ON oi.ORDER_ID_FK = o.ORDER_ID
LEFT JOIN CONSUMPTION_SCH.RESTAURANT_DIM r
    ON o.RESTAURANT_ID_FK = r.RESTAURANT_ID
WHERE r.RESTAURANT_ID IS NULL;


SELECT *
FROM CLEAN_SCH.CUSTOMER_ADDRESS
WHERE CUSTOMER_ID_FK IN (9, 10);


SELECT *
FROM CLEAN_SCH.RESTAURANT
WHERE RESTAURANT_ID IN (1, 7);


SELECT *
FROM STAGE_SCH.CUSTOMER_ADDRESS
WHERE CUSTOMERID IN ('9', '10');

SELECT *
FROM STAGE_SCH.RESTAURANT
WHERE RESTAURANTID IN ('1', '7');


SHOW TABLES IN SCHEMA SANDBOX.STAGE_SCH;


DESC TABLE SANDBOX.CONSUMPTION_SCH.CUSTOMER_ADDRESS_DIM;

SELECT *
FROM SANDBOX.STAGE_SCH.CUSTOMERADDRESS
WHERE CUSTOMERID IN ('9', '10');

SELECT *
FROM SANDBOX.STAGE_SCH.RESTAURANT
WHERE RESTAURANTID IN ('1', '7');



DESC TABLE SANDBOX.CLEAN_SCH.CUSTOMER_ADDRESS;



INSERT INTO SANDBOX.CLEAN_SCH.CUSTOMER_ADDRESS (
    ADDRESS_ID,
    CUSTOMER_ID_FK,
    FLAT_NO,
    HOUSE_NO,
    FLOOR,
    BUILDING,
    LANDMARK,
    LOCALITY,
    CITY,
    STATE,
    PINCODE,
    COORDINATES,
    PRIMARY_FLAG,
    ADDRESS_TYPE,
    CREATED_DATE,
    MODIFIED_DATE,
    _STG_FILE_NAME,
    _STG_FILE_LOAD_TS,
    _STG_FILE_MD5,
    _COPY_DATA_TS
)
SELECT
    TRY_TO_NUMBER(ADDRESSID),
    TRY_TO_NUMBER(CUSTOMERID),
    FLATNO,
    HOUSENO,
    FLOOR,
    BUILDING,
    LANDMARK,
    LOCALITY,
    CITY,
    STATE,
    PINCODE,
    COORDINATES,
    PRIMARYFLAG,
    ADDRESSTYPE,
    TRY_TO_TIMESTAMP_TZ(CREATEDDATE),
    TRY_TO_TIMESTAMP_TZ(MODIFIEDDATE),
    _STG_FILE_NAME,
    _STG_FILE_LOAD_TS,
    _STG_FILE_MD5,
    _COPY_DATA_TS
FROM SANDBOX.STAGE_SCH.CUSTOMERADDRESS s
WHERE CUSTOMERID IN ('9', '10')
AND NOT EXISTS (
    SELECT 1
    FROM SANDBOX.CLEAN_SCH.CUSTOMER_ADDRESS c
    WHERE c.ADDRESS_ID = TRY_TO_NUMBER(s.ADDRESSID)
);

SELECT ADDRESS_ID, CUSTOMER_ID_FK, BUILDING, LOCALITY, CITY
FROM SANDBOX.CLEAN_SCH.CUSTOMER_ADDRESS
WHERE CUSTOMER_ID_FK IN (9, 10)
ORDER BY CUSTOMER_ID_FK;


SELECT QUERY_TEXT
FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
WHERE QUERY_TEXT ILIKE '%CUSTOMER_ADDRESS_DIM%'
  AND QUERY_TEXT ILIKE '%MERGE%'
ORDER BY START_TIME DESC
LIMIT 5;


INSERT INTO SANDBOX.CONSUMPTION_SCH.CUSTOMER_ADDRESS_DIM (
    CUSTOMER_ADDRESS_HK,
    ADDRESS_ID,
    CUSTOMER_ID_FK,
    FLAT_NO,
    HOUSE_NO,
    FLOOR,
    BUILDING,
    LANDMARK,
    LOCALITY,
    CITY,
    STATE,
    PINCODE,
    COORDINATES,
    PRIMARY_FLAG,
    ADDRESS_TYPE,
    EFF_START_DATE,
    EFF_END_DATE,
    IS_CURRENT
)
SELECT
    HASH(SHA1_HEX(CONCAT(
        c.ADDRESS_ID,
        c.CUSTOMER_ID_FK,
        c.FLAT_NO,
        c.HOUSE_NO,
        c.FLOOR,
        c.BUILDING,
        c.LANDMARK,
        c.LOCALITY,
        c.CITY,
        c.STATE,
        c.PINCODE,
        c.COORDINATES,
        c.PRIMARY_FLAG,
        c.ADDRESS_TYPE
    ))),
    c.ADDRESS_ID,
    TO_VARCHAR(c.CUSTOMER_ID_FK),
    c.FLAT_NO,
    c.HOUSE_NO,
    c.FLOOR,
    c.BUILDING,
    c.LANDMARK,
    c.LOCALITY,
    c.CITY,
    c.STATE,
    c.PINCODE,
    c.COORDINATES,
    c.PRIMARY_FLAG,
    c.ADDRESS_TYPE,
    CURRENT_TIMESTAMP(),
    NULL,
    TRUE
FROM SANDBOX.CLEAN_SCH.CUSTOMER_ADDRESS c
WHERE c.CUSTOMER_ID_FK IN (9, 10)
AND NOT EXISTS (
    SELECT 1
    FROM SANDBOX.CONSUMPTION_SCH.CUSTOMER_ADDRESS_DIM d
    WHERE d.ADDRESS_ID = c.ADDRESS_ID
      AND d.IS_CURRENT = TRUE
);

SELECT
    ADDRESS_ID,
    CUSTOMER_ID_FK,
    BUILDING,
    LOCALITY,
    CITY,
    CUSTOMER_ADDRESS_HK,
    IS_CURRENT
FROM SANDBOX.CONSUMPTION_SCH.CUSTOMER_ADDRESS_DIM
WHERE CUSTOMER_ID_FK IN ('9', '10')
ORDER BY CUSTOMER_ID_FK;



SELECT
    oi.ORDER_ITEM_ID,
    oi.ORDER_ID_FK AS ORDER_ID,
    o.CUSTOMER_ID_FK,
    ca.CUSTOMER_ADDRESS_HK,
    o.RESTAURANT_ID_FK,
    r.RESTAURANT_HK,
    m.MENU_DIM_HK,
    d.DELIVERY_STATUS,
    da.DELIVERY_AGENT_HK,
    dd.DATE_DIM_HK
FROM SANDBOX.CLEAN_SCH.ORDER_ITEM oi
JOIN SANDBOX.CLEAN_SCH.ORDERS o
    ON oi.ORDER_ID_FK = o.ORDER_ID
LEFT JOIN SANDBOX.CONSUMPTION_SCH.CUSTOMER_DIM c
    ON o.CUSTOMER_ID_FK = c.CUSTOMER_ID
   AND c.IS_CURRENT = TRUE
LEFT JOIN SANDBOX.CONSUMPTION_SCH.CUSTOMER_ADDRESS_DIM ca
    ON c.CUSTOMER_ID = ca.CUSTOMER_ID_FK
   AND ca.IS_CURRENT = TRUE
LEFT JOIN SANDBOX.CONSUMPTION_SCH.RESTAURANT_DIM r
    ON o.RESTAURANT_ID_FK = r.RESTAURANT_ID
LEFT JOIN SANDBOX.CONSUMPTION_SCH.MENU_DIM m
    ON oi.MENU_ID_FK = m.MENU_ID
LEFT JOIN SANDBOX.CLEAN_SCH.DELIVERY d
    ON o.ORDER_ID = d.ORDER_ID_FK
LEFT JOIN SANDBOX.CONSUMPTION_SCH.DELIVERY_AGENT_DIM da
    ON d.DELIVERY_AGENT_ID_FK = da.DELIVERY_AGENT_ID
LEFT JOIN SANDBOX.CONSUMPTION_SCH.DATE_DIM dd
    ON dd.CALENDAR_DATE = DATE(o.ORDER_DATE)
ORDER BY oi.ORDER_ITEM_ID;


SELECT
    RESTAURANTID,
    NAME,
    LOCATIONID
FROM SANDBOX.STAGE_SCH.RESTAURANT
ORDER BY RESTAURANTID;



SELECT
    RESTAURANT_ID,
    RESTAURANT_HK,
    LOCATION_ID_FK
FROM SANDBOX.CONSUMPTION_SCH.RESTAURANT_DIM
ORDER BY RESTAURANT_ID;



DESC TABLE SANDBOX.CONSUMPTION_SCH.ORDER_ITEM_FACT;



CREATE OR REPLACE TEMPORARY TABLE
SANDBOX.CONSUMPTION_SCH.ORDER_ITEM_FACT_SOURCE AS

SELECT
    oi.ORDER_ITEM_ID,
    o.ORDER_ID,

    c.CUSTOMER_HK AS CUSTOMER_DIM_KEY,
    ca.CUSTOMER_ADDRESS_HK AS CUSTOMER_ADDRESS_DIM_KEY,
    r.RESTAURANT_HK AS RESTAURANT_DIM_KEY,
    rl.RESTAURANT_LOCATION_HK AS RESTAURANT_LOCATION_DIM_KEY,
    m.MENU_HK AS MENU_DIM_KEY,
    da.DELIVERY_AGENT_HK AS DELIVERY_AGENT_DIM_KEY,
    dd.DATE_DIM_KEY AS ORDER_DATE_DIM_KEY,

    oi.QUANTITY,
    oi.PRICE,
    oi.SUBTOTAL,

    d.DELIVERY_STATUS,
    d.ESTIMATED_TIME

FROM SANDBOX.CLEAN_SCH.ORDER_ITEM oi

INNER JOIN SANDBOX.CLEAN_SCH.ORDERS o
    ON oi.ORDER_ID_FK = o.ORDER_ID

INNER JOIN SANDBOX.CONSUMPTION_SCH.CUSTOMER_DIM c
    ON o.CUSTOMER_ID_FK = c.CUSTOMER_ID
    AND c.IS_CURRENT = TRUE

LEFT JOIN SANDBOX.CONSUMPTION_SCH.CUSTOMER_ADDRESS_DIM ca
    ON c.CUSTOMER_ID = ca.CUSTOMER_ID_FK
    AND ca.IS_CURRENT = TRUE

LEFT JOIN SANDBOX.CONSUMPTION_SCH.RESTAURANT_DIM r
    ON o.RESTAURANT_ID_FK = r.RESTAURANT_ID
    AND r.IS_CURRENT = TRUE

INNER JOIN SANDBOX.CONSUMPTION_SCH.MENU_DIM m
    ON oi.MENU_ID_FK = m.MENU_ID

LEFT JOIN SANDBOX.CONSUMPTION_SCH.DELIVERY d
    ON o.ORDER_ID = d.ORDER_ID_FK

LEFT JOIN SANDBOX.CONSUMPTION_SCH.DELIVERY_AGENT_DIM da
    ON d.DELIVERY_AGENT_ID_FK = da.DELIVERY_AGENT_ID
    AND da.IS_CURRENT = TRUE

LEFT JOIN SANDBOX.CONSUMPTION_SCH.RESTAURANT_LOCATION_DIM rl
    ON r.LOCATION_ID_FK = rl.LOCATION_ID

INNER JOIN SANDBOX.CONSUMPTION_SCH.DATE_DIM dd
    ON dd.CALENDAR_DATE = DATE(o.ORDER_DATE);


CREATE OR REPLACE TEMPORARY TABLE
SANDBOX.CONSUMPTION_SCH.ORDER_ITEM_FACT_SOURCE AS

SELECT
    oi.ORDER_ITEM_ID,
    o.ORDER_ID,
    c.CUSTOMER_HK AS CUSTOMER_DIM_KEY,
    ca.CUSTOMER_ADDRESS_HK AS CUSTOMER_ADDRESS_DIM_KEY,
    r.RESTAURANT_HK AS RESTAURANT_DIM_KEY,
    rl.RESTAURANT_LOCATION_HK AS RESTAURANT_LOCATION_DIM_KEY,
    m.MENU_HK AS MENU_DIM_KEY,
    da.DELIVERY_AGENT_HK AS DELIVERY_AGENT_DIM_KEY,
    dd.DATE_DIM_KEY AS ORDER_DATE_DIM_KEY,
    oi.QUANTITY,
    oi.PRICE,
    oi.SUBTOTAL,
    d.DELIVERY_STATUS,
    d.ESTIMATED_TIME

FROM SANDBOX.CLEAN_SCH.ORDER_ITEM oi

INNER JOIN SANDBOX.CLEAN_SCH.ORDERS o
    ON oi.ORDER_ID_FK = o.ORDER_ID

INNER JOIN SANDBOX.CONSUMPTION_SCH.CUSTOMER_DIM c
    ON o.CUSTOMER_ID_FK = c.CUSTOMER_ID
    AND c.IS_CURRENT = TRUE

LEFT JOIN SANDBOX.CONSUMPTION_SCH.CUSTOMER_ADDRESS_DIM ca
    ON c.CUSTOMER_ID = ca.CUSTOMER_ID_FK
    AND ca.IS_CURRENT = TRUE

LEFT JOIN SANDBOX.CONSUMPTION_SCH.RESTAURANT_DIM r
    ON o.RESTAURANT_ID_FK = r.RESTAURANT_ID
    AND r.IS_CURRENT = TRUE

INNER JOIN SANDBOX.CONSUMPTION_SCH.MENU_DIM m
    ON oi.MENU_ID_FK = m.MENU_ID

LEFT JOIN SANDBOX.CLEAN_SCH.DELIVERY d
    ON o.ORDER_ID = d.ORDER_ID_FK

LEFT JOIN SANDBOX.CONSUMPTION_SCH.DELIVERY_AGENT_DIM da
    ON d.DELIVERY_AGENT_ID_FK = da.DELIVERY_AGENT_ID
    AND da.IS_CURRENT = TRUE

LEFT JOIN SANDBOX.CONSUMPTION_SCH.RESTAURANT_LOCATION_DIM rl
    ON r.LOCATION_ID_FK = rl.LOCATION_ID

INNER JOIN SANDBOX.CONSUMPTION_SCH.DATE_DIM dd
    ON dd.CALENDAR_DATE = DATE(o.ORDER_DATE);


    DESC TABLE SANDBOX.CONSUMPTION_SCH.MENU_DIM;

    DESC TABLE SANDBOX.CLEAN_SCH.RESTAURANT;

    DESC TABLE SANDBOX.CONSUMPTION_SCH.RESTAURANT_DIM;



SELECT
    RESTAURANT_ID,
    NAME,
    LOCATION_ID_FK,
    CREATED_DT,
    MODIFIED_DT
FROM SANDBOX.CLEAN_SCH.RESTAURANT
ORDER BY RESTAURANT_ID;




SELECT
    RESTAURANT_HK,
    RESTAURANT_ID,
    NAME,
    EFF_START_DATE,
    EFF_END_DATE,
    IS_CURRENT
FROM SANDBOX.CONSUMPTION_SCH.RESTAURANT_DIM
ORDER BY RESTAURANT_ID, EFF_START_DATE;



INSERT INTO SANDBOX.CLEAN_SCH.RESTAURANT
(
    RESTAURANT_ID,
    NAME,
    CUISINE_TYPE,
    PRICING_FOR_TWO,
    RESTAURANT_PHONE,
    OPERATING_HOURS,
    LOCATION_ID_FK,
    ACTIVE_FLAG,
    OPEN_STATUS,
    LOCALITY,
    RESTAURANT_ADDRESS,
    LATITUDE,
    LONGITUDE,
    CREATED_DT,
    MODIFIED_DT
)
SELECT
    v.RESTAURANT_ID,
    v.NAME,
    v.CUISINE_TYPE,
    v.PRICING_FOR_TWO,
    v.RESTAURANT_PHONE,
    v.OPERATING_HOURS,
    v.LOCATION_ID_FK,
    v.ACTIVE_FLAG,
    v.OPEN_STATUS,
    v.LOCALITY,
    v.RESTAURANT_ADDRESS,
    v.LATITUDE,
    v.LONGITUDE,
    TO_TIMESTAMP_TZ(v.CREATED_DT),
    TO_TIMESTAMP_TZ(v.MODIFIED_DT)
FROM VALUES
(
    1, 'Rustom''s',
    'Parsi, Street Food, North Indian, Desserts, Beverages',
    2100, '9069050000', '8:00 AM - 16:00 PM',
    1, 'Yes', 'Open', 'ITO, New Delhi',
    'Delhi Parsi Anjuman, LNJP Colony, Bahadur Shah Zafar Road, Near Maulana Azad Medical College Campus, ITO, New Delhi',
    28.638167, 77.240472,
    '2020-10-07 20:19:43.902836',
    '2024-08-14 18:03:26.315582'
),
(
    2, 'Cafe Lota',
    'Cafe, South Indian, North Indian, Beverages',
    1200, '8320181288', '10:00 AM - 18:00 PM',
    1, 'Yes', 'Open', 'Pragati Maidan, New Delhi',
    'National Crafts Museum, Gate 2, Bhairon Marg, Pragati Maidan, New Delhi',
    28.613429, 77.242471,
    '2020-05-22 10:40:09.631036',
    '2023-12-15 10:15:48.123670'
),
(
    6, 'Carnatic Cafe',
    'South Indian',
    600, '6246571395', '8:00 AM - 16:00 PM',
    1, 'Yes', 'Open', 'Lodhi Colony, New Delhi',
    '84-85, Meharchand Market, Lodhi Road, Near Lodhi Colony, New Delhi',
    28.584296, 77.226613,
    '2021-03-28 02:18:58.484116',
    '2024-02-05 20:31:43.350854'
),
(
    7, 'Cocktails & Dreams, Speakeasy',
    'Nepalese, Tibetan, Beverages',
    2500, '6738584603', '10:00 AM - 18:00 PM',
    1, 'Yes', 'Open', 'Sector 15, Gurgaon',
    'SCO 23, Sector 15, Gurgaon',
    28.45797541, 77.0446267,
    '2020-06-06 04:47:51.235628',
    '2024-05-13 20:34:51.989615'
),
(
    8, 'Natural Ice Cream',
    'Ice Cream, Desserts',
    400, '9577412181', '8:00 AM - 16:00 PM',
    1, 'Yes', 'Open', 'Connaught Place, New Delhi',
    'L-8, L Block, Outer Circle, Connaught Place, New Delhi',
    28.63444917, 77.22203437,
    '2021-09-13 21:56:23.175242',
    '2023-12-21 20:36:30.937905'
) AS v
(
    RESTAURANT_ID, NAME, CUISINE_TYPE, PRICING_FOR_TWO,
    RESTAURANT_PHONE, OPERATING_HOURS, LOCATION_ID_FK,
    ACTIVE_FLAG, OPEN_STATUS, LOCALITY, RESTAURANT_ADDRESS,
    LATITUDE, LONGITUDE, CREATED_DT, MODIFIED_DT
)
WHERE NOT EXISTS (
    SELECT 1
    FROM SANDBOX.CLEAN_SCH.RESTAURANT c
    WHERE c.RESTAURANT_ID = v.RESTAURANT_ID
);


SELECT
    RESTAURANT_ID,
    NAME,
    CREATED_DT,
    MODIFIED_DT
FROM SANDBOX.CLEAN_SCH.RESTAURANT
ORDER BY RESTAURANT_ID;


SELECT GET_DDL(
    'TABLE',
    'SANDBOX.CONSUMPTION_SCH.RESTAURANT_DIM'
);



SELECT
    START_TIME,
    QUERY_TYPE,
    QUERY_TEXT
FROM TABLE(
    SANDBOX.INFORMATION_SCHEMA.QUERY_HISTORY(
        RESULT_LIMIT => 1000
    )
)
WHERE QUERY_TEXT ILIKE '%RESTAURANT_DIM%'
  AND (
      QUERY_TEXT ILIKE '%INSERT%'
      OR QUERY_TEXT ILIKE '%MERGE%'
  )
ORDER BY START_TIME DESC
LIMIT 20;


SELECT
    C.RESTAURANT_ID,
    C.NAME AS CLEAN_NAME,
    D.NAME AS DIM_NAME,
    D.RESTAURANT_HK,
    CASE
        WHEN D.RESTAURANT_ID IS NULL THEN 'MISSING IN DIMENSION'
        WHEN
            EQUAL_NULL(C.NAME, D.NAME)
            AND EQUAL_NULL(C.CUISINE_TYPE, D.CUISINE_TYPE)
            AND EQUAL_NULL(C.PRICING_FOR_TWO, D.PRICING_FOR_TWO)
            AND EQUAL_NULL(C.RESTAURANT_PHONE, D.RESTAURANT_PHONE)
            AND EQUAL_NULL(C.OPERATING_HOURS, D.OPERATING_HOURS)
            AND EQUAL_NULL(C.LOCATION_ID_FK, D.LOCATION_ID_FK)
            AND EQUAL_NULL(C.ACTIVE_FLAG, D.ACTIVE_FLAG)
            AND EQUAL_NULL(C.OPEN_STATUS, D.OPEN_STATUS)
            AND EQUAL_NULL(C.LOCALITY, D.LOCALITY)
            AND EQUAL_NULL(C.RESTAURANT_ADDRESS, D.RESTAURANT_ADDRESS)
            AND EQUAL_NULL(C.LATITUDE, D.LATITUDE)
            AND EQUAL_NULL(C.LONGITUDE, D.LONGITUDE)
        THEN 'MATCH'
        ELSE 'ATTRIBUTE DIFFERENCE'
    END AS AUDIT_STATUS
FROM SANDBOX.CLEAN_SCH.RESTAURANT C
LEFT JOIN SANDBOX.CONSUMPTION_SCH.RESTAURANT_DIM D
    ON C.RESTAURANT_ID = D.RESTAURANT_ID
    AND D.IS_CURRENT = TRUE
ORDER BY C.RESTAURANT_ID;





WITH MISSING_RESTAURANTS AS (
    SELECT C.*
    FROM SANDBOX.CLEAN_SCH.RESTAURANT C
    LEFT JOIN SANDBOX.CONSUMPTION_SCH.RESTAURANT_DIM D
        ON C.RESTAURANT_ID = D.RESTAURANT_ID
        AND D.IS_CURRENT = TRUE
    WHERE D.RESTAURANT_ID IS NULL
),
HASHED AS (
    SELECT
        RESTAURANT_ID,
        NAME,
        HASH(SHA1_HEX(CONCAT(
            RESTAURANT_ID, NAME,
            CUISINE_TYPE, PRICING_FOR_TWO,
            RESTAURANT_PHONE, OPERATING_HOURS,
            LOCATION_ID_FK, ACTIVE_FLAG,
            OPEN_STATUS, LOCALITY,
            RESTAURANT_ADDRESS, LATITUDE,
            LONGITUDE
        ))) AS NEW_RESTAURANT_HK
    FROM MISSING_RESTAURANTS
)
SELECT
    H.RESTAURANT_ID,
    H.NAME,
    H.NEW_RESTAURANT_HK,
    D.RESTAURANT_ID AS EXISTING_DIM_RESTAURANT_ID,
    D.IS_CURRENT AS EXISTING_IS_CURRENT
FROM HASHED H
LEFT JOIN SANDBOX.CONSUMPTION_SCH.RESTAURANT_DIM D
    ON H.NEW_RESTAURANT_HK = D.RESTAURANT_HK
ORDER BY H.RESTAURANT_ID;




INSERT INTO SANDBOX.CONSUMPTION_SCH.RESTAURANT_DIM (
    RESTAURANT_HK,
    RESTAURANT_ID,
    NAME,
    CUISINE_TYPE,
    PRICING_FOR_TWO,
    RESTAURANT_PHONE,
    OPERATING_HOURS,
    LOCATION_ID_FK,
    ACTIVE_FLAG,
    OPEN_STATUS,
    LOCALITY,
    RESTAURANT_ADDRESS,
    LATITUDE,
    LONGITUDE,
    EFF_START_DATE,
    EFF_END_DATE,
    IS_CURRENT
)
SELECT
    HASH(SHA1_HEX(CONCAT(
        C.RESTAURANT_ID, C.NAME,
        C.CUISINE_TYPE, C.PRICING_FOR_TWO,
        C.RESTAURANT_PHONE, C.OPERATING_HOURS,
        C.LOCATION_ID_FK, C.ACTIVE_FLAG,
        C.OPEN_STATUS, C.LOCALITY,
        C.RESTAURANT_ADDRESS, C.LATITUDE,
        C.LONGITUDE
    ))),
    C.RESTAURANT_ID,
    C.NAME,
    C.CUISINE_TYPE,
    C.PRICING_FOR_TWO,
    C.RESTAURANT_PHONE,
    C.OPERATING_HOURS,
    C.LOCATION_ID_FK,
    C.ACTIVE_FLAG,
    C.OPEN_STATUS,
    C.LOCALITY,
    C.RESTAURANT_ADDRESS,
    C.LATITUDE,
    C.LONGITUDE,
    CURRENT_TIMESTAMP(),
    NULL,
    TRUE
FROM SANDBOX.CLEAN_SCH.RESTAURANT C
WHERE C.RESTAURANT_ID IN (1, 2, 6, 7, 8)
AND NOT EXISTS (
    SELECT 1
    FROM SANDBOX.CONSUMPTION_SCH.RESTAURANT_DIM D
    WHERE D.RESTAURANT_ID = C.RESTAURANT_ID
      AND D.IS_CURRENT = TRUE
);



SELECT
    COUNT(*) AS TOTAL_DIM_RECORDS,
    COUNT_IF(IS_CURRENT = TRUE) AS CURRENT_RECORDS,
    COUNT(DISTINCT IFF(IS_CURRENT = TRUE, RESTAURANT_ID, NULL))
        AS UNIQUE_CURRENT_RESTAURANTS
FROM SANDBOX.CONSUMPTION_SCH.RESTAURANT_DIM;



SELECT RESTAURANT_ID, NAME, RESTAURANT_HK, IS_CURRENT
FROM SANDBOX.CONSUMPTION_SCH.RESTAURANT_DIM
ORDER BY RESTAURANT_ID;



SELECT
    'CLEAN_MENU' AS TABLE_NAME,
    COUNT(*) AS TOTAL_RECORDS,
    COUNT(DISTINCT MENU_ID) AS UNIQUE_IDS
FROM SANDBOX.CLEAN_SCH.MENU

UNION ALL

SELECT
    'MENU_DIM',
    COUNT(*),
    COUNT(DISTINCT MENU_ID)
FROM SANDBOX.CONSUMPTION_SCH.MENU_DIM;





SELECT
    C.MENU_ID,
    D.MENU_ID AS DIM_MENU_ID,
    D.MENU_DIM_HK,
    D.IS_CURRENT,
    CASE
        WHEN D.MENU_ID IS NULL THEN 'MISSING IN DIMENSION'
        WHEN D.IS_CURRENT = TRUE THEN 'MATCH'
        ELSE 'NO CURRENT RECORD'
    END AS AUDIT_STATUS
FROM SANDBOX.CLEAN_SCH.MENU C
LEFT JOIN SANDBOX.CONSUMPTION_SCH.MENU_DIM D
    ON C.MENU_ID = D.MENU_ID
    AND D.IS_CURRENT = TRUE
ORDER BY C.MENU_ID;



SELECT
    C.MENU_ID,
    CASE
        WHEN
            EQUAL_NULL(C.RESTAURANT_ID_FK, D.RESTAURANT_ID_FK)
            AND EQUAL_NULL(C.MENU_NAME, D.MENU_NAME)
            AND EQUAL_NULL(C.DESCRIPTION, D.DESCRIPTION)
            AND EQUAL_NULL(C.PRICE, D.PRICE)
            AND EQUAL_NULL(C.CATEGORY, D.CATEGORY)
        THEN 'MATCH'
        ELSE 'ATTRIBUTE DIFFERENCE'
    END AS AUDIT_STATUS
FROM SANDBOX.CLEAN_SCH.MENU C
JOIN SANDBOX.CONSUMPTION_SCH.MENU_DIM D
    ON C.MENU_ID = D.MENU_ID
    AND D.IS_CURRENT = TRUE
ORDER BY C.MENU_ID;


DESC TABLE SANDBOX.CLEAN_SCH.MENU;


DESC TABLE SANDBOX.CONSUMPTION_SCH.MENU_DIM;




SELECT
    C.MENU_ID,
    D.MENU_DIM_HK,

    CASE WHEN C.RESTAURANT_ID_FK IS DISTINCT FROM D.RESTAURANT_ID_FK
         THEN 'DIFFERENT' ELSE 'MATCH' END AS RESTAURANT_CHECK,

    CASE WHEN C.ITEM_NAME IS DISTINCT FROM D.ITEM_NAME
         THEN 'DIFFERENT' ELSE 'MATCH' END AS ITEM_NAME_CHECK,

    CASE WHEN C.DESCRIPTION IS DISTINCT FROM D.DESCRIPTION
         THEN 'DIFFERENT' ELSE 'MATCH' END AS DESCRIPTION_CHECK,

    CASE WHEN C.PRICE IS DISTINCT FROM D.PRICE
         THEN 'DIFFERENT' ELSE 'MATCH' END AS PRICE_CHECK,

    CASE WHEN C.CATEGORY IS DISTINCT FROM D.CATEGORY
         THEN 'DIFFERENT' ELSE 'MATCH' END AS CATEGORY_CHECK,

    CASE WHEN C.AVAILABILITY IS DISTINCT FROM D.AVAILABILITY
         THEN 'DIFFERENT' ELSE 'MATCH' END AS AVAILABILITY_CHECK,

    CASE WHEN C.ITEM_TYPE IS DISTINCT FROM D.ITEM_TYPE
         THEN 'DIFFERENT' ELSE 'MATCH' END AS ITEM_TYPE_CHECK,

    CASE
        WHEN D.MENU_ID IS NULL THEN 'MISSING IN DIMENSION'
        WHEN C.RESTAURANT_ID_FK IS DISTINCT FROM D.RESTAURANT_ID_FK
          OR C.ITEM_NAME IS DISTINCT FROM D.ITEM_NAME
          OR C.DESCRIPTION IS DISTINCT FROM D.DESCRIPTION
          OR C.PRICE IS DISTINCT FROM D.PRICE
          OR C.CATEGORY IS DISTINCT FROM D.CATEGORY
          OR C.AVAILABILITY IS DISTINCT FROM D.AVAILABILITY
          OR C.ITEM_TYPE IS DISTINCT FROM D.ITEM_TYPE
        THEN 'ATTRIBUTE MISMATCH'
        ELSE 'MATCH'
    END AS OVERALL_STATUS

FROM SANDBOX.CLEAN_SCH.MENU C
LEFT JOIN SANDBOX.CONSUMPTION_SCH.MENU_DIM D
    ON C.MENU_ID = D.MENU_ID
   AND D.IS_CURRENT = TRUE
ORDER BY C.MENU_ID;





DESC TABLE SANDBOX.CLEAN_SCH.DELIVERY_AGENT;

DESC TABLE SANDBOX.CONSUMPTION_SCH.DELIVERY_AGENT_DIM;





SELECT
    C.DELIVERY_AGENT_ID,
    D.DELIVERY_AGENT_HK,
    D.IS_CURRENT,
    CASE
        WHEN D.DELIVERY_AGENT_ID IS NULL THEN 'MISSING IN DIMENSION'
        WHEN D.IS_CURRENT = TRUE THEN 'MATCH'
        ELSE 'NO CURRENT RECORD'
    END AS AUDIT_STATUS
FROM SANDBOX.CLEAN_SCH.DELIVERY_AGENT C
LEFT JOIN SANDBOX.CONSUMPTION_SCH.DELIVERY_AGENT_DIM D
    ON C.DELIVERY_AGENT_ID = D.DELIVERY_AGENT_ID
   AND D.IS_CURRENT = TRUE
ORDER BY C.DELIVERY_AGENT_ID;








SELECT
    C.DELIVERY_AGENT_ID,
    D.DELIVERY_AGENT_HK,

    CASE WHEN C.NAME IS DISTINCT FROM D.NAME
         THEN 'DIFFERENT' ELSE 'MATCH' END AS NAME_CHECK,

    CASE WHEN C.PHONE IS DISTINCT FROM D.PHONE
         THEN 'DIFFERENT' ELSE 'MATCH' END AS PHONE_CHECK,

    CASE WHEN C.VEHICLE_TYPE IS DISTINCT FROM D.VEHICLE_TYPE
         THEN 'DIFFERENT' ELSE 'MATCH' END AS VEHICLE_TYPE_CHECK,

    CASE WHEN C.LOCATION_ID_FK IS DISTINCT FROM D.LOCATION_ID_FK
         THEN 'DIFFERENT' ELSE 'MATCH' END AS LOCATION_CHECK,

    CASE WHEN C.STATUS IS DISTINCT FROM D.STATUS
         THEN 'DIFFERENT' ELSE 'MATCH' END AS STATUS_CHECK,

    CASE WHEN C.GENDER IS DISTINCT FROM D.GENDER
         THEN 'DIFFERENT' ELSE 'MATCH' END AS GENDER_CHECK,

    CASE WHEN C.RATING IS DISTINCT FROM D.RATING
         THEN 'DIFFERENT' ELSE 'MATCH' END AS RATING_CHECK,

    CASE
        WHEN C.NAME IS DISTINCT FROM D.NAME
          OR C.PHONE IS DISTINCT FROM D.PHONE
          OR C.VEHICLE_TYPE IS DISTINCT FROM D.VEHICLE_TYPE
          OR C.LOCATION_ID_FK IS DISTINCT FROM D.LOCATION_ID_FK
          OR C.STATUS IS DISTINCT FROM D.STATUS
          OR C.GENDER IS DISTINCT FROM D.GENDER
          OR C.RATING IS DISTINCT FROM D.RATING
        THEN 'ATTRIBUTE MISMATCH'
        ELSE 'MATCH'
    END AS OVERALL_STATUS

FROM SANDBOX.CLEAN_SCH.DELIVERY_AGENT C
JOIN SANDBOX.CONSUMPTION_SCH.DELIVERY_AGENT_DIM D
    ON C.DELIVERY_AGENT_ID = D.DELIVERY_AGENT_ID
   AND D.IS_CURRENT = TRUE
ORDER BY C.DELIVERY_AGENT_ID;




SELECT
    S.DELIVERYAGENTID AS SOURCE_AGENT_ID,
    COUNT(*) AS STAGING_RECORDS,
    CASE
        WHEN C.DELIVERY_AGENT_ID IS NULL THEN 'MISSING IN CLEAN'
        ELSE 'PRESENT IN CLEAN'
    END AS CLEAN_STATUS
FROM SANDBOX.STAGE_SCH.DELIVERYAGENT S
LEFT JOIN SANDBOX.CLEAN_SCH.DELIVERY_AGENT C
    ON S.DELIVERYAGENTID = C.DELIVERY_AGENT_ID
GROUP BY
    S.DELIVERYAGENTID,
    C.DELIVERY_AGENT_ID
ORDER BY SOURCE_AGENT_ID;


LIST @SANDBOX.STAGE_SCH.CSV_STG;



SELECT
    DELIVERYAGENTID,
    _STG_FILE_NAME,
    NAME,
    PHONE,
    VEHICLE_TYPE,
    LOCATION_ID_FK,
    STATUS,
    GENDER,
    RATING
FROM SANDBOX.STAGE_SCH.DELIVERYAGENT
ORDER BY DELIVERYAGENTID, _STG_FILE_NAME;


DESC TABLE SANDBOX.STAGE_SCH.DELIVERYAGENT;



SELECT
    DELIVERYAGENTID,
    _STG_FILE_NAME,
    NAME,
    PHONE,
    VEHICLETYPE, 
    LOCATIONID,
    STATUS,
    GENDER,
    RATING,
    CREATEDDATE,
    MODIFIEDDATE
FROM SANDBOX.STAGE_SCH.DELIVERYAGENT
ORDER BY TRY_TO_NUMBER(DELIVERYAGENTID), _STG_FILE_NAME;



SELECT
    FILE_NAME,
    STATUS,
    ROW_COUNT,
    ROW_PARSED,
    ERROR_COUNT,
    FIRST_ERROR_MESSAGE,
    LAST_LOAD_TIME
FROM TABLE(
    SANDBOX.INFORMATION_SCHEMA.COPY_HISTORY(
        TABLE_NAME => 'SANDBOX.STAGE_SCH.DELIVERYAGENT',
        START_TIME => DATEADD(DAY, -7, CURRENT_TIMESTAMP())
    )
)
ORDER BY LAST_LOAD_TIME;



SELECT
    $1 AS DELIVERYAGENTID,
    $2 AS NAME,
    $3 AS PHONE,
    $4 AS VEHICLETYPE,
    $5 AS LOCATIONID,
    $6 AS STATUS,
    $7 AS GENDER,
    $8 AS RATING,
    METADATA$FILENAME AS SOURCE_FILE
FROM @SANDBOX.STAGE_SCH.CSV_STG/delta/delivery-agent/day-01-delivery-agent.csv
(FILE_FORMAT => 'SANDBOX.STAGE_SCH.CSV_FORMAT')
LIMIT 20;



DESC TABLE SANDBOX.STAGE_SCH.DELIVERYAGENT;


SHOW FILE FORMATS IN SCHEMA SANDBOX.STAGE_SCH;


SELECT
    $1 AS DELIVERYAGENTID,
    $2 AS NAME,
    $3 AS PHONE,
    $4 AS VEHICLETYPE,
    $5 AS LOCATIONID,
    $6 AS STATUS,
    $7 AS GENDER,
    $8 AS RATING,
    $9 AS CREATEDDATE,
    $10 AS MODIFIEDDATE,
    METADATA$FILENAME AS SOURCE_FILE
FROM @SANDBOX.STAGE_SCH.CSV_STG/delta/delivery-agent/day-01-delivery-agent.csv
(FILE_FORMAT => 'SANDBOX.STAGE_SCH.CSV_FILE_FORMAT');



SHOW STREAMS IN DATABASE SANDBOX;


SHOW TASKS IN DATABASE SANDBOX;


SELECT *
FROM SANDBOX.CLEAN_SCH.DELIVERY_AGENT_STM;


SHOW PROCEDURES IN SCHEMA SANDBOX.CLEAN_SCH;



SHOW PROCEDURES IN SCHEMA SANDBOX.STAGE_SCH;


SELECT
    DELIVERYAGENTID,
    NAME,
    VEHICLETYPE,
    RATING,
    _STG_FILE_NAME
FROM SANDBOX.STAGE_SCH.DELIVERYAGENT
WHERE DELIVERYAGENTID IN ('6', '7', '8', '9', '10')
ORDER BY DELIVERYAGENTID;


SELECT
    $1 AS DELIVERYAGENTID,
    $2 AS NAME,
    $3 AS PHONE,
    $4 AS VEHICLETYPE,
    $5 AS LOCATIONID,
    $6 AS STATUS,
    $7 AS GENDER,
    $8 AS RATING,
    $9 AS CREATEDDATE,
    $10 AS MODIFIEDDATE
FROM @SANDBOX.STAGE_SCH.CSV_STG/delta/delivery-agent/day-01-delivery-agent.csv
(FILE_FORMAT => 'SANDBOX.STAGE_SCH.CSV_FILE_FORMAT')
WHERE $1 IN ('6', '7', '8', '9')
ORDER BY $1;



SELECT GET_DDL(
    'TABLE',
    'SANDBOX.STAGE_SCH.DELIVERYAGENT'
);


SELECT
    DELIVERYAGENTID,
    _STG_FILE_NAME,
    _STG_FILE_LOAD_TS,
    _STG_FILE_MD5,
    _COPY_DATA_TS
FROM SANDBOX.STAGE_SCH.DELIVERYAGENT
WHERE DELIVERYAGENTID = '10';


SELECT
    'DAY_01_SOURCE' AS CHECK_TYPE,
    COUNT(*) AS RECORD_COUNT
FROM @SANDBOX.STAGE_SCH.CSV_STG/delta/delivery-agent/day-01-delivery-agent.csv
(FILE_FORMAT => 'SANDBOX.STAGE_SCH.CSV_FILE_FORMAT')
WHERE $1 IN ('6', '7', '8', '9')

UNION ALL

SELECT
    'STAGING_TABLE' AS CHECK_TYPE,
    COUNT(*) AS RECORD_COUNT
FROM SANDBOX.STAGE_SCH.DELIVERYAGENT
WHERE DELIVERYAGENTID IN ('6', '7', '8', '9');



INSERT INTO SANDBOX.STAGE_SCH.DELIVERYAGENT
(
    DELIVERYAGENTID,
    NAME,
    PHONE,
    VEHICLETYPE,
    LOCATIONID,
    STATUS,
    GENDER,
    RATING,
    CREATEDDATE,
    MODIFIEDDATE,
    _STG_FILE_NAME,
    _STG_FILE_LOAD_TS,
    _STG_FILE_MD5,
    _COPY_DATA_TS
)
SELECT
    src.DELIVERYAGENTID,
    src.NAME,
    src.PHONE,
    src.VEHICLETYPE,
    src.LOCATIONID,
    src.STATUS,
    src.GENDER,
    src.RATING,
    src.CREATEDDATE,
    src.MODIFIEDDATE,
    src.SOURCE_FILE,
    CURRENT_TIMESTAMP()::TIMESTAMP_NTZ,
    src.FILE_MD5,
    CURRENT_TIMESTAMP()::TIMESTAMP_NTZ
FROM
(
    SELECT
        $1 AS DELIVERYAGENTID,
        $2 AS NAME,
        $3 AS PHONE,
        $4 AS VEHICLETYPE,
        $5 AS LOCATIONID,
        $6 AS STATUS,
        $7 AS GENDER,
        $8 AS RATING,
        $9 AS CREATEDDATE,
        $10 AS MODIFIEDDATE,
        METADATA$FILENAME AS SOURCE_FILE,
        METADATA$FILE_CONTENT_KEY AS FILE_MD5
    FROM @SANDBOX.STAGE_SCH.CSV_STG/delta/delivery-agent/day-01-delivery-agent.csv
    (FILE_FORMAT => 'SANDBOX.STAGE_SCH.CSV_FILE_FORMAT')
    WHERE $1 IN ('6', '7', '8', '9')
) src
WHERE NOT EXISTS (
    SELECT 1
    FROM SANDBOX.STAGE_SCH.DELIVERYAGENT t
    WHERE t.DELIVERYAGENTID = src.DELIVERYAGENTID
);



SELECT
    DELIVERYAGENTID,
    NAME,
    VEHICLETYPE,
    RATING,
    _STG_FILE_NAME
FROM SANDBOX.STAGE_SCH.DELIVERYAGENT
WHERE DELIVERYAGENTID IN ('6', '7', '8', '9', '10')
ORDER BY DELIVERYAGENTID;



SELECT GET_DDL(
    'STREAM',
    'SANDBOX.STAGE_SCH.DELIVERYAGENT_STM'
);





SELECT
    DELIVERYAGENTID,
    NAME,
    VEHICLETYPE,
    RATING,
    _STG_FILE_NAME,
    METADATA$ACTION,
    METADATA$ISUPDATE
FROM SANDBOX.STAGE_SCH.DELIVERYAGENT_STM
ORDER BY DELIVERYAGENTID;











SELECT
    DELIVERY_AGENT_ID,
    NAME,
    VEHICLE_TYPE,
    RATING,
    MODIFIED_DT
FROM SANDBOX.CLEAN_SCH.DELIVERY_AGENT
WHERE DELIVERY_AGENT_ID IN (1, 6, 7, 8, 9, 10)
ORDER BY DELIVERY_AGENT_ID;





SELECT
    START_TIME,
    QUERY_TYPE,
    QUERY_TEXT
FROM TABLE(
    SANDBOX.INFORMATION_SCHEMA.QUERY_HISTORY(
        END_TIME_RANGE_START => DATEADD('DAY', -14, CURRENT_TIMESTAMP()),
        RESULT_LIMIT => 10000
    )
)
WHERE QUERY_TEXT ILIKE '%DELIVERY_AGENT%'
  AND (
      QUERY_TEXT ILIKE '%MERGE%'
      OR QUERY_TEXT ILIKE '%INSERT%'
  )
ORDER BY START_TIME DESC;




SELECT
    START_TIME,
    QUERY_TYPE,
    QUERY_TEXT
FROM TABLE(
    SANDBOX.INFORMATION_SCHEMA.QUERY_HISTORY(
        END_TIME_RANGE_START => DATEADD('DAY', -6, CURRENT_TIMESTAMP()),
        RESULT_LIMIT => 10000
    )
)
WHERE QUERY_TEXT ILIKE '%DELIVERY_AGENT%'
  AND (
      QUERY_TEXT ILIKE '%MERGE%'
      OR QUERY_TEXT ILIKE '%INSERT%'
  )
ORDER BY START_TIME DESC;






SELECT
    s.DELIVERYAGENTID,
    s.NAME,
    s.VEHICLETYPE AS STAGE_VEHICLE,
    s.RATING AS STAGE_RATING,
    c.VEHICLE_TYPE AS CLEAN_VEHICLE,
    c.RATING AS CLEAN_RATING,
    CASE
        WHEN c.DELIVERY_AGENT_ID IS NULL THEN 'NEW RECORD'
        WHEN c.VEHICLE_TYPE IS DISTINCT FROM s.VEHICLETYPE
          OR c.RATING IS DISTINCT FROM TRY_TO_DECIMAL(s.RATING, 4, 2)
        THEN 'VALUE DIFFERENCE'
        ELSE 'MATCH'
    END AS RECORD_STATUS
FROM SANDBOX.STAGE_SCH.DELIVERYAGENT_STM s
LEFT JOIN SANDBOX.CLEAN_SCH.DELIVERY_AGENT c
    ON c.DELIVERY_AGENT_ID = TRY_TO_NUMBER(s.DELIVERYAGENTID)
ORDER BY TRY_TO_NUMBER(s.DELIVERYAGENTID);



SELECT
    s.DELIVERYAGENTID,
    s.VEHICLETYPE AS STAGE_VEHICLE,
    s.RATING AS STAGE_RATING,
    s.MODIFIEDDATE AS STAGE_MODIFIED,
    c.VEHICLE_TYPE AS CLEAN_VEHICLE,
    c.RATING AS CLEAN_RATING,
    c.MODIFIED_DT AS CLEAN_MODIFIED
FROM SANDBOX.STAGE_SCH.DELIVERYAGENT_STM s
JOIN SANDBOX.CLEAN_SCH.DELIVERY_AGENT c
    ON c.DELIVERY_AGENT_ID = TRY_TO_NUMBER(s.DELIVERYAGENTID)
WHERE s.DELIVERYAGENTID IN ('1', '10')
ORDER BY TRY_TO_NUMBER(s.DELIVERYAGENTID);






INSERT INTO SANDBOX.CLEAN_SCH.DELIVERY_AGENT
(
    DELIVERY_AGENT_ID,
    NAME,
    PHONE,
    VEHICLE_TYPE,
    LOCATION_ID_FK,
    STATUS,
    GENDER,
    RATING,
    CREATED_DT,
    MODIFIED_DT,
    _STG_FILE_NAME,
    _STG_FILE_LOAD_TS,
    _STG_FILE_MD5,
    _COPY_DATA_TS
)
SELECT
    TRY_TO_NUMBER(s.DELIVERYAGENTID),
    s.NAME,
    s.PHONE,
    s.VEHICLETYPE,
    TRY_TO_NUMBER(s.LOCATIONID),
    s.STATUS,
    s.GENDER,
    TRY_TO_NUMBER(s.RATING),
    TRY_TO_TIMESTAMP_NTZ(s.CREATEDDATE),
    TRY_TO_TIMESTAMP_NTZ(s.MODIFIEDDATE),
    s._STG_FILE_NAME,
    s._STG_FILE_LOAD_TS,
    s._STG_FILE_MD5,
    CURRENT_TIMESTAMP()
FROM SANDBOX.STAGE_SCH.DELIVERYAGENT s
WHERE s.DELIVERYAGENTID IN ('6', '7', '8', '9')
AND NOT EXISTS (
    SELECT 1
    FROM SANDBOX.CLEAN_SCH.DELIVERY_AGENT c
    WHERE c.DELIVERY_AGENT_ID = TRY_TO_NUMBER(s.DELIVERYAGENTID)
);


SELECT
    DELIVERY_AGENT_ID,
    NAME,
    VEHICLE_TYPE,
    RATING,
    MODIFIED_DT
FROM SANDBOX.CLEAN_SCH.DELIVERY_AGENT
WHERE DELIVERY_AGENT_ID BETWEEN 6 AND 10
ORDER BY DELIVERY_AGENT_ID;





SELECT
    c.DELIVERY_AGENT_ID,
    c.NAME,
    c.VEHICLE_TYPE,
    c.RATING AS CLEAN_RATING,
    d.RATING AS DIM_RATING,
    d.IS_CURRENT,
    CASE
        WHEN d.DELIVERY_AGENT_ID IS NULL THEN 'MISSING IN DIMENSION'
        WHEN c.RATING = d.RATING THEN 'MATCH'
        ELSE 'RATING DIFFERENCE'
    END AS AUDIT_STATUS
FROM SANDBOX.CLEAN_SCH.DELIVERY_AGENT c
LEFT JOIN SANDBOX.CONSUMPTION_SCH.DELIVERY_AGENT_DIM d
    ON c.DELIVERY_AGENT_ID = d.DELIVERY_AGENT_ID
    AND d.IS_CURRENT = TRUE
WHERE c.DELIVERY_AGENT_ID BETWEEN 1 AND 10
ORDER BY c.DELIVERY_AGENT_ID;



INSERT INTO SANDBOX.CONSUMPTION_SCH.DELIVERY_AGENT_DIM
(
    DELIVERY_AGENT_HK,
    DELIVERY_AGENT_ID,
    NAME,
    PHONE,
    VEHICLE_TYPE,
    LOCATION_ID_FK,
    STATUS,
    GENDER,
    RATING,
    EFF_START_DATE,
    EFF_END_DATE,
    IS_CURRENT
)
SELECT
    HASH(SHA1_HEX(CONCAT(
        c.DELIVERY_AGENT_ID,
        c.NAME,
        c.PHONE,
        c.VEHICLE_TYPE,
        c.LOCATION_ID_FK,
        c.STATUS,
        c.GENDER,
        c.RATING
    ))),
    c.DELIVERY_AGENT_ID,
    c.NAME,
    c.PHONE,
    c.VEHICLE_TYPE,
    c.LOCATION_ID_FK,
    c.STATUS,
    c.GENDER,
    c.RATING,
    CURRENT_TIMESTAMP(),
    NULL,
    TRUE
FROM SANDBOX.CLEAN_SCH.DELIVERY_AGENT c
WHERE c.DELIVERY_AGENT_ID IN (6, 7, 8, 9)
AND NOT EXISTS (
    SELECT 1
    FROM SANDBOX.CONSUMPTION_SCH.DELIVERY_AGENT_DIM d
    WHERE d.DELIVERY_AGENT_ID = c.DELIVERY_AGENT_ID
      AND d.IS_CURRENT = TRUE
);



SELECT
    DELIVERY_AGENT_ID,
    NAME,
    VEHICLE_TYPE,
    RATING,
    IS_CURRENT
FROM SANDBOX.CONSUMPTION_SCH.DELIVERY_AGENT_DIM
WHERE DELIVERY_AGENT_ID BETWEEN 6 AND 9
  AND IS_CURRENT = TRUE
ORDER BY DELIVERY_AGENT_ID;


DESC TABLE SANDBOX.CLEAN_SCH.RESTAURANT_LOCATION;


DESC TABLE SANDBOX.CONSUMPTION_SCH.RESTAURANT_LOCATION_DIM;



SELECT
    c.LOCATION_ID,
    c.CITY,
    c.STATE,
    d.RESTAURANT_LOCATION_HK,
    d.CURRENT_FLAG,
    CASE
        WHEN d.LOCATION_ID IS NULL THEN 'MISSING IN DIMENSION'
        WHEN c.CITY IS DISTINCT FROM d.CITY
          OR c.STATE IS DISTINCT FROM d.STATE
          OR c.STATE_CODE IS DISTINCT FROM d.STATE_CODE
          OR c.IS_UNION_TERRITORY IS DISTINCT FROM d.IS_UNION_TERRITORY
          OR c.CAPITAL_CITY_FLAG IS DISTINCT FROM d.CAPITAL_CITY_FLAG
          OR c.CITY_TIER IS DISTINCT FROM d.CITY_TIER
          OR c.ZIP_CODE IS DISTINCT FROM d.ZIP_CODE
          OR c.ACTIVE_FLAG IS DISTINCT FROM d.ACTIVE_FLAG
        THEN 'ATTRIBUTE DIFFERENCE'
        ELSE 'MATCH'
    END AS AUDIT_STATUS
FROM SANDBOX.CLEAN_SCH.RESTAURANT_LOCATION c
LEFT JOIN SANDBOX.CONSUMPTION_SCH.RESTAURANT_LOCATION_DIM d
    ON c.LOCATION_ID = d.LOCATION_ID
   AND d.CURRENT_FLAG = TRUE
ORDER BY c.LOCATION_ID;




SELECT
    ORDER_ID,
    COUNT(*) AS RECORD_COUNT
FROM SANDBOX.CLEAN_SCH.ORDERS
GROUP BY ORDER_ID
ORDER BY ORDER_ID;




SELECT
    ORDERID,
    COUNT(*) AS RECORD_COUNT
FROM SANDBOX.STAGE_SCH.ORDERS
WHERE TRY_TO_NUMBER(ORDERID) BETWEEN 9 AND 12
GROUP BY ORDERID
ORDER BY TRY_TO_NUMBER(ORDERID);




LIST @SANDBOX.STAGE_SCH.CSV_STG;


DESC TABLE SANDBOX.STAGE_SCH.ORDERS;



SELECT
    'INITIAL' AS SOURCE_FILE,
    $1 AS ORDERID,
    $2 AS CUSTOMERID,
    $3 AS RESTAURANTID,
    $4 AS ORDERDATE,
    $5 AS TOTALAMOUNT,
    $6 AS STATUS,
    $7 AS PAYMENTMETHOD,
    $8 AS CREATEDDATE,
    $9 AS MODIFIEDDATE
FROM @SANDBOX.STAGE_SCH.CSV_STG/initial/orders/orders-initial.csv
(FILE_FORMAT => 'SANDBOX.STAGE_SCH.CSV_FILE_FORMAT')
WHERE $1 IN ('9', '10', '11', '12')

UNION ALL

SELECT
    'DAY-01',
    $1, $2, $3, $4, $5, $6, $7, $8, $9
FROM @SANDBOX.STAGE_SCH.CSV_STG/delta/orders/day-01-orders.csv
(FILE_FORMAT => 'SANDBOX.STAGE_SCH.CSV_FILE_FORMAT')
WHERE $1 IN ('9', '10', '11', '12')

UNION ALL

SELECT
    'DAY-02',
    $1, $2, $3, $4, $5, $6, $7, $8, $9
FROM @SANDBOX.STAGE_SCH.CSV_STG/delta/orders/day-02-orders.csv
(FILE_FORMAT => 'SANDBOX.STAGE_SCH.CSV_FILE_FORMAT')
WHERE $1 IN ('9', '10', '11', '12')

ORDER BY ORDERID, SOURCE_FILE;



INSERT INTO SANDBOX.STAGE_SCH.ORDERS
(
    ORDERID, CUSTOMERID, RESTAURANTID, ORDERDATE,
    TOTALAMOUNT, STATUS, PAYMENTMETHOD, CREATEDDATE,
    MODIFIEDDATE, _STG_FILE_NAME, _STG_FILE_LOAD_TS,
    _STG_FILE_MD5, _COPY_DATA_TS
)
SELECT
    src.ORDERID,
    src.CUSTOMERID,
    src.RESTAURANTID,
    src.ORDERDATE,
    src.TOTALAMOUNT,
    src.STATUS,
    src.PAYMENTMETHOD,
    src.CREATEDDATE,
    src.MODIFIEDDATE,
    src.SOURCE_FILE,
    CURRENT_TIMESTAMP()::TIMESTAMP_NTZ,
    src.FILE_MD5,
    CURRENT_TIMESTAMP()::TIMESTAMP_NTZ
FROM
(
    SELECT
        $1 AS ORDERID,
        $2 AS CUSTOMERID,
        $3 AS RESTAURANTID,
        $4 AS ORDERDATE,
        $5 AS TOTALAMOUNT,
        $6 AS STATUS,
        $7 AS PAYMENTMETHOD,
        $8 AS CREATEDDATE,
        $9 AS MODIFIEDDATE,
        METADATA$FILENAME AS SOURCE_FILE,
        METADATA$FILE_CONTENT_KEY AS FILE_MD5
    FROM @SANDBOX.STAGE_SCH.CSV_STG/delta/orders/day-01-orders.csv
    (FILE_FORMAT => 'SANDBOX.STAGE_SCH.CSV_FILE_FORMAT')
    WHERE $1 IN ('9', '10', '11', '12')
) src
WHERE NOT EXISTS (
    SELECT 1
    FROM SANDBOX.STAGE_SCH.ORDERS t
    WHERE t.ORDERID = src.ORDERID
);



SELECT ORDERID, CUSTOMERID, RESTAURANTID,
       TOTALAMOUNT, STATUS, _STG_FILE_NAME
FROM SANDBOX.STAGE_SCH.ORDERS
WHERE ORDERID IN ('9', '10', '11', '12')
ORDER BY TRY_TO_NUMBER(ORDERID);



DESC TABLE SANDBOX.CLEAN_SCH.ORDERS;


SELECT
    ORDER_SK,
    ORDER_ID,
    CUSTOMER_ID_FK,
    RESTAURANT_ID_FK,
    TOTAL_AMOUNT,
    STATUS,
    _STG_FILE_NAME
FROM SANDBOX.CLEAN_SCH.ORDERS
ORDER BY ORDER_SK DESC
LIMIT 10;



SELECT
    ORDER_SK,
    ORDER_ID,
    CUSTOMER_ID_FK,
    RESTAURANT_ID_FK,
    TOTAL_AMOUNT,
    STATUS,
    _STG_FILE_NAME
FROM SANDBOX.CLEAN_SCH.ORDERS
WHERE ORDER_ID IS NULL
   OR ORDER_ID BETWEEN 9 AND 12
ORDER BY ORDER_SK;


SELECT
    COUNT(*) AS TOTAL_RECORDS,
    COUNT(DISTINCT ORDER_ID) AS UNIQUE_ORDERS,
    MIN(ORDER_ID) AS MIN_ORDER_ID,
    MAX(ORDER_ID) AS MAX_ORDER_ID
FROM SANDBOX.CLEAN_SCH.ORDERS;




SELECT
    QUERY_ID,
    START_TIME,
    EXECUTION_STATUS,
    ROWS_INSERTED,
    ROWS_UPDATED,
    ERROR_MESSAGE,
    QUERY_TEXT
FROM TABLE(SANDBOX.INFORMATION_SCHEMA.QUERY_HISTORY(
    RESULT_LIMIT => 100
))
WHERE QUERY_TEXT ILIKE '%INSERT INTO SANDBOX.CLEAN_SCH.ORDERS%'
ORDER BY START_TIME DESC
LIMIT 5;




SELECT
    QUERY_ID,
    START_TIME,
    EXECUTION_STATUS,
    ROWS_INSERTED,
    ERROR_MESSAGE,
    QUERY_TEXT
FROM TABLE(SANDBOX.INFORMATION_SCHEMA.QUERY_HISTORY(
    RESULT_LIMIT => 100
))
WHERE QUERY_TEXT ILIKE '%INSERT INTO SANDBOX.CLEAN_SCH.ORDERS%'
ORDER BY START_TIME DESC
LIMIT 5;




SELECT
    QUERY_ID,
    START_TIME,
    EXECUTION_STATUS,
    ROWS_INSERTED,
    ERROR_MESSAGE,
    QUERY_TEXT
FROM TABLE(SANDBOX.INFORMATION_SCHEMA.QUERY_HISTORY(
    RESULT_LIMIT => 1000
))
WHERE QUERY_TEXT ILIKE '%INSERT%'
  AND QUERY_TEXT ILIKE '%ORDERS%'
ORDER BY START_TIME DESC
LIMIT 20;



MERGE INTO SANDBOX.CLEAN_SCH.ORDERS AS target
USING (
    SELECT
        TRY_TO_NUMBER(ORDERID) AS ORDER_ID,
        TRY_TO_NUMBER(CUSTOMERID) AS CUSTOMER_ID_FK,
        TRY_TO_NUMBER(RESTAURANTID) AS RESTAURANT_ID_FK,
        TRY_TO_TIMESTAMP_NTZ(ORDERDATE) AS ORDER_DATE,
        TRY_TO_DECIMAL(TOTALAMOUNT, 10, 2) AS TOTAL_AMOUNT,
        STATUS,
        PAYMENTMETHOD AS PAYMENT_METHOD,
        TRY_TO_TIMESTAMP_TZ(CREATEDDATE) AS CREATED_DT,
        TRY_TO_TIMESTAMP_TZ(MODIFIEDDATE) AS MODIFIED_DT,
        _STG_FILE_NAME,
        _STG_FILE_LOAD_TS,
        _STG_FILE_MD5
    FROM SANDBOX.STAGE_SCH.ORDERS
    WHERE ORDERID IN ('9', '10', '11', '12')
) AS source
ON target.ORDER_ID = source.ORDER_ID

WHEN NOT MATCHED THEN INSERT (
    ORDER_ID, CUSTOMER_ID_FK, RESTAURANT_ID_FK,
    ORDER_DATE, TOTAL_AMOUNT, STATUS, PAYMENT_METHOD,
    CREATED_DT, MODIFIED_DT, _STG_FILE_NAME,
    _STG_FILE_LOAD_TS, _STG_FILE_MD5
)
VALUES (
    source.ORDER_ID, source.CUSTOMER_ID_FK,
    source.RESTAURANT_ID_FK, source.ORDER_DATE,
    source.TOTAL_AMOUNT, source.STATUS,
    source.PAYMENT_METHOD, source.CREATED_DT,
    source.MODIFIED_DT, source._STG_FILE_NAME,
    source._STG_FILE_LOAD_TS, source._STG_FILE_MD5
);


SELECT ORDER_ID, CUSTOMER_ID_FK, RESTAURANT_ID_FK,
       TOTAL_AMOUNT, STATUS
FROM SANDBOX.CLEAN_SCH.ORDERS
WHERE ORDER_ID BETWEEN 9 AND 12
ORDER BY ORDER_ID;


DESC TABLE SANDBOX.STAGE_SCH.DELIVERY;

DESC TABLE SANDBOX.CLEAN_SCH.DELIVERY;



SELECT
    TRY_TO_NUMBER(s.DELIVERYID) AS DELIVERY_ID,
    COUNT(*) AS STAGE_RECORDS,
    COUNT(c.DELIVERY_ID) AS CLEAN_MATCHES
FROM SANDBOX.STAGE_SCH.DELIVERY s
LEFT JOIN SANDBOX.CLEAN_SCH.DELIVERY c
    ON TRY_TO_NUMBER(s.DELIVERYID) = c.DELIVERY_ID
GROUP BY TRY_TO_NUMBER(s.DELIVERYID)
ORDER BY DELIVERY_ID;



LIST @SANDBOX.STAGE_SCH.CSV_STG
PATTERN = '.*[Dd]elivery.*';


SELECT
    METADATA$FILENAME AS SOURCE_FILE,
    $1 AS DELIVERYID,
    $2 AS ORDERID,
    $3 AS DELIVERYAGENTID,
    $4 AS DELIVERYSTATUS,
    $5 AS ESTIMATEDTIME,
    $6 AS ADDRESSID,
    $7 AS DELIVERYDATE,
    $8 AS CREATEDDATE,
    $9 AS MODIFIEDDATE
FROM @SANDBOX.STAGE_SCH.CSV_STG/csv_stg/delta/delivery/day-01-delivery.csv
(FILE_FORMAT => 'SANDBOX.STAGE_SCH.CSV_FILE_FORMAT')

UNION ALL

SELECT
    METADATA$FILENAME,
    $1, $2, $3, $4, $5, $6, $7, $8, $9
FROM @SANDBOX.STAGE_SCH.CSV_STG/csv_stg/delta/delivery/day-02-delivery.csv
(FILE_FORMAT => 'SANDBOX.STAGE_SCH.CSV_FILE_FORMAT')

ORDER BY TRY_TO_NUMBER(DELIVERYID);



DESC FILE FORMAT SANDBOX.STAGE_SCH.CSV_FILE_FORMAT;


SELECT
    METADATA$FILENAME,
    $1, $2, $3, $4, $5,
    $6, $7, $8, $9
FROM @SANDBOX.STAGE_SCH.CSV_STG/csv_stg/delta/delivery/day-01-delivery.csv
(FILE_FORMAT => 'SANDBOX.STAGE_SCH.CSV_FILE_FORMAT');

SELECT
    METADATA$FILENAME,
    $1, $2, $3, $4, $5,
    $6, $7, $8, $9
FROM @SANDBOX.STAGE_SCH.CSV_STG/csv_stg/delta/delivery/day-01-delivery.csv
(FILE_FORMAT => (
    TYPE = 'CSV'
    FIELD_DELIMITER = ','
    SKIP_HEADER = 0
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
));


SELECT
    METADATA$FILENAME,
    $1, $2, $3, $4, $5,
    $6, $7, $8, $9
FROM @SANDBOX.STAGE_SCH.CSV_STG/csv_stg/delta/delivery/day-01-delivery.csv
(
    FILE_FORMAT => (
        TYPE = 'CSV',
        FIELD_DELIMITER = ',',
        SKIP_HEADER = 0,
        FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    )
);



SELECT
    METADATA$FILENAME,
    $1, $2, $3, $4, $5,
    $6, $7, $8, $9
FROM @SANDBOX.STAGE_SCH.CSV_STG/csv_stg/delta/delivery/day-01-delivery.csv
(FILE_FORMAT => 'SANDBOX.STAGE_SCH.CSV_FILE_FORMAT')
LIMIT 10;


LIST @SANDBOX.STAGE_SCH.CSV_STG/csv_stg/delta/delivery/;


LIST @SANDBOX.STAGE_SCH.CSV_STG;


SELECT
    METADATA$FILENAME AS SOURCE_FILE,
    $1 AS DELIVERYID,
    $2 AS ORDERID,
    $3 AS DELIVERYAGENTID,
    $4 AS DELIVERYSTATUS,
    $5 AS ESTIMATEDTIME,
    $6 AS ADDRESSID,
    $7 AS DELIVERYDATE,
    $8 AS CREATEDDATE,
    $9 AS MODIFIEDDATE
FROM @SANDBOX.STAGE_SCH.CSV_STG
(FILE_FORMAT => 'SANDBOX.STAGE_SCH.CSV_FILE_FORMAT')
WHERE METADATA$FILENAME ILIKE '%delta/delivery/%'
ORDER BY SOURCE_FILE, TRY_TO_NUMBER($1);


SELECT
    TRY_TO_NUMBER(s.$1) AS DELIVERY_ID,
    s.METADATA$FILENAME AS SOURCE_FILE,
    c.DELIVERY_ID AS CLEAN_DELIVERY_ID,
    CASE
        WHEN c.DELIVERY_ID IS NULL THEN 'MISSING'
        ELSE 'ALREADY EXISTS'
    END AS RECORD_STATUS
FROM @SANDBOX.STAGE_SCH.CSV_STG
(FILE_FORMAT => 'SANDBOX.STAGE_SCH.CSV_FILE_FORMAT') s
LEFT JOIN SANDBOX.CLEAN_SCH.DELIVERY c
    ON TRY_TO_NUMBER(s.$1) = c.DELIVERY_ID
WHERE s.METADATA$FILENAME ILIKE '%delta/delivery/%'
ORDER BY DELIVERY_ID;



INSERT INTO SANDBOX.STAGE_SCH.DELIVERY (
    DELIVERYID,
    ORDERID,
    DELIVERYAGENTID,
    DELIVERYSTATUS,
    ESTIMATEDTIME,
    ADDRESSID,
    DELIVERYDATE,
    CREATEDDATE,
    MODIFIEDDATE,
    _STG_FILE_NAME,
    _STG_FILE_LOAD_TS,
    _STG_FILE_MD5
)
SELECT
    s.$1,
    s.$2,
    s.$3,
    s.$4,
    s.$5,
    s.$6,
    s.$7,
    s.$8,
    s.$9,
    s.METADATA$FILENAME,
    CURRENT_TIMESTAMP()::TIMESTAMP_NTZ,
    NULL
FROM @SANDBOX.STAGE_SCH.CSV_STG
(FILE_FORMAT => 'SANDBOX.STAGE_SCH.CSV_FILE_FORMAT') s
WHERE s.METADATA$FILENAME ILIKE '%delta/delivery/%'
AND NOT EXISTS (
    SELECT 1
    FROM SANDBOX.STAGE_SCH.DELIVERY existing
    WHERE existing.DELIVERYID = s.$1
);

SELECT
    DELIVERYID,
    ORDERID,
    DELIVERYAGENTID,
    DELIVERYSTATUS,
    ESTIMATEDTIME,
    ADDRESSID,
    DELIVERYDATE,
    MODIFIEDDATE
FROM SANDBOX.STAGE_SCH.DELIVERY
WHERE TRY_TO_NUMBER(DELIVERYID) BETWEEN 9 AND 16
ORDER BY TRY_TO_NUMBER(DELIVERYID);


MERGE INTO SANDBOX.CLEAN_SCH.DELIVERY AS target
USING (
    SELECT
        TRY_TO_NUMBER(DELIVERYID) AS DELIVERY_ID,
        TRY_TO_NUMBER(ORDERID) AS ORDER_ID_FK,
        TRY_TO_NUMBER(DELIVERYAGENTID) AS DELIVERY_AGENT_ID_FK,
        DELIVERYSTATUS AS DELIVERY_STATUS,
        ESTIMATEDTIME AS ESTIMATED_TIME,
        TRY_TO_NUMBER(ADDRESSID) AS CUSTOMER_ADDRESS_ID_FK,
        TRY_TO_TIMESTAMP_NTZ(DELIVERYDATE) AS DELIVERY_DATE,
        TRY_TO_TIMESTAMP_NTZ(CREATEDDATE) AS CREATED_DATE,
        TRY_TO_TIMESTAMP_NTZ(MODIFIEDDATE) AS MODIFIED_DATE,
        _STG_FILE_NAME,
        _STG_FILE_LOAD_TS,
        _STG_FILE_MD5
    FROM SANDBOX.STAGE_SCH.DELIVERY
    WHERE TRY_TO_NUMBER(DELIVERYID) BETWEEN 9 AND 16
) AS source
ON target.DELIVERY_ID = source.DELIVERY_ID

WHEN NOT MATCHED THEN INSERT (
    DELIVERY_ID,
    ORDER_ID_FK,
    DELIVERY_AGENT_ID_FK,
    DELIVERY_STATUS,
    ESTIMATED_TIME,
    CUSTOMER_ADDRESS_ID_FK,
    DELIVERY_DATE,
    CREATED_DATE,
    MODIFIED_DATE,
    _STG_FILE_NAME,
    _STG_FILE_LOAD_TS,
    _STG_FILE_MD5
)
VALUES (
    source.DELIVERY_ID,
    source.ORDER_ID_FK,
    source.DELIVERY_AGENT_ID_FK,
    source.DELIVERY_STATUS,
    source.ESTIMATED_TIME,
    source.CUSTOMER_ADDRESS_ID_FK,
    source.DELIVERY_DATE,
    source.CREATED_DATE,
    source.MODIFIED_DATE,
    source._STG_FILE_NAME,
    source._STG_FILE_LOAD_TS,
    source._STG_FILE_MD5
);


SELECT
    DELIVERY_ID,
    ORDER_ID_FK,
    DELIVERY_AGENT_ID_FK,
    DELIVERY_STATUS,
    ESTIMATED_TIME,
    CUSTOMER_ADDRESS_ID_FK
FROM SANDBOX.CLEAN_SCH.DELIVERY
ORDER BY DELIVERY_ID;



SELECT
    COUNT(*) AS TOTAL_RECORDS,
    COUNT(DISTINCT DELIVERY_ID) AS UNIQUE_DELIVERIES,
    MIN(DELIVERY_ID) AS MIN_ID,
    MAX(DELIVERY_ID) AS MAX_ID
FROM SANDBOX.CLEAN_SCH.DELIVERY;



DESC TABLE SANDBOX.STAGE_SCH.ORDER_ITEM;
DESC TABLE SANDBOX.CLEAN_SCH.ORDER_ITEM;
SHOW TABLES IN SCHEMA SANDBOX.STAGE_SCH;

DESC TABLE SANDBOX.STAGE_SCH.ORDERITEM;


SELECT
    _STG_FILE_NAME,
    COUNT(*) AS TOTAL_ROWS,
    COUNT(DISTINCT TRY_TO_NUMBER(ORDERITEMID)) AS UNIQUE_IDS,
    MIN(TRY_TO_NUMBER(ORDERITEMID)) AS MIN_ID,
    MAX(TRY_TO_NUMBER(ORDERITEMID)) AS MAX_ID
FROM SANDBOX.STAGE_SCH.ORDERITEM
GROUP BY _STG_FILE_NAME
ORDER BY _STG_FILE_NAME;


SELECT
    COUNT(*) AS TOTAL_ROWS,
    COUNT(DISTINCT ORDER_ITEM_ID) AS UNIQUE_IDS,
    MIN(ORDER_ITEM_ID) AS MIN_ID,
    MAX(ORDER_ITEM_ID) AS MAX_ID
FROM SANDBOX.CLEAN_SCH.ORDER_ITEM;


SELECT
    METADATA$FILENAME AS SOURCE_FILE,
    COUNT(*) AS TOTAL_ROWS,
    COUNT(DISTINCT TRY_TO_NUMBER($1)) AS UNIQUE_ORDER_ITEM_IDS,
    MIN(TRY_TO_NUMBER($1)) AS MIN_ID,
    MAX(TRY_TO_NUMBER($1)) AS MAX_ID
FROM @SANDBOX.STAGE_SCH.CSV_STG
(FILE_FORMAT => 'SANDBOX.STAGE_SCH.CSV_FILE_FORMAT')
WHERE METADATA$FILENAME ILIKE '%initial/order-items/%'
GROUP BY METADATA$FILENAME
ORDER BY SOURCE_FILE;


SELECT
    METADATA$FILENAME AS SOURCE_FILE,
    $1 AS ORDERITEMID,
    $2 AS ORDERID,
    $3 AS MENUID,
    $4 AS QUANTITY,
    $5 AS PRICE,
    $6 AS SUBTOTAL,
    $7 AS CREATEDDATE,
    $8 AS MODIFIEDDATE
FROM @SANDBOX.STAGE_SCH.CSV_STG
(FILE_FORMAT => 'SANDBOX.STAGE_SCH.CSV_FILE_FORMAT')
WHERE METADATA$FILENAME ILIKE '%initial/order-items/%'
  AND (
      METADATA$FILENAME ILIKE '%order-Item-initial.csv'
      OR METADATA$FILENAME ILIKE '%order-item-initial-v2.csv'
  )
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY METADATA$FILENAME
    ORDER BY TRY_TO_NUMBER($1)
) <= 10
ORDER BY SOURCE_FILE, TRY_TO_NUMBER(ORDERITEMID);



SELECT
    'STAGE' AS TABLE_NAME,
    COUNT(*) AS TOTAL_ROWS,
    COUNT(DISTINCT TRY_TO_NUMBER(ORDERITEMID)) AS UNIQUE_IDS,
    COUNT_IF(TRY_TO_NUMBER(ORDERITEMID) BETWEEN 1 AND 14) AS IDS_1_TO_14
FROM SANDBOX.STAGE_SCH.ORDERITEM

UNION ALL

SELECT
    'CLEAN',
    COUNT(*),
    COUNT(DISTINCT ORDER_ITEM_ID),
    COUNT_IF(ORDER_ITEM_ID BETWEEN 1 AND 14)
FROM SANDBOX.CLEAN_SCH.ORDER_ITEM;


INSERT INTO SANDBOX.STAGE_SCH.ORDERITEM (
    ORDERITEMID,
    ORDERID,
    MENUID,
    QUANTITY,
    PRICE,
    SUBTOTAL,
    CREATEDDATE,
    MODIFIEDDATE,
    _STG_FILE_NAME,
    _STG_FILE_LOAD_TS,
    _STG_FILE_MD5
)
SELECT
    $1,
    $2,
    $3,
    $4,
    $5,
    $6,
    $7,
    $8,
    METADATA$FILENAME,
    CURRENT_TIMESTAMP()::TIMESTAMP_NTZ,
    METADATA$FILE_CONTENT_KEY
FROM @SANDBOX.STAGE_SCH.CSV_STG
(FILE_FORMAT => 'SANDBOX.STAGE_SCH.CSV_FILE_FORMAT')
WHERE METADATA$FILENAME ILIKE '%initial/order-items/order-Item-initial.csv'
  AND TRY_TO_NUMBER($1) BETWEEN 1 AND 14
  AND NOT EXISTS (
      SELECT 1
      FROM SANDBOX.STAGE_SCH.ORDERITEM S
      WHERE TRY_TO_NUMBER(S.ORDERITEMID) = TRY_TO_NUMBER($1)
  );


  SELECT
    METADATA$FILENAME AS SOURCE_FILE,
    $1 AS ORDERITEMID,
    COUNT(*) OVER () AS SOURCE_ROWS
FROM @SANDBOX.STAGE_SCH.CSV_STG
(FILE_FORMAT => 'SANDBOX.STAGE_SCH.CSV_FILE_FORMAT')
WHERE METADATA$FILENAME ILIKE '%order-Item-initial.csv'
  AND TRY_TO_NUMBER($1) BETWEEN 1 AND 14
ORDER BY TRY_TO_NUMBER($1);



WITH SOURCE_DATA AS (
    SELECT
        $1 AS ORDERITEMID,
        $2 AS ORDERID,
        $3 AS MENUID,
        $4 AS QUANTITY,
        $5 AS PRICE,
        $6 AS SUBTOTAL,
        $7 AS CREATEDDATE,
        $8 AS MODIFIEDDATE,
        METADATA$FILENAME AS SOURCE_FILE,
        METADATA$FILE_CONTENT_KEY AS FILE_MD5
    FROM @SANDBOX.STAGE_SCH.CSV_STG
    (FILE_FORMAT => 'SANDBOX.STAGE_SCH.CSV_FILE_FORMAT')
    WHERE METADATA$FILENAME ILIKE '%order-Item-initial.csv'
)
SELECT S.*
FROM SOURCE_DATA S
LEFT JOIN SANDBOX.STAGE_SCH.ORDERITEM T
    ON TRY_TO_NUMBER(T.ORDERITEMID) = TRY_TO_NUMBER(S.ORDERITEMID)
WHERE T.ORDERITEMID IS NULL
ORDER BY TRY_TO_NUMBER(S.ORDERITEMID);



INSERT INTO SANDBOX.STAGE_SCH.ORDERITEM (
    ORDERITEMID,
    ORDERID,
    MENUID,
    QUANTITY,
    PRICE,
    SUBTOTAL,
    CREATEDDATE,
    MODIFIEDDATE,
    _STG_FILE_NAME,
    _STG_FILE_LOAD_TS,
    _STG_FILE_MD5
)
SELECT
    S.$1,
    S.$2,
    S.$3,
    S.$4,
    S.$5,
    S.$6,
    S.$7,
    S.$8,
    S.SOURCE_FILE,
    CURRENT_TIMESTAMP()::TIMESTAMP_NTZ,
    S.FILE_MD5
FROM (
    SELECT
        $1, $2, $3, $4, $5, $6, $7, $8,
        METADATA$FILENAME AS SOURCE_FILE,
        METADATA$FILE_CONTENT_KEY AS FILE_MD5
    FROM @SANDBOX.STAGE_SCH.CSV_STG
    (FILE_FORMAT => 'SANDBOX.STAGE_SCH.CSV_FILE_FORMAT')
    WHERE METADATA$FILENAME ILIKE '%order-Item-initial.csv'
) S
LEFT JOIN SANDBOX.STAGE_SCH.ORDERITEM T
    ON TRY_TO_NUMBER(T.ORDERITEMID) = TRY_TO_NUMBER(S.$1)
WHERE T.ORDERITEMID IS NULL;



MERGE INTO SANDBOX.CLEAN_SCH.ORDER_ITEM T
USING (
    SELECT *
    FROM SANDBOX.STAGE_SCH.ORDERITEM
    WHERE TRY_TO_NUMBER(ORDERITEMID) BETWEEN 1 AND 14
) S
ON T.ORDER_ITEM_ID = TRY_TO_NUMBER(S.ORDERITEMID)

WHEN NOT MATCHED THEN INSERT (
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
);




SELECT
    'STAGE' AS TABLE_NAME,
    COUNT(*) AS TOTAL_ROWS,
    COUNT(DISTINCT TRY_TO_NUMBER(ORDERITEMID)) AS UNIQUE_IDS,
    MIN(TRY_TO_NUMBER(ORDERITEMID)) AS MIN_ID,
    MAX(TRY_TO_NUMBER(ORDERITEMID)) AS MAX_ID
FROM SANDBOX.STAGE_SCH.ORDERITEM

UNION ALL

SELECT
    'CLEAN',
    COUNT(*),
    COUNT(DISTINCT ORDER_ITEM_ID),
    MIN(ORDER_ITEM_ID),
    MAX(ORDER_ITEM_ID)
FROM SANDBOX.CLEAN_SCH.ORDER_ITEM;



DESC TABLE SANDBOX.CLEAN_SCH.ORDER_ITEM;

DESC TABLE SANDBOX.CLEAN_SCH.ORDERS;

DESC TABLE SANDBOX.CLEAN_SCH.DELIVERY;


SELECT
    oi.ORDER_ITEM_ID,
    oi.ORDER_ID_FK
FROM SANDBOX.CLEAN_SCH.ORDER_ITEM oi
LEFT JOIN SANDBOX.CLEAN_SCH.ORDERS o
    ON oi.ORDER_ID_FK = o.ORDER_ID
WHERE o.ORDER_ID IS NULL
ORDER BY oi.ORDER_ITEM_ID;



SELECT
    'CLEAN' AS TABLE_NAME,
    ORDER_ID
FROM SANDBOX.CLEAN_SCH.ORDERS
WHERE ORDER_ID = 17

UNION ALL

SELECT
    'STAGE',
    TRY_TO_NUMBER(ORDERID)
FROM SANDBOX.STAGE_SCH.ORDERS
WHERE TRY_TO_NUMBER(ORDERID) = 17;



SELECT
    METADATA$FILENAME AS SOURCE_FILE,
    $1 AS ORDER_ID,
    $2 AS CUSTOMER_ID,
    $3 AS RESTAURANT_ID,
    $4 AS ORDER_DATE,
    $5 AS TOTAL_AMOUNT,
    $6 AS STATUS,
    $7 AS PAYMENT_METHOD
FROM @SANDBOX.STAGE_SCH.CSV_STG
(FILE_FORMAT => 'SANDBOX.STAGE_SCH.CSV_FILE_FORMAT')
WHERE METADATA$FILENAME ILIKE '%orders/%'
  AND TRY_TO_NUMBER($1) = 17;


  SELECT
    ORDER_ITEM_ID,
    ORDER_ID_FK,
    MENU_ID_FK,
    QUANTITY,
    PRICE,
    SUBTOTAL,
    _STG_FILE_NAME
FROM SANDBOX.CLEAN_SCH.ORDER_ITEM
WHERE ORDER_ITEM_ID = 39;



SELECT
    METADATA$FILENAME AS SOURCE_FILE,
    $1 AS ORDER_ID,
    $2 AS CUSTOMER_ID,
    $3 AS RESTAURANT_ID,
    $4 AS ORDER_DATE,
    $5 AS TOTAL_AMOUNT,
    $6 AS STATUS,
    $7 AS PAYMENT_METHOD
FROM @SANDBOX.STAGE_SCH.CSV_STG
(FILE_FORMAT => 'SANDBOX.STAGE_SCH.CSV_FILE_FORMAT')
WHERE (
       METADATA$FILENAME ILIKE '%orders-initial.csv'
    OR METADATA$FILENAME ILIKE '%day-01-orders.csv'
    OR METADATA$FILENAME ILIKE '%day-02-orders.csv'
)
AND TRY_TO_NUMBER($1) = 17;


LIST @SANDBOX.STAGE_SCH.CSV_STG;


SELECT
    METADATA$FILENAME AS SOURCE_FILE,
    $1 AS ORDER_ID,
    $2 AS CUSTOMER_ID,
    $3 AS RESTAURANT_ID,
    $4 AS ORDER_DATE,
    $5 AS TOTAL_AMOUNT,
    $6 AS STATUS,
    $7 AS PAYMENT_METHOD
FROM @SANDBOX.STAGE_SCH.CSV_STG
(FILE_FORMAT => 'SANDBOX.STAGE_SCH.CSV_FILE_FORMAT')
WHERE (
       METADATA$FILENAME ILIKE '%orders-initial.csv'
    OR METADATA$FILENAME ILIKE '%day-01-orders.csv'
    OR METADATA$FILENAME ILIKE '%day-02-orders.csv'
)
AND TRY_TO_NUMBER($1) = 17;


SELECT
    METADATA$FILENAME AS SOURCE_FILE,
    COUNT(*) AS TOTAL_ROWS,
    MIN(TRY_TO_NUMBER($1)) AS MIN_ORDER_ID,
    MAX(TRY_TO_NUMBER($1)) AS MAX_ORDER_ID,
    COUNT_IF(TRY_TO_NUMBER($1) = 17) AS ORDER_17_COUNT
FROM @SANDBOX.STAGE_SCH.CSV_STG
(FILE_FORMAT => 'SANDBOX.STAGE_SCH.CSV_FILE_FORMAT')
WHERE METADATA$FILENAME ILIKE '%orders-initial.csv'
   OR METADATA$FILENAME ILIKE '%day-01-orders.csv'
   OR METADATA$FILENAME ILIKE '%day-02-orders.csv'
GROUP BY METADATA$FILENAME
ORDER BY SOURCE_FILE;


SELECT
    oi.ORDER_ITEM_ID,
    oi.MENU_ID_FK
FROM SANDBOX.CLEAN_SCH.ORDER_ITEM oi
LEFT JOIN SANDBOX.CONSUMPTION_SCH.MENU_DIM m
    ON oi.MENU_ID_FK = m.MENU_DIM_HK
WHERE m.MENU_DIM_HK IS NULL
ORDER BY oi.ORDER_ITEM_ID;


DESC TABLE SANDBOX.CONSUMPTION_SCH.MENU_DIM;


SELECT
    oi.ORDER_ITEM_ID,
    oi.MENU_ID_FK
FROM SANDBOX.CLEAN_SCH.ORDER_ITEM oi
LEFT JOIN SANDBOX.CONSUMPTION_SCH.MENU_DIM m
    ON oi.MENU_ID_FK = m.MENU_ID
WHERE m.MENU_ID IS NULL
ORDER BY oi.ORDER_ITEM_ID;


SELECT
    MENU_ID,
    RESTAURANT_ID_FK,
    ITEM_NAME,
    PRICE
FROM SANDBOX.CLEAN_SCH.MENU
WHERE MENU_ID BETWEEN 18 AND 26
ORDER BY MENU_ID;


SELECT
    METADATA$FILENAME AS SOURCE_FILE,
    TRY_TO_NUMBER($1) AS MENU_ID,
    $2 AS RESTAURANT_ID,
    $3 AS ITEM_NAME,
    $4 AS DESCRIPTION,
    $5 AS PRICE
FROM @SANDBOX.STAGE_SCH.CSV_STG
(FILE_FORMAT => 'SANDBOX.STAGE_SCH.CSV_FILE_FORMAT')
WHERE (
       METADATA$FILENAME ILIKE '%menu%'
)
AND TRY_TO_NUMBER($1) BETWEEN 18 AND 26
ORDER BY MENU_ID;


DESC TABLE SANDBOX.STAGE_SCH.MENU;


SELECT
    TRY_TO_NUMBER(MENUID) AS MENU_ID,
    _STG_FILE_NAME
FROM SANDBOX.STAGE_SCH.MENU
WHERE TRY_TO_NUMBER(MENUID) BETWEEN 18 AND 26
ORDER BY MENU_ID;


DESC TABLE SANDBOX.CLEAN_SCH.MENU;




MERGE INTO SANDBOX.CLEAN_SCH.MENU T
USING (
    SELECT
        TRY_TO_NUMBER(MENUID) AS MENU_ID,
        TRY_TO_NUMBER(RESTAURANTID) AS RESTAURANT_ID_FK,
        ITEMNAME AS ITEM_NAME,
        DESCRIPTION,
        TRY_TO_NUMBER(PRICE, 10, 2) AS PRICE,
        CATEGORY,
        TRY_TO_BOOLEAN(AVAILABILITY) AS AVAILABILITY,
        ITEMTYPE AS ITEM_TYPE,
        TRY_TO_TIMESTAMP_NTZ(CREATEDDATE) AS CREATED_DT,
        TRY_TO_TIMESTAMP_NTZ(MODIFIEDDATE) AS MODIFIED_DT,
        _STG_FILE_NAME,
        _STG_FILE_LOAD_TS,
        _STG_FILE_MD5,
        _COPY_DATA_TS
    FROM SANDBOX.STAGE_SCH.MENU
    WHERE TRY_TO_NUMBER(MENUID) BETWEEN 18 AND 26
) S
ON T.MENU_ID = S.MENU_ID




INSERT INTO SANDBOX.CLEAN_SCH.MENU (
    MENU_ID,
    RESTAURANT_ID_FK,
    ITEM_NAME,
    DESCRIPTION,
    PRICE,
    CATEGORY,
    AVAILABILITY,
    ITEM_TYPE,
    CREATED_DT,
    MODIFIED_DT,
    _STG_FILE_NAME,
    _STG_FILE_LOAD_TS,
    _STG_FILE_MD5,
    _COPY_DATA_TS
)
SELECT
    TRY_TO_NUMBER(s.MENUID),
    TRY_TO_NUMBER(s.RESTAURANTID),
    s.ITEMNAME,
    s.DESCRIPTION,
    TRY_TO_NUMBER(s.PRICE, 10, 2),
    s.CATEGORY,
    TRY_TO_BOOLEAN(s.AVAILABILITY),
    s.ITEMTYPE,
    TRY_TO_TIMESTAMP_NTZ(s.CREATEDDATE),
    TRY_TO_TIMESTAMP_NTZ(s.MODIFIEDDATE),
    s._STG_FILE_NAME,
    s._STG_FILE_LOAD_TS,
    s._STG_FILE_MD5,
    s._COPY_DATA_TS
FROM SANDBOX.STAGE_SCH.MENU s
WHERE TRY_TO_NUMBER(s.MENUID) BETWEEN 18 AND 26
  AND NOT EXISTS (
      SELECT 1
      FROM SANDBOX.CLEAN_SCH.MENU t
      WHERE t.MENU_ID = TRY_TO_NUMBER(s.MENUID)
  );


  SELECT
    MENU_ID,
    RESTAURANT_ID_FK,
    ITEM_NAME,
    PRICE
FROM SANDBOX.CLEAN_SCH.MENU
WHERE MENU_ID BETWEEN 18 AND 26
ORDER BY MENU_ID;



SELECT
    MENU_ID,
    MENU_DIM_HK,
    RESTAURANT_ID_FK,
    ITEM_NAME,
    PRICE,
    IS_CURRENT
FROM SANDBOX.CONSUMPTION_SCH.MENU_DIM
WHERE MENU_ID BETWEEN 18 AND 26
ORDER BY MENU_ID;


SELECT *
FROM SANDBOX.CONSUMPTION_SCH.MENU_DIM
ORDER BY MENU_ID;

SELECT
    MENU_ID,
    MENU_DIM_HK,
    HASH(SHA1_HEX(CONCAT(
        MENU_ID,
        RESTAURANT_ID_FK,
        ITEM_NAME,
        DESCRIPTION,
        PRICE,
        CATEGORY,
        AVAILABILITY,
        ITEM_TYPE
    ))) AS CALCULATED_HK,
    MENU_DIM_HK = HASH(SHA1_HEX(CONCAT(
        MENU_ID,
        RESTAURANT_ID_FK,
        ITEM_NAME,
        DESCRIPTION,
        PRICE,
        CATEGORY,
        AVAILABILITY,
        ITEM_TYPE
    ))) AS HASH_MATCH
FROM SANDBOX.CONSUMPTION_SCH.MENU_DIM
ORDER BY MENU_ID;


INSERT INTO SANDBOX.CONSUMPTION_SCH.MENU_DIM (
    MENU_DIM_HK,
    MENU_ID,
    RESTAURANT_ID_FK,
    ITEM_NAME,
    DESCRIPTION,
    PRICE,
    CATEGORY,
    AVAILABILITY,
    ITEM_TYPE,
    EFF_START_DATE,
    EFF_END_DATE,
    IS_CURRENT
)
SELECT
    HASH(SHA1_HEX(CONCAT(
        c.MENU_ID,
        c.RESTAURANT_ID_FK,
        c.ITEM_NAME,
        c.DESCRIPTION,
        c.PRICE,
        c.CATEGORY,
        c.AVAILABILITY,
        c.ITEM_TYPE
    ))),
    c.MENU_ID,
    c.RESTAURANT_ID_FK,
    c.ITEM_NAME,
    c.DESCRIPTION,
    c.PRICE,
    c.CATEGORY,
    c.AVAILABILITY,
    c.ITEM_TYPE,
    CURRENT_TIMESTAMP()::TIMESTAMP_NTZ,
    NULL,
    TRUE
FROM SANDBOX.CLEAN_SCH.MENU c
WHERE c.MENU_ID BETWEEN 18 AND 26
  AND NOT EXISTS (
      SELECT 1
      FROM SANDBOX.CONSUMPTION_SCH.MENU_DIM d
      WHERE d.MENU_ID = c.MENU_ID
        AND d.IS_CURRENT = TRUE
  );

  SELECT
    MENU_ID,
    MENU_DIM_HK,
    ITEM_NAME,
    RESTAURANT_ID_FK,
    IS_CURRENT
FROM SANDBOX.CONSUMPTION_SCH.MENU_DIM
WHERE MENU_ID BETWEEN 18 AND 26
ORDER BY MENU_ID;



SELECT
    oi.ORDER_ITEM_ID,
    oi.MENU_ID_FK
FROM SANDBOX.CLEAN_SCH.ORDER_ITEM oi
LEFT JOIN SANDBOX.CONSUMPTION_SCH.MENU_DIM m
    ON oi.MENU_ID_FK = m.MENU_ID
   AND m.IS_CURRENT = TRUE
WHERE m.MENU_ID IS NULL
ORDER BY oi.ORDER_ITEM_ID;




SELECT
    COUNT(*) AS TOTAL_RECORDS,
    COUNT(DISTINCT ORDER_ITEM_ID) AS UNIQUE_ORDER_ITEMS,
    SUM(SUBTOTAL) AS TOTAL_REVENUE
FROM SANDBOX.CONSUMPTION_SCH.ORDER_ITEM_FACT;



SELECT
    TABLE_SCHEMA,
    TABLE_NAME,
    COLUMN_NAME,
    DATA_TYPE,
    ORDINAL_POSITION
FROM SANDBOX.INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_SCHEMA = 'CONSUMPTION_SCH'
  AND TABLE_NAME IN (
      'ORDER_ITEM_FACT',
      'CUSTOMER_DIM',
      'CUSTOMER_ADDRESS_DIM',
      'RESTAURANT_DIM',
      'RESTAURANT_LOCATION_DIM',
      'MENU_DIM',
      'DELIVERY_AGENT_DIM',
      'DATE_DIM'
  )
ORDER BY TABLE_NAME, ORDINAL_POSITION;

SELECT 'ORDER_ITEM' AS TABLE_NAME, COUNT(*) AS RECORDS
FROM SANDBOX.CLEAN_SCH.ORDER_ITEM

UNION ALL

SELECT 'ORDERS', COUNT(*)
FROM SANDBOX.CLEAN_SCH.ORDERS

UNION ALL

SELECT 'DELIVERY', COUNT(*)
FROM SANDBOX.CLEAN_SCH.DELIVERY;



SELECT 'ORDER_ITEM' AS TABLE_NAME, COUNT(*) AS TOTAL_RECORDS
FROM SANDBOX.CLEAN_SCH.ORDER_ITEM

UNION ALL

SELECT 'ORDERS', COUNT(*)
FROM SANDBOX.CLEAN_SCH.ORDERS

UNION ALL

SELECT 'DELIVERY', COUNT(*)
FROM SANDBOX.CLEAN_SCH.DELIVERY

UNION ALL

SELECT 'CUSTOMER_DIM', COUNT(*)
FROM SANDBOX.CONSUMPTION_SCH.CUSTOMER_DIM
WHERE IS_CURRENT = TRUE

UNION ALL

SELECT 'CUSTOMER_ADDRESS_DIM', COUNT(*)
FROM SANDBOX.CONSUMPTION_SCH.CUSTOMER_ADDRESS_DIM
WHERE IS_CURRENT = TRUE

UNION ALL

SELECT 'RESTAURANT_DIM', COUNT(*)
FROM SANDBOX.CONSUMPTION_SCH.RESTAURANT_DIM
WHERE IS_CURRENT = TRUE

UNION ALL

SELECT 'MENU_DIM', COUNT(*)
FROM SANDBOX.CONSUMPTION_SCH.MENU_DIM
WHERE IS_CURRENT = TRUE

UNION ALL

SELECT 'DELIVERY_AGENT_DIM', COUNT(*)
FROM SANDBOX.CONSUMPTION_SCH.DELIVERY_AGENT_DIM
WHERE IS_CURRENT = TRUE

UNION ALL

SELECT 'RESTAURANT_LOCATION_DIM', COUNT(*)
FROM SANDBOX.CONSUMPTION_SCH.RESTAURANT_LOCATION_DIM
WHERE CURRENT_FLAG = TRUE

UNION ALL

SELECT 'DATE_DIM', COUNT(*)
FROM SANDBOX.CONSUMPTION_SCH.DATE_DIM;



SELECT 'ORDER_ITEM' AS TABLE_NAME, COUNT(*) AS TOTAL_RECORDS
FROM SANDBOX.CLEAN_SCH.ORDER_ITEM

UNION ALL

SELECT 'ORDERS', COUNT(*)
FROM SANDBOX.CLEAN_SCH.ORDERS

UNION ALL

SELECT 'DELIVERY', COUNT(*)
FROM SANDBOX.CLEAN_SCH.DELIVERY

UNION ALL

SELECT 'CUSTOMER_DIM', COUNT(*)
FROM SANDBOX.CONSUMPTION_SCH.CUSTOMER_DIM
WHERE IS_CURRENT = TRUE

UNION ALL

SELECT 'CUSTOMER_ADDRESS_DIM', COUNT(*)
FROM SANDBOX.CONSUMPTION_SCH.CUSTOMER_ADDRESS_DIM
WHERE IS_CURRENT = TRUE;



SELECT
    ORDER_ID_FK,
    COUNT(*) AS DELIVERY_COUNT
FROM SANDBOX.CLEAN_SCH.DELIVERY
GROUP BY ORDER_ID_FK
HAVING COUNT(*) > 1
ORDER BY ORDER_ID_FK;



SELECT
    COUNT(*) AS TOTAL_JOINED_ROWS,
    COUNT(DISTINCT oi.ORDER_ITEM_ID) AS UNIQUE_ORDER_ITEMS,

    COUNT_IF(o.ORDER_ID IS NULL) AS MISSING_ORDERS,
    COUNT_IF(c.CUSTOMER_HK IS NULL) AS MISSING_CUSTOMERS,
    COUNT_IF(ca.CUSTOMER_ADDRESS_HK IS NULL) AS MISSING_ADDRESSES,
    COUNT_IF(r.RESTAURANT_HK IS NULL) AS MISSING_RESTAURANTS,
    COUNT_IF(rl.RESTAURANT_LOCATION_HK IS NULL) AS MISSING_LOCATIONS,
    COUNT_IF(m.MENU_DIM_HK IS NULL) AS MISSING_MENU_ITEMS,
    COUNT_IF(da.DELIVERY_AGENT_HK IS NULL) AS MISSING_DELIVERY_AGENTS,
    COUNT_IF(d.DATE_DIM_HK IS NULL) AS MISSING_ORDER_DATES,
    COUNT_IF(del.DELIVERY_ID IS NULL) AS MISSING_DELIVERIES

FROM SANDBOX.CLEAN_SCH.ORDER_ITEM oi

LEFT JOIN SANDBOX.CLEAN_SCH.ORDERS o
    ON oi.ORDER_ID_FK = o.ORDER_ID

LEFT JOIN SANDBOX.CLEAN_SCH.DELIVERY del
    ON oi.ORDER_ID_FK = del.ORDER_ID_FK

LEFT JOIN SANDBOX.CONSUMPTION_SCH.CUSTOMER_DIM c
    ON o.CUSTOMER_ID_FK = c.CUSTOMER_ID
    AND c.IS_CURRENT = TRUE

LEFT JOIN SANDBOX.CONSUMPTION_SCH.CUSTOMER_ADDRESS_DIM ca
    ON del.CUSTOMER_ADDRESS_ID_FK = ca.ADDRESS_ID
    AND ca.IS_CURRENT = TRUE

LEFT JOIN SANDBOX.CONSUMPTION_SCH.RESTAURANT_DIM r
    ON o.RESTAURANT_ID_FK = r.RESTAURANT_ID
    AND r.IS_CURRENT = TRUE

LEFT JOIN SANDBOX.CONSUMPTION_SCH.RESTAURANT_LOCATION_DIM rl
    ON r.LOCATION_ID_FK = rl.LOCATION_ID
    AND rl.CURRENT_FLAG = TRUE

LEFT JOIN SANDBOX.CONSUMPTION_SCH.MENU_DIM m
    ON oi.MENU_ID_FK = m.MENU_ID
    AND m.IS_CURRENT = TRUE

LEFT JOIN SANDBOX.CONSUMPTION_SCH.DELIVERY_AGENT_DIM da
    ON del.DELIVERY_AGENT_ID_FK = da.DELIVERY_AGENT_ID
    AND da.IS_CURRENT = TRUE

LEFT JOIN SANDBOX.CONSUMPTION_SCH.DATE_DIM d
    ON TO_DATE(o.ORDER_DATE) = d.CALENDAR_DATE;


    SELECT
    oi.ORDER_ITEM_ID,
    oi.ORDER_ID_FK,
    o.CUSTOMER_ID_FK,
    del.CUSTOMER_ADDRESS_ID_FK,
    ca.ADDRESS_ID AS MATCHED_ADDRESS_ID,
    ca.CUSTOMER_ID_FK AS ADDRESS_CUSTOMER_ID
FROM SANDBOX.CLEAN_SCH.ORDER_ITEM oi
LEFT JOIN SANDBOX.CLEAN_SCH.ORDERS o
    ON oi.ORDER_ID_FK = o.ORDER_ID
LEFT JOIN SANDBOX.CLEAN_SCH.DELIVERY del
    ON oi.ORDER_ID_FK = del.ORDER_ID_FK
LEFT JOIN SANDBOX.CONSUMPTION_SCH.CUSTOMER_ADDRESS_DIM ca
    ON del.CUSTOMER_ADDRESS_ID_FK = ca.ADDRESS_ID
    AND ca.IS_CURRENT = TRUE
WHERE o.ORDER_ID IS NOT NULL
  AND (
      NOT EXISTS (
          SELECT 1
          FROM SANDBOX.CONSUMPTION_SCH.CUSTOMER_DIM c
          WHERE c.CUSTOMER_ID = o.CUSTOMER_ID_FK
            AND c.IS_CURRENT = TRUE
      )
      OR ca.ADDRESS_ID IS NULL
  )
ORDER BY oi.ORDER_ITEM_ID;



SELECT
    'CUSTOMER_DIM' AS TABLE_NAME,
    CUSTOMER_ID AS SOURCE_ID,
    IS_CURRENT
FROM SANDBOX.CONSUMPTION_SCH.CUSTOMER_DIM

UNION ALL

SELECT
    'CUSTOMER_ADDRESS_DIM',
    ADDRESS_ID,
    IS_CURRENT
FROM SANDBOX.CONSUMPTION_SCH.CUSTOMER_ADDRESS_DIM

ORDER BY TABLE_NAME, SOURCE_ID;




SELECT
    'CUSTOMER' AS TABLE_NAME,
    CUSTOMER_ID AS RECORD_ID
FROM SANDBOX.CLEAN_SCH.CUSTOMER
WHERE CUSTOMER_ID IN (6, 7, 8)

UNION ALL

SELECT
    'CUSTOMER_ADDRESS',
    ADDRESS_ID
FROM SANDBOX.CLEAN_SCH.CUSTOMER_ADDRESS
WHERE ADDRESS_ID IN (6, 7, 8)

ORDER BY TABLE_NAME, RECORD_ID;



SELECT
    'CUSTOMER' AS TABLE_NAME,
    CUSTOMER_ID AS RECORD_ID
FROM SANDBOX.STAGE_SCH.CUSTOMER
WHERE CUSTOMER_ID IN (6, 7, 8)

UNION ALL

SELECT
    'CUSTOMER_ADDRESS',
    ADDRESS_ID
FROM SANDBOX.STAGE_SCH.CUSTOMER_ADDRESS
WHERE ADDRESS_ID IN (6, 7, 8)

ORDER BY TABLE_NAME, RECORD_ID;



SHOW TABLES IN SCHEMA SANDBOX.STAGE_SCH;




SELECT
    'CUSTOMER' AS TABLE_NAME,
    CUSTOMER_ID AS RECORD_ID
FROM SANDBOX.STAGE_SCH.CUSTOMER
WHERE TRY_TO_NUMBER(CUSTOMER_ID) IN (6, 7, 8)

UNION ALL

SELECT
    'CUSTOMERADDRESS',
    ADDRESS_ID
FROM SANDBOX.STAGE_SCH.CUSTOMERADDRESS
WHERE TRY_TO_NUMBER(ADDRESS_ID) IN (6, 7, 8)

ORDER BY TABLE_NAME, RECORD_ID;



DESC TABLE SANDBOX.STAGE_SCH.CUSTOMER;


DESC TABLE SANDBOX.STAGE_SCH.CUSTOMERADDRESS;


SELECT
    'CUSTOMER' AS TABLE_NAME,
    CUSTOMERID AS RECORD_ID,
    NULL AS CUSTOMER_ID_FK
FROM SANDBOX.STAGE_SCH.CUSTOMER
WHERE TRY_TO_NUMBER(CUSTOMERID) IN (6, 7, 8)

UNION ALL

SELECT
    'CUSTOMERADDRESS',
    ADDRESSID,
    CUSTOMERID
FROM SANDBOX.STAGE_SCH.CUSTOMERADDRESS
WHERE TRY_TO_NUMBER(ADDRESSID) IN (6, 7, 8)

ORDER BY TABLE_NAME, RECORD_ID;


DESC TABLE SANDBOX.CLEAN_SCH.CUSTOMER;


DESC TABLE SANDBOX.CLEAN_SCH.CUSTOMER_ADDRESS;



SELECT
    'CUSTOMER_CLEAN' AS TABLE_NAME,
    CUSTOMER_ID AS RECORD_ID,
    _STG_FILE_NAME,
    _STG_FILE_LOAD_TS,
    _STG_FILE_MD5
FROM SANDBOX.CLEAN_SCH.CUSTOMER
WHERE CUSTOMER_ID IN ('5', '9', '10')

UNION ALL

SELECT
    'CUSTOMER_STAGE',
    CUSTOMERID,
    _STG_FILE_NAME,
    _STG_FILE_LOAD_TS,
    _STG_FILE_MD5
FROM SANDBOX.STAGE_SCH.CUSTOMER
WHERE TRY_TO_NUMBER(CUSTOMERID) IN (6, 7, 8)

UNION ALL

SELECT
    'ADDRESS_STAGE',
    ADDRESSID,
    _STG_FILE_NAME,
    _STG_FILE_LOAD_TS,
    _STG_FILE_MD5
FROM SANDBOX.STAGE_SCH.CUSTOMERADDRESS
WHERE TRY_TO_NUMBER(ADDRESSID) IN (6, 7, 8)

ORDER BY TABLE_NAME, RECORD_ID;


SELECT *
FROM SANDBOX.STAGE_SCH.CUSTOMER
WHERE TRY_TO_NUMBER(CUSTOMERID) IN (6, 7, 8)
ORDER BY TRY_TO_NUMBER(CUSTOMERID);



SELECT *
FROM SANDBOX.STAGE_SCH.CUSTOMERADDRESS
WHERE TRY_TO_NUMBER(ADDRESSID) IN (6, 7, 8)
ORDER BY TRY_TO_NUMBER(ADDRESSID);






INSERT INTO SANDBOX.CLEAN_SCH.CUSTOMER (
    CUSTOMER_ID,
    NAME,
    MOBILE,
    EMAIL,
    LOGIN_BY_USING,
    GENDER,
    DOB,
    ANNIVERSARY,
    PREFERENCES,
    CREATED_DT,
    MODIFIED_DT,
    _STG_FILE_NAME,
    _STG_FILE_LOAD_TS,
    _STG_FILE_MD5,
    _COPY_DATA_TS
)
SELECT
    s.CUSTOMERID,
    s.NAME,
    s.MOBILE,
    s.EMAIL,
    s.LOGINBYUSING,
    s.GENDER,
    TRY_TO_DATE(s.DOB),
    TRY_TO_DATE(s.ANNIVERSARY),
    s.PREFERENCES,
    TRY_TO_TIMESTAMP_TZ(s.CREATEDDATE),
    TRY_TO_TIMESTAMP_TZ(s.MODIFIEDDATE),
    s._STG_FILE_NAME,
    s._STG_FILE_LOAD_TS,
    s._STG_FILE_MD5,
    s._COPY_DATA_TS
FROM SANDBOX.STAGE_SCH.CUSTOMER s
WHERE TRY_TO_NUMBER(s.CUSTOMERID) IN (6, 7, 8)
  AND NOT EXISTS (
      SELECT 1
      FROM SANDBOX.CLEAN_SCH.CUSTOMER c
      WHERE c.CUSTOMER_ID = s.CUSTOMERID
  );

 INSERT INTO SANDBOX.CLEAN_SCH.CUSTOMER_ADDRESS (
    ADDRESS_ID,
    CUSTOMER_ID_FK,
    FLAT_NO,
    HOUSE_NO,
    FLOOR,
    BUILDING,
    LANDMARK,
    LOCALITY,
    CITY,
    STATE,
    PINCODE,
    COORDINATES,
    PRIMARY_FLAG,
    ADDRESS_TYPE,
    CREATED_DATE,
    MODIFIED_DATE,
    _STG_FILE_NAME,
    _STG_FILE_LOAD_TS,
    _STG_FILE_MD5,
    _COPY_DATA_TS
)
SELECT
    TRY_TO_NUMBER(s.ADDRESSID),
    TRY_TO_NUMBER(s.CUSTOMERID),
    s.FLATNO,
    s.HOUSENO,
    s.FLOOR,
    s.BUILDING,
    s.LANDMARK,
    s.LOCALITY,
    s.CITY,
    s.STATE,
    s.PINCODE,
    s.COORDINATES,
    s.PRIMARYFLAG,
    s.ADDRESSTYPE,
    TRY_TO_TIMESTAMP_TZ(s.CREATEDDATE),
    TRY_TO_TIMESTAMP_TZ(s.MODIFIEDDATE),
    s._STG_FILE_NAME,
    s._STG_FILE_LOAD_TS,
    s._STG_FILE_MD5,
    s._COPY_DATA_TS
FROM SANDBOX.STAGE_SCH.CUSTOMERADDRESS s
WHERE TRY_TO_NUMBER(s.ADDRESSID) IN (6, 7, 8)
  AND NOT EXISTS (
      SELECT 1
      FROM SANDBOX.CLEAN_SCH.CUSTOMER_ADDRESS a
      WHERE a.ADDRESS_ID = TRY_TO_NUMBER(s.ADDRESSID)
  ); 


SELECT
    'CUSTOMER' AS TABLE_NAME,
    CUSTOMER_ID AS RECORD_ID
FROM SANDBOX.CLEAN_SCH.CUSTOMER
WHERE CUSTOMER_ID IN ('6', '7', '8')

UNION ALL

SELECT
    'CUSTOMER_ADDRESS',
    TO_VARCHAR(ADDRESS_ID)
FROM SANDBOX.CLEAN_SCH.CUSTOMER_ADDRESS
WHERE ADDRESS_ID IN (6, 7, 8)

ORDER BY TABLE_NAME, RECORD_ID;


DESC TABLE SANDBOX.CONSUMPTION_SCH.CUSTOMER_DIM;

DESC TABLE SANDBOX.CONSUMPTION_SCH.CUSTOMER_ADDRESS_DIM;



SELECT *
FROM SANDBOX.CONSUMPTION_SCH.CUSTOMER_DIM
WHERE IS_CURRENT = TRUE
ORDER BY TRY_TO_NUMBER(CUSTOMER_ID);


SELECT *
FROM SANDBOX.CLEAN_SCH.CUSTOMER
WHERE CUSTOMERID IN ('6', '7', '8')
ORDER BY TRY_TO_NUMBER(CUSTOMERID);



DESC TABLE SANDBOX.CLEAN_SCH.CUSTOMER;




DESC TABLE SANDBOX.CONSUMPTION_SCH.CUSTOMER_DIM;



SELECT *
FROM SANDBOX.CONSUMPTION_SCH.CUSTOMER_DIM
WHERE CUSTOMER_ID IN ('6','7','8');


SELECT *
FROM SANDBOX.CLEAN_SCH.CUSTOMER
WHERE CUSTOMER_ID IN ('6','7','8')
ORDER BY TRY_TO_NUMBER(CUSTOMER_ID);



INSERT INTO SANDBOX.CONSUMPTION_SCH.CUSTOMER_DIM
(
    CUSTOMER_HK,
    CUSTOMER_ID,
    NAME,
    MOBILE,
    EMAIL,
    LOGIN_BY_USING,
    GENDER,
    DOB,
    ANNIVERSARY,
    PREFERENCES,
    EFF_START_DATE,
    EFF_END_DATE,
    IS_CURRENT
)
SELECT
    ROW_NUMBER() OVER (ORDER BY TRY_TO_NUMBER(CUSTOMER_ID))
    + (SELECT MAX(CUSTOMER_HK) FROM SANDBOX.CONSUMPTION_SCH.CUSTOMER_DIM),
    CUSTOMER_ID,
    NAME,
    MOBILE,
    EMAIL,
    LOGIN_BY_USING,
    GENDER,
    DOB,
    ANNIVERSARY,
    PREFERENCES,
    CURRENT_TIMESTAMP(),
    NULL,
    TRUE
FROM SANDBOX.CLEAN_SCH.CUSTOMER
WHERE CUSTOMER_ID IN ('6','7','8')
AND CUSTOMER_ID NOT IN (
    SELECT CUSTOMER_ID
    FROM SANDBOX.CONSUMPTION_SCH.CUSTOMER_DIM
    WHERE IS_CURRENT = TRUE
);
SELECT *
FROM SANDBOX.CONSUMPTION_SCH.CUSTOMER_DIM
WHERE CUSTOMER_ID IN ('6','7','8');




SELECT CUSTOMER_HK,
       CUSTOMER_ID,
       NAME,
       IS_CURRENT
FROM SANDBOX.CONSUMPTION_SCH.CUSTOMER_DIM
WHERE CUSTOMER_ID IN ('6','7','8')
ORDER BY TRY_TO_NUMBER(CUSTOMER_ID);


SELECT COUNT(*)
FROM SANDBOX.CONSUMPTION_SCH.CUSTOMER_DIM
WHERE IS_CURRENT = TRUE;

DESC TABLE SANDBOX.CONSUMPTION_SCH.CUSTOMER_ADDRESS_DIM;


SELECT *
FROM SANDBOX.CONSUMPTION_SCH.CUSTOMER_ADDRESS_DIM
WHERE ADDRESS_ID IN (6,7,8);

SELECT *
FROM SANDBOX.CLEAN_SCH.CUSTOMER_ADDRESS
WHERE ADDRESS_ID IN (6,7,8)
ORDER BY ADDRESS_ID;


INSERT INTO SANDBOX.CONSUMPTION_SCH.CUSTOMER_ADDRESS_DIM
(
    CUSTOMER_ADDRESS_HK,
    ADDRESS_ID,
    CUSTOMER_ID_FK,
    FLAT_NO,
    HOUSE_NO,
    FLOOR,
    BUILDING,
    LANDMARK,
    LOCALITY,
    CITY,
    STATE,
    PINCODE,
    COORDINATES,
    PRIMARY_FLAG,
    ADDRESS_TYPE,
    EFF_START_DATE,
    EFF_END_DATE,
    IS_CURRENT
)
SELECT
    ROW_NUMBER() OVER (ORDER BY ADDRESS_ID)
    + (SELECT MAX(CUSTOMER_ADDRESS_HK)
       FROM SANDBOX.CONSUMPTION_SCH.CUSTOMER_ADDRESS_DIM),
    ADDRESS_ID,
    CUSTOMER_ID,
    FLAT_NO,
    HOUSE_NO,
    FLOOR,
    BUILDING,
    LANDMARK,
    LOCALITY,
    CITY,
    STATE,
    PINCODE,
    COORDINATES,
    PRIMARY_FLAG,
    ADDRESS_TYPE,
    CURRENT_TIMESTAMP(),
    NULL,
    TRUE
FROM SANDBOX.CLEAN_SCH.CUSTOMER_ADDRESS
WHERE ADDRESS_ID IN (6,7,8)
AND ADDRESS_ID NOT IN
(
    SELECT ADDRESS_ID
    FROM SANDBOX.CONSUMPTION_SCH.CUSTOMER_ADDRESS_DIM
    WHERE IS_CURRENT = TRUE
);


DESC TABLE SANDBOX.CLEAN_SCH.CUSTOMER_ADDRESS;



INSERT INTO SANDBOX.CONSUMPTION_SCH.CUSTOMER_ADDRESS_DIM
(
    CUSTOMER_ADDRESS_HK,
    ADDRESS_ID,
    CUSTOMER_ID_FK,
    FLAT_NO,
    HOUSE_NO,
    FLOOR,
    BUILDING,
    LANDMARK,
    LOCALITY,
    CITY,
    STATE,
    PINCODE,
    COORDINATES,
    PRIMARY_FLAG,
    ADDRESS_TYPE,
    EFF_START_DATE,
    EFF_END_DATE,
    IS_CURRENT
)
SELECT
    ROW_NUMBER() OVER (ORDER BY ADDRESS_ID)
      + (SELECT COALESCE(MAX(CUSTOMER_ADDRESS_HK),0)
           FROM SANDBOX.CONSUMPTION_SCH.CUSTOMER_ADDRESS_DIM),
    ADDRESS_ID,
    CUSTOMER_ID_FK,
    FLAT_NO,
    HOUSE_NO,
    FLOOR,
    BUILDING,
    LANDMARK,
    LOCALITY,
    CITY,
    STATE,
    PINCODE,
    COORDINATES,
    PRIMARY_FLAG,
    ADDRESS_TYPE,
    CURRENT_TIMESTAMP(),
    NULL,
    TRUE
FROM SANDBOX.CLEAN_SCH.CUSTOMER_ADDRESS
WHERE ADDRESS_ID IN (6,7,8)
AND NOT EXISTS
(
    SELECT 1
    FROM SANDBOX.CONSUMPTION_SCH.CUSTOMER_ADDRESS_DIM D
    WHERE D.ADDRESS_ID = CUSTOMER_ADDRESS.ADDRESS_ID
      AND D.IS_CURRENT = TRUE
);



SELECT
    ADDRESS_ID,
    CUSTOMER_ID_FK,
    IS_CURRENT
FROM SANDBOX.CONSUMPTION_SCH.CUSTOMER_ADDRESS_DIM
WHERE ADDRESS_ID IN (6,7,8)
ORDER BY ADDRESS_ID;




INSERT INTO SANDBOX.CONSUMPTION_SCH.CUSTOMER_ADDRESS_DIM
(
    CUSTOMER_ADDRESS_HK,
    ADDRESS_ID,
    CUSTOMER_ID_FK,
    FLAT_NO,
    HOUSE_NO,
    FLOOR,
    BUILDING,
    LANDMARK,
    LOCALITY,
    CITY,
    STATE,
    PINCODE,
    COORDINATES,
    PRIMARY_FLAG,
    ADDRESS_TYPE,
    EFF_START_DATE,
    EFF_END_DATE,
    IS_CURRENT
)
SELECT
    ROW_NUMBER() OVER (ORDER BY S.ADDRESS_ID)
      + (SELECT COALESCE(MAX(CUSTOMER_ADDRESS_HK),0)
           FROM SANDBOX.CONSUMPTION_SCH.CUSTOMER_ADDRESS_DIM),
    S.ADDRESS_ID,
    S.CUSTOMER_ID_FK,
    S.FLAT_NO,
    S.HOUSE_NO,
    S.FLOOR,
    S.BUILDING,
    S.LANDMARK,
    S.LOCALITY,
    S.CITY,
    S.STATE,
    S.PINCODE,
    S.COORDINATES,
    S.PRIMARY_FLAG,
    S.ADDRESS_TYPE,
    CURRENT_TIMESTAMP(),
    NULL,
    TRUE
FROM SANDBOX.CLEAN_SCH.CUSTOMER_ADDRESS S
WHERE S.ADDRESS_ID IN (6,7,8)
AND NOT EXISTS
(
    SELECT 1
    FROM SANDBOX.CONSUMPTION_SCH.CUSTOMER_ADDRESS_DIM D
    WHERE D.ADDRESS_ID = S.ADDRESS_ID
      AND D.IS_CURRENT = TRUE
);




SELECT
    COUNT(*) AS TOTAL_ORDER_ITEMS,
    COUNT(DISTINCT OI.ORDER_ITEM_ID) AS UNIQUE_ORDER_ITEMS,
    SUM(CASE WHEN C.CUSTOMER_HK IS NULL THEN 1 ELSE 0 END) AS MISSING_CUSTOMERS,
    SUM(CASE WHEN CA.CUSTOMER_ADDRESS_HK IS NULL THEN 1 ELSE 0 END) AS MISSING_ADDRESSES
FROM SANDBOX.CLEAN_SCH.ORDER_ITEM OI
LEFT JOIN SANDBOX.CLEAN_SCH.ORDERS O
    ON OI.ORDER_ID = O.ORDER_ID
LEFT JOIN SANDBOX.CONSUMPTION_SCH.CUSTOMER_DIM C
    ON O.CUSTOMER_ID = C.CUSTOMER_ID
   AND C.IS_CURRENT = TRUE
LEFT JOIN SANDBOX.CONSUMPTION_SCH.CUSTOMER_ADDRESS_DIM CA
    ON O.ADDRESS_ID = CA.ADDRESS_ID
   AND CA.IS_CURRENT = TRUE;


DESC TABLE SANDBOX.CLEAN_SCH.DELIVERY;



SELECT
    COUNT(*) AS TOTAL_ROWS,
    COUNT(DISTINCT OI.ORDER_ITEM_ID) AS UNIQUE_ORDER_ITEMS,

    SUM(CASE WHEN C.CUSTOMER_HK IS NULL THEN 1 ELSE 0 END) AS MISSING_CUSTOMERS,

    SUM(CASE WHEN CA.CUSTOMER_ADDRESS_HK IS NULL THEN 1 ELSE 0 END) AS MISSING_ADDRESSES

FROM SANDBOX.CLEAN_SCH.ORDER_ITEM OI

LEFT JOIN SANDBOX.CLEAN_SCH.ORDERS O
    ON OI.ORDER_ID_FK = O.ORDER_ID

LEFT JOIN SANDBOX.CONSUMPTION_SCH.CUSTOMER_DIM C
    ON CAST(O.CUSTOMER_ID_FK AS VARCHAR) = C.CUSTOMER_ID
   AND C.IS_CURRENT = TRUE

LEFT JOIN SANDBOX.CLEAN_SCH.DELIVERY D
    ON O.ORDER_ID = D.ORDER_ID_FK

LEFT JOIN SANDBOX.CONSUMPTION_SCH.CUSTOMER_ADDRESS_DIM CA
    ON D.CUSTOMER_ADDRESS_ID_FK = CA.ADDRESS_ID
   AND CA.IS_CURRENT = TRUE;   


   DESC TABLE SANDBOX.CLEAN_SCH.ORDER_ITEM;


   DESC TABLE SANDBOX.CLEAN_SCH.ORDERS;


   SELECT
    O.ORDER_ID,
    O.CUSTOMER_ID_FK
FROM SANDBOX.CLEAN_SCH.ORDERS O
LEFT JOIN SANDBOX.CONSUMPTION_SCH.CUSTOMER_DIM C
    ON CAST(O.CUSTOMER_ID_FK AS VARCHAR) = C.CUSTOMER_ID
   AND C.IS_CURRENT = TRUE
WHERE C.CUSTOMER_HK IS NULL;



SELECT
    D.ORDER_ID_FK,
    D.CUSTOMER_ADDRESS_ID_FK
FROM SANDBOX.CLEAN_SCH.DELIVERY D
LEFT JOIN SANDBOX.CONSUMPTION_SCH.CUSTOMER_ADDRESS_DIM CA
    ON D.CUSTOMER_ADDRESS_ID_FK = CA.ADDRESS_ID
   AND CA.IS_CURRENT = TRUE
WHERE CA.CUSTOMER_ADDRESS_HK IS NULL;



SELECT
    OI.ORDER_ITEM_ID,
    OI.ORDER_ID_FK
FROM SANDBOX.CLEAN_SCH.ORDER_ITEM OI
LEFT JOIN SANDBOX.CLEAN_SCH.ORDERS O
    ON OI.ORDER_ID_FK = O.ORDER_ID
WHERE O.ORDER_ID IS NULL;


SELECT
    COUNT(*) TOTAL_ORDER_ITEMS
FROM SANDBOX.CLEAN_SCH.ORDER_ITEM;


SELECT
    COUNT(DISTINCT ORDER_ITEM_ID) UNIQUE_ORDER_ITEMS
FROM SANDBOX.CLEAN_SCH.ORDER_ITEM;


SELECT
    COUNT(*) AS MISSING_RESTAURANTS
FROM SANDBOX.CLEAN_SCH.ORDERS O
LEFT JOIN SANDBOX.CONSUMPTION_SCH.RESTAURANT_DIM R
    ON CAST(O.RESTAURANT_ID_FK AS VARCHAR) = R.RESTAURANT_ID
   AND R.IS_CURRENT = TRUE
WHERE R.RESTAURANT_HK IS NULL;



SELECT
    COUNT(*) AS MISSING_MENUS
FROM SANDBOX.CLEAN_SCH.ORDER_ITEM OI
LEFT JOIN SANDBOX.CONSUMPTION_SCH.MENU_DIM M
    ON CAST(OI.MENU_ID_FK AS VARCHAR) = M.MENU_ID
   AND M.IS_CURRENT = TRUE
WHERE M.MENU_HK IS NULL;

DESC TABLE SANDBOX.CONSUMPTION_SCH.MENU_DIM;


SELECT
    COUNT(*) AS MISSING_MENUS
FROM SANDBOX.CLEAN_SCH.ORDER_ITEM OI
LEFT JOIN SANDBOX.CONSUMPTION_SCH.MENU_DIM M
    ON OI.MENU_ID_FK = M.MENU_ID
   AND M.IS_CURRENT = TRUE
WHERE M.MENU_DIM_HK IS NULL;


DESC TABLE SANDBOX.CONSUMPTION_SCH.DELIVERY_AGENT_DIM;



SELECT
    COUNT(*) AS MISSING_DELIVERY_AGENTS
FROM SANDBOX.CLEAN_SCH.DELIVERY D
LEFT JOIN SANDBOX.CONSUMPTION_SCH.DELIVERY_AGENT_DIM DA
    ON D.DELIVERY_AGENT_ID_FK = DA.DELIVERY_AGENT_ID
   AND DA.IS_CURRENT = TRUE
WHERE DA.DELIVERY_AGENT_HK IS NULL;




SELECT
    OI.ORDER_ITEM_ID,
    OI.ORDER_ID_FK AS ORDER_ID,

    C.CUSTOMER_HK AS CUSTOMER_DIM_KEY,
    CA.CUSTOMER_ADDRESS_HK AS CUSTOMER_ADDRESS_DIM_KEY,
    R.RESTAURANT_HK AS RESTAURANT_DIM_KEY,
    RL.LOCATION_HK AS RESTAURANT_LOCATION_DIM_KEY,
    M.MENU_DIM_HK AS MENU_DIM_KEY,
    DA.DELIVERY_AGENT_HK AS DELIVERY_AGENT_DIM_KEY,

    DD.DATE_DIM_HK AS ORDER_DATE_DIM_KEY,

    OI.QUANTITY,
    OI.PRICE,
    OI.SUBTOTAL,

    D.DELIVERY_STATUS,
    D.ESTIMATED_TIME

FROM SANDBOX.CLEAN_SCH.ORDER_ITEM OI

LEFT JOIN SANDBOX.CLEAN_SCH.ORDERS O
    ON OI.ORDER_ID_FK = O.ORDER_ID

LEFT JOIN SANDBOX.CONSUMPTION_SCH.CUSTOMER_DIM C
    ON CAST(O.CUSTOMER_ID_FK AS VARCHAR) = C.CUSTOMER_ID
   AND C.IS_CURRENT = TRUE

LEFT JOIN SANDBOX.CLEAN_SCH.DELIVERY D
    ON O.ORDER_ID = D.ORDER_ID_FK

LEFT JOIN SANDBOX.CONSUMPTION_SCH.CUSTOMER_ADDRESS_DIM CA
    ON D.CUSTOMER_ADDRESS_ID_FK = CA.ADDRESS_ID
   AND CA.IS_CURRENT = TRUE

LEFT JOIN SANDBOX.CONSUMPTION_SCH.RESTAURANT_DIM R
    ON CAST(O.RESTAURANT_ID_FK AS VARCHAR) = R.RESTAURANT_ID
   AND R.IS_CURRENT = TRUE

LEFT JOIN SANDBOX.CONSUMPTION_SCH.MENU_DIM M
    ON OI.MENU_ID_FK = M.MENU_ID
   AND M.IS_CURRENT = TRUE

LEFT JOIN SANDBOX.CONSUMPTION_SCH.DELIVERY_AGENT_DIM DA
    ON D.DELIVERY_AGENT_ID_FK = DA.DELIVERY_AGENT_ID
   AND DA.IS_CURRENT = TRUE

LEFT JOIN SANDBOX.CONSUMPTION_SCH.RESTAURANT_LOCATION_DIM RL
    ON R.LOCATION_ID_FK = RL.LOCATION_ID
   AND RL.IS_CURRENT = TRUE

LEFT JOIN SANDBOX.CONSUMPTION_SCH.DATE_DIM DD
    ON CAST(O.ORDER_DATE AS DATE) = DD.FULL_DATE

ORDER BY OI.ORDER_ITEM_ID;



DESC TABLE SANDBOX.CONSUMPTION_SCH.RESTAURANT_LOCATION_DIM;



SELECT
    COUNT(*) AS MISSING_MENUS
FROM SANDBOX.CLEAN_SCH.ORDER_ITEM OI
LEFT JOIN SANDBOX.CONSUMPTION_SCH.MENU_DIM M
    ON OI.MENU_ID_FK = M.MENU_ID
   AND M.IS_CURRENT = TRUE
WHERE M.MENU_DIM_HK IS NULL;


DESC TABLE SANDBOX.CONSUMPTION_SCH.ORDER_ITEM_FACT;


DESC TABLE SANDBOX.CONSUMPTION_SCH.DATE_DIM;



SELECT COUNT(*)
FROM SANDBOX.CLEAN_SCH.ORDER_ITEM OI

LEFT JOIN SANDBOX.CLEAN_SCH.ORDERS O
    ON OI.ORDER_ID_FK = O.ORDER_ID

LEFT JOIN SANDBOX.CONSUMPTION_SCH.CUSTOMER_DIM C
    ON CAST(O.CUSTOMER_ID_FK AS VARCHAR) = C.CUSTOMER_ID
   AND C.IS_CURRENT = TRUE

LEFT JOIN SANDBOX.CLEAN_SCH.DELIVERY D
    ON O.ORDER_ID = D.ORDER_ID_FK

LEFT JOIN SANDBOX.CONSUMPTION_SCH.CUSTOMER_ADDRESS_DIM CA
    ON D.CUSTOMER_ADDRESS_ID_FK = CA.ADDRESS_ID
   AND CA.IS_CURRENT = TRUE

LEFT JOIN SANDBOX.CONSUMPTION_SCH.RESTAURANT_DIM R
    ON CAST(O.RESTAURANT_ID_FK AS VARCHAR) = R.RESTAURANT_ID
   AND R.IS_CURRENT = TRUE

LEFT JOIN SANDBOX.CONSUMPTION_SCH.MENU_DIM M
    ON OI.MENU_ID_FK = M.MENU_ID
   AND M.IS_CURRENT = TRUE

LEFT JOIN SANDBOX.CONSUMPTION_SCH.DELIVERY_AGENT_DIM DA
    ON D.DELIVERY_AGENT_ID_FK = DA.DELIVERY_AGENT_ID
   AND DA.IS_CURRENT = TRUE

LEFT JOIN SANDBOX.CONSUMPTION_SCH.RESTAURANT_LOCATION_DIM RL
    ON R.LOCATION_ID_FK = RL.LOCATION_ID
   AND RL.CURRENT_FLAG = TRUE

LEFT JOIN SANDBOX.CONSUMPTION_SCH.DATE_DIM DD
    ON CAST(O.ORDER_DATE AS DATE) = DD.CALENDAR_DATE;




INSERT INTO SANDBOX.CONSUMPTION_SCH.ORDER_ITEM_FACT
(
    ORDER_ITEM_ID,
    ORDER_ID,
    CUSTOMER_DIM_KEY,
    CUSTOMER_ADDRESS_DIM_KEY,
    RESTAURANT_DIM_KEY,
    RESTAURANT_LOCATION_DIM_KEY,
    MENU_DIM_KEY,
    DELIVERY_AGENT_DIM_KEY,
    ORDER_DATE_DIM_KEY,
    QUANTITY,
    PRICE,
    SUBTOTAL,
    DELIVERY_STATUS,
    ESTIMATED_TIME
)
SELECT
    OI.ORDER_ITEM_ID,
    OI.ORDER_ID_FK,

    C.CUSTOMER_HK,
    CA.CUSTOMER_ADDRESS_HK,
    R.RESTAURANT_HK,
    RL.RESTAURANT_LOCATION_HK,
    M.MENU_DIM_HK,
    DA.DELIVERY_AGENT_HK,
    DD.DATE_DIM_HK,

    OI.QUANTITY,
    OI.PRICE,
    OI.SUBTOTAL,

    D.DELIVERY_STATUS,
    D.ESTIMATED_TIME

FROM SANDBOX.CLEAN_SCH.ORDER_ITEM OI

LEFT JOIN SANDBOX.CLEAN_SCH.ORDERS O
    ON OI.ORDER_ID_FK = O.ORDER_ID

LEFT JOIN SANDBOX.CONSUMPTION_SCH.CUSTOMER_DIM C
    ON CAST(O.CUSTOMER_ID_FK AS VARCHAR) = C.CUSTOMER_ID
   AND C.IS_CURRENT = TRUE

LEFT JOIN SANDBOX.CLEAN_SCH.DELIVERY D
    ON O.ORDER_ID = D.ORDER_ID_FK

LEFT JOIN SANDBOX.CONSUMPTION_SCH.CUSTOMER_ADDRESS_DIM CA
    ON D.CUSTOMER_ADDRESS_ID_FK = CA.ADDRESS_ID
   AND CA.IS_CURRENT = TRUE

LEFT JOIN SANDBOX.CONSUMPTION_SCH.RESTAURANT_DIM R
    ON CAST(O.RESTAURANT_ID_FK AS VARCHAR) = R.RESTAURANT_ID
   AND R.IS_CURRENT = TRUE

LEFT JOIN SANDBOX.CONSUMPTION_SCH.MENU_DIM M
    ON OI.MENU_ID_FK = M.MENU_ID
   AND M.IS_CURRENT = TRUE

LEFT JOIN SANDBOX.CONSUMPTION_SCH.DELIVERY_AGENT_DIM DA
    ON D.DELIVERY_AGENT_ID_FK = DA.DELIVERY_AGENT_ID
   AND DA.IS_CURRENT = TRUE

LEFT JOIN SANDBOX.CONSUMPTION_SCH.RESTAURANT_LOCATION_DIM RL
    ON R.LOCATION_ID_FK = RL.LOCATION_ID
   AND RL.CURRENT_FLAG = TRUE

LEFT JOIN SANDBOX.CONSUMPTION_SCH.DATE_DIM DD
    ON CAST(O.ORDER_DATE AS DATE) = DD.CALENDAR_DATE;



SELECT COUNT(*)
FROM SANDBOX.CONSUMPTION_SCH.ORDER_ITEM_FACT;


SELECT
    COUNT(DISTINCT ORDER_ITEM_ID)
FROM SANDBOX.CONSUMPTION_SCH.ORDER_ITEM_FACT;


SELECT COUNT(*)
FROM SANDBOX.CONSUMPTION_SCH.VW_YEARLY_REVENUE_KPIS;




SELECT COUNT(*)
FROM SANDBOX.CONSUMPTION_SCH.VW_MONTHLY_REVENUE_KPIS;


SELECT COUNT(*)
FROM SANDBOX.CONSUMPTION_SCH.VW_MONTHLY_REVENUE_BY_RESTAURANT;

SELECT *
FROM SANDBOX.CONSUMPTION_SCH.VW_YEARLY_REVENUE_KPIS;


SELECT *
FROM SANDBOX.CONSUMPTION_SCH.VW_MONTHLY_REVENUE_KPIS
ORDER BY 1,2;
