------------
-- STREAMS
------------
-- A stream object records data manipulation language (DML) changes made to tables, 
--     including inserts, updates, and deletes, as well as metadata about each change, 
--     so that actions can be taken using the changed data. This process is referred to 
--     as change data capture (CDC). This topic introduces key concepts for change data
--     capture using streams.
-- When you query a stream and consume (i.e., process or record) the changes, 
--    the stream marks those changes as "processed". 
-- After the changes are consumed and processed, they are considered "used" and may be removed from the stream, 
--    especially once the retention period elapses.
--
-- Other Resources:
-- CDC in Snowflake - https://www.clearpeaks.com/implementing-change-data-capture-cdc-in-snowflake/
-- Examples - https://docs.snowflake.com/en/user-guide/streams-examples
-- Quick Start - Getting Started with Streams & Tasks
--     https://quickstarts.snowflake.com/guide/getting_started_with_streams_and_tasks/index.html?index=..%2F..index#0

-- Below is a simple example to show how steams can be used to capture
--    updates in the STAGE schema and copy them to the PROD schem


create or replace database demo_streams_tasks;
create schema stage;
create schema prod;
use database demo_streams_tasks;


-- Create the table in STAGE schema
--
CREATE or replace TABLE stage.students (
    student_id INT ,
    name STRING,
    major STRING,
    last_update timestamp
);

-- Create the stream
-- 
create or  replace  stream stage.students_stream on table stage.students;

-- Verify there is nothing in the steam
--
select * from stage.students_stream;


-- Insert five records into stage.students
--
INSERT INTO stage.students (student_id, name, major, last_update) VALUES
(1, 'Alice Johnson', 'Computer Science', current_timestamp),
(2, 'Bob Smith', 'Mathematics', current_timestamp),
(3, 'Charlie Brown', 'Biology', current_timestamp),
(4, 'Diana Prince', 'Psychology', current_timestamp),
(5, 'Ethan Hunt', 'Engineering', current_timestamp);

-- see the 5 student records
--
select * from stage.students;

-- see the inserts in the stream;
--
select * from stage.students_stream;



-- Create the destination table in PROD schema
--
CREATE or replace TABLE prod.students (
    student_id INT ,
    name STRING,
    major STRING,
    last_update timestamp
);

-- no records in our table yet
--
select * from prod.students;


-- see the five records are still in our steam
--
select * from stage.students_stream;


-- Use the stream to merge changes into PROD
-- The SQL statement below could become a scheduled task and 
--     could be run at a specified interval, time or could be dependend on another task.
--
-- Stream fields:
--  1) METADATA$ACTION - Indicates the DML operation (INSERT, DELETE) recorded. 
--  2) METADATA$ISUPDATE -  Indicates whether the operation was part of an UPDATE statement. 
--     Updates to rows in the source object are represented as a pair of DELETE and INSERT records 
--     in the stream with a metadata column METADATA$ISUPDATE values set to TRUE. 
--  3) METADATA$ROW_ID - Specifies the unique and immutable ID for the row, 
--    which can be used to track changes to specific rows over time.
--
MERGE INTO prod.students p using stage.students_stream s on p.student_id = s.student_id
WHEN MATCHED AND metadata$action = 'DELETE'and metadata$isupdate = 'FALSE'
    THEN DELETE
    -- logic has deleting the record, your logic has update row to N
WHEN MATCHED AND metadata$action = 'INSERT' and metadata$isupdate = 'TRUE'
    THEN UPDATE SET
    p.name = s.name, 
    p.major = s.major,
    p.last_update = s.last_update
WHEN MATCHED AND metadata$action = 'INSERT' and metadata$isupdate = 'FALSE'
    THEN UPDATE SET
    p.name = s.name,
    p.major = s.major,
    p.last_update = s.last_update   
WHEN NOT MATCHED and metadata$action = 'INSERT' and metadata$isupdate = 'FALSE'
    THEN INSERT 
(student_id, name, major, last_update) VALUES (s.student_id, s.name, s.major, s.last_update);


-- Verify PROD now has five records
--
select * from prod.students;
 

-- add another record to STAGE
--
INSERT INTO stage.students (student_id, name, major, last_update) VALUES
(6, 'Andy James', 'Engineering', current_timestamp);


-- See our new record is in the stream
--  other records have been removed from the stream because they were processed
select * from stage.students_stream;


-- Merge our new record into PROD
--
MERGE INTO prod.students p using stage.students_stream s on p.student_id = s.student_id
WHEN MATCHED AND metadata$action = 'DELETE'and metadata$isupdate = 'FALSE'
    THEN DELETE
WHEN MATCHED AND metadata$action = 'INSERT' and metadata$isupdate = 'TRUE'
    THEN UPDATE SET
    p.name = s.name,
    p.major = s.major,
    p.last_update = s.last_update
WHEN MATCHED AND metadata$action = 'INSERT' and metadata$isupdate = 'FALSE'
    THEN UPDATE SET
    p.name = s.name,
    p.major = s.major,
    p.last_update = s.last_update   
WHEN NOT MATCHED and metadata$action = 'INSERT' and metadata$isupdate = 'FALSE'
    THEN INSERT 
(student_id, name, major, last_update) VALUES (s.student_id, s.name, s.major, s.last_update);


-- Now we have six records in PROD
--
select * from prod.students;



-- Update an existing record in stage.students
-- Change Major of student_id =1 from Computer Science to Business
--
Update stage.students set major = 'Business', last_update = current_timestamp where student_id = 1;

 


-- See the changed record in the stream
--  An update appears as an INSERT and DELETE. The update deletes the existing record and adds the new record
select * from stage.students_stream;

-- Merge our new record into PROD
--
MERGE INTO prod.students p using stage.students_stream s on p.student_id = s.student_id
WHEN MATCHED AND metadata$action = 'DELETE'and metadata$isupdate = 'FALSE'
    THEN DELETE   
WHEN MATCHED AND metadata$action = 'INSERT' and metadata$isupdate = 'TRUE'
    THEN UPDATE SET
    p.name = s.name,
    p.major = s.major,
    p.last_update = s.last_update
WHEN MATCHED AND metadata$action = 'INSERT' and metadata$isupdate = 'FALSE'
    THEN UPDATE SET
    p.name = s.name,
    p.major = s.major,
    p.last_update = s.last_update   
WHEN NOT MATCHED and metadata$action = 'INSERT' and metadata$isupdate = 'FALSE'
    THEN INSERT 
(student_id, name, major, last_update) VALUES (s.student_id, s.name, s.major, s.last_update);


-- See student_id 1 now has a Major of business
--
select * from prod.students;


-- Insert the same record id as an exsiting record
-- In this scenario Charlie Brown switched his major from Biology to Math
--
INSERT INTO stage.students (student_id, name, major, last_update) VALUES
(3, 'Charlie Brown', 'Math', current_timestamp);


-- See the changed record in the stream
--   Note the METADATA$ACTION = INSERT and METADATA$ISUPDATE = FALSE
--   Since FAU will be doing full reloads this is more like their scenario
select * from stage.students_stream;


-- Merge the changes, current logic means this will add another record, this is not what we want
-- The insert is not allowed, no error, no change of major
-- Had to add additional logic for - MATCHED AND metadata$action = 'INSERT' and metadata$isupdate = 'FALSE'
MERGE INTO prod.students p using stage.students_stream s on p.student_id = s.student_id
WHEN MATCHED AND metadata$action = 'DELETE'and metadata$isupdate = 'FALSE'
    THEN DELETE
WHEN MATCHED AND metadata$action = 'INSERT' and metadata$isupdate = 'TRUE'
    THEN UPDATE SET
    p.name = s.name,
    p.major = s.major,
    p.last_update = s.last_update
WHEN MATCHED AND metadata$action = 'INSERT' and metadata$isupdate = 'FALSE'
    THEN UPDATE SET
    p.name = s.name,
    p.major = s.major,
    p.last_update = s.last_update   
WHEN NOT MATCHED and metadata$action = 'INSERT' and metadata$isupdate = 'FALSE'
    THEN INSERT 
(student_id, name, major, last_update) VALUES (s.student_id, s.name, s.major, s.last_update);
-- See student_id 1 now has a Major of business
--
select * from prod.students;


-- We effectively leveraging the Steam now we want to automat the updates.
-- Create a scheduled task to automate
-- Task will check to see if there is data in the stream DEMO_STREAMS.STAGE.STUDENTS_STREAM
--    if there is no data the taks will not run
-- Tasks can be scheuled to run at an interval, fixed time or via cron
 
create or replace task demo_streams_tasks.stage.load_stage_to_prod
-- can specify a dedicate warehosue, below us using serverless task
USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL'
-- SCHEDULE exaples - uncomment the one you want to use
-- every minute example
 SCHEDULE = '5 minute'
-- cron example, timezone can be UTC or other local timezones
--    Below runs at 9:15AM EST
-- SCHEDULE = 'USING CRON 15 9 * * * America/New_York'
COMMENT = 'TEST'
 when
 -- check stream for data, below is a stream example
    SYSTEM$STREAM_HAS_DATA('DEMO_STREAMS_TASKS.STAGE.STUDENTS_STREAM') 
as
MERGE INTO prod.students p using stage.students_stream s on p.student_id = s.student_id
WHEN MATCHED AND metadata$action = 'DELETE'and metadata$isupdate = 'FALSE'
    THEN DELETE
WHEN MATCHED AND metadata$action = 'INSERT' and metadata$isupdate = 'TRUE'
    THEN UPDATE SET
    p.name = s.name,
    p.major = s.major,
    p.last_update = s.last_update
WHEN MATCHED AND metadata$action = 'INSERT' and metadata$isupdate = 'FALSE'
    THEN UPDATE SET
    p.name = s.name,
    p.major = s.major,
    p.last_update = s.last_update   
WHEN NOT MATCHED and metadata$action = 'INSERT' and metadata$isupdate = 'FALSE'
    THEN INSERT 
(student_id, name, major, last_update) VALUES (s.student_id, s.name, s.major, s.last_update);


-- Task has been created, but must be started
alter task demo_streams_tasks.stage.load_stage_to_prod RESUME;


-- Check the status of the task
-- Fields to note:
--    Schedule 
--    State 
--    Definition
--    Condition
show tasks like 'load_stage_to_prod' IN DEMO_STREAMS_TASKS.STAGE;
show tasks like 'LOAD_STAGE_TO_PROD' IN DEMO_STREAMS_TASKS.STAGE;;
show tasks  IN DEMO_STREAMS_TASKS.STAGE;
describe task DEMO_STREAMS_TASKS.STAGE.LOAD_STAGE_TO_PROD;






-- add a record and test our Task
INSERT INTO stage.students (student_id, name, major, last_update) VALUES
(10, 'Cher Brown', 'Math', current_timestamp);

INSERT INTO stage.students (student_id, name, major, last_update) VALUES
(11, 'Arnold James', 'Math', current_timestamp);

INSERT INTO stage.students (student_id, name, major, last_update) VALUES
(12, 'Amy Mitchell', 'Math', current_timestamp);



--See if there is data in the stream
--    Returns TRUE  / FALSE
SELECT SYSTEM$STREAM_HAS_DATA('stage.students_stream');
SELECT SYSTEM$STREAM_HAS_DATA('STAGE.STUDENTS_STREAM');
SELECT SYSTEM$STREAM_HAS_DATA('DEMO_STREAMS_TASKS.STAGE.STUDENTS_STREAM');
-- See record level details in the stream
select * from stage.students_stream;


-- See if the task has moved the new records to PROD 
select * from prod.students order by last_update desc;


-- To stop the task:
alter task demo_streams.stage.load_stage_to_prod SUSPEND;

-- Monitor task spend / usage, adjust dates a appropriate
-- Note in monitoring there are two tabs
--    1) Task Graph Runs - shows just the task that ran
--    2) Task Run - show a task that did not pass the Conditional Expression, thus did not run no CPU consumed.
select *
  from table(snowflake.information_schema.serverless_task_history(
    date_range_start=>'2024-12-17 00:00:00.000',
    date_range_end=>'2024-12-17 20:00:00.000'));


-- Clean up
drop  database demo_streams_tasks;
