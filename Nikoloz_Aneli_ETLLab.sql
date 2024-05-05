use warehouse STUDENT_WAREHOUSE;

use database STUDENT_NANELI;

create or replace schema STUDENT_NANELI.BRONZE;
create or replace schema STUDENT_NANELI.SILVER;
create or replace schema STUDENT_NANELI.GOLD;



/*-----------BRONZE-----------*/

use schema BRONZE;

create or replace stage STUDENT_NANELI.BRONZE.external_bucket
url = 'URL_TO_BUCKET'
credentials = (aws_key_id = 'AWS_KEY_ID', aws_secret_key = 'AWS_SECRET_KEY');


/*-------------------------------------------------------------
-------create tables for bronze and silver scheme -- START ----
--------------------------------------------------------------*/

/*----------------------BRONZE---------------------*/
-- table for molecules
create or replace table student_naneli.bronze.stg_molecules(
    json_data variant,
    filename varchar,
    amnd_user varchar default current_user(),
    amnd_date datetime default current_timestamp
);

-- table for batches
create or replace table student_naneli.bronze.stg_batches(
    json_data variant,
    filename varchar,
    amnd_user varchar default current_user(),
    amnd_date datetime default current_timestamp
);

-- table for projects
create or replace table student_naneli.bronze.stg_projects(
    id number,
    project_name varchar,
    filename varchar,
    amnd_user varchar default current_user(),
    amnd_date datetime default current_timestamp
);


/*----------------------SILVER---------------------*/
-- create silver table for molecules
create or replace table student_naneli.silver.molecules (
    seq_id int autoincrement start = 1 increment = 1,
    id number,
    class varchar,
    created_at datetime,
    modified_at datetime,
    name varchar,
    owner varchar,
    molecular_weight number,
    formula varchar,
    batch_id number,
    filename varchar,
    amnd_user varchar,
    amnd_date datetime
);

-- create silver table for molecules_projects
create or replace table student_naneli.silver.molecules_projects(
    molecule_id number,
    project_id number,
    filename varchar,
    amnd_user varchar,
    amnd_date datetime
);

-- create silver table for molecules_batches
create or replace table student_naneli.silver.molecules_batches(
    molecule_id number,
    batch_id number,
    filename varchar,
    amnd_user varchar,
    amnd_date datetime
);

-- create silver table for batches-- create silver table for molecules_projects
create or replace table student_naneli.silver.batches(
    seq_id int autoincrement start = 1 increment = 1,
    id number,
    class varchar,
    created_at datetime,
    modified_at datetime,
    name varchar,
    owner varchar,
    formula_weight number,
    filename varchar,
    amnd_user varchar,
    amnd_date datetime
);

-- create silver table for projects
create or replace table student_naneli.silver.projects(
    seq_id int autoincrement start = 1 increment = 1,
    id number,
    project_name varchar,
    comment varchar,
    filename varchar,
    amnd_user varchar,
    amnd_date datetime
);

/*-------END TABLES-------*/



/*-------------------------------------------------
------- Create Pipes for automation -- START ------
--------------------------------------------------*/

-- pipe for molecules
create or replace pipe student_naneli.bronze.stg_molecules_pipe auto_ingest=true as
  copy into student_naneli.bronze.stg_molecules(json_data, filename)
    from(
        select $1, metadata$filename
        from @external_bucket/data/molecules/
        )
file_format = (type = 'JSON' STRIP_OUTER_ARRAY = TRUE)
pattern='.*molecule.*json';

-- pipe for batches
create or replace pipe student_naneli.bronze.stg_batches_pipe auto_ingest=true as
  copy into student_naneli.bronze.stg_batches(json_data, filename)
    from (
        select $1, metadata$filename
        from @external_bucket/data/batches/
        )
file_format = (type = 'JSON' STRIP_OUTER_ARRAY = TRUE)
pattern='.*batches.*json';

-- pipe for projects
create or replace pipe student_naneli.bronze.stg_projects_pipe auto_ingest=true as
    copy into student_naneli.bronze.stg_projects(id, project_name, filename)
        from(
        select $1, $2, metadata$filename
        from @external_bucket/data/projects/
        )
    file_format = (type = 'CSV'  FIELD_DELIMITER = '^' skip_header = 1 field_optionally_enclosed_by='"')
    pattern = '.*csv';

/*-------END PIPES-------*/








/*-------------------------------------------------
------- Streams -- START --------------------------
--------------------------------------------------*/
-- stg_molecules table is divided with 3 streams -- stg_molecules, 
                                                 -- stg_molecules_projects 
                                                 -- stg_molecules_projects
create or replace stream student_naneli.bronze.stream_stg_molecules on table student_naneli.bronze.stg_molecules append_only=true;

create or replace stream student_naneli.bronze.stream_stg_molecules_projects on table student_naneli.bronze.stg_molecules append_only=true;

create or replace stream student_naneli.bronze.stream_stg_molecules_batches on table student_naneli.bronze.stg_molecules append_only=true;

create or replace stream student_naneli.bronze.stream_stg_batches on table student_naneli.bronze.stg_batches append_only = true;

create or replace stream student_naneli.bronze.stream_stg_projects on table student_naneli.bronze.stg_projects append_only = true;

/*-------END STREAMS-------*/



/* ------------------------------------------------
-------------- UDF to add comment -----------------
---------------------------------------------------*/
create or replace function student_naneli.bronze.add_comment(param_1 varchar)
    returns varchar
    language sql
    as $$
        param_1 || ' added by Nikoloz Aneli'
    $$;
/*--------------- END UDF --------------*/







/*-------------------------------------------------
------- Procedures -- START --------------------------
--------------------------------------------------*/

-- move_molecules
CREATE OR REPLACE PROCEDURE student_naneli.bronze.move_molecules()
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
BEGIN
    INSERT INTO student_naneli.silver.molecules (
        id,
        class,
        created_at,
        modified_at,
        name,
        owner,
        molecular_weight,
        formula,
        batch_id,
        filename,
        amnd_user,
        amnd_date
    )
    SELECT
        DISTINCT
            parse_json(json_data):id::number as id,
            parse_json(json_data):class::varchar as class,
            parse_json(json_data):created_at::datetime as created_ad,
            parse_json(json_data):modified_at::datetime as modified_at,
            parse_json(json_data):name::varchar as name,
            parse_json(json_data):owner::varchar as owner,
            parse_json(json_data):molecular_weight::number as molecular_weight,
            parse_json(json_data):formula::varchar as formula,
            parse_json(json_data):batches[0]:id::number as batch_id,
            filename,
            amnd_user,
            amnd_date,
    FROM student_naneli.bronze.stream_stg_molecules
    WHERE METADATA$ACTION = 'INSERT'
    QUALIFY RANK() OVER (PARTITION BY parse_json(json_data):id ORDER BY parse_json(json_data):modified_at DESC) = 1;
    RETURN 'done';
END;
$$;


-- move_molecules_projects
create or replace procedure student_naneli.bronze.move_molecules_projects()
returns varchar language SQL AS
$$
begin
    INSERT INTO student_naneli.silver.molecules_projects(
        molecule_id,
        project_id ,
        filename,
        amnd_user,
        amnd_date 
        )
    select
        distinct 
        parse_json(json_data):id::number as molecule_id,
        p.value:id::NUMBER AS project_id,
        t.filename,
        t.amnd_user,
        t.amnd_date,
    from student_naneli.bronze.stream_stg_molecules_projects t,
    LATERAL FLATTEN(INPUT => parse_json(json_data):projects) p
    where metadata$action = 'INSERT'
    QUALIFY RANK() OVER (PARTITION BY parse_json(json_data):id, p.value:id ORDER BY parse_json(json_data):modified_at DESC) = 1;
    return 'Done';    
end;
$$;


-- move_molecules_batches
create or replace procedure student_naneli.bronze.move_molecules_batches()
returns varchar language SQL AS
$$
begin
    INSERT INTO student_naneli.silver.molecules_batches(
        molecule_id,
        batch_id,
        filename,
        amnd_user,
        amnd_date 
        )
    select
        distinct 
        parse_json(json_data):id::number as molecule_id,
        parse_json(json_data):batches[0]:id::number as batch_id,
        filename,
        amnd_user,
        amnd_date,
    from student_naneli.bronze.stream_stg_molecules_batches
    where metadata$action = 'INSERT'
    QUALIFY RANK() OVER (PARTITION BY parse_json(json_data):batches[0]:id ORDER BY parse_json(json_data):modified_at DESC) = 1;
    return 'Done';    
end;
$$;


-- move_batches
create or replace procedure student_naneli.bronze.move_batches()
returns varchar language sql as
$$ 
begin
    insert into student_naneli.silver.batches(
        id,
        class,
        created_at,
        modified_at,
        name,
        owner,
        formula_weight,
        filename,
        amnd_user,
        amnd_date
    )
    select 
        distinct parse_json(json_data):id::number as id,
        parse_json(json_data):class::varchar as class,
        parse_json(json_data):created_at::datetime as created_at,
        parse_json(json_data):modified_at::datetime as modified_at,
        parse_json(json_data):name::varchar as name,
        parse_json(json_data):owner::varchar as owner,
        parse_json(json_data):formula_weight::number as formula_weight,
        filename,
        amnd_user,
        amnd_date
    from student_naneli.bronze.stream_stg_batches
    where metadata$action = 'INSERT'
    QUALIFY RANK() OVER (PARTITION BY parse_json(json_data):id ORDER BY parse_json(json_data):modified_at DESC) = 1;
    return 'Done';
end;
$$;


-- move_projects
create or replace procedure student_naneli.bronze.move_projects()
returns varchar language sql as
$$
begin
    insert into student_naneli.silver.projects(
        id, project_name, comment, filename, amnd_user, amnd_date
        )
    select 
        id,
        project_name,
        student_naneli.bronze.add_comment(project_name),
        filename,
        amnd_user,
        amnd_date
    from student_naneli.bronze.stream_stg_projects
    where metadata$action = 'INSERT';
    return 'Done';
end;
$$;

/*------- END PROCEDURES ------- */


-- !IMPORTANT! - to move data from Bronze to Silver manually there are commands below 

call student_naneli.bronze.move_molecules();
call student_naneli.bronze.move_molecules_projects();
call student_naneli.bronze.move_molecules_batches();
call student_naneli.bronze.move_batches();
call student_naneli.bronze.move_projects();


/*-------------------------------------------------
----- TASKS to automate process (once a day) ------
--------------------------------------------------*/
-- move molecules
create task student_naneli.bronze.task_to_move_molecules
  warehouse = student_warehouse
  schedule = 'USING CRON 0 0 * * * UTC' 
when
  system$stream_has_data('student_naneli.bronze.stream_stg_molecules') 
as
  call student_naneli.bronze.move_molecules();

  
-- move molecules_projects
create task student_naneli.bronze.task_to_move_molecules_projects
  warehouse = student_warehouse
  schedule = 'USING CRON 0 0 * * * UTC' 
when
  system$stream_has_data('student_naneli.bronze.stream_stg_molecules_projects') 
as
  call student_naneli.bronze.move_molecules_projects();


-- move molecules_batches
create task student_naneli.bronze.task_to_move_molecules_batches
  warehouse = student_warehouse
  schedule = 'USING CRON 0 0 * * * UTC' 
when
  system$stream_has_data('student_naneli.bronze.stream_stg_molecules_batches') 
as
  call student_naneli.bronze.move_molecules_batches();


-- move batches
create task student_naneli.bronze.task_to_move_batches
  warehouse = student_warehouse
  schedule = 'USING CRON 0 0 * * * UTC' 
when
  system$stream_has_data('student_naneli.bronze.stream_stg_batches') 
as
  call student_naneli.bronze.move_batches();

  
-- move projects
create task student_naneli.bronze.task_to_move_projects
  warehouse = student_warehouse
  schedule = 'USING CRON 0 0 * * * UTC' 
when
  system$stream_has_data('student_naneli.bronze.stream_stg_projects') 
as
  call student_naneli.bronze.move_projects();

/*--------------- END TASKS --------------*/




/*-----------------------------------------------------------------------*/
/*--------------- GOLD SCHEMA - view of joining all tables --------------*/
/*-----------------------------------------------------------------------*/
  create or replace view STUDENT_NANELI.GOLD.v_molecules as
    select 
        m.id as molecule_id,
        m.class as molecule_class,
        m.owner as molecule_owner,
        m.molecular_weight as molecular_weight,
        m.formula as formula,
        p.comment as comment,
        b.name as batch_name
    from 
        student_naneli.silver.molecules m 
        join 
        student_naneli.silver.molecules_projects mp
        on m.id =  mp.molecule_id
        join 
        student_naneli.silver.projects p 
        on mp.project_id = p.id
        join 
        student_naneli.silver.molecules_batches mb
        on m.id = mb.molecule_id
        join 
        student_naneli.silver.batches b
        on mb.batch_id = b.id;
        
/*------------------------------ END VIEW ----------------------------------*/


-- Test the results
select * from gold.v_molecules;




-- tools to reset pipes
truncate table stg_molecules;
alter pipe stg_molecules_pipe refresh;


truncate table stg_batches;
alter pipe stg_batches_pipe refresh;


truncate table stg_projects;
alter pipe stg_projects_pipe refresh;






