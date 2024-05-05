# Development of a data warehouse and a data loading process.

##### JSON files are:

    Molecules: defines the list of molecules with names, their synonyms, related projects, owner, formula and batches. In general, molecule is a formula.
    Batches: Contains the list of batches. Batch is the molecule physical representation.

##### CSV file:

     Projects: contains all the project within the Company.

######  Solution requirement:
  * DB: Snowflake
  * Cloud provider: Amazon
  * JSON data structure as present in the example files.

  * S3 bucket: 'URL'


    AWS_KEY_ID = "AWS_KEY_ID"
    AWS_SECRET_KEY = "AWS_SECRET_KEY"
  * Folders in the S3 bucket:


      /data
        /batches
        /molecules
        /projects
  * Files(Name starts with the entity type and could have postfix.):


      batches*.json
      molecules*.json
      projects*.csv



### Requirements:

***High level: Load and transform files data. Workflow: S3 bucket => Bronze schema => Silver schema => Gold schema***
 

#### BRONZE schema:



* SF stage with name: external_bucket
* 3 tables: stg_molecules, stg_batches, stg_projects.

      - Columns for the JSON files:

              json_data variant
             filename varchar (source data filename)
             amnd_user varchar (user who uploaded the data)
             amnd_date datetime (date-time when the data was uploaded)

      - Columns for the project file:
                id number
                project_name varchar
                filename varchar
                amnd_user varchar
                amnd_date datetime
  
* Pipelines to load the data automatically.


* Streams to capture the changes on the bronze schema tables.


* Procedures for copying data from bronze to silver.


* Tasks for continuous ELT. Scheduled interval - once a day.


* user-defined function to add comment


#### SILVER schema:

* 7 tables:

        * molecules(seq_id,molecule_id,class,created_at,modified_at,name,owner,molecular_weight,formula,filename,amnd_user,amnd_date)
        * molecules_projects(seq_id,molecule_id,project_id,filename,amnd_user,amnd_date)
        * molecules_batches(seq_id,molecule_id,batch_id,filename,amnd_user,amnd_date)
        * batches(seq_id,id,class,created_at,modified_at,name,owner,formula_weight,filename,amnd_user,amnd_date)
        * projects(seq_id,id,project_name,comment,filename,amnd_user,amnd_date)
  
  
* Molecules files data goes into 3 tables: molecules, molecules_projects, molecules_batches.


* seq_id: Number type incremental column.


* filename: source filename.


* amnd_user(varchar) column with SF session user(Who copied the data into the staging table.).


* amnd_date(datetime) column with SF session timestamp(When row was copied into the staging table.).


* comment: add_comment(project_name) - Function is described above.
       


#### GOLD schema:

* View  v_molecules:
  * Purpose: joins all silver tables.
  
  * columns:
                
  
      molecule_id (lab.silver.molecules.id)
      molecule_class (lab.silver.molecules.class)
      molecule_owner (lab.silver.molecules.owner)
      molecule_molecular_weight (lab.silver.molecules.molecular_weight)
      molecule_formula (lab.silver.molecules.formula)
      project_comment (lab.silver.projects.comment)
      batch_name (lab.silver.batches.name)
