# Ingest with firebolt-cli 
> **NOTE:**  The ingestion functionality is currently in beta testing.

This is a getting started tutorial, how to use firebolt-cli for ingestion. It is based upon Firebolt [Getting started tutorial](https://docs.firebolt.io/getting-started.html). And it will teach you to create external and fact tables using `firebolt-cli` and also ingest the data from one to another.

### Prerequisites:
Before starting, you must have:
- `firebolt-cli` installed and configured.
- Database in `us-east-1` region (for this tutorial, as the bucket is in `us-east-1`). 
- General-purpose engine attached to the database and running (recommended `B2` instance type).

## Create an external and fact tables
First you have to create a YAML file `lineitem_table.yaml`. The purpose of this file is to define both external and internal tables:
```yaml
table_name: lineitem
columns:
- name: l_orderkey
  alias: id
  type: LONG
- name: l_partkey
  type: LONG
- name: l_suppkey
  type: LONG
- name: l_linenumber
  type: INT
- name: l_quantity
  type: LONG
- name: l_extendedprice
  type: LONG
- name: l_discount
  type: LONG
- name: l_tax
  type: LONG
- name: l_returnflag
  type: TEXT
- name: l_linestatus
  type: TEXT
- name: l_shipdate
  type: TEXT
- name: l_commitdate
  type: TEXT
- name: l_receiptdate
  type: TEXT
- name: l_shipinstruct
  type: TEXT
- name: l_shipmode
  type: TEXT
- name: l_comment
  type: TEXT
file_type: PARQUET
object_pattern:
- '*.parquet'
primary_index:
- id
- l_linenumber
```

Note, you can additionally specify an alias for a column. In this case, the column in the internal table will be named after the alias. 
Specifying an alias is optional, unless you are using parquet grouping and referring to the columns as e.g. "time.member". 

#### Create an external table:
```shell
$ firebolt table create-external \ 
      --file lineitem_table.yaml \
      --s3-url 's3://firebolt-publishing-public/samples/tpc-h/parquet/lineitem/' 

External table (ex_lineitem) was successfully created                           
```
External tables automatically get an `ex_` prefix.

If s3 bucket is private, you can pass credentials by setting corresponding environment variables:
- Either `FIREBOLT_AWS_KEY_ID` and `FIREBOLT_AWS_SECRET_KEY` 
- Or `FIREBOLT_AWS_ROLE_ARN`, in this case you can also specify `FIREBOLT_AWS_ROLE_EXTERNAL_ID` environment variable  


#### Create fact table:
```shell
$ firebolt table create-fact --file lineitem_table.yaml \
                             --add-file-metadata

Fact table (lineitem) was successfully created
```
The `--add-file-metadata` flag is optional. It adds `source_file_name` and `source_file_timestamp` columns to your fact table. Having these columns will enable ingestion script to also work in `append` mode.  

### Ingestion
Now, you are ready to do the ingestion.
```shell
$ firebolt ingest --file lineitem_table.yaml \
                  --mode overwrite
                  
Ingestion from 'ex_lineitem' to 'lineitem' was successful.
```

After the ingestion, `firebolt-cli` validates the ingestion. In case of data discrepancy between external and fact tables after the ingestion, the cli outputs an error message and returns non-zero exit code.

Ingestion could be done in two different modes:
- overwrite - fully recreates the internal table from external (slow)
- append - ingest only the newest files from the external table (fast). Uses `source_file_name` and `source_file_timestamp` meta-columns to figure out the missing information. This mode doesn't guarantee correct ingestion.
If some data discrepancy will be found, cli will output a warning and return a non-zero exit code. In this case you will have to recreate the table by using overwrite mode.

For better control of the ingestion you can set following flags, which will be executed as a set statement:
- firebolt_dont_wait_for_upload_to_s3: Don't wait for upload part to S3 on insert query finish.
- advanced_mode: execute set advanced_mode=1
- use_short_column_path_parquet: Use short parquet column path (skipping repeated nodes and their child node).
