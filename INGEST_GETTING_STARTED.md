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
- l_orderkey
- l_linenumber
```

#### Create an external table:
```shell
$ firebolt table create-external \ 
      --file lineitem_table.yaml \
      --s3-url 's3://firebolt-publishing-public/samples/tpc-h/parquet/lineitem/' 

External table (ex_lineitem) was successfully created                           
```
External tables automatically get an `ex_` prefix. 

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
$ firebolt ingest --external-table-name ex_lineitem \
                  --fact-table-name lineitem \
                  --mode overwrite
                  
Ingestion from 'ex_lineitem' to 'lineitem' was successful.
```

After the ingestion, `firebolt-cli` validates the ingestion. In case of data discrepancy between external and fact tables after the ingestion, the cli outputs an error message and returns non-zero exit code.  