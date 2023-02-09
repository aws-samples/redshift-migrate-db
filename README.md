# redshift-migrate-db
Migrate a single database within a Redshift cluster to another cluster via Data Sharing.

## Purpose
Migrate a single database from one cluster to another using data sharing. 

## Prerequisites
1. An EC2 instance running Amazon Linux or CentOS with access to both the Source and Target clusters on port 5439.
2. Access to the EC2 instance via SSH.

## Linux Setup
1. Ensure you have the PostgreSQL client installed.

`sudo yum install postgresql.x86_64 -y`

2. Configure the `.pgpass` file so you can connect to the Source and Target clusters without being prompted for a password.

```
echo "Source"
echo "source.account.us-east-2.redshift.amazonaws.com:5439:*:awsuser:P@ssword1" > ~/.pgpass
echo "Target"
echo "target.account.us-east-2.redshift-serverless.amazonaws.com:5439:*:awsuser:P@ssword1" >> ~/.pgpass
echo "Security"
chmod 600 ~/.pgpass
```

In the above example, the awsuser has the password P@ssword1 for both the Source and Target clusters. Also be sure to change the cluster entries from the example to your actual cluster endpoints.  More information here on the .pgpass file: https://www.postgresql.org/docs/current/libpq-pgpass.html


3. Install the git client.

`sudo yum install git -y`

4. Clone this repository.

```
git clone https://github.com/aws-samples/redshift-migrate-db.git
cd redshift-migrate-db
```

## Script setup
1. cp `config.sh.example` to `config.sh`

2. Edit `config.sh` and make changes for your Redshift Source and Target clusters.

3. All schemas found in the Source cluster will be migrated to the Target cluster except for those exluded in the config.sh file. Edit the `EXCLUDED_SCHEMAS` variable to add additional schemas.

4. Most steps in the migration are executed in parallel and the level of parallelism is handled by the `LOAD_THREADS` variable in `config.sh`. Testing has shown the default has worked well.

5. This is automatic retry logic in the scripts in case of a failure. This can be caused by a table that is attempted to be created with a reference to a table that has not yet been created. Or a view that references another view that has not yet been created. The number of retries is based on the `RETRY` variable in the `config.sh` file. Testing has shown the default has worked well.

## Script execution
You can run the 0*.sh scripts one at a time like this:

`./01_create_users_groups.sh`

or you can use the `migrate.sh` script to run all of the scripts like this:

`./migrate.sh`

For larger migrations, you may want to run this in the background like this:

`nohup ./migrate.sh > migrate.log 2>&1 &`

You can monitor the progress by tailing the migrate.log file like this:

`tail -f migrate.log`


### Script Detail
**01_create_users_groups.sh** migrates existing users and groups to the target database. It will also add users to the groups. If the user doesn't exist in the target cluster, a default password is used in the target cluster and the password will be set to be expired. As of now, Roles are out of scope for this utility.

**02_migrate_ddl.sh** migrates schemas, tables (with primary keys), foreign keys, functions, and procedures to the target database. All functions have retry logic except for create schema. This ensures objects with dependencies eventually get created.

The create_table logic is robust and uses the following logic:

```
Does target table exist? 
 ├── Yes
 │   └── Does target table have an identity column?
 │       └── Yes
 │           ├── Does target table have data in it?
 │           │   ├── No
 │           │   │   └── Does source table have data in it?
 │           │   │       └── Yes -> Get max values from identity columns in source and target. Are seed values diffent?
 │           │   │           ├── Yes -> Recreate target table with new seed and use default identity instead of identity.
 │           │   │           └── No -> Do nothing
 │           │   └── Yes -> Do nothing
 │           └── No -> Do nothing
 └── No
     └── Does source table have an identity column? 
         ├── Yes
         │   └── Does source table have data in it?
         │       ├── Yes -> Get max value from identity column in source, create table in target with max + 1 as seed and default identity instead of identity.
         │       └── No -> Use 1 as seed and create table with default identity instead of identity.
         └── No -> Create table in target with source DDL.
```

**03_migrate_permissions.sh** migrates permissions from the source to the target. These include 
- schema, table, function, and procedure ownership
- grants on schemas, tables, functions, and procedures to users
- grants on schemas, tables, functions, and procedures to groups
- default permissions to schemas to users, schemas to groups, users to users, and users to groups
Role permissions are not included in this utility.

**04_setup_datasharing.sh** creates the datashare in the source, add schemas to the datashare, grants permission to the datashare in the target cluster, and creates the external schemas. Testing for multi-region and multi-account has not been performed yet.

**05_migrate_data.sh** migrates data from the source to the target. It also migrates materialized views. There is retry logic here to handle dependencies.

**06_migrate_views.sh** migrates views from the source to the target. There is retry logic here to handle dependencies. After views are created, the ownership and grants are performed on the views.

**config.sh.example** This should be copied to `config.sh` to run the scripts. It has the important variables you need to update for the migration.

**common.sh** A script that has commonly used functions by the scripts.

**get_table_ddl.sh** Used by migrate DDL script to get the table DDL.

**migrate.sh** A script that simplifies executing all of the scripts. 

## Security

See [CONTRIBUTING](CONTRIBUTING.md#security-issue-notifications) for more information.

## License

This library is licensed under the MIT-0 License. See the LICENSE file.
