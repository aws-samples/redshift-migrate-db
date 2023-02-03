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

## Security

See [CONTRIBUTING](CONTRIBUTING.md#security-issue-notifications) for more information.

## License

This library is licensed under the MIT-0 License. See the LICENSE file.
