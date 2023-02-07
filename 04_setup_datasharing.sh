#!/bin/bash
set -e

source ${PWD}/config.sh

create_datashare()
{
	count=$(psql -h $SOURCE_PGHOST -p $SOURCE_PGPORT -d $SOURCE_PGDATABASE -U $SOURCE_PGUSER -t -A -c "SELECT COUNT(*) FROM SVV_DATASHARES WHERE share_name = '${SOURCE_SHARE_NAME}'")
	if [ "${count}" -eq "0" ]; then
		psql -h $SOURCE_PGHOST -p $SOURCE_PGPORT -d $SOURCE_PGDATABASE -U $SOURCE_PGUSER -t -A -c "CREATE DATASHARE ${SOURCE_SHARE_NAME} SET PUBLICACCESSIBLE TRUE;" -e
	else
		echo "INFO: Datashare ${SOURCE_SHARE_NAME} alredy exists."
	fi
}
add_schemas()
{
	prefix="add_schema"
	i="0"
	obj_count=$(psql -h $SOURCE_PGHOST -p $SOURCE_PGPORT -d $SOURCE_PGDATABASE -U $SOURCE_PGUSER -t -A -c "SELECT COUNT(*) FROM svv_all_schemas WHERE database_name = current_database() AND schema_type = 'local' AND schema_name NOT IN ${EXCLUDED_SCHEMAS}")
	echo "INFO: ${prefix}:creating ${obj_count}"
	for schema_name in $(psql -h $SOURCE_PGHOST -p $SOURCE_PGPORT -d $SOURCE_PGDATABASE -U $SOURCE_PGUSER -t -A -c "SELECT schema_name FROM svv_all_schemas WHERE database_name = current_database() AND schema_type = 'local' AND schema_name NOT IN ${EXCLUDED_SCHEMAS} ORDER BY schema_name"); do
		count=$(psql -h $SOURCE_PGHOST -p $SOURCE_PGPORT -d $SOURCE_PGDATABASE -U $SOURCE_PGUSER -t -A -c "SELECT COUNT(*) FROM svv_datashare_objects WHERE share_name = '${SOURCE_SHARE_NAME}' AND object_type = 'schema' AND object_name = '${schema_name}'")
		i=$((i+1))
		echo "INFO: ${prefix}:${i}:${obj_count}:${schema_name}"
		if [ "${count}" -eq "0" ]; then
			psql -h $SOURCE_PGHOST -p $SOURCE_PGPORT -d $SOURCE_PGDATABASE -U $SOURCE_PGUSER -t -A -c "ALTER DATASHARE ${SOURCE_SHARE_NAME} ADD SCHEMA \"${schema_name}\";" -e > $PWD/log/${prefix}_${i}.log
		fi
		psql -h $SOURCE_PGHOST -p $SOURCE_PGPORT -d $SOURCE_PGDATABASE -U $SOURCE_PGUSER -t -A -c "ALTER DATASHARE ${SOURCE_SHARE_NAME} ADD ALL TABLES IN SCHEMA \"${schema_name}\";" -e >> $PWD/log/${prefix}_${i}.log
	done
}
grant_datashare()
{
	consumer_namespace=$(psql -h $TARGET_PGHOST -p $TARGET_PGPORT -d $TARGET_PGDATABASE -U $TARGET_PGUSER -t -A -c "SELECT current_namespace")
	count=$(psql -h $SOURCE_PGHOST -p $SOURCE_PGPORT -d $SOURCE_PGDATABASE -U $SOURCE_PGUSER -t -A -c "SELECT COUNT(*) FROM svv_datashare_consumers WHERE share_name = '${SOURCE_SHARE_NAME}' AND consumer_namespace = '${consumer_namespace}'")
	if [ "${count}" -eq "0" ]; then
		psql -h $SOURCE_PGHOST -p $SOURCE_PGPORT -d $SOURCE_PGDATABASE -U $SOURCE_PGUSER -t -A -c "GRANT USAGE ON DATASHARE ${SOURCE_SHARE_NAME} TO NAMESPACE '${consumer_namespace}'" -e
	else
		echo "INFO: Namespace ${consumer_namespace} already granted to ${SOURCE_SHARE_NAME}."
	fi
}
create_database()
{
	producer_namespace=$(psql -h $SOURCE_PGHOST -p $SOURCE_PGPORT -d $SOURCE_PGDATABASE -U $SOURCE_PGUSER -t -A -c "SELECT current_namespace")
	count=$(psql -h $TARGET_PGHOST -p $TARGET_PGPORT -d $TARGET_PGDATABASE -U $TARGET_PGUSER -t -A -c "SELECT COUNT(*) FROM pg_database WHERE datname = '${TARGET_DATASHARE_DATABASE}'")
	if [ "${count}" -eq "0" ]; then
		psql -h $TARGET_PGHOST -p $TARGET_PGPORT -d $TARGET_PGDATABASE -U $TARGET_PGUSER -t -A -c "CREATE DATABASE ${TARGET_DATASHARE_DATABASE} FROM DATASHARE ${SOURCE_SHARE_NAME} OF NAMESPACE '${producer_namespace}'" -e
	else
		echo "INFO: ${TARGET_DATASHARE_DATABASE} already exists."
	fi	
}
create_external_schemas()
{
	prefix="create_external_schema"
	i="0"
	obj_count=$(psql -h $SOURCE_PGHOST -p $SOURCE_PGPORT -d $SOURCE_PGDATABASE -U $SOURCE_PGUSER -t -A -c "SELECT COUNT(*) FROM svv_all_schemas WHERE database_name = current_database() AND schema_type = 'local' AND schema_name NOT IN ${EXCLUDED_SCHEMAS}")
	echo "INFO: ${prefix}:creating ${obj_count}"
	for schema_name in $(psql -h $SOURCE_PGHOST -p $SOURCE_PGPORT -d $SOURCE_PGDATABASE -U $SOURCE_PGUSER -t -A -c "SELECT schema_name FROM svv_all_schemas WHERE database_name = current_database() AND schema_type = 'local' AND schema_name NOT IN ${EXCLUDED_SCHEMAS} ORDER BY schema_name"); do
		count=$(psql -h $TARGET_PGHOST -p $TARGET_PGPORT -d $TARGET_PGDATABASE -U $TARGET_PGUSER -t -A -c "SELECT COUNT(*) FROM svv_all_schemas WHERE database_name = current_database() AND schema_type = 'external' AND schema_name = 'ext_${schema_name}'")
		i=$((i+1))
		echo "INFO: ${prefix}:${i}:${obj_count}:${schema_name}"
		if [ "${count}" -eq "0" ]; then
			psql -h $TARGET_PGHOST -p $TARGET_PGPORT -d $TARGET_PGDATABASE -U $TARGET_PGUSER -t -A -c "CREATE EXTERNAL SCHEMA \"ext_${schema_name}\" FROM REDSHIFT DATABASE '${TARGET_DATASHARE_DATABASE}' SCHEMA '${schema_name}'" -e > $PWD/log/${prefix}_${i}.log
		fi
	done
}

create_datashare
add_schemas
grant_datashare
create_database
create_external_schemas

echo "INFO: Setup datasharing step complete"
