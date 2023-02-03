#!/bin/bash
set -e

source ${PWD}/config.sh
source ${PWD}/common.sh
exec_dir="exec_users"
rm -rf $PWD/${exec_dir}
mkdir -p $PWD/${exec_dir}
tmp_password="P@ssword1"
expire_password=$(date +%Y-%m-%d)

alter_schema_owner()
{
	prefix="alter_schema_owner"
	OLDIFS=$IFS
	IFS=$'\n'
	i="0"
	obj_count=$(psql -h $SOURCE_PGHOST -p $SOURCE_PGPORT -d $SOURCE_PGDATABASE -U $SOURCE_PGUSER -t -A -c "SELECT COUNT(*) FROM svv_all_schemas WHERE database_name = current_database() AND schema_type = 'local' AND schema_name NOT IN ${EXCLUDED_SCHEMAS} AND pg_get_userbyid(schema_owner) <> 'rdsdb'")
	echo "INFO: ${prefix}:creating ${obj_count}"
	for x in $(psql -h $SOURCE_PGHOST -p $SOURCE_PGPORT -d $SOURCE_PGDATABASE -U $SOURCE_PGUSER -t -A -c "SELECT schema_name, pg_get_userbyid(schema_owner) FROM svv_all_schemas WHERE database_name = current_database() AND schema_type = 'local' AND schema_name NOT IN ${EXCLUDED_SCHEMAS} AND pg_get_userbyid(schema_owner) <> 'rdsdb'"); do
		i=$((i+1))
		schema_name=$(echo ${x} | awk -F '|' '{print $1}')
		schema_owner=$(echo ${x} | awk -F '|' '{print $2}')
		wait_for_threads "${tag}"
		echo "INFO: ${prefix}:${i}:${obj_count}:${schema_name}"
		psql -h $TARGET_PGHOST -p $TARGET_PGPORT -d $TARGET_PGDATABASE -U $TARGET_PGUSER -c "ALTER SCHEMA \"${schema_name}\" OWNER TO \"${schema_owner}\"" -v tag=${tag} -e > $PWD/log/${prefix}_${i}.log 2>&1 &
	done
	wait_for_remaining "${tag}" 
	IFS=$OLDIFS
}
alter_table_owner()
{
	prefix="alter_table_owner"
	OLDIFS=$IFS
	IFS=$'\n'
	i="0"
	obj_count=$(psql -h $SOURCE_PGHOST -p $SOURCE_PGPORT -d $SOURCE_PGDATABASE -U $SOURCE_PGUSER -t -A -c "SELECT COUNT(*) FROM pg_class c JOIN pg_namespace n ON c.relnamespace = n.oid JOIN pg_user u ON c.relowner = u.usesysid JOIN svv_all_schemas s ON s.schema_name = n.nspname WHERE s.database_name = current_database() AND s.schema_type = 'local' AND s.schema_name NOT IN ${EXCLUDED_SCHEMAS} AND relkind IN ('r', 'v') AND LOWER(pg_get_viewdef(c.oid)) NOT LIKE '%materialized%'")
	echo "INFO: ${prefix}:creating ${obj_count}"
	for schema_name in $(psql -h $SOURCE_PGHOST -p $SOURCE_PGPORT -d $SOURCE_PGDATABASE -U $SOURCE_PGUSER -t -A -c "SELECT schema_name FROM svv_all_schemas WHERE database_name = current_database() AND schema_type = 'local' AND schema_name NOT IN ${EXCLUDED_SCHEMAS}"); do
		for x in $(psql -h $SOURCE_PGHOST -p $SOURCE_PGPORT -d $SOURCE_PGDATABASE -U $SOURCE_PGUSER -t -A -c "SELECT c.relname, u.usename FROM pg_class c JOIN pg_namespace n ON c.relnamespace = n.oid JOIN pg_user u ON c.relowner = u.usesysid WHERE n.nspname = '${schema_name}' AND relkind IN ('r', 'v') AND LOWER(pg_get_viewdef(c.oid)) NOT LIKE '%materialized%' ORDER BY c.relname"); do
			i=$((i+1))
			table_name=$(echo ${x} | awk -F '|' '{print $1}')
			table_owner=$(echo ${x} | awk -F '|' '{print $2}')
			wait_for_threads "${tag}"
			echo "INFO: ${prefix}:${i}:${obj_count}:${schema_name}.${table_name}"
			psql -h $TARGET_PGHOST -p $TARGET_PGPORT -d $TARGET_PGDATABASE -U $TARGET_PGUSER -c "ALTER TABLE \"${schema_name}\".\"${table_name}\" OWNER TO \"${table_owner}\"" -v tag=${tag} -e > $PWD/log/${prefix}_${i}.log 2>&1 &
		done
	done
	wait_for_remaining "${tag}" 
	IFS=$OLDIFS
}
alter_function_owner()
{
	prefix="alter_function_owner"
	i="0"
	OLDIFS=$IFS
	IFS=$'\n'
	obj_count=$(psql -h $SOURCE_PGHOST -p $SOURCE_PGPORT -d $SOURCE_PGDATABASE -U $SOURCE_PGUSER -t -A -c "SELECT COUNT(*) FROM pg_proc p JOIN pg_namespace n ON p.pronamespace = n.oid JOIN pg_language l ON p.prolang = l.oid JOIN pg_user u ON p.proowner = u.usesysid JOIN svv_all_schemas s ON s.schema_name = n.nspname WHERE s.database_name = current_database() AND s.schema_type = 'local' AND s.schema_name NOT IN ${EXCLUDED_SCHEMAS} AND l.lanname IN ('sql', 'plpythonu') AND u.usename <> 'rdsdb'")
	echo "INFO: ${prefix}:creating ${obj_count}"
	for schema_name in $(psql -h $SOURCE_PGHOST -p $SOURCE_PGPORT -d $SOURCE_PGDATABASE -U $SOURCE_PGUSER -t -A -c "SELECT schema_name FROM svv_all_schemas WHERE database_name = current_database() AND schema_type = 'local' AND schema_name NOT IN ${EXCLUDED_SCHEMAS}"); do
		for x in $(psql -h $SOURCE_PGHOST -p $SOURCE_PGPORT -d $SOURCE_PGDATABASE -U $SOURCE_PGUSER -t -A -c "SELECT p.proname, oidvectortypes(p.proargtypes), pg_get_userbyid(p.proowner) FROM pg_proc p JOIN pg_namespace n ON p.pronamespace = n.oid JOIN pg_language l ON p.prolang = l.oid JOIN pg_user u ON p.proowner = u.usesysid WHERE n.nspname = '${schema_name}' AND l.lanname IN ('sql', 'plpythonu') AND u.usename <> 'rdsdb'"); do
			i=$((i+1))
			proname=$(echo ${x} | awk -F '|' '{print $1}')
			proparams=$(echo ${x} | awk -F '|' '{print $2}')
			proowner=$(echo ${x} | awk -F '|' '{print $3}')
			wait_for_threads "${tag}"
			echo "INFO: ${prefix}:${i}:${obj_count}:${schema_name}.${proname}"
			psql -h $TARGET_PGHOST -p $TARGET_PGPORT -d $TARGET_PGDATABASE -U $TARGET_PGUSER -c "ALTER FUNCTION \"${schema_name}\".\"${proname}\"(${proparams}) OWNER TO \"${proowner}\"" -v tag=${tag} -e > $PWD/log/${prefix}_${i}.log 2>&1 &
		done
	done
	wait_for_remaining "${tag}" 
	IFS=$OLDIFS
}
alter_procedure_owner()
{
	prefix="alter_proceduer_owner"
	i="0"
	OLDIFS=$IFS
	IFS=$'\n'
	obj_count=$(psql -h $SOURCE_PGHOST -p $SOURCE_PGPORT -d $SOURCE_PGDATABASE -U $SOURCE_PGUSER -t -A -c "SELECT COUNT(*) FROM pg_proc p JOIN pg_namespace n ON p.pronamespace = n.oid JOIN pg_language l ON p.prolang = l.oid JOIN pg_user u ON p.proowner = u.usesysid JOIN svv_all_schemas s ON s.schema_name = n.nspname WHERE s.database_name = current_database() AND s.schema_type = 'local' AND s.schema_name NOT IN ${EXCLUDED_SCHEMAS} AND l.lanname = 'plpgsql' AND p.proname NOT LIKE 'mv_sp__%' AND u.usename <> 'rdsdb'")
	echo "INFO: ${prefix}:creating ${obj_count}"
	for schema_name in $(psql -h $SOURCE_PGHOST -p $SOURCE_PGPORT -d $SOURCE_PGDATABASE -U $SOURCE_PGUSER -t -A -c "SELECT schema_name FROM svv_all_schemas WHERE database_name = current_database() AND schema_type = 'local' AND schema_name NOT IN ${EXCLUDED_SCHEMAS}"); do
		for x in $(psql -h $SOURCE_PGHOST -p $SOURCE_PGPORT -d $SOURCE_PGDATABASE -U $SOURCE_PGUSER -t -A -c "SELECT p.proname, oidvectortypes(p.proargtypes), pg_get_userbyid(p.proowner) FROM pg_proc p JOIN pg_namespace n ON p.pronamespace = n.oid JOIN pg_language l ON p.prolang = l.oid JOIN pg_user u ON p.proowner = u.usesysid WHERE n.nspname = '${schema_name}' AND l.lanname = 'plpgsql' AND p.proname NOT LIKE 'mv_sp__%' AND u.usename <> 'rdsdb'"); do
			i=$((i+1))
			proname=$(echo ${x} | awk -F '|' '{print $1}')
			proparams=$(echo ${x} | awk -F '|' '{print $2}')
			proowner=$(echo ${x} | awk -F '|' '{print $3}')
			echo "INFO: ${prefix}:${i}:${obj_count}:${schema_name}.${proname}"
			psql -h $TARGET_PGHOST -p $TARGET_PGPORT -d $TARGET_PGDATABASE -U $TARGET_PGUSER -c "ALTER PROCEDURE \"${schema_name}\".\"${proname}\"(${proparams}) OWNER TO \"${proowner}\"" -v tag=${tag} -e > $PWD/log/${prefix}_${i}.log 2>&1 &
		done
	done
	wait_for_remaining "${tag}" 
	IFS=$OLDIFS
}
grant_user_schema()
{
	prefix="grant_user_schema"
	i="0"
	previous_schema_name=""
	exec_sql=""
	OLDIFS=$IFS
	IFS=$'\n'
	obj_count=$(psql -h $SOURCE_PGHOST -p $SOURCE_PGPORT -d $SOURCE_PGDATABASE -U $SOURCE_PGUSER -t -A -c "SELECT COUNT(DISTINCT sub.nspname) FROM (SELECT n.nspname, split_part(array_to_string(nspacl, ','), ',', i) AS acl FROM (SELECT nspname, generate_series(1, array_upper(nspacl, 1)) AS i, nspacl FROM pg_namespace) AS n WHERE split_part(array_to_string(nspacl, ','), ',', i) NOT LIKE 'group %') AS sub JOIN pg_user u ON u.usename = split_part(sub.acl, '=', 1) WHERE u.usename <> 'rdsdb' AND u.usesuper IS FALSE AND sub.nspname NOT IN ${EXCLUDED_SCHEMAS}")
	echo "INFO: ${prefix}:creating ${obj_count}"
	for x in $(psql -h $SOURCE_PGHOST -p $SOURCE_PGPORT -d $SOURCE_PGDATABASE -U $SOURCE_PGUSER -t -A -c "SELECT sub.nspname, split_part(sub.acl, '=', 1) AS usename, split_part(split_part(sub.acl, '=', 2), '/', 1) AS usegrant FROM (SELECT n.nspname, split_part(array_to_string(nspacl, ','), ',', i) AS acl FROM (SELECT nspname, generate_series(1, array_upper(nspacl, 1)) AS i, nspacl FROM pg_namespace) AS n WHERE split_part(array_to_string(nspacl, ','), ',', i) NOT LIKE 'group %') AS sub JOIN pg_user u ON u.usename = split_part(sub.acl, '=', 1) WHERE u.usename <> 'rdsdb' AND u.usesuper IS FALSE AND sub.nspname NOT IN ${EXCLUDED_SCHEMAS} ORDER BY 1, 2"); do
		schema_name=$(echo ${x} | awk -F '|' '{print $1}')
		grantee=$(echo ${x} | awk -F '|' '{print $2}')
		use_grant=$(echo ${x} | awk -F '|' '{print $3}')
		grant_count=$(echo -n "${use_grant}" | wc -m)
		#change to 0 base
		grant_count=$((grant_count-1))
		counter="0"
		if [ "${i}" -gt "0" ]; then
			if [ ! "${schema_name}" == "${previous_schema_name}" ]; then
				wait_for_threads ${tag}
				echo "INFO: ${prefix}:${i}:${obj_count}:${previous_schema_name}"
				psql -h $TARGET_PGHOST -p $TARGET_PGPORT -d $TARGET_PGDATABASE -U $TARGET_PGUSER -f "${exec_sql}" -v tag=${tag} -e > $PWD/log/${prefix}_${i}.log 2>&1 & 
			fi
		fi
		if [ ! "${schema_name}" == "${previous_schema_name}" ]; then
			# new schema to add grants to in a script
			i=$((i+1))
			exec_sql="${exec_dir}/${prefix}_${i}.sql"
		fi
		for y in $(seq 0 ${grant_count}); do
			grant=${use_grant:y:1}
			if [ "${grant}" == "U" ]; then
				grant_action="USAGE"
			elif [ "${grant}" == "C" ]; then
				grant_action="CREATE"
			else
				grant_action="NONE"
			fi
			if [ ! "${grant_action}" == "NONE" ]; then
				counter=$((counter+1))
			fi
			if [ ! "$grant_action" == "NONE" ]; then
				if [ "${counter}" -eq "1" ]; then
					sql_cmd="GRANT ${grant_action}"
				else
					sql_cmd+=", ${grant_action}"
				fi;
			fi
		done
		if [ "${counter}" -gt "0" ]; then
			sql_cmd+=" ON SCHEMA \"${schema_name}\" TO \"${grantee}\";"
			echo "${sql_cmd}" >> ${exec_sql}
		fi
		previous_schema_name="${schema_name}"
	done
	wait_for_threads ${tag} 
	echo "INFO: ${prefix}:${i}:${obj_count}:${schema_name}"
	psql -h $TARGET_PGHOST -p $TARGET_PGPORT -d $TARGET_PGDATABASE -U $TARGET_PGUSER -f "${exec_sql}" -v tag=${tag} -e > $PWD/log/${prefix}_${i}.log 2>&1 & 

	wait_for_remaining "${tag}" 
	IFS=$OLDIFS
}
grant_user_table()
{
	prefix="grant_user_table"
	i="0"
	previous_schema_name=""
	previous_table_name=""
	exec_sql=""
	OLDIFS=$IFS
	IFS=$'\n'
	obj_count=$(psql -h $SOURCE_PGHOST -p $SOURCE_PGPORT -d $SOURCE_PGDATABASE -U $SOURCE_PGUSER -t -A -c "SELECT COUNT(DISTINCT sub2.nspname || sub2.relname) FROM (SELECT sub.nspname, sub.relname, split_part(array_to_string(sub.relacl, ','), ',', i) AS acl FROM (SELECT n.nspname, c.relname, generate_series(1, array_upper(c.relacl, 1)) AS i, c.relacl FROM pg_class c JOIN pg_namespace n ON c.relnamespace = n.oid) AS sub WHERE split_part(array_to_string(sub.relacl, ','), ',', i) NOT LIKE 'group %') AS sub2 JOIN pg_user u ON u.usename = split_part(sub2.acl, '=', 1) WHERE u.usename <> 'rdsdb' AND u.usesuper IS FALSE AND sub2.nspname NOT IN ${EXCLUDED_SCHEMAS}")
	echo "INFO: ${prefix}:creating ${obj_count}"
	for x in $(psql -h $SOURCE_PGHOST -p $SOURCE_PGPORT -d $SOURCE_PGDATABASE -U $SOURCE_PGUSER -t -A -c "SELECT sub2.nspname, sub2.relname, split_part(sub2.acl, '=', 1) AS usename, split_part(split_part(sub2.acl, '=', 2), '/', 1) AS usegrant FROM (SELECT sub.nspname, sub.relname, split_part(array_to_string(sub.relacl, ','), ',', i) AS acl FROM (SELECT n.nspname, c.relname, generate_series(1, array_upper(c.relacl, 1)) AS i, c.relacl FROM pg_class c JOIN pg_namespace n ON c.relnamespace = n.oid) AS sub WHERE split_part(array_to_string(sub.relacl, ','), ',', i) NOT LIKE 'group %') AS sub2 JOIN pg_user u ON u.usename = split_part(sub2.acl, '=', 1) WHERE u.usename <> 'rdsdb' AND u.usesuper IS FALSE AND sub2.nspname NOT IN ${EXCLUDED_SCHEMAS} ORDER BY 1, 2;"); do
		schema_name=$(echo ${x} | awk -F '|' '{print $1}')
		table_name=$(echo ${x} | awk -F '|' '{print $2}')
		grantee=$(echo ${x} | awk -F '|' '{print $3}')
		use_grant=$(echo ${x} | awk -F '|' '{print $4}')
		grant_count=$(echo -n "${use_grant}" | wc -m)
		#change to 0 base
		grant_count=$((grant_count-1))
		counter="0"
		if [ "${i}" -gt "0" ]; then
			if [[ ! "${schema_name}" == "${previous_schema_name}" || ! "${table_name}" == "${previous_table_name}" ]]; then
				wait_for_threads ${tag}
				echo "INFO: ${prefix}:${i}:${obj_count}:${previous_schema_name}.${previous_table_name}"
				psql -h $TARGET_PGHOST -p $TARGET_PGPORT -d $TARGET_PGDATABASE -U $TARGET_PGUSER -f "${exec_sql}" -v tag=${tag} -e > $PWD/log/${prefix}_${i}.log 2>&1 & 
			fi
		fi
		if [[ ! "${schema_name}" == "${previous_schema_name}" || ! "${table_name}" == "${previous_table_name}" ]]; then
			i=$((i+1))
			exec_sql="${exec_dir}/${prefix}_${i}.sql"
		fi
		for y in $(seq 0 ${grant_count}); do
			grant=${use_grant:y:1}
			if [ "${grant}" == "a" ]; then
				grant_action="INSERT"
			elif [ "${grant}" == "w" ]; then
				grant_action="UPDATE"
			elif [ "${grant}" == "d" ]; then
				grant_action="DELETE"
			elif [ "${grant}" == "r" ]; then
				grant_action="SELECT"
			elif [ "${grant}" == "x" ]; then
				grant_action="REFERENCES"
			elif [ "${grant}" == "t" ]; then
				grant_action="TRIGGER"
			elif [ "${grant}" == "R" ]; then
				grant_action="RULE"
			else
				grant_action="NONE"
			fi
			if [ ! "${grant_action}" == "NONE" ]; then
				counter=$((counter+1))
			fi
			if [ ! "$grant_action" == "NONE" ]; then
				if [ "${counter}" -eq "1" ]; then
					sql_cmd="GRANT ${grant_action}"
				else
					sql_cmd+=", ${grant_action}"
				fi;
			fi
		done
		if [ "${counter}" -gt "0" ]; then
			sql_cmd+=" ON TABLE \"${schema_name}\".\"${table_name}\" TO \"${grantee}\";"
			echo "${sql_cmd}" >> ${exec_sql}
		fi
		previous_schema_name="${schema_name}"
		previous_table_name="${table_name}"

	done
	wait_for_threads ${tag}
	echo "INFO: ${prefix}:${i}:${obj_count}:${schema_name}.${table_name}"
	psql -h $TARGET_PGHOST -p $TARGET_PGPORT -d $TARGET_PGDATABASE -U $TARGET_PGUSER -f "${exec_sql}" -v tag=${tag} -e > $PWD/log/${prefix}_${i}.log 2>&1 & 
	wait_for_remaining "${tag}" 
	IFS=$OLDIFS
}
grant_user_function()
{
	prefix="grant_user_function"
	i="0"
	previous_schema_name=""
	previous_proname=""
	previous_proparams=""
	exec_sql=""
	OLDIFS=$IFS
	IFS=$'\n'
	obj_count=$(psql -h $SOURCE_PGHOST -p $SOURCE_PGPORT -d $SOURCE_PGDATABASE -U $SOURCE_PGUSER -t -A -c "SELECT COUNT(DISTINCT sub2.nspname || sub2.proname || coalesce(sub2.proargs, '')) FROM (SELECT sub.nspname, sub.proname, sub.proargs, split_part(array_to_string(sub.proacl, ','), ',', i) AS acl, split_part(array_to_string(sub.proacl, ','), ',', i) FROM (SELECT n.nspname, p.proname, oidvectortypes(p.proargtypes) proargs, generate_series(1, array_upper(p.proacl, 1)) AS i, p.proacl FROM pg_proc p JOIN pg_namespace n ON p.pronamespace = n.oid JOIN pg_language l ON p.prolang = l.oid JOIN pg_user u ON p.proowner = u.usesysid WHERE l.lanname IN ('sql', 'plpythonu') AND u.usename <> 'rdsdb' AND p.proacl IS NOT NULL) AS sub WHERE split_part(array_to_string(sub.proacl, ','), ',', i) NOT LIKE 'group %') AS sub2 WHERE split_part(sub2.acl, '=', 1) <> '' AND split_part(split_part(sub2.acl, '=', 2), '/', 1) = 'X' AND sub2.nspname NOT IN ${EXCLUDED_SCHEMAS}")
	echo "INFO: ${prefix}:creating ${obj_count}"
	for x in $(psql -h $SOURCE_PGHOST -p $SOURCE_PGPORT -d $SOURCE_PGDATABASE -U $SOURCE_PGUSER -t -A -c "SELECT sub2.nspname, sub2.proname, sub2.proargs, split_part(sub2.acl, '=', 1) AS grantee FROM (SELECT sub.nspname, sub.proname, sub.proargs, split_part(array_to_string(sub.proacl, ','), ',', i) AS acl, split_part(array_to_string(sub.proacl, ','), ',', i) FROM (SELECT n.nspname, p.proname, oidvectortypes(p.proargtypes) proargs, generate_series(1, array_upper(p.proacl, 1)) AS i, p.proacl FROM pg_proc p JOIN pg_namespace n ON p.pronamespace = n.oid JOIN pg_language l ON p.prolang = l.oid JOIN pg_user u ON p.proowner = u.usesysid WHERE l.lanname IN ('sql', 'plpythonu') AND u.usename <> 'rdsdb' AND p.proacl IS NOT NULL) AS sub WHERE split_part(array_to_string(sub.proacl, ','), ',', i) NOT LIKE 'group %') AS sub2 WHERE split_part(sub2.acl, '=', 1) <> '' AND split_part(split_part(sub2.acl, '=', 2), '/', 1) = 'X' AND sub2.nspname NOT IN ${EXCLUDED_SCHEMAS} ORDER BY 1, 2"); do
		schema_name=$(echo ${x} | awk -F '|' '{print $1}')
		proname=$(echo ${x} | awk -F '|' '{print $2}')
		proparams=$(echo ${x} | awk -F '|' '{print $3}')
		grantee=$(echo ${x} | awk -F '|' '{print $4}')
		if [ "${i}" -gt "0" ]; then
			if [[ ! "${schema_name}" == "${previous_schema_name}" || ! "${proname}" == "${previous_proname}" || ! "${proparams}" == "${previous_proparams}" ]]; then
				wait_for_threads ${tag}
				echo "INFO: ${prefix}:${i}:${obj_count}:${previous_schema_name}.${previous_proname}"
				psql -h $TARGET_PGHOST -p $TARGET_PGPORT -d $TARGET_PGDATABASE -U $TARGET_PGUSER -f "${exec_sql}" -v tag=${tag} -e > $PWD/log/${prefix}_${i}.log 2>&1 & 
			fi
		fi
		if [[ ! "${schema_name}" == "${previous_schema_name}" || ! "${proname}" == "${previous_proname}" || ! "${proparams}" == "${previous_proparams}" ]]; then
			i=$((i+1))
			exec_sql="${exec_dir}/${prefix}_${i}.sql"
		fi
		sql_cmd="GRANT EXECUTE ON FUNCTION \"${schema_name}\".\"${proname}\"(${proparams}) TO \"${grantee}\";"
		echo "${sql_cmd}" >> ${exec_sql}
		previous_schema_name="${schema_name}"
		previous_proname="${proname}"
		previous_proparams="${proparams}"
	done
	wait_for_threads ${tag}
	echo "INFO: ${prefix}:${i}:${obj_count}:${schema_name}.${proname}"
	psql -h $TARGET_PGHOST -p $TARGET_PGPORT -d $TARGET_PGDATABASE -U $TARGET_PGUSER -f "${exec_sql}" -v tag=${tag} -e > $PWD/log/${prefix}_${i}.log 2>&1 & 
	wait_for_remaining "${tag}" 
	IFS=$OLDIFS
}
grant_user_procedure()
{
	prefix="grant_user_procedure"
	i="0"
	previous_schema_name=""
	previous_proname=""
	previous_proparams=""
	exec_sql=""
	OLDIFS=$IFS
	IFS=$'\n'
	obj_count=$(psql -h $SOURCE_PGHOST -p $SOURCE_PGPORT -d $SOURCE_PGDATABASE -U $SOURCE_PGUSER -t -A -c "SELECT COUNT(DISTINCT sub2.nspname || sub2.proname || coalesce(sub2.proargs, '')) FROM (SELECT sub.nspname, sub.proname, sub.proargs, split_part(array_to_string(sub.proacl, ','), ',', i) AS acl, split_part(array_to_string(sub.proacl, ','), ',', i) FROM (SELECT n.nspname, p.proname, oidvectortypes(p.proargtypes) proargs, generate_series(1, array_upper(p.proacl, 1)) AS i, p.proacl FROM pg_proc p JOIN pg_namespace n ON p.pronamespace = n.oid JOIN pg_language l ON p.prolang = l.oid JOIN pg_user u ON p.proowner = u.usesysid WHERE l.lanname = 'plpgsql' AND u.usename <> 'rdsdb' AND p.proacl IS NOT NULL) AS sub WHERE split_part(array_to_string(sub.proacl, ','), ',', i) NOT LIKE 'group %') AS sub2 WHERE split_part(sub2.acl, '=', 1) <> '' AND split_part(split_part(sub2.acl, '=', 2), '/', 1) = 'X' AND sub2.nspname NOT IN ${EXCLUDED_SCHEMAS}")
	echo "INFO: ${prefix}:creating ${obj_count}"
	for x in $(psql -h $SOURCE_PGHOST -p $SOURCE_PGPORT -d $SOURCE_PGDATABASE -U $SOURCE_PGUSER -t -A -c "SELECT sub2.nspname, sub2.proname, sub2.proargs, split_part(sub2.acl, '=', 1) AS grantee FROM (SELECT sub.nspname, sub.proname, sub.proargs, split_part(array_to_string(sub.proacl, ','), ',', i) AS acl, split_part(array_to_string(sub.proacl, ','), ',', i) FROM (SELECT n.nspname, p.proname, oidvectortypes(p.proargtypes) proargs, generate_series(1, array_upper(p.proacl, 1)) AS i, p.proacl FROM pg_proc p JOIN pg_namespace n ON p.pronamespace = n.oid JOIN pg_language l ON p.prolang = l.oid JOIN pg_user u ON p.proowner = u.usesysid WHERE l.lanname = 'plpgsql' AND u.usename <> 'rdsdb' AND p.proacl IS NOT NULL) AS sub WHERE split_part(array_to_string(sub.proacl, ','), ',', i) NOT LIKE 'group %') AS sub2 WHERE split_part(sub2.acl, '=', 1) <> '' AND split_part(split_part(sub2.acl, '=', 2), '/', 1) = 'X' AND sub2.nspname NOT IN ${EXCLUDED_SCHEMAS} ORDER BY 1, 2"); do
		schema_name=$(echo ${x} | awk -F '|' '{print $1}')
		proname=$(echo ${x} | awk -F '|' '{print $2}')
		proparams=$(echo ${x} | awk -F '|' '{print $3}')
		grantee=$(echo ${x} | awk -F '|' '{print $4}')
		if [ "${i}" -gt "0" ]; then
			if [[ ! "${schema_name}" == "${previous_schema_name}" || ! "${proname}" == "${previous_proname}" || ! "${proparams}" == "${previous_proparams}" ]]; then
				wait_for_threads ${tag}
				echo "INFO: ${prefix}:${i}:${obj_count}:${previous_schema_name}.${previous_proname}"
				psql -h $TARGET_PGHOST -p $TARGET_PGPORT -d $TARGET_PGDATABASE -U $TARGET_PGUSER -f "${exec_sql}" -v tag=${tag} -e > $PWD/log/${prefix}_${i}.log 2>&1 & 
			fi
		fi
		if [[ ! "${schema_name}" == "${previous_schema_name}" || ! "${proname}" == "${previous_proname}" || ! "${proparams}" == "${previous_proparams}" ]]; then
			i=$((i+1))
			exec_sql="${exec_dir}/${prefix}_${i}.sql"
		fi
		sql_cmd="GRANT EXECUTE ON PROCEDURE \"${schema_name}\".\"${proname}\"(${proparams}) TO \"${grantee}\";"
		echo "${sql_cmd}" >> ${exec_sql}
		previous_schema_name="${schema_name}"
		previous_proname="${proname}"
		previous_proparams="${proparams}"
	done
	wait_for_threads ${tag}
	echo "INFO: ${prefix}:${i}:${obj_count}.${schema_name}.${proname}"
	psql -h $TARGET_PGHOST -p $TARGET_PGPORT -d $TARGET_PGDATABASE -U $TARGET_PGUSER -f "${exec_sql}" -v tag=${tag} -e > $PWD/log/${prefix}_${i}.log 2>&1 & 
	wait_for_remaining "${tag}" 
	IFS=$OLDIFS
}
grant_group_schema()
{
	prefix="grant_group_schema"
	i="0"
	previous_schema_name=""
	exec_sql=""
	OLDIFS=$IFS
	IFS=$'\n'
	obj_count=$(psql -h $SOURCE_PGHOST -p $SOURCE_PGPORT -d $SOURCE_PGDATABASE -U $SOURCE_PGUSER -t -A -c "SELECT COUNT(DISTINCT sub.nspname) FROM (SELECT n.nspname, split_part(split_part(array_to_string(nspacl, ','), ',', i), ' ', 2) AS acl FROM (SELECT nspname, generate_series(1, array_upper(nspacl, 1)) AS i, nspacl FROM pg_namespace) AS n WHERE split_part(array_to_string(nspacl, ','), ',', i) LIKE 'group %') AS sub WHERE sub.nspname NOT IN ${EXCLUDED_SCHEMAS}")
	echo "INFO: ${prefix}:creating ${obj_count}"
	for x in $(psql -h $SOURCE_PGHOST -p $SOURCE_PGPORT -d $SOURCE_PGDATABASE -U $SOURCE_PGUSER -t -A -c "SELECT sub.nspname, split_part(sub.acl, '=', 1) AS groname, split_part(split_part(sub.acl, '=', 2), '/', 1) AS grogrant FROM (SELECT n.nspname, split_part(split_part(array_to_string(nspacl, ','), ',', i), ' ', 2) AS acl FROM (SELECT nspname, generate_series(1, array_upper(nspacl, 1)) AS i, nspacl FROM pg_namespace) AS n WHERE split_part(array_to_string(nspacl, ','), ',', i) LIKE 'group %') AS sub WHERE sub.nspname NOT IN ${EXCLUDED_SCHEMAS} ORDER BY 1, 2"); do
		schema_name=$(echo ${x} | awk -F '|' '{print $1}')
		group_name=$(echo ${x} | awk -F '|' '{print $2}')
		group_grant=$(echo ${x} | awk -F '|' '{print $3}')
		grant_count=$(echo -n "${group_grant}" | wc -m)
		#change to 0 base
		grant_count=$((grant_count-1))
		counter="0"
		if [ "${i}" -gt "0" ]; then
			if [ ! "${schema_name}" == "${previous_schema_name}" ]; then
				wait_for_threads ${tag}
				echo "INFO: ${prefix}:${i}:${obj_count}.${previous_schema_name}"
				psql -h $TARGET_PGHOST -p $TARGET_PGPORT -d $TARGET_PGDATABASE -U $TARGET_PGUSER -f "${exec_sql}" -v tag=${tag} -e > $PWD/log/${prefix}_${i}.log 2>&1 & 
			fi
		fi
		if [ ! "${schema_name}" == "${previous_schema_name}" ]; then
			# new schema to add grants to in a script
			i=$((i+1))
			exec_sql="${exec_dir}/${prefix}_${i}.sql"
		fi
		for y in $(seq 0 ${grant_count}); do
			grant=${group_grant:y:1}
			if [ "${grant}" == "U" ]; then
				grant_action="USAGE"
			elif [ "${grant}" == "C" ]; then
				grant_action="CREATE"
			else
				grant_action="NONE"
			fi
			if [ ! "${grant_action}" == "NONE" ]; then
				counter=$((counter+1))
			fi
			if [ ! "$grant_action" == "NONE" ]; then
				if [ "${counter}" -eq "1" ]; then
					sql_cmd="GRANT ${grant_action}"
				else
					sql_cmd+=", ${grant_action}"
				fi;
			fi
		done
		if [ "${counter}" -gt "0" ]; then
			sql_cmd+=" ON SCHEMA \"${schema_name}\" TO GROUP \"${group_name}\";"
			echo "${sql_cmd}" >> ${exec_sql}
		fi
		previous_schema_name="${schema_name}"
	done
	wait_for_threads ${tag}
	echo "INFO: ${prefix}:${i}:${obj_count}:${schema_name}"
	psql -h $TARGET_PGHOST -p $TARGET_PGPORT -d $TARGET_PGDATABASE -U $TARGET_PGUSER -f "${exec_sql}" -v tag=${tag} -e > $PWD/log/${prefix}_${i}.log 2>&1 & 

	wait_for_remaining "${tag}" 
	IFS=$OLDIFS
}
grant_group_table()
{
	prefix="grant_group_table"
	i="0"
	previous_schema_name=""
	previous_table_name=""
	exec_sql=""
	OLDIFS=$IFS
	IFS=$'\n'
	obj_count=$(psql -h $SOURCE_PGHOST -p $SOURCE_PGPORT -d $SOURCE_PGDATABASE -U $SOURCE_PGUSER -t -A -c "SELECT COUNT(DISTINCT sub2.nspname || sub2.relname) FROM (SELECT sub.nspname, sub.relname, split_part(split_part(array_to_string(sub.relacl, ','), ',', i), ' ', 2) AS acl FROM (SELECT n.nspname, c.relname, generate_series(1, array_upper(c.relacl, 1)) AS i, c.relacl FROM pg_class c JOIN pg_namespace n ON c.relnamespace = n.oid) AS sub WHERE split_part(array_to_string(sub.relacl, ','), ',', i) LIKE 'group %') AS sub2 WHERE sub2.nspname NOT IN ${EXCLUDED_SCHEMAS}")
	echo "INFO: ${prefix}:creating ${obj_count}"
	for x in $(psql -h $SOURCE_PGHOST -p $SOURCE_PGPORT -d $SOURCE_PGDATABASE -U $SOURCE_PGUSER -t -A -c "SELECT sub2.nspname, sub2.relname, split_part(sub2.acl, '=', 1) AS groname, split_part(split_part(sub2.acl, '=', 2), '/', 1) AS grogrant FROM (SELECT sub.nspname, sub.relname, split_part(split_part(array_to_string(sub.relacl, ','), ',', i), ' ', 2) AS acl FROM (SELECT n.nspname, c.relname, generate_series(1, array_upper(c.relacl, 1)) AS i, c.relacl FROM pg_class c JOIN pg_namespace n ON c.relnamespace = n.oid) AS sub WHERE split_part(array_to_string(sub.relacl, ','), ',', i) LIKE 'group %') AS sub2 WHERE sub2.nspname NOT IN ${EXCLUDED_SCHEMAS} ORDER BY 1, 2;"); do
		schema_name=$(echo ${x} | awk -F '|' '{print $1}')
		table_name=$(echo ${x} | awk -F '|' '{print $2}')
		group_name=$(echo ${x} | awk -F '|' '{print $3}')
		group_grant=$(echo ${x} | awk -F '|' '{print $4}')
		grant_count=$(echo -n "${group_grant}" | wc -m)
		#change to 0 base
		grant_count=$((grant_count-1))
		counter="0"
		if [ "${i}" -gt "0" ]; then
			if [[ ! "${schema_name}" == "${previous_schema_name}" || ! "${table_name}" == "${previous_table_name}" ]]; then
				wait_for_threads ${tag}
				echo "INFO: ${prefix}:${i}:${obj_count}:${previous_schema_name}.${previous_table_name}"
				psql -h $TARGET_PGHOST -p $TARGET_PGPORT -d $TARGET_PGDATABASE -U $TARGET_PGUSER -f "${exec_sql}" -v tag=${tag} -e > $PWD/log/${prefix}_${i}.log 2>&1 & 
			fi
		fi
		if [[ ! "${schema_name}" == "${previous_schema_name}" || ! "${table_name}" == "${previous_table_name}" ]]; then
			i=$((i+1))
			exec_sql="${exec_dir}/${prefix}_${i}.sql"
		fi
		for y in $(seq 0 ${grant_count}); do
			grant=${group_grant:y:1}
			if [ "${grant}" == "a" ]; then
				grant_action="INSERT"
			elif [ "${grant}" == "w" ]; then
				grant_action="UPDATE"
			elif [ "${grant}" == "d" ]; then
				grant_action="DELETE"
			elif [ "${grant}" == "r" ]; then
				grant_action="SELECT"
			elif [ "${grant}" == "x" ]; then
				grant_action="REFERENCES"
			elif [ "${grant}" == "t" ]; then
				grant_action="TRIGGER"
			elif [ "${grant}" == "R" ]; then
				grant_action="RULE"
			else
				grant_action="NONE"
			fi
			if [ ! "${grant_action}" == "NONE" ]; then
				counter=$((counter+1))
			fi
			if [ ! "$grant_action" == "NONE" ]; then
				if [ "${counter}" -eq "1" ]; then
					sql_cmd="GRANT ${grant_action}"
				else
					sql_cmd+=", ${grant_action}"
				fi;
			fi
		done
		if [ "${counter}" -gt "0" ]; then
			sql_cmd+=" ON TABLE \"${schema_name}\".\"${table_name}\" TO GROUP \"${group_name}\";"
			echo "${sql_cmd}" >> ${exec_sql}
		fi
		previous_schema_name="${schema_name}"
		previous_table_name="${table_name}"
	done
	wait_for_threads ${tag}
	echo "INFO: ${prefix}:${i}:${obj_count}:${schema_name}.${table_name}"
	psql -h $TARGET_PGHOST -p $TARGET_PGPORT -d $TARGET_PGDATABASE -U $TARGET_PGUSER -f "${exec_sql}" -v tag=${tag} -e > $PWD/log/${prefix}_${i}.log 2>&1 & 
	wait_for_remaining "${tag}" 
	IFS=$OLDIFS
}
grant_group_function()
{
	prefix="grant_group_function"
	i="0"
	previous_schema_name=""
	previous_proname=""
	previous_proparams=""
	exec_sql=""
	OLDIFS=$IFS
	IFS=$'\n'
	obj_count=$(psql -h $SOURCE_PGHOST -p $SOURCE_PGPORT -d $SOURCE_PGDATABASE -U $SOURCE_PGUSER -t -A -c "SELECT COUNT(DISTINCT sub2.nspname || sub2.proname || coalesce(sub2.proargs, '')) FROM (SELECT sub.nspname, sub.proname, sub.proargs, split_part(split_part(array_to_string(sub.proacl, ','), ',', i), ' ', 2) AS acl FROM (SELECT n.nspname, p.proname, oidvectortypes(p.proargtypes) proargs, generate_series(1, array_upper(p.proacl, 1)) AS i, p.proacl FROM pg_proc p JOIN pg_namespace n ON p.pronamespace = n.oid JOIN pg_language l ON p.prolang = l.oid JOIN pg_user u ON p.proowner = u.usesysid WHERE l.lanname IN ('sql', 'plpythonu') AND u.usename <> 'rdsdb' AND p.proacl IS NOT NULL) AS sub WHERE split_part(array_to_string(sub.proacl, ','), ',', i) LIKE 'group %') AS sub2 WHERE sub2.nspname NOT IN ${EXCLUDED_SCHEMAS}")
	echo "INFO: ${prefix}:creating ${obj_count}"
	for x in $(psql -h $SOURCE_PGHOST -p $SOURCE_PGPORT -d $SOURCE_PGDATABASE -U $SOURCE_PGUSER -t -A -c "SELECT sub2.nspname, sub2.proname, sub2.proargs, split_part(sub2.acl, '=', 1) AS groname, split_part(split_part(sub2.acl, '=', 2), '/', 1) AS grogrant FROM (SELECT sub.nspname, sub.proname, sub.proargs, split_part(split_part(array_to_string(sub.proacl, ','), ',', i), ' ', 2) AS acl FROM (SELECT n.nspname, p.proname, oidvectortypes(p.proargtypes) proargs, generate_series(1, array_upper(p.proacl, 1)) AS i, p.proacl FROM pg_proc p JOIN pg_namespace n ON p.pronamespace = n.oid JOIN pg_language l ON p.prolang = l.oid JOIN pg_user u ON p.proowner = u.usesysid WHERE l.lanname IN ('sql', 'plpythonu') AND u.usename <> 'rdsdb' AND p.proacl IS NOT NULL) AS sub WHERE split_part(array_to_string(sub.proacl, ','), ',', i) LIKE 'group %') AS sub2 WHERE sub2.nspname NOT IN ${EXCLUDED_SCHEMAS} ORDER BY 1, 2"); do
		schema_name=$(echo ${x} | awk -F '|' '{print $1}')
		proname=$(echo ${x} | awk -F '|' '{print $2}')
		proparams=$(echo ${x} | awk -F '|' '{print $3}')
		group_name=$(echo ${x} | awk -F '|' '{print $4}')
		group_grant=$(echo ${x} | awk -F '|' '{print $4}')
		if [ "${i}" -gt "0" ]; then
			if [[ ! "${schema_name}" == "${previous_schema_name}" || ! "${proname}" == "${previous_proname}" || ! "${proparams}" == "${previous_proparams}" ]]; then
				wait_for_threads ${tag}
				echo "INFO: ${prefix}:${i}:${obj_count}:${previous_schema_name}.${previous_proname}"
				psql -h $TARGET_PGHOST -p $TARGET_PGPORT -d $TARGET_PGDATABASE -U $TARGET_PGUSER -f "${exec_sql}" -v tag=${tag} -e > $PWD/log/${prefix}_${i}.log 2>&1 & 
			fi
		fi
		if [[ ! "${schema_name}" == "${previous_schema_name}" || ! "${proname}" == "${previous_proname}" || ! "${proparams}" == "${previous_proparams}" ]]; then
			i=$((i+1))
			exec_sql="${exec_dir}/${prefix}_${i}.sql"
		fi
		sql_cmd="GRANT EXECUTE ON FUNCTION \"${schema_name}\".\"${proname}\"(${proparams}) TO GROUP \"${group_name}\";"
		echo "${sql_cmd}" >> ${exec_sql}
		previous_schema_name="${schema_name}"
		previous_proname="${proname}"
		previous_proparams="${proparams}"
	done
	wait_for_threads ${tag}
	echo "INFO: ${prefix}:${i}:${obj_count}:${schema_name}.${proname}"
	psql -h $TARGET_PGHOST -p $TARGET_PGPORT -d $TARGET_PGDATABASE -U $TARGET_PGUSER -f "${exec_sql}" -v tag=${tag} -e > $PWD/log/${prefix}_${i}.log 2>&1 & 
	wait_for_remaining "${tag}" 
	IFS=$OLDIFS
}
grant_group_procedure()
{
	prefix="grant_group_procedure"
	i="0"
	previous_schema_name=""
	previous_proname=""
	previous_proparams=""
	exec_sql=""
	OLDIFS=$IFS
	IFS=$'\n'
	obj_count=$(psql -h $SOURCE_PGHOST -p $SOURCE_PGPORT -d $SOURCE_PGDATABASE -U $SOURCE_PGUSER -t -A -c "SELECT COUNT(DISTINCT sub2.nspname || sub2.proname || coalesce(sub2.proargs, '')) FROM (SELECT sub.nspname, sub.proname, sub.proargs, split_part(split_part(array_to_string(sub.proacl, ','), ',', i), ' ', 2) AS acl FROM (SELECT n.nspname, p.proname, oidvectortypes(p.proargtypes) proargs, generate_series(1, array_upper(p.proacl, 1)) AS i, p.proacl FROM pg_proc p JOIN pg_namespace n ON p.pronamespace = n.oid JOIN pg_language l ON p.prolang = l.oid JOIN pg_user u ON p.proowner = u.usesysid WHERE l.lanname = 'plpgsql' AND u.usename <> 'rdsdb' AND p.proacl IS NOT NULL) AS sub WHERE split_part(array_to_string(sub.proacl, ','), ',', i) LIKE 'group %') AS sub2 WHERE sub2.nspname NOT IN ${EXCLUDED_SCHEMAS}")
	echo "INFO: ${prefix}:creating ${obj_count}"
	for x in $(psql -h $SOURCE_PGHOST -p $SOURCE_PGPORT -d $SOURCE_PGDATABASE -U $SOURCE_PGUSER -t -A -c "SELECT sub2.nspname, sub2.proname, sub2.proargs, split_part(sub2.acl, '=', 1) AS groname, split_part(split_part(sub2.acl, '=', 2), '/', 1) AS grogrant FROM (SELECT sub.nspname, sub.proname, sub.proargs, split_part(split_part(array_to_string(sub.proacl, ','), ',', i), ' ', 2) AS acl FROM (SELECT n.nspname, p.proname, oidvectortypes(p.proargtypes) proargs, generate_series(1, array_upper(p.proacl, 1)) AS i, p.proacl FROM pg_proc p JOIN pg_namespace n ON p.pronamespace = n.oid JOIN pg_language l ON p.prolang = l.oid JOIN pg_user u ON p.proowner = u.usesysid WHERE l.lanname = 'plpgsql' AND u.usename <> 'rdsdb' AND p.proacl IS NOT NULL) AS sub WHERE split_part(array_to_string(sub.proacl, ','), ',', i) LIKE 'group %') AS sub2 WHERE sub2.nspname NOT IN ${EXCLUDED_SCHEMAS} ORDER BY 1, 2"); do
		schema_name=$(echo ${x} | awk -F '|' '{print $1}')
		proname=$(echo ${x} | awk -F '|' '{print $2}')
		proparams=$(echo ${x} | awk -F '|' '{print $3}')
		group_name=$(echo ${x} | awk -F '|' '{print $4}')
		group_grant=$(echo ${x} | awk -F '|' '{print $4}')
		if [ "${i}" -gt "0" ]; then
			if [[ ! "${schema_name}" == "${previous_schema_name}" || ! "${proname}" == "${previous_proname}" || ! "${proparams}" == "${previous_proparams}" ]]; then
				wait_for_threads ${tag}
				echo "INFO: ${prefix}:${i}:${obj_count}:${previous_schema_name}.${previous_proname}"
				psql -h $TARGET_PGHOST -p $TARGET_PGPORT -d $TARGET_PGDATABASE -U $TARGET_PGUSER -f "${exec_sql}" -v tag=${tag} -e > $PWD/log/${prefix}_${i}.log 2>&1 & 
			fi
		fi
		if [[ ! "${schema_name}" == "${previous_schema_name}" || ! "${proname}" == "${previous_proname}" || ! "${proparams}" == "${previous_proparams}" ]]; then
			i=$((i+1))
			exec_sql="${exec_dir}/${prefix}_${i}.sql"
		fi
		sql_cmd="GRANT EXECUTE ON PROCEDURE \"${schema_name}\".\"${proname}\"(${proparams}) TO GROUP \"${group_name}\";"
		echo "${sql_cmd}" >> ${exec_sql}
		previous_schema_name="${schema_name}"
		previous_proname="${proname}"
		previous_proparams="${proparams}"
	done
	wait_for_threads ${tag}
	echo "INFO: ${prefix}:${i}:${obj_count}:${schema_name}.${proname}"
	psql -h $TARGET_PGHOST -p $TARGET_PGPORT -d $TARGET_PGDATABASE -U $TARGET_PGUSER -f "${exec_sql}" -v tag=${tag} -e > $PWD/log/${prefix}_${i}.log 2>&1 & 
	wait_for_remaining "${tag}" 
	IFS=$OLDIFS
}
grant_default_schema_user()
{
	prefix="grant_default_schema_user"
	i="0"
	previous_schema_name=""
	exec_sql=""
	OLDIFS=$IFS
	IFS=$'\n'
	obj_count=$(psql -h $SOURCE_PGHOST -p $SOURCE_PGPORT -d $SOURCE_PGDATABASE -U $SOURCE_PGUSER -t -A -c "SELECT COUNT(DISTINCT sub2.nspname) FROM (SELECT sub.defaclobjtype, sub.nspname, split_part(array_to_string(sub.defaclacl, ','), ',', i) AS acl FROM (SELECT d.defaclobjtype, n.nspname, generate_series(1, array_upper(d.defaclacl, 1)) AS i, d.defaclacl FROM pg_default_acl d JOIN pg_namespace n ON d.defaclnamespace = n.oid) AS sub WHERE split_part(array_to_string(sub.defaclacl, ','), ',', i) NOT LIKE 'group %') AS sub2 WHERE sub2.nspname NOT IN ${EXCLUDED_SCHEMAS}")
	echo "INFO: ${prefix}:creating ${obj_count}"
	for x in $(psql -h $SOURCE_PGHOST -p $SOURCE_PGPORT -d $SOURCE_PGDATABASE -U $SOURCE_PGUSER -t -A -c "SELECT sub2.nspname, sub2.defaclobjtype, split_part(sub2.acl, '=', 1) AS usename, split_part(split_part(sub2.acl, '=', 2), '/', 1) AS usegrant FROM (SELECT sub.defaclobjtype, sub.nspname, split_part(array_to_string(sub.defaclacl, ','), ',', i) AS acl FROM (SELECT d.defaclobjtype, n.nspname, generate_series(1, array_upper(d.defaclacl, 1)) AS i, d.defaclacl FROM pg_default_acl d JOIN pg_namespace n ON d.defaclnamespace = n.oid) AS sub WHERE split_part(array_to_string(sub.defaclacl, ','), ',', i) NOT LIKE 'group %') AS sub2 WHERE sub2.nspname NOT IN ${EXCLUDED_SCHEMAS} ORDER BY 1, 2, 3"); do
		schema_name=$(echo ${x} | awk -F '|' '{print $1}')
		object_type=$(echo ${x} | awk -F '|' '{print $2}')
		grantee=$(echo ${x} | awk -F '|' '{print $3}')
		use_grant=$(echo ${x} | awk -F '|' '{print $4}')
		grant_count=$(echo -n "${use_grant}" | wc -m)
		#change to 0 base
		grant_count=$((grant_count-1))
		counter="0"
		if [ "${i}" -gt "0" ]; then
			if [ ! "${schema_name}" == "${previous_schema_name}" ]; then
				wait_for_threads ${tag}
				echo "INFO: ${prefix}:${i}:${obj_count}:${previous_schema_name}"
				psql -h $TARGET_PGHOST -p $TARGET_PGPORT -d $TARGET_PGDATABASE -U $TARGET_PGUSER -f "${exec_sql}" -v tag=${tag} -e > $PWD/log/${prefix}_${i}.log 2>&1 & 
			fi
		fi
		if [ ! "${schema_name}" == "${previous_schema_name}" ]; then
			i=$((i+1))
			exec_sql="${exec_dir}/${prefix}_${i}.sql"
		fi
		for y in $(seq 0 ${grant_count}); do
			grant=${use_grant:y:1}
			#r=tables
			if [ "${object_type}" == "r" ]; then
				if [ "${grant}" == "a" ]; then
					grant_action="INSERT"
				elif [ "${grant}" == "w" ]; then
					grant_action="UPDATE"
				elif [ "${grant}" == "d" ]; then
					grant_action="DELETE"
				elif [ "${grant}" == "r" ]; then
					grant_action="SELECT"
				elif [ "${grant}" == "x" ]; then
					grant_action="REFERENCES"
				elif [ "${grant}" == "D" ]; then
					grant_action="DROP"
				elif [ "${grant}" == "t" ]; then
					grant_action="TRIGGER"
				elif [ "${grant}" == "R" ]; then
					grant_action="RULE"
				else
					grant_action="NONE"
				fi
			#f=functions; p=procedures
			elif [[ "${object_type}" == "f" || "${object_type}" == "p" ]]; then
				if [ "${grant}" == "X" ]; then
					grant_action="EXECUTE"
				else
					grant_action="NONE"
				fi
			fi
			if [ ! "${grant_action}" == "NONE" ]; then
				counter=$((counter+1))
			fi
			if [ "${counter}" -eq "1" ]; then
				sql_cmd="ALTER DEFAULT PRIVILEGES IN SCHEMA \"${schema_name}\" GRANT ${grant_action}"
			else
				sql_cmd+=", ${grant_action}"
			fi;
		done
		if [ "${counter}" -gt "0" ]; then
			if [ "${object_type}" == "r" ]; then
				sql_cmd+=" ON TABLES TO \"${grantee}\";"
			elif [ "${object_type}" == "f" ]; then
				sql_cmd+=" ON FUNCTIONS TO \"${grantee}\";"
			elif [ "${object_type}" == "p" ]; then
				sql_cmd+=" ON PROCEDURES TO \"${grantee}\";"
			else
				sql_cmd=""
			fi
			echo "${sql_cmd}" >> ${exec_sql}
		fi
		previous_schema_name="${schema_name}"
	done
	wait_for_threads ${tag}
	echo "INFO: ${prefix}:${i}:${obj_count}:${schema_name}"
	psql -h $TARGET_PGHOST -p $TARGET_PGPORT -d $TARGET_PGDATABASE -U $TARGET_PGUSER -f "${exec_sql}" -v tag=${tag} -e > $PWD/log/${prefix}_${i}.log 2>&1 & 
	wait_for_remaining "${tag}" 
	IFS=$OLDIFS
}
grant_default_schema_group()
{
	prefix="grant_default_schema_group"
	i="0"
	previous_schema_name=""
	exec_sql=""
	OLDIFS=$IFS
	IFS=$'\n'
	obj_count=$(psql -h $SOURCE_PGHOST -p $SOURCE_PGPORT -d $SOURCE_PGDATABASE -U $SOURCE_PGUSER -t -A -c "SELECT COUNT(DISTINCT sub2.nspname) FROM (SELECT sub.defaclobjtype, sub.nspname, split_part(array_to_string(sub.defaclacl, ','), ',', i) AS acl FROM (SELECT d.defaclobjtype, n.nspname, generate_series(1, array_upper(d.defaclacl, 1)) AS i, d.defaclacl FROM pg_default_acl d JOIN pg_namespace n ON d.defaclnamespace = n.oid) AS sub WHERE split_part(array_to_string(sub.defaclacl, ','), ',', i) LIKE 'group %') AS sub2 WHERE sub2.nspname NOT IN ${EXCLUDED_SCHEMAS}")
	echo "INFO: ${prefix}:creating ${obj_count}"
	for x in $(psql -h $SOURCE_PGHOST -p $SOURCE_PGPORT -d $SOURCE_PGDATABASE -U $SOURCE_PGUSER -t -A -c "SELECT sub2.nspname, sub2.defaclobjtype, split_part(split_part(sub2.acl, '=', 1), ' ', 2) AS groname, split_part(split_part(sub2.acl, '=', 2), '/', 1) AS grogrant FROM (SELECT sub.defaclobjtype, sub.nspname, split_part(array_to_string(sub.defaclacl, ','), ',', i) AS acl FROM (SELECT d.defaclobjtype, n.nspname, generate_series(1, array_upper(d.defaclacl, 1)) AS i, d.defaclacl FROM pg_default_acl d JOIN pg_namespace n ON d.defaclnamespace = n.oid) AS sub WHERE split_part(array_to_string(sub.defaclacl, ','), ',', i) LIKE 'group %') AS sub2 WHERE sub2.nspname NOT IN ${EXCLUDED_SCHEMAS} ORDER BY 1, 2, 3"); do
		schema_name=$(echo ${x} | awk -F '|' '{print $1}')
		object_type=$(echo ${x} | awk -F '|' '{print $2}')
		group_name=$(echo ${x} | awk -F '|' '{print $3}')
		group_grant=$(echo ${x} | awk -F '|' '{print $4}')
		grant_count=$(echo -n "${group_grant}" | wc -m)
		#change to 0 base
		grant_count=$((grant_count-1))
		counter="0"
		if [ "${i}" -gt "0" ]; then
			if [ ! "${schema_name}" == "${previous_schema_name}" ]; then
				wait_for_threads ${tag}
				echo "INFO: ${prefix}:${i}:${obj_count}:${previous_schema_name}"
				psql -h $TARGET_PGHOST -p $TARGET_PGPORT -d $TARGET_PGDATABASE -U $TARGET_PGUSER -f "${exec_sql}" -v tag=${tag} -e > $PWD/log/${prefix}_${i}.log 2>&1 & 
			fi
		fi
		if [ ! "${schema_name}" == "${previous_schema_name}" ]; then
			i=$((i+1))
			exec_sql="${exec_dir}/${prefix}_${i}.sql"
		fi
		for y in $(seq 0 ${grant_count}); do
			grant=${group_grant:y:1}
			#r=tables
			if [ "${object_type}" == "r" ]; then
				if [ "${grant}" == "a" ]; then
					grant_action="INSERT"
				elif [ "${grant}" == "w" ]; then
					grant_action="UPDATE"
				elif [ "${grant}" == "d" ]; then
					grant_action="DELETE"
				elif [ "${grant}" == "r" ]; then
					grant_action="SELECT"
				elif [ "${grant}" == "x" ]; then
					grant_action="REFERENCES"
				elif [ "${grant}" == "D" ]; then
					grant_action="DROP"
				elif [ "${grant}" == "t" ]; then
					grant_action="TRIGGER"
				elif [ "${grant}" == "R" ]; then
					grant_action="RULE"
				else
					grant_action="NONE"
				fi
			#f=functions; p=procedures
			elif [[ "${object_type}" == "f" || "${object_type}" == "p" ]]; then
				if [ "${grant}" == "X" ]; then
					grant_action="EXECUTE"
				else
					grant_action="NONE"
				fi
			fi
			if [ ! "${grant_action}" == "NONE" ]; then
				counter=$((counter+1))
			fi
			if [ "${counter}" -eq "1" ]; then
				sql_cmd="ALTER DEFAULT PRIVILEGES IN SCHEMA \"${schema_name}\" GRANT ${grant_action}"
			else
				sql_cmd+=", ${grant_action}"
			fi;
		done
		if [ "${counter}" -gt "0" ]; then
			if [ "${object_type}" == "r" ]; then
				sql_cmd+=" ON TABLES TO GROUP \"${group_name}\";"
			elif [ "${object_type}" == "f" ]; then
				sql_cmd+=" ON FUNCTIONS TO GROUP \"${group_name}\";"
			elif [ "${object_type}" == "p" ]; then
				sql_cmd+=" ON PROCEDURES TO GROUP \"${group_name}\";"
			else
				sql_cd=""
			fi
			echo "${sql_cmd}" >> ${exec_sql}
		fi
		previous_schema_name="${schema_name}"
	done
	wait_for_threads ${tag}
	echo "INFO: ${prefix}:${i}:${obj_count}:${schema_name}"
	psql -h $TARGET_PGHOST -p $TARGET_PGPORT -d $TARGET_PGDATABASE -U $TARGET_PGUSER -f "${exec_sql}" -v tag=${tag} -e > $PWD/log/${prefix}_${i}.log 2>&1 & 
	wait_for_remaining "${tag}" 
	IFS=$OLDIFS
}
grant_default_user_user()
{
	prefix="grant_default_user_user"
	i="0"
	previous_usename=""
	exec_sql=""
        OLDIFS=$IFS
        IFS=$'\n'
	obj_count=$(psql -h $SOURCE_PGHOST -p $SOURCE_PGPORT -d $SOURCE_PGDATABASE -U $SOURCE_PGUSER -t -A -c "SELECT COUNT(DISTINCT split_part(split_part(sub2.acl, '=', 2), '/', 2)) FROM (SELECT sub.defaclobjtype, split_part(array_to_string(sub.defaclacl, ','), ',', i) AS acl FROM (SELECT d.defaclobjtype, generate_series(1, array_upper(d.defaclacl, 1)) AS i, d.defaclacl FROM pg_default_acl d WHERE defaclnamespace = 0) AS sub WHERE split_part(array_to_string(sub.defaclacl, ','), ',', i) NOT LIKE 'group %') AS sub2 WHERE split_part(sub2.acl, '=', 1) <> split_part(split_part(sub2.acl, '=', 2), '/', 2) AND split_part(sub2.acl, '=', 1) <> ''")
	echo "INFO: ${prefix}:creating ${obj_count}"
        for x in $(psql -h $SOURCE_PGHOST -p $SOURCE_PGPORT -d $SOURCE_PGDATABASE -U $SOURCE_PGUSER -t -A -c "SELECT split_part(sub2.acl, '=', 1) AS grantee, sub2.defaclobjtype, split_part(split_part(sub2.acl, '=', 2), '/', 1) AS usegrant, split_part(split_part(sub2.acl, '=', 2), '/', 2) AS usename FROM (SELECT sub.defaclobjtype, split_part(array_to_string(sub.defaclacl, ','), ',', i) AS acl FROM (SELECT d.defaclobjtype, generate_series(1, array_upper(d.defaclacl, 1)) AS i, d.defaclacl FROM pg_default_acl d WHERE defaclnamespace = 0) AS sub WHERE split_part(array_to_string(sub.defaclacl, ','), ',', i) NOT LIKE 'group %') AS sub2 WHERE split_part(sub2.acl, '=', 1) <> split_part(split_part(sub2.acl, '=', 2), '/', 2) AND split_part(sub2.acl, '=', 1) <> '' ORDER BY 1, 2;"); do
		grantee=$(echo ${x} | awk -F '|' '{print $1}')
		object_type=$(echo ${x} | awk -F '|' '{print $2}')
		use_grant=$(echo ${x} | awk -F '|' '{print $3}')
		usename=$(echo ${x} | awk -F '|' '{print $4}')
		grant_count=$(echo -n "${use_grant}" | wc -m)
		#change to 0 base
		grant_count=$((grant_count-1))
		counter="0"
		if [ "${i}" -gt "0" ]; then
			if [ ! "${usename}" == "${previous_usename}" ]; then
				wait_for_threads ${tag}
				echo "INFO: ${prefix}:${i}:${obj_count}:${previous_usename}"
				psql -h $TARGET_PGHOST -p $TARGET_PGPORT -d $TARGET_PGDATABASE -U $TARGET_PGUSER -f "${exec_sql}" -v tag=${tag} -e > $PWD/log/${prefix}_${i}.log 2>&1 & 
			fi
		fi
		if [ ! "${usename}" == "${previous_usename}" ]; then
			i=$((i+1))
			exec_sql="${exec_dir}/${prefix}_${i}.sql"
		fi
		for y in $(seq 0 ${grant_count}); do
			grant=${use_grant:y:1}
			#r=tables
			if [ "${object_type}" == "r" ]; then
				if [ "${grant}" == "a" ]; then
					grant_action="INSERT"
				elif [ "${grant}" == "w" ]; then
					grant_action="UPDATE"
				elif [ "${grant}" == "d" ]; then
					grant_action="DELETE"
				elif [ "${grant}" == "r" ]; then
					grant_action="SELECT"
				elif [ "${grant}" == "x" ]; then
					grant_action="REFERENCES"
				elif [ "${grant}" == "D" ]; then
					grant_action="DROP"
				elif [ "${grant}" == "t" ]; then
					grant_action="TRIGGER"
				elif [ "${grant}" == "R" ]; then
					grant_action="RULE"
				else
					grant_action="NONE"
				fi
			#f=functions; p=procedures
			elif [[ "${object_type}" == "f" || "${object_type}" == "p" ]]; then
				if [ "${grant}" == "X" ]; then
					grant_action="EXECUTE"
				else
					grant_action="NONE"
				fi
			fi
			if [ ! "${grant_action}" == "NONE" ]; then
				counter=$((counter+1))
			fi
			if [ "${counter}" -eq "1" ]; then
				sql_cmd="ALTER DEFAULT PRIVILEGES FOR USER \"${usename}\" GRANT ${grant_action}"
			else
				sql_cmd+=", ${grant_action}"
			fi;
		done
		if [ "${counter}" -gt "0" ]; then
			if [ "${object_type}" == "r" ]; then
				sql_cmd+=" ON TABLES TO \"${grantee}\";"
			elif [ "${object_type}" == "f" ]; then
				sql_cmd+=" ON FUNCTIONS TO \"${grantee}\";"
			elif [ "${object_type}" == "p" ]; then
				sql_cmd+=" ON PROCEDURES TO \"${grantee}\";"
			else
				sql_cmd=""
			fi
			echo "${sql_cmd}" >> ${exec_sql}
		fi
		previous_usename="${usename}"
	done
	wait_for_threads ${tag}
	echo "INFO: ${prefix}:${i}:${obj_count}:${usename}"
	psql -h $TARGET_PGHOST -p $TARGET_PGPORT -d $TARGET_PGDATABASE -U $TARGET_PGUSER -f "${exec_sql}" -v tag=${tag} -e > $PWD/log/${prefix}_${i}.log 2>&1 & 
	wait_for_remaining "${tag}" 
	IFS=$OLDIFS
}
grant_default_user_group()
{
	prefix="grant_default_user_group"
	i="0"
	previous_group_name=""
	exec_sql=""
        OLDIFS=$IFS
        IFS=$'\n'
	obj_count=$(psql -h $SOURCE_PGHOST -p $SOURCE_PGPORT -d $SOURCE_PGDATABASE -U $SOURCE_PGUSER -t -A -c "SELECT COUNT(DISTINCT split_part(split_part(sub2.acl, '=', 1), ' ', 2)) FROM (SELECT sub.defaclobjtype, split_part(array_to_string(sub.defaclacl, ','), ',', i) AS acl FROM (SELECT d.defaclobjtype, generate_series(1, array_upper(d.defaclacl, 1)) AS i, d.defaclacl FROM pg_default_acl d WHERE defaclnamespace = 0) AS sub WHERE split_part(array_to_string(sub.defaclacl, ','), ',', i) LIKE 'group %') AS sub2")
	echo "INFO: ${prefix}:creating ${obj_count}"
        for x in $(psql -h $SOURCE_PGHOST -p $SOURCE_PGPORT -d $SOURCE_PGDATABASE -U $SOURCE_PGUSER -t -A -c "SELECT split_part(split_part(sub2.acl, '=', 1), ' ', 2) AS group_name, sub2.defaclobjtype, split_part(split_part(sub2.acl, '=', 2), '/', 1) AS grogrant, split_part(split_part(sub2.acl, '=', 2), '/', 2) AS usename FROM (SELECT sub.defaclobjtype, split_part(array_to_string(sub.defaclacl, ','), ',', i) AS acl FROM (SELECT d.defaclobjtype, generate_series(1, array_upper(d.defaclacl, 1)) AS i, d.defaclacl FROM pg_default_acl d WHERE defaclnamespace = 0) AS sub WHERE split_part(array_to_string(sub.defaclacl, ','), ',', i) LIKE 'group %') AS sub2 ORDER BY 1, 2;"); do
		group_name=$(echo ${x} | awk -F '|' '{print $1}')
		object_type=$(echo ${x} | awk -F '|' '{print $2}')
		group_grant=$(echo ${x} | awk -F '|' '{print $3}')
		usename=$(echo ${x} | awk -F '|' '{print $4}')
		grant_count=$(echo -n "${group_grant}" | wc -m)
		#change to 0 base
		grant_count=$((grant_count-1))
		counter="0"
		if [ "${i}" -gt "0" ]; then
			if [ ! "${group_name}" == "${previous_group_name}" ]; then
				wait_for_threads ${tag}
				echo "INFO: ${prefix}:${i}:${obj_count}:${previous_group_name}"
				psql -h $TARGET_PGHOST -p $TARGET_PGPORT -d $TARGET_PGDATABASE -U $TARGET_PGUSER -f "${exec_sql}" -v tag=${tag} -e > $PWD/log/${prefix}_${i}.log 2>&1 & 
			fi
		fi
		if [ ! "${group_name}" == "${previous_group_name}" ]; then
			i=$((i+1))
			exec_sql="${exec_dir}/${prefix}_${i}.sql"
		fi
		for y in $(seq 0 ${grant_count}); do
			grant=${group_grant:y:1}
			#r=tables
			if [ "${object_type}" == "r" ]; then
				if [ "${grant}" == "a" ]; then
					grant_action="INSERT"
				elif [ "${grant}" == "w" ]; then
					grant_action="UPDATE"
				elif [ "${grant}" == "d" ]; then
					grant_action="DELETE"
				elif [ "${grant}" == "r" ]; then
					grant_action="SELECT"
				elif [ "${grant}" == "x" ]; then
					grant_action="REFERENCES"
				elif [ "${grant}" == "D" ]; then
					grant_action="DROP"
				elif [ "${grant}" == "t" ]; then
					grant_action="TRIGGER"
				elif [ "${grant}" == "R" ]; then
					grant_action="RULE"
				else
					grant_action="NONE"
				fi
			#f=functions; p=procedures
			elif [[ "${object_type}" == "f" || "${object_type}" == "p" ]]; then
				if [ "${grant}" == "X" ]; then
					grant_action="EXECUTE"
				else
					grant_action="NONE"
				fi
			fi
			if [ ! "${grant_action}" == "NONE" ]; then
				counter=$((counter+1))
			fi
			if [ "${counter}" -eq "1" ]; then
				sql_cmd="ALTER DEFAULT PRIVILEGES FOR USER \"${usename}\" GRANT ${grant_action}"
			else
				sql_cmd+=", ${grant_action}"
			fi;
		done
		if [ "${counter}" -gt "0" ]; then
			if [ "${object_type}" == "r" ]; then
				sql_cmd+=" ON TABLES TO GROUP \"${group_name}\";"
			elif [ "${object_type}" == "f" ]; then
				sql_cmd+=" ON FUNCTIONS TO GROUP \"${group_name}\";"
			elif [ "${object_type}" == "p" ]; then
				sql_cmd+=" ON PROCEDURES TO GROUP \"${group_name}\";"
			else
				sql_cmd=""
			fi
			echo "${sql_cmd}" >> ${exec_sql}
		fi
		previous_group_name="${group_name}"
	done
	wait_for_threads ${tag}
	echo "INFO: ${prefix}:${i}:${obj_count}:${group_name}"
	psql -h $TARGET_PGHOST -p $TARGET_PGPORT -d $TARGET_PGDATABASE -U $TARGET_PGUSER -f "${exec_sql}" -v tag=${tag} -e > $PWD/log/${prefix}_${i}.log 2>&1 & 
	wait_for_remaining "${tag}" 
	IFS=$OLDIFS
}

##Owners
alter_schema_owner
alter_table_owner
alter_function_owner
alter_procedure_owner

##Grant: Users
grant_user_schema
grant_user_table
grant_user_function
grant_user_procedure

##Grant: Groups
grant_group_schema
grant_group_table
grant_group_function
grant_group_procedure

##Defaults
grant_default_schema_user
grant_default_schema_group
grant_default_user_user
grant_default_user_group

echo "INFO: Migrate permissions step complete"
