#!/bin/bash
set -e

source ${PWD}/config.sh
source ${PWD}/common.sh

exec_dir="exec_permissions"
rm -rf $PWD/${exec_dir}
mkdir -p $PWD/${exec_dir}
tmp_password="P@ssword1"
expire_password=$(date +%Y-%m-%d)

get_params()
{
	params="("
	for y in $(seq 1 ${param_count}); do
		if [ "${y}" -eq "1" ]; then
			param=$(psql -h $SOURCE_PGHOST -p $SOURCE_PGPORT -d $SOURCE_PGDATABASE -U $SOURCE_PGUSER -t -A -c "
			SELECT CASE 
				WHEN p.proargmodes IS NULL THEN ' IN' 
				WHEN (p.proargmodes[${y}]) = 'i' THEN ' IN' 
				WHEN (p.proargmodes[${y}]) = 'o' THEN ' OUT' 
				WHEN (p.proargmodes[${y}]) = 'b' THEN ' INOUT' END || ' ' || 
				COALESCE(p.proargnames[${y}], '') || ' ' || COALESCE(t.typname, split_part(oidvectortypes(p.proargtypes), ',', ${y})) 
			FROM pg_proc_info p 
			LEFT JOIN pg_type t ON t.oid = p.proallargtypes[${y}] 
			WHERE p.prooid = ${oid}")
			params+="${param}"
		else
			param=$(psql -h $SOURCE_PGHOST -p $SOURCE_PGPORT -d $SOURCE_PGDATABASE -U $SOURCE_PGUSER -t -A -c "
			SELECT CASE 
				WHEN p.proargmodes IS NULL THEN ' IN' 
				WHEN (p.proargmodes[${y}]) = 'i' THEN ' IN' 
				WHEN (p.proargmodes[${y}]) = 'o' THEN ' OUT' 
				WHEN (p.proargmodes[${y}]) = 'b' THEN ' INOUT' END || ' ' || 
				COALESCE(p.proargnames[${y}], '') || ' ' || COALESCE(t.typname, split_part(oidvectortypes(p.proargtypes), ',', ${y})) 
			FROM pg_proc_info p 
			LEFT JOIN pg_type t ON t.oid = p.proallargtypes[${y}] WHERE p.prooid = ${oid}")
			params+=", ${param}"
		fi
	done
	params+=")"
}
get_grant_actions()
{
	#calling function sets grant_count and use_grant 
	#return grants
	grants=""
	for y in $(seq 0 ${grant_count}); do
		grant=${all_grants:y:1}
		if [ "${grant}" == "U" ]; then
			grant_action="USAGE"
		elif [ "${grant}" == "C" ]; then
			grant_action="CREATE"
		elif [ "${grant}" == "a" ]; then
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
		elif [ "${grant}" == "X" ]; then
			grant_action="EXECUTE"
		elif [ "${grant}" == "D" ]; then
			grant_action="DROP"
		fi
		if [ "${y}" -eq "0" ]; then
			grants="${grant_action}"
		else
			grants+=", ${grant_action}"
		fi
	done
}
get_public_check()
{
	public_check=$(psql -h $SOURCE_PGHOST -p $SOURCE_PGPORT -d $SOURCE_PGDATABASE -U $SOURCE_PGUSER -t -A -c "
	SELECT COUNT(*) 
	FROM 	(
		SELECT nspname, split_part(nspacl, '=', 1) AS grantee, split_part(split_part(nspacl, '=', 2), '/', 1) AS nspacl 
		FROM 	(
			SELECT nspname, split_part(array_to_string(nspacl, ','), ',', i) AS nspacl 
			FROM 	(
				SELECT nspname, generate_series(1, array_upper(nspacl, '1')) AS i, nspacl 
				FROM pg_namespace 
				WHERE nspname = '${schema_name}'
				)
			)
		) 
	WHERE grantee = '' AND nspacl = 'UC';")
}
alter_schema_owner()
{
	prefix="alter_schema_owner"
	OLDIFS=$IFS
	IFS=$'\n'
	i="0"
	obj_count=$(psql -h $SOURCE_PGHOST -p $SOURCE_PGPORT -d $SOURCE_PGDATABASE -U $SOURCE_PGUSER -t -A -c "
	SELECT COUNT(*) 
	FROM svv_all_schemas 
	WHERE database_name = current_database() AND schema_type = 'local' AND schema_name NOT IN ${EXCLUDED_SCHEMAS} AND pg_get_userbyid(schema_owner) <> 'rdsdb'")
	echo "INFO: ${prefix}:creating ${obj_count}"
	for x in $(psql -h $SOURCE_PGHOST -p $SOURCE_PGPORT -d $SOURCE_PGDATABASE -U $SOURCE_PGUSER -t -A -c "
	SELECT schema_name, pg_get_userbyid(schema_owner) 
	FROM svv_all_schemas 
	WHERE database_name = current_database() AND schema_type = 'local' AND schema_name NOT IN ${EXCLUDED_SCHEMAS} AND pg_get_userbyid(schema_owner) <> 'rdsdb'"); do
		i=$((i+1))
		exec_sql="${exec_dir}/${prefix}_${i}.sql"
		schema_name=$(echo ${x} | awk -F '|' '{print $1}')
		schema_owner=$(echo ${x} | awk -F '|' '{print $2}')
		sql_cmd="ALTER SCHEMA \"${schema_name}\" OWNER TO \"${schema_owner}\";"
		echo "${sql_cmd}" > "${exec_sql}"

		wait_for_threads "${exec_dir}"
		echo "INFO: ${prefix}:${i}:${obj_count}:${schema_name}"
		psql -h $TARGET_PGHOST -p $TARGET_PGPORT -d $TARGET_PGDATABASE -U $TARGET_PGUSER -f "${exec_sql}" -e > $PWD/log/${prefix}_${i}.log 2>&1 &
	done
	wait_for_remaining "${exec_dir}" 
	IFS=$OLDIFS
}
alter_table_owner()
{
	prefix="alter_table_owner"
	OLDIFS=$IFS
	IFS=$'\n'
	i="0"
	obj_count=$(psql -h $SOURCE_PGHOST -p $SOURCE_PGPORT -d $SOURCE_PGDATABASE -U $SOURCE_PGUSER -t -A -c "
	SELECT COUNT(*) 
	FROM pg_class c 
	JOIN pg_namespace n ON c.relnamespace = n.oid 
	JOIN pg_user u ON c.relowner = u.usesysid 
	JOIN svv_all_schemas s ON s.schema_name = n.nspname 
	JOIN (SELECT attrelid, MIN(attsortkeyord) AS min_attsortkeyord FROM pg_attribute GROUP BY attrelid) a ON a.attrelid = c.oid 
	WHERE s.database_name = current_database() AND s.schema_type = 'local' AND s.schema_name NOT IN ${EXCLUDED_SCHEMAS} AND relkind = 'r' AND c.relname NOT LIKE 'mv_tbl__%' AND a.min_attsortkeyord >= 0")
	echo "INFO: ${prefix}:creating ${obj_count}"
	for schema_name in $(psql -h $SOURCE_PGHOST -p $SOURCE_PGPORT -d $SOURCE_PGDATABASE -U $SOURCE_PGUSER -t -A -c "
	SELECT schema_name 
	FROM svv_all_schemas 
	WHERE database_name = current_database() AND schema_type = 'local' AND schema_name NOT IN ${EXCLUDED_SCHEMAS} 
	ORDER BY schema_name"); do
		for x in $(psql -h $SOURCE_PGHOST -p $SOURCE_PGPORT -d $SOURCE_PGDATABASE -U $SOURCE_PGUSER -t -A -c "
		SELECT c.relname, u.usename 
		FROM pg_class c 
		JOIN pg_namespace n ON c.relnamespace = n.oid 
		JOIN pg_user u ON c.relowner = u.usesysid 
		JOIN (SELECT attrelid, MIN(attsortkeyord) AS min_attsortkeyord FROM pg_attribute GROUP BY attrelid) a ON a.attrelid = c.oid 
		WHERE n.nspname = '${schema_name}' AND relkind = 'r' AND c.relname NOT LIKE 'mv_tbl__%' AND a.min_attsortkeyord >= 0 
		ORDER BY c.relname"); do
			i=$((i+1))
			exec_sql="${exec_dir}/${prefix}_${i}.sql"
			table_name=$(echo ${x} | awk -F '|' '{print $1}')
			table_owner=$(echo ${x} | awk -F '|' '{print $2}')
			sql_cmd="ALTER TABLE \"${schema_name}\".\"${table_name}\" OWNER TO \"${table_owner}\";"
			echo "${sql_cmd}" > "${exec_sql}"

			wait_for_threads "${exec_dir}"
			echo "INFO: ${prefix}:${i}:${obj_count}:${schema_name}.${table_name}"
			psql -h $TARGET_PGHOST -p $TARGET_PGPORT -d $TARGET_PGDATABASE -U $TARGET_PGUSER -f "${exec_sql}" -e > $PWD/log/${prefix}_${i}.log 2>&1 &
		done
	done
	wait_for_remaining "${exec_dir}"
	IFS=$OLDIFS
}
alter_function_owner()
{
	prefix="alter_function_owner"
	i="0"
	OLDIFS=$IFS
	IFS=$'\n'
	obj_count=$(psql -h $SOURCE_PGHOST -p $SOURCE_PGPORT -d $SOURCE_PGDATABASE -U $SOURCE_PGUSER -t -A -c "
	SELECT COUNT(*) 
	FROM pg_proc_info p 
	JOIN pg_namespace n ON p.pronamespace = n.oid 
	JOIN pg_language l ON p.prolang = l.oid 
	JOIN pg_user u ON p.proowner = u.usesysid 
	JOIN svv_all_schemas s ON s.schema_name = n.nspname 
	WHERE s.database_name = current_database() AND s.schema_type = 'local' AND s.schema_name NOT IN ${EXCLUDED_SCHEMAS} AND l.lanname IN ('sql', 'plpythonu') AND u.usename <> 'rdsdb' AND p.prokind = 'f'")
	echo "INFO: ${prefix}:creating ${obj_count}"
	for schema_name in $(psql -h $SOURCE_PGHOST -p $SOURCE_PGPORT -d $SOURCE_PGDATABASE -U $SOURCE_PGUSER -t -A -c "
	SELECT schema_name 
	FROM svv_all_schemas 
	WHERE database_name = current_database() AND schema_type = 'local' AND schema_name NOT IN ${EXCLUDED_SCHEMAS} 
	ORDER BY schema_name"); do
		for x in $(psql -h $SOURCE_PGHOST -p $SOURCE_PGPORT -d $SOURCE_PGDATABASE -U $SOURCE_PGUSER -t -A -c "
		SELECT p.prooid, p.proname, pg_get_userbyid(p.proowner), CASE WHEN p.proallargtypes IS NULL THEN CASE WHEN oidvectortypes(p.proargtypes) = '' THEN 0 ELSE regexp_count(oidvectortypes(p.proargtypes), ',')+1 END ELSE array_upper(proallargtypes, 1) END AS param_count 
		FROM pg_proc_info p 
		JOIN pg_namespace n ON p.pronamespace = n.oid 
		JOIN pg_language l ON p.prolang = l.oid 
		JOIN pg_user u ON p.proowner = u.usesysid 
		WHERE n.nspname = '${schema_name}' AND l.lanname IN ('sql', 'plpythonu') AND u.usename <> 'rdsdb' AND p.prokind = 'f'
		ORDER BY p.proname"); do
			i=$((i+1))
			exec_sql="${exec_dir}/${prefix}_${i}.sql"
			oid=$(echo ${x} | awk -F '|' '{print $1}')
			proname=$(echo ${x} | awk -F '|' '{print $2}')
			proowner=$(echo ${x} | awk -F '|' '{print $3}')
			param_count=$(echo ${x} | awk -F '|' '{print $4}')
			get_params
			sql_cmd="ALTER FUNCTION \"${schema_name}\".\"${proname}\"${params} OWNER TO \"${proowner}\";"
			echo "${sql_cmd}" > "${exec_sql}"

			wait_for_threads "${exec_sql}"
			echo "INFO: ${prefix}:${i}:${obj_count}:${schema_name}.${proname}"
			psql -h $TARGET_PGHOST -p $TARGET_PGPORT -d $TARGET_PGDATABASE -U $TARGET_PGUSER -f "${exec_sql}" -e > $PWD/log/${prefix}_${i}.log 2>&1 &
		done
	done
	wait_for_remaining "${exec_sql}" 
	IFS=$OLDIFS
}
alter_procedure_owner()
{
	prefix="alter_procedure_owner"
	i="0"
	OLDIFS=$IFS
	IFS=$'\n'
	obj_count=$(psql -h $SOURCE_PGHOST -p $SOURCE_PGPORT -d $SOURCE_PGDATABASE -U $SOURCE_PGUSER -t -A -c "
	SELECT COUNT(*) 
	FROM pg_proc_info p 
	JOIN pg_namespace n ON p.pronamespace = n.oid 
	JOIN pg_language l ON p.prolang = l.oid 
	JOIN pg_user u ON p.proowner = u.usesysid 
	JOIN svv_all_schemas s ON s.schema_name = n.nspname 
	WHERE s.database_name = current_database() AND s.schema_type = 'local' AND s.schema_name NOT IN ${EXCLUDED_SCHEMAS} AND l.lanname = 'plpgsql' AND p.proname NOT LIKE 'mv_sp__%' AND u.usename <> 'rdsdb' AND p.prokind = 'p'")
	echo "INFO: ${prefix}:creating ${obj_count}"
	for schema_name in $(psql -h $SOURCE_PGHOST -p $SOURCE_PGPORT -d $SOURCE_PGDATABASE -U $SOURCE_PGUSER -t -A -c "
	SELECT schema_name 
	FROM svv_all_schemas 
	WHERE database_name = current_database() AND schema_type = 'local' AND schema_name NOT IN ${EXCLUDED_SCHEMAS} 
	ORDER BY schema_name"); do
		for x in $(psql -h $SOURCE_PGHOST -p $SOURCE_PGPORT -d $SOURCE_PGDATABASE -U $SOURCE_PGUSER -t -A -c "
		SELECT p.prooid, p.proname, pg_get_userbyid(p.proowner), CASE WHEN p.proallargtypes IS NULL THEN CASE WHEN oidvectortypes(p.proargtypes) = '' THEN 0 ELSE regexp_count(oidvectortypes(p.proargtypes), ',')+1 END ELSE array_upper(proallargtypes, 1) END AS param_count 
		FROM pg_proc_info p 
		JOIN pg_namespace n ON p.pronamespace = n.oid 
		JOIN pg_language l ON p.prolang = l.oid 
		JOIN pg_user u ON p.proowner = u.usesysid 
		WHERE n.nspname = '${schema_name}' AND l.lanname = 'plpgsql' AND p.proname NOT LIKE 'mv_sp__%' AND u.usename <> 'rdsdb' AND p.prokind = 'p'
		ORDER BY p.proname"); do
			i=$((i+1))
			exec_sql="${exec_dir}/${prefix}_${i}.sql"
			oid=$(echo ${x} | awk -F '|' '{print $1}')
			proname=$(echo ${x} | awk -F '|' '{print $2}')
			proowner=$(echo ${x} | awk -F '|' '{print $3}')
			param_count=$(echo ${x} | awk -F '|' '{print $4}')
			get_params
			sql_cmd="ALTER PROCEDURE \"${schema_name}\".\"${proname}\"${params} OWNER TO \"${proowner}\";"
			echo "${sql_cmd}" > "${exec_sql}"

			wait_for_threads "${exec_sql}"
			echo "INFO: ${prefix}:${i}:${obj_count}:${schema_name}.${proname}"
			psql -h $TARGET_PGHOST -p $TARGET_PGPORT -d $TARGET_PGDATABASE -U $TARGET_PGUSER -f "${exec_sql}" -e > $PWD/log/${prefix}_${i}.log 2>&1 &
		done
	done
	wait_for_remaining "${exec_sql}"
	IFS=$OLDIFS
}
grant_user_schema()
{
	prefix="grant_user_schema"
	i="0"
	previous_schema_name=""
	OLDIFS=$IFS
	IFS=$'\n'
	obj_count=$(psql -h $SOURCE_PGHOST -p $SOURCE_PGPORT -d $SOURCE_PGDATABASE -U $SOURCE_PGUSER -t -A -c "
	SELECT COUNT(DISTINCT sub.nspname) 
	FROM 	(
		SELECT n.nspname, split_part(array_to_string(nspacl, ','), ',', i) AS acl 
		FROM 	(
			SELECT nspname, generate_series(1, array_upper(nspacl, 1)) AS i, nspacl 
			FROM pg_namespace
			) AS n 
		WHERE split_part(array_to_string(nspacl, ','), ',', i) NOT LIKE 'group %'
		) AS sub 
	JOIN pg_user u ON u.usename = split_part(sub.acl, '=', 1) WHERE u.usename <> 'rdsdb' AND u.usesuper IS FALSE AND sub.nspname NOT IN ${EXCLUDED_SCHEMAS}")
	echo "INFO: ${prefix}:creating ${obj_count}"
	for x in $(psql -h $SOURCE_PGHOST -p $SOURCE_PGPORT -d $SOURCE_PGDATABASE -U $SOURCE_PGUSER -t -A -c "
	SELECT sub.nspname, split_part(sub.acl, '=', 1) AS usename, split_part(split_part(sub.acl, '=', 2), '/', 1) AS usegrant 
	FROM 	(
		SELECT n.nspname, split_part(array_to_string(nspacl, ','), ',', i) AS acl 
		FROM 	(
			SELECT nspname, generate_series(1, array_upper(nspacl, 1)) AS i, nspacl 
			FROM 
			pg_namespace
			) AS n 
		WHERE split_part(array_to_string(nspacl, ','), ',', i) NOT LIKE 'group %'
		) AS sub 
	JOIN pg_user u ON u.usename = split_part(sub.acl, '=', 1) 
	WHERE u.usename <> 'rdsdb' AND u.usesuper IS FALSE AND sub.nspname NOT IN ${EXCLUDED_SCHEMAS} 
	ORDER BY sub.nspname, split_part(sub.acl, '=', 1)"); do
		schema_name=$(echo ${x} | awk -F '|' '{print $1}')
		grantee=$(echo ${x} | awk -F '|' '{print $2}')
		all_grants=$(echo ${x} | awk -F '|' '{print $3}')
		grant_count=$(echo -n "${all_grants}" | wc -m)
		grant_count=$((grant_count-1))
		if [ "${i}" -gt "0" ]; then
			if [ ! "${schema_name}" == "${previous_schema_name}" ]; then
				wait_for_threads ${exec_dir}
				echo "INFO: ${prefix}:${i}:${obj_count}:${previous_schema_name}"
				psql -h $TARGET_PGHOST -p $TARGET_PGPORT -d $TARGET_PGDATABASE -U $TARGET_PGUSER -f "${exec_sql}" -e > $PWD/log/${prefix}_${i}.log 2>&1 &
			fi
		fi
		if [ ! "${schema_name}" == "${previous_schema_name}" ]; then
			# new schema to add grants to in a script
			i=$((i+1))
			exec_sql="${exec_dir}/${prefix}_${i}.sql"
		fi
		get_grant_actions
		sql_cmd="GRANT ${grants} ON SCHEMA \"${schema_name}\" TO \"${grantee}\";"
		echo "${sql_cmd}" >> "${exec_sql}"
		previous_schema_name="${schema_name}"
	done
	wait_for_threads "${exec_dir}"
	echo "INFO: ${prefix}:${i}:${obj_count}:${schema_name}:${grantee}"
	psql -h $TARGET_PGHOST -p $TARGET_PGPORT -d $TARGET_PGDATABASE -U $TARGET_PGUSER -f "${exec_sql}" -e > $PWD/log/${prefix}_${i}.log 2>&1 &

	wait_for_remaining "${exec_dir}"
	IFS=$OLDIFS
}
grant_user_table()
{
	prefix="grant_user_table"
	i="0"
	ii="0"
	previous_schema_name=""
	previous_table_name=""
	exec_sql=""
	OLDIFS=$IFS
	IFS=$'\n'
	obj_count=$(psql -h $SOURCE_PGHOST -p $SOURCE_PGPORT -d $SOURCE_PGDATABASE -U $SOURCE_PGUSER -t -A -c "
	SELECT COUNT(DISTINCT n.nspname) 
	FROM pg_class c 
	JOIN pg_namespace n ON c.relnamespace = n.oid 
	JOIN svv_all_schemas s ON s.schema_name = n.nspname 
	JOIN (SELECT attrelid, MIN(attsortkeyord) AS min_attsortkeyord FROM pg_attribute GROUP BY attrelid) a ON a.attrelid = c.oid 
	WHERE s.database_name = current_database() AND s.schema_type = 'local' 
	AND c.relkind = 'r' AND a.min_attsortkeyord >= 0 AND c.relname NOT LIKE 'mv_tbl__%'
	AND schema_name NOT IN ${EXCLUDED_SCHEMAS}")

	echo "INFO: ${prefix}:creating ${obj_count}"
	for x in $(psql -h $SOURCE_PGHOST -p $SOURCE_PGPORT -d $SOURCE_PGDATABASE -U $SOURCE_PGUSER -t -A -c "
	SELECT s.schema_name, COUNT(*) 
	FROM pg_class c 
	JOIN pg_namespace n ON c.relnamespace = n.oid 
	JOIN svv_all_schemas s ON s.schema_name = n.nspname 
	JOIN (SELECT attrelid, MIN(attsortkeyord) AS min_attsortkeyord FROM pg_attribute GROUP BY attrelid) a ON a.attrelid = c.oid 
	WHERE s.database_name = current_database() AND s.schema_type = 'local' 
	AND c.relkind = 'r' AND a.min_attsortkeyord >= 0 AND c.relname NOT LIKE 'mv_tbl__%'
	AND schema_name NOT IN ${EXCLUDED_SCHEMAS} 
	GROUP BY s.schema_name
	ORDER BY schema_name"); do
		schema_name=$(echo ${x} | awk -F '|' '{print $1}')
		table_count=$(echo ${x} | awk -F '|' '{print $2}')
		i=$((i+1))
		echo -ne "INFO: ${prefix}:${i}:${obj_count}:${schema_name}".
		get_public_check

		if [ "${public_check}" -eq "0" ]; then
			#get count of tables each user has for this schema
			for z in $(psql -h $SOURCE_PGHOST -p $SOURCE_PGPORT -d $SOURCE_PGDATABASE -U $SOURCE_PGUSER -t -A -c "
			SELECT sub3.usename, sub3.usegrant, COUNT(*) 
			FROM 	(
				SELECT sub2.nspname, sub2.relname, split_part(sub2.acl, '=', 1) AS usename, split_part(split_part(sub2.acl, '=', 2), '/', 1) AS usegrant 
				FROM 	(
					SELECT sub.nspname, sub.relname, split_part(array_to_string(sub.relacl, ','), ',', i) AS acl 
					FROM (
						SELECT n.nspname, c.relname, generate_series(1, array_upper(c.relacl, 1)) AS i, c.relacl 
						FROM pg_class c 
						JOIN pg_namespace n ON c.relnamespace = n.oid 
						JOIN (SELECT attrelid, MIN(attsortkeyord) AS min_attsortkeyord FROM pg_attribute GROUP BY attrelid) a ON a.attrelid = c.oid 
						WHERE c.relkind = 'r' AND a.min_attsortkeyord >= 0 AND c.relname NOT LIKE 'mv_tbl__%'
						) AS sub 
					WHERE split_part(array_to_string(sub.relacl, ','), ',', i) NOT LIKE 'group %'
					) AS sub2 
				JOIN pg_user u ON u.usename = split_part(sub2.acl, '=', 1) 
				WHERE u.usename <> 'rdsdb' AND u.usesuper IS FALSE AND sub2.nspname NOT IN ${EXCLUDED_SCHEMAS}
				) AS sub3
			WHERE sub3.nspname = '${schema_name}'
			GROUP BY sub3.usename, sub3.usegrant"); do
				grantee=$(echo ${z} | awk -F '|' '{print $1}')
				all_grants=$(echo ${z} | awk -F '|' '{print $2}')
				grant_table_count=$(echo ${z} | awk -F '|' '{print $3}')
				if [ "${table_count}" -eq "${grant_table_count}" ]; then
					#user has permission to all tables. Short-circuit and use GRANT ON ALL
					grant_count=$(echo -n "${all_grants}" | wc -m)
					grant_count=$((grant_count-1))
					get_grant_actions
					sql_cmd="GRANT ${grants} ON ALL TABLES IN SCHEMA \"${schema_name}\" TO \"${grantee}\";"
					ii=$((ii+1))
					exec_sql="${exec_dir}/${prefix}_${ii}.sql"
					echo "${sql_cmd}" > ${exec_sql}
					echo -ne "."
					psql -h $TARGET_PGHOST -p $TARGET_PGPORT -d $TARGET_PGDATABASE -U $TARGET_PGUSER -f "${exec_sql}" -e > $PWD/log/${prefix}_${ii}.log
				else
					#user does not have permisssion to all tables. Execute grant for each in this schema.
					for zz in $(psql -h $SOURCE_PGHOST -p $SOURCE_PGPORT -d $SOURCE_PGDATABASE -U $SOURCE_PGUSER -t -A -c "
					SELECT sub2.relname, split_part(split_part(sub2.acl, '=', 2), '/', 1) AS usegrant 
					FROM (
						SELECT sub.nspname, sub.relname, split_part(array_to_string(sub.relacl, ','), ',', i) AS acl 
						FROM (
							SELECT n.nspname, c.relname, generate_series(1, array_upper(c.relacl, 1)) AS i, c.relacl 
							FROM pg_class c 
							JOIN pg_namespace n ON c.relnamespace = n.oid 
							JOIN (SELECT attrelid, MIN(attsortkeyord) AS min_attsortkeyord FROM pg_attribute GROUP BY attrelid) a ON a.attrelid = c.oid 
							WHERE c.relkind = 'r' AND a.min_attsortkeyord >= 0 AND c.relname NOT LIKE 'mv_tbl__%'
							) AS sub 
						WHERE split_part(array_to_string(sub.relacl, ','), ',', i) NOT LIKE 'group %') AS sub2 
					JOIN pg_user u ON u.usename = split_part(sub2.acl, '=', 1) 
					WHERE u.usename = '${grantee}' AND u.usesuper IS FALSE AND sub2.nspname = '${schema_name}' ORDER BY 1, 2"); do
						table_name=$(echo ${zz} | awk -F '|' '{print $1}')
						all_grants=$(echo ${zz} | awk -F '|' '{print $2}')

						grant_count=$(echo -n "${all_grants}" | wc -m)
						grant_count=$((grant_count-1))
						get_grant_actions
						sql_cmd="GRANT ${grants} ON TABLE \"${schema_name}\".\"${table_name}\" TO \"${grantee}\";"
						ii=$((ii+1))
						exec_sql="${exec_dir}/${prefix}_${ii}.sql"
						echo "${sql_cmd}" > ${exec_sql}
						echo -ne "."
						psql -h $TARGET_PGHOST -p $TARGET_PGPORT -d $TARGET_PGDATABASE -U $TARGET_PGUSER -f "${exec_sql}" -e > $PWD/log/${prefix}_${ii}.log
					done
				fi
			done
		fi
		echo "."
	done
	IFS=$OLDIFS
}
grant_user_function()
{
	prefix="grant_user_function"
	i="0"
	ii="0"
	OLDIFS=$IFS
	IFS=$'\n'
	obj_count=$(psql -h $SOURCE_PGHOST -p $SOURCE_PGPORT -d $SOURCE_PGDATABASE -U $SOURCE_PGUSER -t -A -c "
	SELECT COUNT(DISTINCT n.nspname) 
	FROM pg_proc_info p
	JOIN pg_namespace n ON p.pronamespace = n.oid
	JOIN pg_language l ON p.prolang = l.oid
	JOIN pg_user u ON p.proowner = u.usesysid
	WHERE l.lanname IN ('sql', 'plpythonu') AND u.usename <> 'rdsdb' AND p.prokind = 'f' 
	AND n.nspname NOT IN ${EXCLUDED_SCHEMAS}")

	echo "INFO: ${prefix}:creating ${obj_count}"

	for x in $(psql -h $SOURCE_PGHOST -p $SOURCE_PGPORT -d $SOURCE_PGDATABASE -U $SOURCE_PGUSER -t -A -c "
	SELECT n.nspname, COUNT(*) 
	FROM pg_proc_info p
	JOIN pg_namespace n ON p.pronamespace = n.oid
	JOIN pg_language l ON p.prolang = l.oid
	JOIN pg_user u ON p.proowner = u.usesysid
	WHERE l.lanname IN ('sql', 'plpythonu') AND u.usename <> 'rdsdb' AND p.prokind = 'f' 
	AND n.nspname NOT IN ${EXCLUDED_SCHEMAS} 
	GROUP BY n.nspname
	ORDER BY n.nspname"); do
		schema_name=$(echo ${x} | awk -F '|' '{print $1}')
		routine_count=$(echo ${x} | awk -F '|' '{print $2}')
		i=$((i+1))
		echo -ne "INFO: ${prefix}:${i}:${obj_count}:${schema_name}".

		get_public_check

		if [ "${public_check}" -eq "0" ]; then
			#get count of routines each user has for this schema
			for z in $(psql -h $SOURCE_PGHOST -p $SOURCE_PGPORT -d $SOURCE_PGDATABASE -U $SOURCE_PGUSER -t -A -c "
			SELECT sub3.grantee, COUNT(*) 
			FROM 	(
				SELECT sub2.oid, sub2.nspname, sub2.param_count, split_part(sub2.acl, '=', 1) AS grantee 
				FROM 	(
					SELECT sub.oid, sub.nspname, sub.param_count, split_part(array_to_string(sub.proacl, ','), ',', i) AS acl 
					FROM 	(
						SELECT p.prooid AS oid, n.nspname, generate_series(1, array_upper(p.proacl, 1)) AS i, CASE WHEN p.proallargtypes IS NULL THEN CASE WHEN oidvectortypes(p.proargtypes) = '' THEN 0 ELSE regexp_count(oidvectortypes(p.proargtypes), ',')+1 END ELSE array_upper(proallargtypes, 1) END AS param_count, p.proacl 
						FROM pg_proc_info p 
						JOIN pg_namespace n ON p.pronamespace = n.oid 
						JOIN pg_language l ON p.prolang = l.oid 
						JOIN pg_user u ON p.proowner = u.usesysid 
						WHERE l.lanname IN ('sql', 'plpythonu') AND u.usename <> 'rdsdb' AND p.proacl IS NOT NULL
						) AS sub 
					WHERE split_part(array_to_string(sub.proacl, ','), ',', i) NOT LIKE 'group %'
					) AS sub2 
				WHERE split_part(sub2.acl, '=', 1) <> '' AND split_part(split_part(sub2.acl, '=', 2), '/', 1) = 'X'
				) AS sub3
			WHERE sub3.nspname = '${schema_name}' 
			GROUP BY sub3.grantee"); do
				grantee=$(echo ${z} | awk -F '|' '{print $1}')
				all_grants="X"
				grant_routine_count=$(echo ${z} | awk -F '|' '{print $2}')
				if [ "${routine_count}" -eq "${grant_routine_count}" ]; then
					#user has permission to all routines. Short-circuit and use GRANT ON ALL
					grant_count=$(echo -n "${all_grants}" | wc -m)
					grant_count=$((grant_count-1))
					get_grant_actions
					sql_cmd="GRANT ${grants} ON ALL FUNCTIONS IN SCHEMA \"${schema_name}\" TO \"${grantee}\";"
					ii=$((ii+1))
					exec_sql="${exec_dir}/${prefix}_${ii}.sql"
					echo "${sql_cmd}" > ${exec_sql}
					echo -ne "."
					psql -h $TARGET_PGHOST -p $TARGET_PGPORT -d $TARGET_PGDATABASE -U $TARGET_PGUSER -f "${exec_sql}" -e > $PWD/log/${prefix}_${ii}.log
				else
					#user does not have permisssion to all routines. Execute grant for each in this schema.
					for zz in $(psql -h $SOURCE_PGHOST -p $SOURCE_PGPORT -d $SOURCE_PGDATABASE -U $SOURCE_PGUSER -t -A -c "
					SELECT sub2.oid, sub2.nspname, sub2.proname, sub2.param_count, split_part(sub2.acl, '=', 1) AS grantee 
					FROM 	(
					SELECT sub.oid, sub.nspname, sub.proname, sub.param_count, split_part(array_to_string(sub.proacl, ','), ',', i) AS acl 
					FROM 	(
						SELECT p.prooid AS oid, n.nspname, p.proname, generate_series(1, array_upper(p.proacl, 1)) AS i, CASE WHEN p.proallargtypes IS NULL THEN CASE WHEN oidvectortypes(p.proargtypes) = '' THEN 0 ELSE regexp_count(oidvectortypes(p.proargtypes), ',')+1 END ELSE array_upper(proallargtypes, 1) END AS param_count, p.proacl
						FROM pg_proc_info p 
						JOIN pg_namespace n ON p.pronamespace = n.oid 
						JOIN pg_language l ON p.prolang = l.oid 
						JOIN pg_user u ON p.proowner = u.usesysid 
						WHERE l.lanname IN ('sql', 'plpythonu') AND u.usename <> 'rdsdb' AND p.proacl IS NOT NULL
						) AS sub 
					WHERE split_part(array_to_string(sub.proacl, ','), ',', i) NOT LIKE 'group %'
					) AS sub2 
					WHERE sub2.nspname = '${schema_name}' AND split_part(sub2.acl, '=', 1) = '${grantee}' AND split_part(split_part(sub2.acl, '=', 2), '/', 1) = 'X'"); do
						oid=$(echo ${zz} | awk -F '|' '{print $1}')
						schema_name=$(echo ${zz} | awk -F '|' '{print $2}')
						proname=$(echo ${zz} | awk -F '|' '{print $3}')
						param_count=$(echo ${zz} | awk -F '|' '{print $4}')
						grantee=$(echo ${zz} | awk -F '|' '{print $5}')
						all_grants="X"
						grant_count=$(echo -n "${all_grants}" | wc -m)
						grant_count=$((grant_count-1))
						get_grant_actions
						get_params
						sql_cmd="GRANT ${grants} ON FUNCTION \"${schema_name}\".\"${proname}\"${params} TO \"${grantee}\";"
						ii=$((ii+1))
						exec_sql="${exec_dir}/${prefix}_${ii}.sql"
						echo "${sql_cmd}" > ${exec_sql}
						echo -ne "."
						psql -h $TARGET_PGHOST -p $TARGET_PGPORT -d $TARGET_PGDATABASE -U $TARGET_PGUSER -f "${exec_sql}" -e > $PWD/log/${prefix}_${ii}.log
					done
				fi
			done
		fi
		echo "."
	done
	IFS=$OLDIFS
}
grant_user_procedure()
{
	prefix="grant_user_procedure"
	i="0"
	ii="0"
	OLDIFS=$IFS
	IFS=$'\n'
	obj_count=$(psql -h $SOURCE_PGHOST -p $SOURCE_PGPORT -d $SOURCE_PGDATABASE -U $SOURCE_PGUSER -t -A -c "
	SELECT COUNT(DISTINCT n.nspname) 
	FROM pg_proc_info p
	JOIN pg_namespace n ON p.pronamespace = n.oid
	JOIN pg_language l ON p.prolang = l.oid
	JOIN pg_user u ON p.proowner = u.usesysid
	WHERE l.lanname = 'plpgsql' AND u.usename <> 'rdsdb' AND p.prokind = 'p' AND p.proname NOT LIKE 'mv_sp__%'
	AND n.nspname NOT IN ${EXCLUDED_SCHEMAS}")

	echo "INFO: ${prefix}:creating ${obj_count}"

	for x in $(psql -h $SOURCE_PGHOST -p $SOURCE_PGPORT -d $SOURCE_PGDATABASE -U $SOURCE_PGUSER -t -A -c "
	SELECT n.nspname, COUNT(*) 
	FROM pg_proc_info p
	JOIN pg_namespace n ON p.pronamespace = n.oid
	JOIN pg_language l ON p.prolang = l.oid
	JOIN pg_user u ON p.proowner = u.usesysid
	WHERE l.lanname = 'plpgsql' AND u.usename <> 'rdsdb' AND p.prokind = 'p' AND p.proname NOT LIKE 'mv_sp__%'
	AND n.nspname NOT IN ${EXCLUDED_SCHEMAS} 
	GROUP BY n.nspname
	ORDER BY n.nspname"); do
		schema_name=$(echo ${x} | awk -F '|' '{print $1}')
		routine_count=$(echo ${x} | awk -F '|' '{print $2}')
		i=$((i+1))
		echo -ne "INFO: ${prefix}:${i}:${obj_count}:${schema_name}".

		get_public_check

		if [ "${public_check}" -eq "0" ]; then
			#get count of routines each user has for this schema
			for z in $(psql -h $SOURCE_PGHOST -p $SOURCE_PGPORT -d $SOURCE_PGDATABASE -U $SOURCE_PGUSER -t -A -c "
			SELECT sub3.grantee, COUNT(*) 
			FROM 	(
				SELECT sub2.oid, sub2.nspname, sub2.param_count, split_part(sub2.acl, '=', 1) AS grantee 
				FROM 	(
					SELECT sub.oid, sub.nspname, sub.param_count, split_part(array_to_string(sub.proacl, ','), ',', i) AS acl 
					FROM 	(
						SELECT p.prooid AS oid, n.nspname, generate_series(1, array_upper(p.proacl, 1)) AS i, CASE WHEN p.proallargtypes IS NULL THEN CASE WHEN oidvectortypes(p.proargtypes) = '' THEN 0 ELSE regexp_count(oidvectortypes(p.proargtypes), ',')+1 END ELSE array_upper(proallargtypes, 1) END AS param_count, p.proacl 
						FROM pg_proc_info p 
						JOIN pg_namespace n ON p.pronamespace = n.oid 
						JOIN pg_language l ON p.prolang = l.oid 
						JOIN pg_user u ON p.proowner = u.usesysid 
						WHERE l.lanname = 'plpgsql' AND u.usename <> 'rdsdb' AND p.proacl IS NOT NULL AND p.prokind = 'p' AND p.proname NOT LIKE 'mv_sp__%'
						) AS sub 
					WHERE split_part(array_to_string(sub.proacl, ','), ',', i) NOT LIKE 'group %'
					) AS sub2 
				WHERE split_part(sub2.acl, '=', 1) <> '' AND split_part(split_part(sub2.acl, '=', 2), '/', 1) = 'X'
				) AS sub3
			WHERE sub3.nspname = '${schema_name}'
			GROUP BY sub3.grantee"); do
				grantee=$(echo ${z} | awk -F '|' '{print $1}')
				all_grants="X"
				grant_routine_count=$(echo ${z} | awk -F '|' '{print $2}')
				if [ "${routine_count}" -eq "${grant_routine_count}" ]; then
					#user has permission to all routines. Short-circuit and use GRANT ON ALL
					grant_count=$(echo -n "${all_grants}" | wc -m)
					grant_count=$((grant_count-1))
					get_grant_actions
					sql_cmd="GRANT ${grants} ON ALL PROCEDURES IN SCHEMA \"${schema_name}\" TO \"${grantee}\";"
					ii=$((ii+1))
					exec_sql="${exec_dir}/${prefix}_${ii}.sql"
					echo "${sql_cmd}" > ${exec_sql}
					echo -ne "."
					psql -h $TARGET_PGHOST -p $TARGET_PGPORT -d $TARGET_PGDATABASE -U $TARGET_PGUSER -f "${exec_sql}" -e > $PWD/log/${prefix}_${ii}.log
				else
					#user does not have permisssion to all routines. Execute grant for each in this schema.
					for zz in $(psql -h $SOURCE_PGHOST -p $SOURCE_PGPORT -d $SOURCE_PGDATABASE -U $SOURCE_PGUSER -t -A -c "
					SELECT sub2.oid, sub2.nspname, sub2.proname, sub2.param_count, split_part(sub2.acl, '=', 1) AS grantee 
					FROM 	(
					SELECT sub.oid, sub.nspname, sub.proname, sub.param_count, split_part(array_to_string(sub.proacl, ','), ',', i) AS acl 
					FROM 	(
						SELECT p.prooid AS oid, n.nspname, p.proname, generate_series(1, array_upper(p.proacl, 1)) AS i, CASE WHEN p.proallargtypes IS NULL THEN CASE WHEN oidvectortypes(p.proargtypes) = '' THEN 0 ELSE regexp_count(oidvectortypes(p.proargtypes), ',')+1 END ELSE array_upper(proallargtypes, 1) END AS param_count, p.proacl
						FROM pg_proc_info p 
						JOIN pg_namespace n ON p.pronamespace = n.oid 
						JOIN pg_language l ON p.prolang = l.oid 
						JOIN pg_user u ON p.proowner = u.usesysid 
						WHERE l.lanname = 'plpgsql' AND u.usename <> 'rdsdb' AND p.proacl IS NOT NULL AND p.prokind = 'p' AND p.proname NOT LIKE 'mv_sp__%'
						) AS sub 
					WHERE split_part(array_to_string(sub.proacl, ','), ',', i) NOT LIKE 'group %'
					) AS sub2 
					WHERE sub2.nspname = '${schema_name}' AND split_part(sub2.acl, '=', 1) = '${grantee}' AND split_part(split_part(sub2.acl, '=', 2), '/', 1) = 'X'"); do
						oid=$(echo ${zz} | awk -F '|' '{print $1}')
						schema_name=$(echo ${zz} | awk -F '|' '{print $2}')
						proname=$(echo ${zz} | awk -F '|' '{print $3}')
						param_count=$(echo ${zz} | awk -F '|' '{print $4}')
						grantee=$(echo ${zz} | awk -F '|' '{print $5}')
						all_grants="X"
						grant_count=$(echo -n "${all_grants}" | wc -m)
						grant_count=$((grant_count-1))
						get_grant_actions
						get_params
						sql_cmd="GRANT ${grants} ON PROCEDURE \"${schema_name}\".\"${proname}\"${params} TO \"${grantee}\";"
						ii=$((ii+1))
						exec_sql="${exec_dir}/${prefix}_${ii}.sql"
						echo "${sql_cmd}" > ${exec_sql}
						echo -ne "."
						psql -h $TARGET_PGHOST -p $TARGET_PGPORT -d $TARGET_PGDATABASE -U $TARGET_PGUSER -f "${exec_sql}" -e > $PWD/log/${prefix}_${ii}.log
					done
				fi
			done
		fi
		echo "."
	done
	IFS=$OLDIFS
}
grant_group_schema()
{
	prefix="grant_group_schema"
	i="0"
	previous_schema_name=""
	OLDIFS=$IFS
	IFS=$'\n'
	obj_count=$(psql -h $SOURCE_PGHOST -p $SOURCE_PGPORT -d $SOURCE_PGDATABASE -U $SOURCE_PGUSER -t -A -c "
	SELECT COUNT(DISTINCT sub.nspname) 
	FROM 	(
		SELECT n.nspname, split_part(split_part(array_to_string(nspacl, ','), ',', i), ' ', 2) AS acl 
		FROM 	(
			SELECT nspname, generate_series(1, array_upper(nspacl, 1)) AS i, nspacl 
			FROM pg_namespace
			) AS n 
		WHERE split_part(array_to_string(nspacl, ','), ',', i) LIKE 'group %'
		) AS sub 
	WHERE sub.nspname NOT IN ${EXCLUDED_SCHEMAS}")
	echo "INFO: ${prefix}:creating ${obj_count}"
	for x in $(psql -h $SOURCE_PGHOST -p $SOURCE_PGPORT -d $SOURCE_PGDATABASE -U $SOURCE_PGUSER -t -A -c "
	SELECT sub.nspname, split_part(sub.acl, '=', 1) AS groname, split_part(split_part(sub.acl, '=', 2), '/', 1) AS grogrant 
	FROM 	(
		SELECT n.nspname, split_part(split_part(array_to_string(nspacl, ','), ',', i), ' ', 2) AS acl 
		FROM 	(
			SELECT nspname, generate_series(1, array_upper(nspacl, 1)) AS i, nspacl 
			FROM pg_namespace
			) AS n 
		WHERE split_part(array_to_string(nspacl, ','), ',', i) LIKE 'group %'
		) AS sub 
	WHERE sub.nspname NOT IN ${EXCLUDED_SCHEMAS} 
	ORDER BY sub.nspname, split_part(sub.acl, '=', 1)"); do
		schema_name=$(echo ${x} | awk -F '|' '{print $1}')
		group_name=$(echo ${x} | awk -F '|' '{print $2}')
		all_grants=$(echo ${x} | awk -F '|' '{print $3}')
		grant_count=$(echo -n "${all_grants}" | wc -m)
		grant_count=$((grant_count-1))
		if [ "${i}" -gt "0" ]; then
			if [ ! "${schema_name}" == "${previous_schema_name}" ]; then
				wait_for_threads ${exec_dir}
				echo "INFO: ${prefix}:${i}:${obj_count}:${previous_schema_name}"
				psql -h $TARGET_PGHOST -p $TARGET_PGPORT -d $TARGET_PGDATABASE -U $TARGET_PGUSER -f "${exec_sql}" -e > $PWD/log/${prefix}_${i}.log 2>&1 &
			fi
		fi
		if [ ! "${schema_name}" == "${previous_schema_name}" ]; then
			# new schema to add grants to in a script
			i=$((i+1))
			exec_sql="${exec_dir}/${prefix}_${i}.sql"
		fi
		get_grant_actions
		sql_cmd="GRANT ${grants} ON SCHEMA \"${schema_name}\" TO GROUP \"${group_name}\";"
		echo "${sql_cmd}" >> "${exec_sql}"
		previous_schema_name="${schema_name}"
	done
	wait_for_threads "${exec_dir}"
	echo "INFO: ${prefix}:${i}:${obj_count}:${schema_name}:${group_name}"
	psql -h $TARGET_PGHOST -p $TARGET_PGPORT -d $TARGET_PGDATABASE -U $TARGET_PGUSER -f "${exec_sql}" -e > $PWD/log/${prefix}_${i}.log 2>&1 &

	wait_for_remaining "${exec_dir}"
	IFS=$OLDIFS
}
grant_group_table()
{
	prefix="grant_group_table"
	i="0"
	ii="0"
	previous_schema_name=""
	previous_table_name=""
	exec_sql=""
	OLDIFS=$IFS
	IFS=$'\n'
	obj_count=$(psql -h $SOURCE_PGHOST -p $SOURCE_PGPORT -d $SOURCE_PGDATABASE -U $SOURCE_PGUSER -t -A -c "
	SELECT COUNT(DISTINCT n.nspname) 
	FROM pg_class c 
	JOIN pg_namespace n ON c.relnamespace = n.oid 
	JOIN svv_all_schemas s ON s.schema_name = n.nspname 
	JOIN (SELECT attrelid, MIN(attsortkeyord) AS min_attsortkeyord FROM pg_attribute GROUP BY attrelid) a ON a.attrelid = c.oid 
	WHERE s.database_name = current_database() AND s.schema_type = 'local' 
	AND c.relkind = 'r' AND a.min_attsortkeyord >= 0 AND c.relname NOT LIKE 'mv_tbl__%'
	AND schema_name NOT IN ${EXCLUDED_SCHEMAS}")

	echo "INFO: ${prefix}:creating ${obj_count}"
	for x in $(psql -h $SOURCE_PGHOST -p $SOURCE_PGPORT -d $SOURCE_PGDATABASE -U $SOURCE_PGUSER -t -A -c "
	SELECT s.schema_name, COUNT(*) 
	FROM pg_class c 
	JOIN pg_namespace n ON c.relnamespace = n.oid 
	JOIN svv_all_schemas s ON s.schema_name = n.nspname 
	JOIN (SELECT attrelid, MIN(attsortkeyord) AS min_attsortkeyord FROM pg_attribute GROUP BY attrelid) a ON a.attrelid = c.oid 
	WHERE s.database_name = current_database() AND s.schema_type = 'local' 
	AND c.relkind = 'r' AND a.min_attsortkeyord >= 0 AND c.relname NOT LIKE 'mv_tbl__%'
	AND schema_name NOT IN ${EXCLUDED_SCHEMAS} 
	GROUP BY s.schema_name
	ORDER BY schema_name"); do
		schema_name=$(echo ${x} | awk -F '|' '{print $1}')
		table_count=$(echo ${x} | awk -F '|' '{print $2}')
		i=$((i+1))
		echo -ne "INFO: ${prefix}:${i}:${obj_count}:${schema_name}".
		get_public_check

		if [ "${public_check}" -eq "0" ]; then
			#get count of tables each user has for this schema
			for z in $(psql -h $SOURCE_PGHOST -p $SOURCE_PGPORT -d $SOURCE_PGDATABASE -U $SOURCE_PGUSER -t -A -c "
			SELECT sub3.groname, sub3.grogrant, COUNT(*) 
			FROM 	(
				SELECT sub2.nspname, sub2.relname, split_part(sub2.acl, '=', 1) AS groname, split_part(split_part(sub2.acl, '=', 2), '/', 1) AS grogrant 
				FROM 	(
					SELECT sub.nspname, sub.relname, split_part(split_part(array_to_string(sub.relacl, ','), ',', i), ' ', 2) AS acl
					FROM (
						SELECT n.nspname, c.relname, generate_series(1, array_upper(c.relacl, 1)) AS i, c.relacl 
						FROM pg_class c 
						JOIN pg_namespace n ON c.relnamespace = n.oid 
						JOIN (SELECT attrelid, MIN(attsortkeyord) AS min_attsortkeyord FROM pg_attribute GROUP BY attrelid) a ON a.attrelid = c.oid 
						WHERE c.relkind = 'r' AND a.min_attsortkeyord >= 0 AND c.relname NOT LIKE 'mv_tbl__%'
						) AS sub 
					WHERE split_part(array_to_string(sub.relacl, ','), ',', i) LIKE 'group %'
					) AS sub2 
				WHERE sub2.nspname NOT IN ${EXCLUDED_SCHEMAS}
				) AS sub3
			WHERE sub3.nspname = '${schema_name}'
			GROUP BY sub3.groname, sub3.grogrant"); do
				grantee=$(echo ${z} | awk -F '|' '{print $1}')
				all_grants=$(echo ${z} | awk -F '|' '{print $2}')
				grant_table_count=$(echo ${z} | awk -F '|' '{print $3}')
				if [ "${table_count}" -eq "${grant_table_count}" ]; then
					#group has permission to all tables. Short-circuit and use GRANT ON ALL
					grant_count=$(echo -n "${all_grants}" | wc -m)
					grant_count=$((grant_count-1))
					get_grant_actions
					sql_cmd="GRANT ${grants} ON ALL TABLES IN SCHEMA \"${schema_name}\" TO GROUP \"${grantee}\";"
					ii=$((ii+1))
					exec_sql="${exec_dir}/${prefix}_${ii}.sql"
					echo "${sql_cmd}" > ${exec_sql}
					echo -ne "."
					psql -h $TARGET_PGHOST -p $TARGET_PGPORT -d $TARGET_PGDATABASE -U $TARGET_PGUSER -f "${exec_sql}" -e > $PWD/log/${prefix}_${ii}.log
				else
					#group does not have permisssion to all tables. Execute grant for each in this schema.
					for zz in $(psql -h $SOURCE_PGHOST -p $SOURCE_PGPORT -d $SOURCE_PGDATABASE -U $SOURCE_PGUSER -t -A -c "
					SELECT sub2.relname, split_part(split_part(sub2.acl, '=', 2), '/', 1) AS grogrant 
					FROM 	(
						SELECT sub.nspname, sub.relname, split_part(split_part(array_to_string(sub.relacl, ','), ',', i), ' ', 2) AS acl
						FROM (
							SELECT n.nspname, c.relname, generate_series(1, array_upper(c.relacl, 1)) AS i, c.relacl 
							FROM pg_class c 
							JOIN pg_namespace n ON c.relnamespace = n.oid 
							JOIN (SELECT attrelid, MIN(attsortkeyord) AS min_attsortkeyord FROM pg_attribute GROUP BY attrelid) a ON a.attrelid = c.oid 
							WHERE c.relkind = 'r' AND a.min_attsortkeyord >= 0 AND c.relname NOT LIKE 'mv_tbl__%'
							) AS sub 
						WHERE split_part(array_to_string(sub.relacl, ','), ',', i) LIKE 'group %'
						) AS sub2 
					WHERE sub2.nspname = '${schema_name}' AND split_part(sub2.acl, '=', 1) = '${grantee}' ORDER BY 1, 2"); do
						table_name=$(echo ${zz} | awk -F '|' '{print $1}')
						all_grants=$(echo ${zz} | awk -F '|' '{print $2}')

						grant_count=$(echo -n "${all_grants}" | wc -m)
						grant_count=$((grant_count-1))
						get_grant_actions
						sql_cmd="GRANT ${grants} ON TABLE \"${schema_name}\".\"${table_name}\" TO GROUP \"${grantee}\";"
						ii=$((ii+1))
						exec_sql="${exec_dir}/${prefix}_${ii}.sql"
						echo "${sql_cmd}" > ${exec_sql}
						echo -ne "."
						psql -h $TARGET_PGHOST -p $TARGET_PGPORT -d $TARGET_PGDATABASE -U $TARGET_PGUSER -f "${exec_sql}" -e > $PWD/log/${prefix}_${ii}.log
					done
				fi
			done
		fi
		echo "."
	done
	IFS=$OLDIFS
}
grant_group_function()
{
	prefix="grant_group_function"
	i="0"
	ii="0"
	OLDIFS=$IFS
	IFS=$'\n'

	obj_count=$(psql -h $SOURCE_PGHOST -p $SOURCE_PGPORT -d $SOURCE_PGDATABASE -U $SOURCE_PGUSER -t -A -c "
	SELECT COUNT(DISTINCT n.nspname) 
	FROM pg_proc_info p
	JOIN pg_namespace n ON p.pronamespace = n.oid
	JOIN pg_language l ON p.prolang = l.oid
	JOIN pg_user u ON p.proowner = u.usesysid
	WHERE l.lanname IN ('sql', 'plpythonu') AND u.usename <> 'rdsdb' AND p.prokind = 'f' 
	AND n.nspname NOT IN ${EXCLUDED_SCHEMAS}")

	echo "INFO: ${prefix}:creating ${obj_count}"

	for x in $(psql -h $SOURCE_PGHOST -p $SOURCE_PGPORT -d $SOURCE_PGDATABASE -U $SOURCE_PGUSER -t -A -c "
	SELECT n.nspname, COUNT(*) 
	FROM pg_proc_info p
	JOIN pg_namespace n ON p.pronamespace = n.oid
	JOIN pg_language l ON p.prolang = l.oid
	JOIN pg_user u ON p.proowner = u.usesysid
	WHERE l.lanname IN ('sql', 'plpythonu') AND u.usename <> 'rdsdb' AND p.prokind = 'f' 
	AND n.nspname NOT IN ${EXCLUDED_SCHEMAS} 
	GROUP BY n.nspname
	ORDER BY n.nspname"); do
		schema_name=$(echo ${x} | awk -F '|' '{print $1}')
		routine_count=$(echo ${x} | awk -F '|' '{print $2}')
		i=$((i+1))
		echo -ne "INFO: ${prefix}:${i}:${obj_count}:${schema_name}".

		get_public_check

		if [ "${public_check}" -eq "0" ]; then
			#get count of routines each user has for this schema
			for z in $(psql -h $SOURCE_PGHOST -p $SOURCE_PGPORT -d $SOURCE_PGDATABASE -U $SOURCE_PGUSER -t -A -c "
			SELECT sub3.grantee, COUNT(*) 
			FROM 	(
				SELECT sub2.oid, sub2.nspname, sub2.param_count, split_part(sub2.acl, '=', 1) AS grantee 
				FROM 	(
					SELECT sub.oid, sub.nspname, sub.param_count, split_part(split_part(array_to_string(sub.proacl, ','), ',', i), ' ', 2) AS acl
					FROM 	(
						SELECT p.prooid AS oid, n.nspname, generate_series(1, array_upper(p.proacl, 1)) AS i, CASE WHEN p.proallargtypes IS NULL THEN CASE WHEN oidvectortypes(p.proargtypes) = '' THEN 0 ELSE regexp_count(oidvectortypes(p.proargtypes), ',')+1 END ELSE array_upper(proallargtypes, 1) END AS param_count, p.proacl 
						FROM pg_proc_info p 
						JOIN pg_namespace n ON p.pronamespace = n.oid 
						JOIN pg_language l ON p.prolang = l.oid 
						JOIN pg_user u ON p.proowner = u.usesysid 
						WHERE l.lanname IN ('sql', 'plpythonu') AND u.usename <> 'rdsdb' AND p.proacl IS NOT NULL AND p.prokind = 'f'
						) AS sub 
					WHERE split_part(array_to_string(sub.proacl, ','), ',', i) LIKE 'group %'
					) AS sub2 
				WHERE split_part(sub2.acl, '=', 1) <> '' AND split_part(split_part(sub2.acl, '=', 2), '/', 1) = 'X'
				) AS sub3
			WHERE sub3.nspname = '${schema_name}'
			GROUP BY sub3.grantee"); do
				grantee=$(echo ${z} | awk -F '|' '{print $1}')
				all_grants="X"
				grant_routine_count=$(echo ${z} | awk -F '|' '{print $2}')
				if [ "${routine_count}" -eq "${grant_routine_count}" ]; then
					#user has permission to all routines. Short-circuit and use GRANT ON ALL
					grant_count=$(echo -n "${all_grants}" | wc -m)
					grant_count=$((grant_count-1))
					get_grant_actions
					sql_cmd="GRANT ${grants} ON ALL FUNCTIONS IN SCHEMA \"${schema_name}\" TO GROUP \"${grantee}\";"
					ii=$((ii+1))
					exec_sql="${exec_dir}/${prefix}_${ii}.sql"
					echo "${sql_cmd}" > ${exec_sql}
					echo -ne "."
					psql -h $TARGET_PGHOST -p $TARGET_PGPORT -d $TARGET_PGDATABASE -U $TARGET_PGUSER -f "${exec_sql}" -e > $PWD/log/${prefix}_${ii}.log
				else
					#user does not have permisssion to all routines. Execute grant for each in this schema.
					for zz in $(psql -h $SOURCE_PGHOST -p $SOURCE_PGPORT -d $SOURCE_PGDATABASE -U $SOURCE_PGUSER -t -A -c "
					SELECT sub2.oid, sub2.nspname, sub2.proname, sub2.param_count, split_part(sub2.acl, '=', 1) AS grantee 
					FROM 	(
						SELECT sub.oid, sub.nspname, sub.proname, sub.param_count, split_part(split_part(array_to_string(sub.proacl, ','), ',', i), ' ', 2) AS acl
						FROM 	(
							SELECT p.prooid AS oid, n.nspname, p.proname, generate_series(1, array_upper(p.proacl, 1)) AS i, CASE WHEN p.proallargtypes IS NULL THEN CASE WHEN oidvectortypes(p.proargtypes) = '' THEN 0 ELSE regexp_count(oidvectortypes(p.proargtypes), ',')+1 END ELSE array_upper(proallargtypes, 1) END AS param_count, p.proacl 
							FROM pg_proc_info p 
							JOIN pg_namespace n ON p.pronamespace = n.oid 
							JOIN pg_language l ON p.prolang = l.oid 
							JOIN pg_user u ON p.proowner = u.usesysid 
							WHERE l.lanname IN ('sql', 'plpythonu') AND u.usename <> 'rdsdb' AND p.proacl IS NOT NULL AND p.prokind = 'f'
							) AS sub 
						WHERE split_part(array_to_string(sub.proacl, ','), ',', i) LIKE 'group %'
						) AS sub2 
					WHERE sub2.nspname = '${schema_name}' AND split_part(sub2.acl, '=', 1) = '${grantee}' AND split_part(split_part(sub2.acl, '=', 2), '/', 1) = 'X'"); do
						oid=$(echo ${zz} | awk -F '|' '{print $1}')
						schema_name=$(echo ${zz} | awk -F '|' '{print $2}')
						proname=$(echo ${zz} | awk -F '|' '{print $3}')
						param_count=$(echo ${zz} | awk -F '|' '{print $4}')
						grantee=$(echo ${zz} | awk -F '|' '{print $5}')
						all_grants="X"
						grant_count=$(echo -n "${all_grants}" | wc -m)
						grant_count=$((grant_count-1))
						get_grant_actions
						get_params
						sql_cmd="GRANT ${grants} ON FUNCTION \"${schema_name}\".\"${proname}\"${params} TO GROUP \"${grantee}\";"
						ii=$((ii+1))
						exec_sql="${exec_dir}/${prefix}_${ii}.sql"
						echo "${sql_cmd}" > ${exec_sql}
						echo -ne "."
						psql -h $TARGET_PGHOST -p $TARGET_PGPORT -d $TARGET_PGDATABASE -U $TARGET_PGUSER -f "${exec_sql}" -e > $PWD/log/${prefix}_${ii}.log
					done
				fi
			done
		fi
		echo "."
	done
	IFS=$OLDIFS
}
grant_group_procedure()
{
	prefix="grant_group_procedure"
	i="0"
	ii="0"
	OLDIFS=$IFS
	IFS=$'\n'

	obj_count=$(psql -h $SOURCE_PGHOST -p $SOURCE_PGPORT -d $SOURCE_PGDATABASE -U $SOURCE_PGUSER -t -A -c "
	SELECT COUNT(DISTINCT n.nspname) 
	FROM pg_proc_info p
	JOIN pg_namespace n ON p.pronamespace = n.oid
	JOIN pg_language l ON p.prolang = l.oid
	JOIN pg_user u ON p.proowner = u.usesysid
	WHERE l.lanname = 'plpgsql' AND u.usename <> 'rdsdb' AND p.prokind = 'p' AND p.proname NOT LIKE 'mv_sp__%'
	AND n.nspname NOT IN ${EXCLUDED_SCHEMAS}")

	echo "INFO: ${prefix}:creating ${obj_count}"

	for x in $(psql -h $SOURCE_PGHOST -p $SOURCE_PGPORT -d $SOURCE_PGDATABASE -U $SOURCE_PGUSER -t -A -c "
	SELECT n.nspname, COUNT(*) 
	FROM pg_proc_info p
	JOIN pg_namespace n ON p.pronamespace = n.oid
	JOIN pg_language l ON p.prolang = l.oid
	JOIN pg_user u ON p.proowner = u.usesysid
	WHERE l.lanname = 'plpgsql' AND u.usename <> 'rdsdb' AND p.prokind = 'p' AND p.proname NOT LIKE 'mv_sp__%'
	AND n.nspname NOT IN ${EXCLUDED_SCHEMAS} 
	GROUP BY n.nspname
	ORDER BY n.nspname"); do
		schema_name=$(echo ${x} | awk -F '|' '{print $1}')
		routine_count=$(echo ${x} | awk -F '|' '{print $2}')
		i=$((i+1))
		echo -ne "INFO: ${prefix}:${i}:${obj_count}:${schema_name}".

		get_public_check

		if [ "${public_check}" -eq "0" ]; then
			#get count of routines each user has for this schema
			for z in $(psql -h $SOURCE_PGHOST -p $SOURCE_PGPORT -d $SOURCE_PGDATABASE -U $SOURCE_PGUSER -t -A -c "
			SELECT sub3.grantee, COUNT(*) 
			FROM 	(
				SELECT sub2.oid, sub2.nspname, sub2.param_count, split_part(sub2.acl, '=', 1) AS grantee 
				FROM 	(
					SELECT sub.oid, sub.nspname, sub.param_count, split_part(split_part(array_to_string(sub.proacl, ','), ',', i), ' ', 2) AS acl
					FROM 	(
						SELECT p.prooid AS oid, n.nspname, generate_series(1, array_upper(p.proacl, 1)) AS i, CASE WHEN p.proallargtypes IS NULL THEN CASE WHEN oidvectortypes(p.proargtypes) = '' THEN 0 ELSE regexp_count(oidvectortypes(p.proargtypes), ',')+1 END ELSE array_upper(proallargtypes, 1) END AS param_count, p.proacl 
						FROM pg_proc_info p 
						JOIN pg_namespace n ON p.pronamespace = n.oid 
						JOIN pg_language l ON p.prolang = l.oid 
						JOIN pg_user u ON p.proowner = u.usesysid 
						WHERE l.lanname = 'plpgsql' AND u.usename <> 'rdsdb' AND p.prokind = 'p' AND p.proname NOT LIKE 'mv_sp__%'
						) AS sub 
					WHERE split_part(array_to_string(sub.proacl, ','), ',', i) LIKE 'group %'
					) AS sub2 
				WHERE split_part(sub2.acl, '=', 1) <> '' AND split_part(split_part(sub2.acl, '=', 2), '/', 1) = 'X'
				) AS sub3
			WHERE sub3.nspname = '${schema_name}'
			GROUP BY sub3.grantee"); do
				grantee=$(echo ${z} | awk -F '|' '{print $1}')
				all_grants="X"
				grant_routine_count=$(echo ${z} | awk -F '|' '{print $2}')
				if [ "${routine_count}" -eq "${grant_routine_count}" ]; then
					#user has permission to all routines. Short-circuit and use GRANT ON ALL
					grant_count=$(echo -n "${all_grants}" | wc -m)
					grant_count=$((grant_count-1))
					get_grant_actions
					sql_cmd="GRANT ${grants} ON ALL PROCEDURES IN SCHEMA \"${schema_name}\" TO GROUP \"${grantee}\";"
					ii=$((ii+1))
					exec_sql="${exec_dir}/${prefix}_${ii}.sql"
					echo "${sql_cmd}" > ${exec_sql}
					echo -ne "."
					psql -h $TARGET_PGHOST -p $TARGET_PGPORT -d $TARGET_PGDATABASE -U $TARGET_PGUSER -f "${exec_sql}" -e > $PWD/log/${prefix}_${ii}.log
				else
					#user does not have permisssion to all routines. Execute grant for each in this schema.
					for zz in $(psql -h $SOURCE_PGHOST -p $SOURCE_PGPORT -d $SOURCE_PGDATABASE -U $SOURCE_PGUSER -t -A -c "
					SELECT sub2.oid, sub2.nspname, sub2.proname, sub2.param_count, split_part(sub2.acl, '=', 1) AS grantee 
					FROM 	(
						SELECT sub.oid, sub.nspname, sub.proname, sub.param_count, split_part(split_part(array_to_string(sub.proacl, ','), ',', i), ' ', 2) AS acl
						FROM 	(
							SELECT p.prooid AS oid, n.nspname, p.proname, generate_series(1, array_upper(p.proacl, 1)) AS i, CASE WHEN p.proallargtypes IS NULL THEN CASE WHEN oidvectortypes(p.proargtypes) = '' THEN 0 ELSE regexp_count(oidvectortypes(p.proargtypes), ',')+1 END ELSE array_upper(proallargtypes, 1) END AS param_count, p.proacl 
							FROM pg_proc_info p 
							JOIN pg_namespace n ON p.pronamespace = n.oid 
							JOIN pg_language l ON p.prolang = l.oid 
							JOIN pg_user u ON p.proowner = u.usesysid 
							WHERE l.lanname = 'plpgsql' AND u.usename <> 'rdsdb' AND p.prokind = 'p' AND p.proname NOT LIKE 'mv_sp__%'
							) AS sub 
						WHERE split_part(array_to_string(sub.proacl, ','), ',', i) LIKE 'group %'
						) AS sub2 
					WHERE sub2.nspname = '${schema_name}' AND split_part(sub2.acl, '=', 1) = '${grantee}' AND split_part(split_part(sub2.acl, '=', 2), '/', 1) = 'X'"); do
						oid=$(echo ${zz} | awk -F '|' '{print $1}')
						schema_name=$(echo ${zz} | awk -F '|' '{print $2}')
						proname=$(echo ${zz} | awk -F '|' '{print $3}')
						param_count=$(echo ${zz} | awk -F '|' '{print $4}')
						grantee=$(echo ${zz} | awk -F '|' '{print $5}')
						all_grants="X"
						grant_count=$(echo -n "${all_grants}" | wc -m)
						grant_count=$((grant_count-1))
						get_grant_actions
						get_params
						sql_cmd="GRANT ${grants} ON PROCEDURE \"${schema_name}\".\"${proname}\"${params} TO GROUP \"${grantee}\";"
						ii=$((ii+1))
						exec_sql="${exec_dir}/${prefix}_${ii}.sql"
						echo "${sql_cmd}" > ${exec_sql}
						echo -ne "."
						psql -h $TARGET_PGHOST -p $TARGET_PGPORT -d $TARGET_PGDATABASE -U $TARGET_PGUSER -f "${exec_sql}" -e > $PWD/log/${prefix}_${ii}.log
					done
				fi
			done
		fi
		echo "."
	done
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
	obj_count=$(psql -h $SOURCE_PGHOST -p $SOURCE_PGPORT -d $SOURCE_PGDATABASE -U $SOURCE_PGUSER -t -A -c "
	SELECT COUNT(DISTINCT sub2.nspname) 
	FROM 	(
		SELECT sub.defaclobjtype, sub.nspname, split_part(array_to_string(sub.defaclacl, ','), ',', i) AS acl 
		FROM 	(
			SELECT d.defaclobjtype, n.nspname, generate_series(1, array_upper(d.defaclacl, 1)) AS i, d.defaclacl 
			FROM pg_default_acl d 
			JOIN pg_namespace n ON d.defaclnamespace = n.oid
			) AS sub 
		WHERE split_part(array_to_string(sub.defaclacl, ','), ',', i) NOT LIKE 'group %'
		) AS sub2 
	WHERE sub2.nspname NOT IN ${EXCLUDED_SCHEMAS}")
	echo "INFO: ${prefix}:creating ${obj_count}"
	for x in $(psql -h $SOURCE_PGHOST -p $SOURCE_PGPORT -d $SOURCE_PGDATABASE -U $SOURCE_PGUSER -t -A -c "
	SELECT sub2.usename, sub2.nspname, sub2.defaclobjtype, split_part(sub2.acl, '=', 1) AS grantee, split_part(split_part(sub2.acl, '=', 2), '/', 1) AS usegrant 
	FROM 	(
		SELECT sub.usename, sub.defaclobjtype, sub.nspname, split_part(array_to_string(sub.defaclacl, ','), ',', i) AS acl 
		FROM 	(
			SELECT u.usename, d.defaclobjtype, n.nspname, generate_series(1, array_upper(d.defaclacl, 1)) AS i, d.defaclacl 
			FROM pg_default_acl d 
			JOIN pg_namespace n ON d.defaclnamespace = n.oid 
			JOIN pg_user u ON u.usesysid = d.defacluser
			) AS sub 
		WHERE split_part(array_to_string(sub.defaclacl, ','), ',', i) NOT LIKE 'group %'
		) AS sub2 
	WHERE sub2.nspname NOT IN ${EXCLUDED_SCHEMAS} ORDER BY 2, 3, 4"); do
		usename=$(echo ${x} | awk -F '|' '{print $1}')
		schema_name=$(echo ${x} | awk -F '|' '{print $2}')
		object_type=$(echo ${x} | awk -F '|' '{print $3}')
		grantee=$(echo ${x} | awk -F '|' '{print $4}')
		all_grants=$(echo ${x} | awk -F '|' '{print $5}')
		grant_count=$(echo -n "${all_grants}" | wc -m)
		#change to 0 base
		grant_count=$((grant_count-1))
		if [ "${i}" -gt "0" ]; then
			if [ ! "${schema_name}" == "${previous_schema_name}" ]; then
				wait_for_threads ${exec_dir}
				echo "INFO: ${prefix}:${i}:${obj_count}:${previous_schema_name}"
				psql -h $TARGET_PGHOST -p $TARGET_PGPORT -d $TARGET_PGDATABASE -U $TARGET_PGUSER -f "${exec_sql}" -e > $PWD/log/${prefix}_${i}.log 2>&1 & 
			fi
		fi
		if [ ! "${schema_name}" == "${previous_schema_name}" ]; then
			i=$((i+1))
			exec_sql="${exec_dir}/${prefix}_${i}.sql"
		fi
		get_grant_actions
		sql_cmd="ALTER DEFAULT PRIVILEGES FOR USER \"${usename}\" IN SCHEMA \"${schema_name}\" GRANT ${grants}"
		if [ "${object_type}" == "r" ]; then
			sql_cmd+=" ON TABLES TO \"${grantee}\";"
		elif [ "${object_type}" == "f" ]; then
			sql_cmd+=" ON FUNCTIONS TO \"${grantee}\";"
		elif [ "${object_type}" == "p" ]; then
			sql_cmd+=" ON PROCEDURES TO \"${grantee}\";"
		fi
		echo "${sql_cmd}" >> ${exec_sql}
		previous_schema_name="${schema_name}"
	done
	wait_for_threads ${exec_dir}
	echo "INFO: ${prefix}:${i}:${obj_count}:${schema_name}"
	psql -h $TARGET_PGHOST -p $TARGET_PGPORT -d $TARGET_PGDATABASE -U $TARGET_PGUSER -f "${exec_sql}" -e > $PWD/log/${prefix}_${i}.log 2>&1 & 
	wait_for_remaining "${exec_dir}" 
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
	obj_count=$(psql -h $SOURCE_PGHOST -p $SOURCE_PGPORT -d $SOURCE_PGDATABASE -U $SOURCE_PGUSER -t -A -c "
	SELECT COUNT(DISTINCT sub2.nspname) 
	FROM 	(
		SELECT sub.defaclobjtype, sub.nspname, split_part(array_to_string(sub.defaclacl, ','), ',', i) AS acl 
		FROM 	(
			SELECT d.defaclobjtype, n.nspname, generate_series(1, array_upper(d.defaclacl, 1)) AS i, d.defaclacl 
			FROM pg_default_acl d JOIN pg_namespace n ON d.defaclnamespace = n.oid
			) AS sub 
		WHERE split_part(array_to_string(sub.defaclacl, ','), ',', i) LIKE 'group %'
		) AS sub2 
	WHERE sub2.nspname NOT IN ${EXCLUDED_SCHEMAS}")
	echo "INFO: ${prefix}:creating ${obj_count}"
	for x in $(psql -h $SOURCE_PGHOST -p $SOURCE_PGPORT -d $SOURCE_PGDATABASE -U $SOURCE_PGUSER -t -A -c "
	SELECT sub2.usename, sub2.nspname, sub2.defaclobjtype, split_part(split_part(sub2.acl, '=', 1), ' ', 2) AS groname, split_part(split_part(sub2.acl, '=', 2), '/', 1) AS grogrant 
	FROM 	(
		SELECT sub.defaclobjtype, sub.usename, sub.nspname, split_part(array_to_string(sub.defaclacl, ','), ',', i) AS acl 
		FROM 	(
			SELECT u.usename, n.nspname, d.defaclobjtype, generate_series(1, array_upper(d.defaclacl, 1)) AS i, d.defaclacl 
			FROM pg_default_acl d 
			JOIN pg_namespace n ON d.defaclnamespace = n.oid 
			JOIN pg_user u ON u.usesysid = d.defacluser
			) AS sub 
		WHERE split_part(array_to_string(sub.defaclacl, ','), ',', i) LIKE 'group %'
		) AS sub2 
	WHERE sub2.nspname NOT IN ${EXCLUDED_SCHEMAS} ORDER BY 2, 3, 4"); do
		usename=$(echo ${x} | awk -F '|' '{print $1}')
		schema_name=$(echo ${x} | awk -F '|' '{print $2}')
		object_type=$(echo ${x} | awk -F '|' '{print $3}')
		group_name=$(echo ${x} | awk -F '|' '{print $4}')
		all_grants=$(echo ${x} | awk -F '|' '{print $5}')
		grant_count=$(echo -n "${all_grants}" | wc -m)
		#change to 0 base
		grant_count=$((grant_count-1))
		counter="0"
		if [ "${i}" -gt "0" ]; then
			if [ ! "${schema_name}" == "${previous_schema_name}" ]; then
				wait_for_threads ${exec_dir}
				echo "INFO: ${prefix}:${i}:${obj_count}:${previous_schema_name}"
				psql -h $TARGET_PGHOST -p $TARGET_PGPORT -d $TARGET_PGDATABASE -U $TARGET_PGUSER -f "${exec_sql}" -e > $PWD/log/${prefix}_${i}.log 2>&1 & 
			fi
		fi
		if [ ! "${schema_name}" == "${previous_schema_name}" ]; then
			i=$((i+1))
			exec_sql="${exec_dir}/${prefix}_${i}.sql"
		fi
		get_grant_actions
		sql_cmd="ALTER DEFAULT PRIVILEGES FOR USER \"${usename}\" IN SCHEMA \"${schema_name}\" GRANT ${grants}"
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
		previous_schema_name="${schema_name}"
	done
	wait_for_threads ${exec_dir}
	echo "INFO: ${prefix}:${i}:${obj_count}:${schema_name}"
	psql -h $TARGET_PGHOST -p $TARGET_PGPORT -d $TARGET_PGDATABASE -U $TARGET_PGUSER -f "${exec_sql}" -e > $PWD/log/${prefix}_${i}.log 2>&1 & 
	wait_for_remaining "${exec_dir}" 
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

echo "INFO: Migrate permissions step complete"
