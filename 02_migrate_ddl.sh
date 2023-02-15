#!/bin/bash
set -e

source ${PWD}/config.sh
source ${PWD}/common.sh

exec_dir="exec_ddl"
rm -rf $PWD/${exec_dir}
mkdir -p $PWD/${exec_dir}

create_schema()
{
	prefix="create_schema"
	i="0"
	OLDIFS=$IFS
	IFS=$'\n'
	obj_count=$(psql -h $SOURCE_PGHOST -p $SOURCE_PGPORT -d $SOURCE_PGDATABASE -U $SOURCE_PGUSER -t -A -c "SELECT COUNT(*) FROM svv_all_schemas WHERE database_name = current_database() AND schema_type = 'local' AND schema_name NOT IN ${EXCLUDED_SCHEMAS}")
	echo "INFO: ${prefix}:creating ${obj_count}"
	for schema_name in $(psql -h $SOURCE_PGHOST -p $SOURCE_PGPORT -d $SOURCE_PGDATABASE -U $SOURCE_PGUSER -t -A -c "SELECT schema_name FROM svv_all_schemas WHERE database_name = current_database() AND schema_type = 'local' AND schema_name NOT IN ${EXCLUDED_SCHEMAS} ORDER BY schema_name"); do
		i=$((i+1))
		exec_script="${exec_dir}/${prefix}_${i}.sh"
		echo -e "#!/bin/bash" > ${exec_script}
		echo -e "count=\$(psql -h $TARGET_PGHOST -p $TARGET_PGPORT -d $TARGET_PGDATABASE -U $TARGET_PGUSER -t -A -c \"SELECT COUNT(*) FROM pg_namespace WHERE nspname = '${schema_name}'\")" >> ${exec_script}
		echo -e "if [ \"\${count}\" -gt \"0\" ]; then" >> ${exec_script}
		echo -e "\techo \"INFO: SCHEMA \\\"${schema_name}\\\" already exists in TARGET\"" >> ${exec_script}
		echo -e "else" >> ${exec_script}
		echo -e "\techo \"INFO: Create Schema \\\"${schema_name}\\\"\"" >> ${exec_script}
		echo -e "\tpsql -h $TARGET_PGHOST -p $TARGET_PGPORT -d $TARGET_PGDATABASE -U $TARGET_PGUSER -t -A -c \"CREATE SCHEMA \\\"${schema_name}\\\"\" -e" >> ${exec_script}
		echo -e "fi" >> ${exec_script}
		chmod 755 ${exec_script}

		wait_for_threads "${exec_dir}"
		echo "INFO: ${prefix}:${i}:${obj_count}:${schema_name}"
		${exec_script} > $PWD/log/${prefix}_${i}.log 2>&1 &
	done
	wait_for_remaining "${exec_dir}"
	IFS=$OLDIFS
}
create_table()
{
	prefix="create_table"
	i="0"
	OLDIFS=$IFS
	IFS=$'\n'
	#function dynamically creates the exec_script file and when run, this script dynamically creates exec_sql.
	obj_count=$(psql -h $SOURCE_PGHOST -p $SOURCE_PGPORT -d $SOURCE_PGDATABASE -U $SOURCE_PGUSER -t -A -c "SELECT COUNT(*) FROM pg_class c JOIN pg_namespace n ON c.relnamespace = n.oid JOIN svv_all_schemas s ON n.nspname = s.schema_name JOIN (SELECT attrelid, MIN(attsortkeyord) AS min_attsortkeyord FROM pg_attribute GROUP BY attrelid) a ON a.attrelid = c.oid WHERE s.schema_type='local' AND s.database_name = current_database() AND s.schema_name NOT IN ${EXCLUDED_SCHEMAS} AND c.relkind = 'r' AND c.relname NOT LIKE 'mv_tbl__%' AND a.min_attsortkeyord >= 0")
	echo "INFO: ${prefix}:creating ${obj_count}"
	for schema_name in $(psql -h $SOURCE_PGHOST -p $SOURCE_PGPORT -d $SOURCE_PGDATABASE -U $SOURCE_PGUSER -t -A -c "SELECT schema_name FROM svv_all_schemas WHERE database_name = current_database() AND schema_type = 'local' AND schema_name NOT IN ${EXCLUDED_SCHEMAS} ORDER BY schema_name"); do
		#edge case for table name with $ in it.
		for table_name in $(psql -h $SOURCE_PGHOST -p $SOURCE_PGPORT -d $SOURCE_PGDATABASE -U $SOURCE_PGUSER -t -A -c "SELECT REPLACE(c.relname, '\\\$', '\\\\\$') FROM pg_class c JOIN pg_namespace n ON c.relnamespace = n.oid JOIN (SELECT attrelid, MIN(attsortkeyord) AS min_attsortkeyord FROM pg_attribute GROUP BY attrelid) a ON a.attrelid = c.oid WHERE c.relkind = 'r' AND n.nspname = '${schema_name}' AND c.relname NOT LIKE 'mv_tbl__%' AND a.min_attsortkeyord >= 0 ORDER BY REPLACE(c.relname, '\\\$', '\\\\\$')"); do 
			i=$((i+1))
			exec_script="${exec_dir}/${prefix}_${i}.sh"
			exec_sql="${exec_dir}/${prefix}_${i}.sql"
			echo -e "#!/bin/bash" > ${exec_script}
			echo -e "echo \"INFO: Creating table \\\"${schema_name}\\\".\\\"${table_name}\\\"\"" >> ${exec_script}
			echo -e "target_table_exists=\$(psql -h $TARGET_PGHOST -p $TARGET_PGPORT -d $TARGET_PGDATABASE -U $TARGET_PGUSER -t -A -c \"SELECT COUNT(*) FROM pg_class c JOIN pg_namespace n ON c.relnamespace = n.oid WHERE c.relkind = 'r' AND n.nspname = '${schema_name}' AND c.relname = '${table_name}'\")" >> ${exec_script}
			echo -e "if [ \"\${target_table_exists}\" -gt \"0\" ]; then" >> ${exec_script}
			echo -e "\tsource_identity_check=\$(psql -h $SOURCE_PGHOST -p $SOURCE_PGPORT -d $SOURCE_PGDATABASE -U $SOURCE_PGUSER -t -A -c \"SELECT COUNT(*) FROM pg_class c JOIN pg_namespace n ON c.relnamespace = n.oid JOIN pg_attribute a ON c.oid = a.attrelid JOIN pg_attrdef ad ON a.attrelid = ad.adrelid AND a.attnum = ad.adnum WHERE c.relkind = 'r' AND n.nspname = '${schema_name}' AND c.relname = '${table_name}' AND ad.adsrc LIKE '%identity%'\")" >> ${exec_script}
			echo -e "\tif [ \"\${source_identity_check}\" -gt \"0\" ]; then" >> ${exec_script}
			echo -e "\t\ttarget_rowcount=\$(psql -h $TARGET_PGHOST -p $TARGET_PGPORT -d $TARGET_PGDATABASE -U $TARGET_PGUSER -t -A -c \"SELECT c.reltuples::bigint FROM pg_class c JOIN pg_namespace n ON c.relnamespace = n.oid WHERE c.relkind = 'r' AND n.nspname = '${schema_name}' AND c.relname = '${table_name}'\")" >> ${exec_script}
			echo -e "\t\tif [ \"\${target_rowcount}\" -eq \"0\" ]; then" >> ${exec_script}
			echo -e "\t\t\tsource_rowcount=\$(psql -h $SOURCE_PGHOST -p $SOURCE_PGPORT -d $SOURCE_PGDATABASE -U $SOURCE_PGUSER -t -A -c \"SELECT c.reltuples::bigint FROM pg_class c JOIN pg_namespace n ON c.relnamespace = n.oid WHERE c.relkind = 'r' AND n.nspname = '${schema_name}' AND c.relname = '${table_name}'\")" >> ${exec_script}
			echo -e "\t\t\tif [ \"\${source_rowcount}\" -gt \"0\" ]; then" >> ${exec_script}
			echo -e "\t\t\t\techo \"INFO: Source has data. Get identity column name.\"" >> ${exec_script}
			echo -e "\t\t\t\tidentity_column_name=\$(psql -h $SOURCE_PGHOST -p $SOURCE_PGPORT -d $SOURCE_PGDATABASE -U $SOURCE_PGUSER -t -A -c \"SELECT a.attname FROM pg_class c JOIN pg_namespace n ON c.relnamespace = n.oid JOIN pg_attribute a ON c.oid = a.attrelid JOIN pg_attrdef ad ON a.attrelid = ad.adrelid AND a.attnum = ad.adnum WHERE c.relkind = 'r' AND n.nspname = '${schema_name}' AND c.relname = '${table_name}' AND ad.adsrc LIKE '%identity%'\")" >> ${exec_script}
			echo -e "\t\t\t\techo \"INFO: Get max value from identity column\"" >> ${exec_script}
			echo -e "\t\t\t\tsource_max=\$(psql -h $SOURCE_PGHOST -p $SOURCE_PGPORT -d $SOURCE_PGDATABASE -U $SOURCE_PGUSER -t -A -c \"SELECT MAX(\${identity_column_name}) FROM \\\"${schema_name}\\\".\\\"${table_name}\\\"\")" >> ${exec_script}
			echo -e "\t\t\t\ttarget_seed=\$(psql -h $TARGET_PGHOST -p $TARGET_PGPORT -d $TARGET_PGDATABASE -U $TARGET_PGUSER -t -A -c \"SELECT split_part(split_part(ad.adsrc, '\\\'', 2), ',', 1) FROM pg_class c JOIN pg_namespace n ON c.relnamespace = n.oid JOIN pg_attribute a ON c.oid = a.attrelid JOIN pg_attrdef ad ON a.attrelid = ad.adrelid AND a.attnum = ad.adnum WHERE c.relkind = 'r' AND n.nspname = '${schema_name}' AND c.relname = '${table_name}' AND ad.adsrc LIKE '%identity%'\")" >> ${exec_script}
			echo -e "\t\t\t\tseed=\$((source_max+1))" >> ${exec_script}
			echo -e "\t\t\t\tif [ \"\${seed}\" -ne \"\${target_seed}\" ]; then" >> ${exec_script}
			echo -e "\t\t\t\t\tpsql -h $TARGET_PGHOST -p $TARGET_PGPORT -d $TARGET_PGDATABASE -U $TARGET_PGUSER -t -A -c \"DROP TABLE \\\"${schema_name}\\\".\\\"${table_name}\\\" CASCADE;\" -e" >> ${exec_script}
			echo -e "\t\t\t\t\tpsql -h $SOURCE_PGHOST -p $SOURCE_PGPORT -d $SOURCE_PGDATABASE -U $SOURCE_PGUSER -t -A -f get_table_ddl.sql -v schema_name=\"'${schema_name}'\" -v table_name=\"'${table_name}'\" > ${exec_sql}" >> ${exec_script}
			echo -e "\t\t\t\t\tdefault_identity_check=\$(grep \"generated by default\" ${exec_sql} | wc -l)" >> ${exec_script}
			echo -e "\t\t\t\t\tif [ \"\${default_identity_check}\" -gt \"0\" ]; then" >> ${exec_script}
			echo -e "\t\t\t\t\t\tsed -i \"s/ identity([0-9]*,[0-9]*)/ identity(\${seed},1)/\" ${exec_sql}" >> ${exec_script}
			echo -e "\t\t\t\t\telse" >> ${exec_script}
			echo -e "\t\t\t\t\t\t#change to generated by default" >> ${exec_script}
			echo -e "\t\t\t\t\t\tsed -i \"s/ identity([0-9]*,[0-9]*)/ generated by default as identity(\${seed},1)/\" ${exec_sql}" >> ${exec_script}
			echo -e "\t\t\t\t\tfi" >> ${exec_script}
			echo -e "\t\t\t\t\tpsql -h $TARGET_PGHOST -p $TARGET_PGPORT -d $TARGET_PGDATABASE -U $TARGET_PGUSER -f ${exec_sql}" >> ${exec_script}
			echo -e "\t\t\t\tfi" >> ${exec_script}
			echo -e "\t\t\tfi" >> ${exec_script}
			echo -e "\t\telse" >> ${exec_script}
			echo -e "\t\t\techo \"INFO: Target table with identity already has data loaded.\"" >> ${exec_script}
			echo -e "\t\tfi" >> ${exec_script}
			echo -e "\telse" >> ${exec_script}
			echo -e "\t\techo \"INFO: Target table without identity already exists.\"" >> ${exec_script}
			echo -e "\tfi" >> ${exec_script}
			echo -e "else" >> ${exec_script}
			echo -e "\tpsql -h $SOURCE_PGHOST -p $SOURCE_PGPORT -d $SOURCE_PGDATABASE -U $SOURCE_PGUSER -t -A -f get_table_ddl.sql -v schema_name=\"'${schema_name}'\" -v table_name=\"'${table_name}'\" > ${exec_sql}" >> ${exec_script}
			echo -e "\tsource_identity_check=\$(psql -h $SOURCE_PGHOST -p $SOURCE_PGPORT -d $SOURCE_PGDATABASE -U $SOURCE_PGUSER -t -A -c \"SELECT COUNT(*) FROM pg_class c JOIN pg_namespace n ON c.relnamespace = n.oid JOIN pg_attribute a ON c.oid = a.attrelid JOIN pg_attrdef ad ON a.attrelid = ad.adrelid AND a.attnum = ad.adnum WHERE c.relkind = 'r' AND n.nspname = '${schema_name}' AND c.relname = '${table_name}' AND ad.adsrc LIKE '%identity%'\")" >> ${exec_script}
			echo -e "\tif [ \"\${source_identity_check}\" -gt \"0\" ]; then" >> ${exec_script}
			echo -e "\t\techo \"INFO: Source has identity column. Get source rowcount.\"" >> ${exec_script}
			echo -e "\t\tseed=\"1\"" >> ${exec_script}
			echo -e "\t\tsource_rowcount=\$(psql -h $SOURCE_PGHOST -p $SOURCE_PGPORT -d $SOURCE_PGDATABASE -U $SOURCE_PGUSER -t -A -c \"SELECT c.reltuples::bigint FROM pg_class c JOIN pg_namespace n ON c.relnamespace = n.oid WHERE c.relkind = 'r' AND n.nspname = '${schema_name}' AND c.relname = '${table_name}'\")" >> ${exec_script}
			echo -e "\t\tif [ \"\${source_rowcount}\" -gt \"0\" ]; then" >> ${exec_script}
			echo -e "\t\t\tidentity_column_name=\$(psql -h $SOURCE_PGHOST -p $SOURCE_PGPORT -d $SOURCE_PGDATABASE -U $SOURCE_PGUSER -t -A -c \"SELECT a.attname FROM pg_class c JOIN pg_namespace n ON c.relnamespace = n.oid JOIN pg_attribute a ON c.oid = a.attrelid JOIN pg_attrdef ad ON a.attrelid = ad.adrelid AND a.attnum = ad.adnum WHERE c.relkind = 'r' AND n.nspname = '${schema_name}' AND c.relname = '${table_name}' AND ad.adsrc LIKE '%identity%'\")" >> ${exec_script}
			echo -e "\t\t\tsource_max=\$(psql -h $SOURCE_PGHOST -p $SOURCE_PGPORT -d $SOURCE_PGDATABASE -U $SOURCE_PGUSER -t -A -c \"SELECT MAX(\${identity_column_name}) FROM \\\"${schema_name}\\\".\\\"${table_name}\\\"\")" >> ${exec_script}
			echo -e "\t\t\tseed=\$((source_max+1))" >> ${exec_script}
			echo -e "\t\tfi" >> ${exec_script}
			echo -e "\t\tdefault_identity_check=\$(grep \" identity\" ${exec_sql} | grep \"generated by default\" | wc -l)" >> ${exec_script}
			echo -e "\t\tif [ \"\${default_identity_check}\" -gt \"0\" ]; then" >> ${exec_script}
			echo -e "\t\t\tsed -i \"s/ identity([0-9]*,[0-9]*)/ identity(\${seed},1)/\" ${exec_sql}" >> ${exec_script}
			echo -e "\t\telse" >> ${exec_script}
			echo -e "\t\t\t#change to generated by default" >> ${exec_script}
			echo -e "\t\t\tsed -i \"s/ identity([0-9]*,[0-9]*)/ generated by default as identity(\${seed},1)/\" ${exec_sql}" >> ${exec_script}
			echo -e "\t\tfi" >> ${exec_script}
			echo -e "\tfi" >> ${exec_script}
			echo -e "\tpsql -h $TARGET_PGHOST -p $TARGET_PGPORT -d $TARGET_PGDATABASE -U $TARGET_PGUSER -f ${exec_sql}" >> ${exec_script}
			echo -e "fi" >> ${exec_script}
			chmod 755 ${exec_script}

			wait_for_threads "${exec_dir}"
			echo "INFO: ${prefix}:${i}:${obj_count}:${schema_name}.${table_name}"
			${exec_script} > $PWD/log/${prefix}_${i}.log 2>&1 &
		done
	done
	wait_for_remaining "${exec_dir}"
	IFS=$OLDIFS
}
create_primary_key()
{
	prefix="create_primary_key"
	i="0"
	OLDIFS=$IFS
	IFS=$'\n'
	obj_count=$(psql -h $SOURCE_PGHOST -p $SOURCE_PGPORT -d $SOURCE_PGDATABASE -U $SOURCE_PGUSER -t -A -c "SELECT COUNT(*) FROM pg_constraint AS con JOIN pg_class AS c ON c.relnamespace = con.connamespace AND c.oid = con.conrelid JOIN pg_namespace AS n ON n.oid = c.relnamespace WHERE c.relkind = 'r' AND con.contype = 'p' AND n.nspname NOT IN ${EXCLUDED_SCHEMAS}")
	echo "INFO: ${prefix}:creating ${obj_count}"
	for schema_name in $(psql -h $SOURCE_PGHOST -p $SOURCE_PGPORT -d $SOURCE_PGDATABASE -U $SOURCE_PGUSER -t -A -c "SELECT schema_name FROM svv_all_schemas WHERE database_name = current_database() AND schema_type = 'local' AND schema_name NOT IN ${EXCLUDED_SCHEMAS} ORDER BY schema_name"); do
		for x in $(psql -h $SOURCE_PGHOST -p $SOURCE_PGPORT -d $SOURCE_PGDATABASE -U $SOURCE_PGUSER -t -A -c "SELECT con.oid, con.conname, c.relname AS table_name FROM pg_constraint AS con JOIN pg_class AS c ON c.relnamespace = con.connamespace AND c.oid = con.conrelid JOIN pg_namespace AS n ON n.oid = c.relnamespace WHERE c.relkind = 'r' AND con.contype = 'p' AND n.nspname = '${schema_name}' ORDER BY c.relname;"); do
			oid=$(echo ${x} | awk -F '|' '{print $1}')
			conname=$(echo ${x} | awk -F '|' '{print $2}')
			table_name=$(echo ${x} | awk -F '|' '{print $3}')
			i=$((i+1))
			exec_script="${exec_dir}/${prefix}_${i}.sh"
			echo -e "#!/bin/bash" > ${exec_script}
			echo -e "constraint_exists=\$(psql -h $TARGET_PGHOST -p $TARGET_PGPORT -d $TARGET_PGDATABASE -U $TARGET_PGUSER -t -A -c \"SELECT COUNT(*) FROM pg_constraint AS con JOIN pg_class AS c ON c.relnamespace = con.connamespace AND c.oid = con.conrelid JOIN pg_namespace AS n ON n.oid = c.relnamespace WHERE c.relkind = 'r' AND con.contype = 'p' AND n.nspname = '${schema_name}' AND c.relname = '${table_name}'\")" >> ${exec_script}
			echo -e "if [ \"\${constraint_exists}\" -eq \"0\" ]; then" >> ${exec_script}
			echo -e "\texec_sql=\"ALTER TABLE \\\"${schema_name}\\\".\\\"${table_name}\\\" ADD CONSTRAINT \\\"${conname}\\\" \"" >> ${exec_script}
			echo -e "\texec_sql+=\$(psql -h $SOURCE_PGHOST -p $SOURCE_PGPORT -d $SOURCE_PGDATABASE -U $SOURCE_PGUSER -t -A -c \"SELECT pg_get_constraintdef(${oid});\")" >> ${exec_script}
			echo -e "\texec_sql+=\";\"" >> ${exec_script}
			echo -e "\tpsql -h $TARGET_PGHOST -p $TARGET_PGPORT -d $TARGET_PGDATABASE -U $TARGET_PGUSER -c \"\${exec_sql}\" -e" >> ${exec_script}
			echo -e "else" >> ${exec_script}
			echo -e "\techo \"INFO: Primary key ${conname} ON ${schema_name}.${table_name} already exists.\"" >> ${exec_script}
			echo -e "fi" >> ${exec_script}
			chmod 755 ${exec_script}

			wait_for_threads "${exec_dir}"
			echo "INFO: ${prefix}:${i}:${obj_count}:${schema_name}.${table_name}:${conname}"
			${exec_script} > $PWD/log/${prefix}_${i}.log 2>&1 &
		done
	done
	wait_for_remaining "${exec_dir}"
	IFS=$OLDIFS
}
create_foreign_key()
{
	prefix="create_foreign_key"
	i="0"
	OLDIFS=$IFS
	IFS=$'\n'
	#function dynamically creates the exec_script file and when run, this script dynamically creates exec_sql.
	obj_count=$(psql -h $SOURCE_PGHOST -p $SOURCE_PGPORT -d $SOURCE_PGDATABASE -U $SOURCE_PGUSER -t -A -c "SELECT COUNT(*) FROM pg_constraint AS con JOIN pg_class AS c ON c.relnamespace = con.connamespace AND c.oid = con.conrelid JOIN pg_namespace AS n ON n.oid = c.relnamespace WHERE c.relkind = 'r' AND con.contype = 'f' AND n.nspname NOT IN ${EXCLUDED_SCHEMAS} AND c.relname NOT LIKE 'mv_tbl__%';")
	echo "INFO: ${prefix}:creating ${obj_count}"
	for schema_name in $(psql -h $SOURCE_PGHOST -p $SOURCE_PGPORT -d $SOURCE_PGDATABASE -U $SOURCE_PGUSER -t -A -c "SELECT schema_name FROM svv_all_schemas WHERE database_name = current_database() AND schema_type = 'local' AND schema_name NOT IN ${EXCLUDED_SCHEMAS} ORDER BY schema_name"); do
		for x in $(psql -h $SOURCE_PGHOST -p $SOURCE_PGPORT -d $SOURCE_PGDATABASE -U $SOURCE_PGUSER -t -A -c "SELECT con.oid, con.conname, c.relname AS table_name, p.relname AS reference_table_name FROM pg_constraint AS con JOIN pg_class AS c ON c.relnamespace = con.connamespace AND c.oid = con.conrelid JOIN pg_class AS p ON p.oid = con.confrelid JOIN pg_namespace AS n ON n.oid = c.relnamespace WHERE c.relkind = 'r' AND con.contype = 'f' AND n.nspname = '${schema_name}' ORDER BY con.oid"); do
			oid=$(echo ${x} | awk -F '|' '{print $1}')
			conname=$(echo ${x} | awk -F '|' '{print $2}')
			table_name=$(echo ${x} | awk -F '|' '{print $3}')
			reference_table_name=$(echo ${x} | awk -F '|' '{print $4}')
			i=$((i+1))
			exec_script="${exec_dir}/${prefix}_${i}.sh"
			echo -e "#!/bin/bash" > ${exec_script}
			echo -e "constraint_exists=\$(psql -h $TARGET_PGHOST -p $TARGET_PGPORT -d $TARGET_PGDATABASE -U $TARGET_PGUSER -t -A -c \"SELECT COUNT(*) FROM pg_constraint AS con JOIN pg_class AS c ON c.relnamespace = con.connamespace AND c.oid = con.conrelid JOIN pg_class AS p ON p.oid = con.confrelid JOIN pg_namespace AS n ON n.oid = c.relnamespace WHERE c.relkind = 'r' AND con.contype = 'f' AND n.nspname = '${schema_name}' AND c.relname = '${table_name}' AND p.relname = '${reference_table_name}' AND con.conname = '${conname}'\")" >> ${exec_script}
			echo -e "if [ \"\${constraint_exists}\" -eq \"0\" ]; then" >> ${exec_script}
			echo -e "\texec_sql=\"ALTER TABLE \\\"${schema_name}\\\".\\\"${table_name}\\\" ADD CONSTRAINT \\\"${conname}\\\" \"" >> ${exec_script}
			echo -e "\texec_sql+=\$(psql -h $SOURCE_PGHOST -p $SOURCE_PGPORT -d $SOURCE_PGDATABASE -U $SOURCE_PGUSER -t -A -c \"SELECT pg_get_constraintdef(${oid});\")" >> ${exec_script}
			echo -e "\texec_sql+=\";\"" >> ${exec_script}
			echo -e "\tpsql -h $TARGET_PGHOST -p $TARGET_PGPORT -d $TARGET_PGDATABASE -U $TARGET_PGUSER -c \"\${exec_sql}\" -e" >> ${exec_script}
			echo -e "else" >> ${exec_script}
			echo -e "\techo \"INFO: Foreign key ${conname} ON ${schema_name}.${table_name} already exists.\"" >> ${exec_script}
			echo -e "fi" >> ${exec_script}
			chmod 755 ${exec_script}

			wait_for_threads "${exec_dir}"
			echo "INFO: ${prefix}:${i}:${obj_count}:${schema_name}.${table_name}:${conname}"
			${exec_script} > $PWD/log/${prefix}_${i}.log 2>&1 &
		done
	done
	wait_for_remaining "${exec_dir}"
	IFS=$OLDIFS
}
create_function()
{
	prefix="create_function"
	i="0"
	OLDIFS=$IFS
	IFS=$'\n'
	obj_count=$(psql -h $SOURCE_PGHOST -p $SOURCE_PGPORT -d $SOURCE_PGDATABASE -U $SOURCE_PGUSER -t -A -c "SELECT COUNT(*) FROM pg_proc_info p JOIN pg_namespace n ON p.pronamespace = n.oid JOIN pg_language l ON p.prolang = l.oid JOIN pg_user u ON p.proowner = u.usesysid JOIN svv_all_schemas s ON s.schema_name = n.nspname WHERE s.database_name = current_database() AND s.schema_type = 'local' AND s.schema_name NOT IN ${EXCLUDED_SCHEMAS} AND l.lanname IN ('sql', 'plpythonu') AND u.usename <> 'rdsdb' AND p.prokind = 'f'")
	echo "INFO: ${prefix}:creating ${obj_count}"
	for schema_name in $(psql -h $SOURCE_PGHOST -p $SOURCE_PGPORT -d $SOURCE_PGDATABASE -U $SOURCE_PGUSER -t -A -c "SELECT schema_name FROM svv_all_schemas WHERE database_name = current_database() AND schema_type = 'local' AND schema_name NOT IN ${EXCLUDED_SCHEMAS} ORDER BY schema_name"); do
		#using OID because the function name can be overloaded
		for x in $(psql -h $SOURCE_PGHOST -p $SOURCE_PGPORT -d $SOURCE_PGDATABASE -U $SOURCE_PGUSER -t -A -c "SELECT p.prooid, p.proname, CASE WHEN p.proallargtypes IS NULL THEN CASE WHEN oidvectortypes(p.proargtypes) = '' THEN 0 ELSE regexp_count(oidvectortypes(p.proargtypes), ',')+1 END ELSE array_upper(proallargtypes, 1) END FROM pg_proc_info p JOIN pg_namespace n ON p.pronamespace = n.oid JOIN pg_language l ON p.prolang = l.oid JOIN pg_user u ON p.proowner = u.usesysid WHERE n.nspname = '${schema_name}' AND l.lanname IN ('sql', 'plpythonu') AND u.usename <> 'rdsdb' AND p.prokind = 'f' ORDER BY p.proname"); do
			oid=$(echo ${x} | awk -F '|' '{print $1}')
			proname=$(echo ${x} | awk -F '|' '{print $2}')
			param_count=$(echo ${x} | awk -F '|' '{print $3}')
			#get parameters
			if [ "${param_count}" -eq "0" ]; then
				exec_sql="CREATE OR REPLACE FUNCTION \"${schema_name}\".\"${proname}\"("
			else
				for y in $(seq 1 ${param_count}); do
					if [ "${y}" -eq "1" ]; then
						param=$(psql -h $SOURCE_PGHOST -p $SOURCE_PGPORT -d $SOURCE_PGDATABASE -U $SOURCE_PGUSER -t -A -c "SELECT CASE WHEN p.proargmodes IS NULL THEN ' IN' WHEN (p.proargmodes[${y}]) = 'i' THEN ' IN' WHEN (p.proargmodes[${y}]) = 'o' THEN ' OUT' WHEN (p.proargmodes[${y}]) = 'b' THEN ' INOUT' END || ' ' || COALESCE(p.proargnames[${y}], '') || ' ' || COALESCE(t.typname, split_part(oidvectortypes(p.proargtypes), ',', ${y})) FROM pg_proc_info p LEFT JOIN pg_type t ON t.oid = p.proallargtypes[${y}] WHERE p.prooid = ${oid}")
						exec_sql="CREATE OR REPLACE FUNCTION \"${schema_name}\".\"${proname}\"(${param}"
					else
						param=$(psql -h $SOURCE_PGHOST -p $SOURCE_PGPORT -d $SOURCE_PGDATABASE -U $SOURCE_PGUSER -t -A -c "SELECT CASE WHEN p.proargmodes IS NULL THEN ' IN' WHEN (p.proargmodes[${y}]) = 'i' THEN ' IN' WHEN (p.proargmodes[${y}]) = 'o' THEN ' OUT' WHEN (p.proargmodes[${y}]) = 'b' THEN ' INOUT' END || ' ' || COALESCE(p.proargnames[${y}], '') || ' ' || COALESCE(t.typname, split_part(oidvectortypes(p.proargtypes), ',', ${y})) FROM pg_proc_info p LEFT JOIN pg_type t ON t.oid = p.proallargtypes[${y}] WHERE p.prooid = ${oid}")
						exec_sql+=", ${param}"
					fi
				done
			fi
			return_type=$(psql -h $SOURCE_PGHOST -p $SOURCE_PGPORT -d $SOURCE_PGDATABASE -U $SOURCE_PGUSER -t -A -c "SELECT t.typname FROM pg_proc_info p JOIN pg_type t ON p.prorettype = t.oid WHERE p.prooid = ${oid};")
			exec_sql+=") returns ${return_type} AS \$\$"
			body=$(psql -h $SOURCE_PGHOST -p $SOURCE_PGPORT -d $SOURCE_PGDATABASE -U $SOURCE_PGUSER -t -A -c "SELECT p.prosrc FROM pg_proc_info p WHERE p.prooid = ${oid}")
			exec_sql+="${body}"
			exec_sql+=" \$\$ "
			for y in $(psql -h $SOURCE_PGHOST -p $SOURCE_PGPORT -d $SOURCE_PGDATABASE -U $SOURCE_PGUSER -t -A -c "SELECT l.lanname, CASE WHEN p.provolatile = 'i' THEN 'immutable' WHEN p.provolatile = 's' THEN 'stable' WHEN p.provolatile = 'v' THEN 'volatile' END FROM pg_proc_info p JOIN pg_language l ON p.prolang = l.oid WHERE p.prooid = ${oid}"); do
				language=$(echo $y | awk -F '|' '{print $1}')
				vol=$(echo $y | awk -F '|' '{print $2}')
			done
			exec_sql+="LANGUAGE ${language} ${vol};"

			wait_for_threads "${tag}"
			i=$((i+1))
			echo "INFO: ${prefix}:${i}:${obj_count}:${schema_name}.${proname}"
			psql -h $TARGET_PGHOST -p $TARGET_PGPORT -d $TARGET_PGDATABASE -U $TARGET_PGUSER -c "${exec_sql}" -v tag=${tag} -e > $PWD/log/${prefix}_${i}.log 2>&1 & 
		done
	done
	wait_for_remaining "${tag}"
	IFS=$OLDIFS
}
create_procedure()
{
	prefix="create_procedure"
	i="0"
	OLDIFS=$IFS
	IFS=$'\n'
	obj_count=$(psql -h $SOURCE_PGHOST -p $SOURCE_PGPORT -d $SOURCE_PGDATABASE -U $SOURCE_PGUSER -t -A -c "SELECT COUNT(*) FROM pg_proc_info p JOIN pg_namespace n ON p.pronamespace = n.oid JOIN pg_language l ON p.prolang = l.oid JOIN pg_user u ON p.proowner = u.usesysid JOIN svv_all_schemas s ON s.schema_name = n.nspname WHERE s.database_name = current_database() AND s.schema_type = 'local' AND s.schema_name NOT IN ${EXCLUDED_SCHEMAS} AND l.lanname = 'plpgsql' AND u.usename <> 'rdsdb' AND p.prokind = 'p'")
	echo "INFO: ${prefix}:creating ${obj_count}"
	for schema_name in $(psql -h $SOURCE_PGHOST -p $SOURCE_PGPORT -d $SOURCE_PGDATABASE -U $SOURCE_PGUSER -t -A -c "SELECT schema_name FROM svv_all_schemas WHERE database_name = current_database() AND schema_type = 'local' AND schema_name NOT IN ${EXCLUDED_SCHEMAS} ORDER BY schema_name"); do
		#using OID because the function name can be overloaded
		for x in $(psql -h $SOURCE_PGHOST -p $SOURCE_PGPORT -d $SOURCE_PGDATABASE -U $SOURCE_PGUSER -t -A -c "SELECT p.prooid, p.proname, CASE WHEN p.proallargtypes IS NULL THEN CASE WHEN oidvectortypes(p.proargtypes) = '' THEN 0 ELSE regexp_count(oidvectortypes(p.proargtypes), ',')+1 END ELSE array_upper(proallargtypes, 1) END FROM pg_proc_info p JOIN pg_namespace n ON p.pronamespace = n.oid JOIN pg_language l ON p.prolang = l.oid JOIN pg_user u ON p.proowner = u.usesysid WHERE n.nspname = '${schema_name}' AND l.lanname = 'plpgsql' AND u.usename <> 'rdsdb' AND p.prokind = 'p' ORDER BY p.proname"); do
			oid=$(echo ${x} | awk -F '|' '{print $1}')
			proname=$(echo ${x} | awk -F '|' '{print $2}')
			param_count=$(echo ${x} | awk -F '|' '{print $3}')
			#get parameters
			if [ "${param_count}" -eq "0" ]; then
				exec_sql="CREATE OR REPLACE PROCEDURE \"${schema_name}\".\"${proname}\"("
			else
				for y in $(seq 1 ${param_count}); do
					if [ "${y}" -eq "1" ]; then
						param=$(psql -h $SOURCE_PGHOST -p $SOURCE_PGPORT -d $SOURCE_PGDATABASE -U $SOURCE_PGUSER -t -A -c "SELECT CASE WHEN p.proargmodes IS NULL THEN ' IN' WHEN (p.proargmodes[${y}]) = 'i' THEN ' IN' WHEN (p.proargmodes[${y}]) = 'o' THEN ' OUT' WHEN (p.proargmodes[${y}]) = 'b' THEN ' INOUT' END || ' ' || COALESCE(p.proargnames[${y}], '') || ' ' || COALESCE(t.typname, split_part(oidvectortypes(p.proargtypes), ',', ${y})) FROM pg_proc_info p LEFT JOIN pg_type t ON t.oid = p.proallargtypes[${y}] WHERE p.prooid = ${oid}")
						exec_sql="CREATE OR REPLACE PROCEDURE \"${schema_name}\".\"${proname}\"(${param}"
					else
						param=$(psql -h $SOURCE_PGHOST -p $SOURCE_PGPORT -d $SOURCE_PGDATABASE -U $SOURCE_PGUSER -t -A -c "SELECT CASE WHEN p.proargmodes IS NULL THEN ' IN' WHEN (p.proargmodes[${y}]) = 'i' THEN ' IN' WHEN (p.proargmodes[${y}]) = 'o' THEN ' OUT' WHEN (p.proargmodes[${y}]) = 'b' THEN ' INOUT' END || ' ' || COALESCE(p.proargnames[${y}], '') || ' ' || COALESCE(t.typname, split_part(oidvectortypes(p.proargtypes), ',', ${y})) FROM pg_proc_info p LEFT JOIN pg_type t ON t.oid = p.proallargtypes[${y}] WHERE p.prooid = ${oid}")
						exec_sql+=", ${param}"
					fi
				done
			fi
			exec_sql+=") AS \$\$"
			body=$(psql -h $SOURCE_PGHOST -p $SOURCE_PGPORT -d $SOURCE_PGDATABASE -U $SOURCE_PGUSER -t -A -c "SELECT p.prosrc FROM pg_proc_info p WHERE p.prooid = ${oid}")
			exec_sql+="${body}"
			exec_sql+=" \$\$ "
			security=$(psql -h $SOURCE_PGHOST -p $SOURCE_PGPORT -d $SOURCE_PGDATABASE -U $SOURCE_PGUSER -t -A -c "SELECT CASE WHEN p.prosecdef THEN 'SECURITY DEFINER' ELSE 'SECURITY INVOKER' END FROM pg_proc_info p WHERE p.prooid = ${oid}")
			exec_sql+="LANGUAGE plpgsql ${security};"

			wait_for_threads "${tag}"
			i=$((i+1))
			echo "INFO: ${prefix}:${i}:${obj_count}:${schema_name}.${proname}"
			psql -h $TARGET_PGHOST -p $TARGET_PGPORT -d $TARGET_PGDATABASE -U $TARGET_PGUSER -c "${exec_sql}" -v tag=${tag} -e > $PWD/log/${prefix}_${i}.log 2>&1 &
		done
	done
	wait_for_remaining "${tag}"
	IFS=$OLDIFS
}
exec_fn "create_schema"
exec_fn "create_table"
exec_fn "create_primary_key"
exec_fn "create_foreign_key"
exec_fn "create_function"
exec_fn "create_procedure"

echo "INFO: Migrate DDL step complete"
