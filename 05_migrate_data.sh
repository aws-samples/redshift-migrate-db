#!/bin/bash
set -e

source ${PWD}/config.sh
source ${PWD}/common.sh

exec_dir="exec_load"
rm -rf $PWD/${exec_dir}
mkdir -p $PWD/${exec_dir}

load_table()
{
	prefix="load_table"
	i="0"
	OLDIFS=$IFS
	IFS=$'\n'
	#min_attsortkeyord used to filter out tables with an interleaved sort key
	obj_count=$(psql -h $SOURCE_PGHOST -p $SOURCE_PGPORT -d $SOURCE_PGDATABASE -U $SOURCE_PGUSER -t -A -c "SELECT COUNT(*) FROM pg_class c JOIN pg_namespace n ON c.relnamespace = n.oid JOIN svv_all_schemas s ON n.nspname = s.schema_name JOIN (SELECT attrelid, MIN(attsortkeyord) AS min_attsortkeyord FROM pg_attribute GROUP BY attrelid) a ON a.attrelid = c.oid WHERE s.schema_type = 'local' AND s.database_name = current_database() AND s.schema_name NOT IN ${EXCLUDED_SCHEMAS} AND c.relkind = 'r' AND c.relname NOT LIKE 'mv_tbl__%' AND a.min_attsortkeyord >= 0")
	echo "INFO: ${prefix}:creating ${obj_count}"
	for schema_name in $(psql -h $SOURCE_PGHOST -p $SOURCE_PGPORT -d $SOURCE_PGDATABASE -U $SOURCE_PGUSER -t -A -c "SELECT schema_name FROM svv_all_schemas WHERE database_name = current_database() AND schema_type = 'local' AND schema_name NOT IN ${EXCLUDED_SCHEMAS} ORDER BY schema_name"); do
		for table_name in $(psql -h $TARGET_PGHOST -p $TARGET_PGPORT -d $TARGET_PGDATABASE -U $TARGET_PGUSER -t -A -c "SELECT REPLACE(c.relname, '\\\$', '\\\\\$') FROM pg_class c JOIN pg_namespace n ON c.relnamespace = n.oid JOIN (SELECT attrelid, MIN(attsortkeyord) AS min_attsortkeyord FROM pg_attribute GROUP BY attrelid) a ON a.attrelid = c.oid WHERE c.relkind = 'r' AND n.nspname = '${schema_name}' AND c.relname NOT LIKE 'mv_tbl__%' AND a.min_attsortkeyord >= 0 ORDER BY c.relname"); do 
			i=$((i+1))
			exec_script="${exec_dir}/${prefix}_${i}.sh"
			echo -e "#!/bin/bash" > ${exec_script}
			echo -e "echo \"INFO: Loading \\\"${schema_name}\\\".\\\"${table_name}\\\"\"" >> ${exec_script}
			echo -e "source_row_count=\$(psql -h $SOURCE_PGHOST -p $SOURCE_PGPORT -d $SOURCE_PGDATABASE -U $SOURCE_PGUSER -t -A -c \"SELECT c.reltuples::bigint FROM pg_class c JOIN pg_namespace n ON c.relnamespace = n.oid WHERE c.relkind = 'r' AND n.nspname = '${schema_name}' AND c.relname = '${table_name}'\")" >> ${exec_script}
			echo -e "echo \"Source Row Count: \${source_row_count}\"" >> ${exec_script}
			echo -e "if [ \"\${source_row_count}\" -eq \"0\" ]; then" >> ${exec_script}
			echo -e "\techo \"INFO: External table \\\"ext_${schema_name}\\\".\\\"${table_name}\\\" empty. Skipping.\"" >> ${exec_script}
			echo -e "else" >> ${exec_script}
			echo -e "\ttarget_row_count=\$(psql -h $TARGET_PGHOST -p $TARGET_PGPORT -d $TARGET_PGDATABASE -U $TARGET_PGUSER -t -A -c \"SELECT 1 FROM \\\"${schema_name}\\\".\\\"${table_name}\\\" LIMIT 1\" | wc -l)" >> ${exec_script}
			echo -e "\tif [ \"\${target_row_count}\" -gt \"0\" ]; then" >> ${exec_script}
			echo -e "\t\techo \"INFO: Target table \\\"ext_${schema_name}\\\".\\\"${table_name}\\\" already loaded. Skipping.\"" >> ${exec_script}
			echo -e "\telse" >> ${exec_script}
			echo -e "\t\ti=\"0\"" >> ${exec_script}
			echo -e "\t\texec_sql=\"INSERT INTO \\\"${schema_name}\\\".\\\"${table_name}\\\" SELECT * FROM \\\"ext_${schema_name}\\\".\\\"${table_name}\\\"\"" >> ${exec_script}
			echo -e "\t\tfor attname in \$(psql -h $SOURCE_PGHOST -p $SOURCE_PGPORT -d $SOURCE_PGDATABASE -U $SOURCE_PGUSER -t -A -c \"SELECT a.attname FROM pg_namespace AS n JOIN pg_class AS c ON n.oid = c.relnamespace JOIN pg_attribute AS a ON c.oid = a.attrelid WHERE c.relkind = 'r' AND a.attsortkeyord > 0 AND n.nspname = '${schema_name}' AND c.relname = '${table_name}' ORDER BY a.attsortkeyord\"); do" >> ${exec_script}
			echo -e "\t\t\ti=\$((i+1))" >> ${exec_script}
			echo -e "\t\t\tif [ \"\${i}\" -eq \"1\" ]; then" >> ${exec_script}
			echo -e "\t\t\t\texec_sql+=\" ORDER BY \\\"\${attname}\\\"\"" >> ${exec_script}
			echo -e "\t\t\telse" >> ${exec_script}
			echo -e "\t\t\t\texec_sql+=\", \\\"\${attname}\\\"\"" >> ${exec_script}
			echo -e "\t\t\tfi" >> ${exec_script}
			echo -e "\t\tdone" >> ${exec_script}
			echo -e "\t\tpsql -h $TARGET_PGHOST -p $TARGET_PGPORT -d $TARGET_PGDATABASE -U $TARGET_PGUSER -t -A -c \"\${exec_sql}\" -e" >> ${exec_script}
			echo -e "\t\texec_sql=\"ANALYZE \\\"${schema_name}\\\".\\\"${table_name}\\\"\"" >> ${exec_script}
			echo -e "\t\tpsql -h $TARGET_PGHOST -p $TARGET_PGPORT -d $TARGET_PGDATABASE -U $TARGET_PGUSER -t -A -c \"\${exec_sql}\" -e" >> ${exec_script}
			echo -e "\tfi" >> ${exec_script}
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
create_materialized_view()
{
	prefix="create_materialized_view"
	i="0"
	OLDIFS=$IFS
	IFS=$'\n'
	obj_count=$(psql -h $SOURCE_PGHOST -p $SOURCE_PGPORT -d $SOURCE_PGDATABASE -U $SOURCE_PGUSER -t -A -c "SELECT COUNT(*) FROM pg_class c JOIN pg_namespace n ON c.relnamespace = n.oid JOIN svv_all_schemas s ON n.nspname = s.schema_name WHERE s.database_name = current_database() AND s.schema_type = 'local' AND s.schema_name NOT IN ${EXCLUDED_SCHEMAS} AND c.relkind = 'v' AND LOWER(pg_get_viewdef(c.oid)) LIKE '%materialized%'")
	echo "INFO: ${prefix}:creating ${obj_count}"
	for schema_name in $(psql -h $SOURCE_PGHOST -p $SOURCE_PGPORT -d $SOURCE_PGDATABASE -U $SOURCE_PGUSER -t -A -c "SELECT schema_name FROM svv_all_schemas WHERE database_name = current_database() AND schema_type = 'local' AND schema_name NOT IN ${EXCLUDED_SCHEMAS} ORDER BY schema_name"); do
		for view_name in $(psql -h $SOURCE_PGHOST -p $SOURCE_PGPORT -d $SOURCE_PGDATABASE -U $SOURCE_PGUSER -t -A -c "SELECT REPLACE(c.relname, '\\\$', '\\\\\$') FROM pg_class c JOIN pg_namespace n ON c.relnamespace = n.oid WHERE c.relkind = 'v' AND n.nspname = '${schema_name}' AND LOWER(pg_get_viewdef(c.oid)) LIKE '%materialized%' ORDER BY 1"); do 
			i=$((i+1))
			exec_script="${exec_dir}/${prefix}_${i}.sh"
			echo -e "#!/bin/bash" > ${exec_script}
			echo -e "echo \"INFO: Creating Materialized View \\\"${schema_name}\\\".\\\"${view_name}\\\"\"" >> ${exec_script}
			echo -e "count=\$(psql -h $TARGET_PGHOST -p $TARGET_PGPORT -d $TARGET_PGDATABASE -U $TARGET_PGUSER -t -A -c \"SELECT COUNT(*) FROM pg_class c JOIN pg_namespace n ON c.relnamespace = n.oid WHERE c.relkind = 'v' AND n.nspname = '${schema_name}' AND c.relname = '${view_name}'\")" >> ${exec_script}
			echo -e "if [ \"\${count}\" -eq \"0\" ]; then" >> ${exec_script}
			echo -e "\tcreate_view_ddl=\$(psql -h $SOURCE_PGHOST -p $SOURCE_PGPORT -d $SOURCE_PGDATABASE -U $SOURCE_PGUSER -t -A -c \"SHOW VIEW \\\"${schema_name}\\\".\\\"${view_name}\\\"\")" >> ${exec_script}
			echo -e "\tpsql -h $TARGET_PGHOST -p $TARGET_PGPORT -d $TARGET_PGDATABASE -U $TARGET_PGUSER -c \"\${create_view_ddl}\" -e" >> ${exec_script} 
			echo -e "else" >> ${exec_script}
			echo -e "\techo \"INFO: MATERIALIZED VIEW \\\"${schema_name}\\\".\\\"${view_name}\\\" already exists in TARGET.\"" >> ${exec_script}
			echo -e "fi" >> ${exec_script}
			chmod 755 ${exec_script}
			
			wait_for_threads "${exec_dir}"
			echo "INFO: ${prefix}:${i}:${obj_count}:${schema_name}.${view_name}"
			${exec_script} > $PWD/log/${prefix}_${i}.log 2>&1 &
		done
	done
	wait_for_remaining "${tag}"
	IFS=$OLDIFS
}

exec_fn "load_table"
exec_fn "create_materialized_view"

echo "INFO: Migrate data step complete"
