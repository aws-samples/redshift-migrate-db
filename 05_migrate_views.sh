#!/bin/bash
set -e

source ${PWD}/config.sh
source ${PWD}/common.sh
exec_dir="exec_load"
rm -rf $PWD/${exec_dir}
mkdir -p $PWD/${exec_dir}

create_view()
{
	prefix="create_view"
	i="0"
	OLDIFS=$IFS
	IFS=$'\n'
	obj_count=$(psql -h $SOURCE_PGHOST -p $SOURCE_PGPORT -d $SOURCE_PGDATABASE -U $SOURCE_PGUSER -t -A -c "SELECT COUNT(*) FROM pg_class c JOIN pg_namespace n ON c.relnamespace = n.oid JOIN svv_all_schemas s ON s.schema_name = n.nspname WHERE s.database_name = current_database() AND s.schema_type = 'local' AND s.schema_name NOT IN ${EXCLUDED_SCHEMAS} AND c.relkind = 'v' AND LOWER(pg_get_viewdef(c.oid)) NOT LIKE '%materialized%'")
	echo "INFO: ${prefix}:creating ${obj_count}"
	for schema_name in $(psql -h $SOURCE_PGHOST -p $SOURCE_PGPORT -d $SOURCE_PGDATABASE -U $SOURCE_PGUSER -t -A -c "SELECT schema_name FROM svv_all_schemas WHERE database_name = current_database() AND schema_type = 'local' AND schema_name NOT IN ${EXCLUDED_SCHEMAS}"); do
		for view_name in $(psql -h $SOURCE_PGHOST -p $SOURCE_PGPORT -d $SOURCE_PGDATABASE -U $SOURCE_PGUSER -t -A -c "SELECT c.relname FROM pg_class c JOIN pg_namespace n ON c.relnamespace = n.oid WHERE c.relkind = 'v' AND n.nspname = '${schema_name}' AND LOWER(pg_get_viewdef(c.oid)) NOT LIKE '%materialized%' ORDER BY c.relname"); do 
			i=$((i+1))
			exec_script="${exec_dir}/${prefix}_${i}.sh"
			echo -e "count=\$(psql -h $TARGET_PGHOST -p $TARGET_PGPORT -d $TARGET_PGDATABASE -U $TARGET_PGUSER -t -A -c \"SELECT COUNT(*) FROM pg_class c JOIN pg_namespace n ON c.relnamespace = n.oid WHERE c.relkind = 'v' AND n.nspname = '${schema_name}' AND c.relname = '${view_name}'\")" >> ${exec_script}
			echo -e "late_binding_check=\$(psql -h $SOURCE_PGHOST -p $SOURCE_PGPORT -d $SOURCE_PGDATABASE -U $SOURCE_PGUSER -t -A -c \"SELECT COUNT(*) FROM pg_get_late_binding_view_cols() cols(view_schema name, view_name name, col_name name, col_type varchar, col_num int) WHERE view_schema = '${schema_name}' AND view_name = '${view_name}'\")" >> ${exec_script}
			echo -e "if [ \"\${count}\" -eq \"0\" ]; then" >> ${exec_script}
			echo -e "\tif [ \"\${late_binding_check}\" -eq \"0\" ]; then" >> ${exec_script}
			echo -e "\t\tcreate_view_ddl=\"CREATE VIEW \\\"${schema_name}\\\".\\\"${view_name}\\\" AS \"" >> ${exec_script}
			echo -e "\telse" >> ${exec_script}
			echo -e "\t\tcreate_view_ddl=\"\"" >> ${exec_script}
			echo -e "\tfi" >> ${exec_script}
			echo -e "\tcreate_view_ddl+=\$(psql -h $SOURCE_PGHOST -p $SOURCE_PGPORT -d $SOURCE_PGDATABASE -U $SOURCE_PGUSER -t -A -c \"SHOW VIEW \\\"${schema_name}\\\".\\\"${view_name}\\\"\")" >> ${exec_script}
			echo -e "\tpsql -h $TARGET_PGHOST -p $TARGET_PGPORT -d $TARGET_PGDATABASE -U $TARGET_PGUSER -c \"\${create_view_ddl}\" -e" >> ${exec_script}
			echo -e "else" >> ${exec_script}
			echo -e "\techo \"INFO: VIEW \\\"${schema_name}\\\".\\\"${view_name}\\\" already exists in TARGET\"" >> ${exec_script}
			echo -e "fi" >> ${exec_script}
			chmod 755 ${exec_script}

			wait_for_threads "${exec_dir}"
			echo "INFO: ${prefix}:${i}:${obj_count}:${schema_name}.${view_name}"
			${exec_script} > $PWD/log/${prefix}_${i}.log 2>&1 &
		done
	done
	wait_for_remaining "${exec_dir}"
	IFS=$OLDIFS
}

exec_fn "create_view"

echo "INFO: Migrate views step complete"
