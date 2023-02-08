#!/bin/bash
set -e

source ${PWD}/config.sh
source ${PWD}/common.sh

exec_dir="exec_users"
rm -rf $PWD/${exec_dir}
mkdir -p $PWD/${exec_dir}
tmp_password="P@ssword1"
expire_password=$(date +%Y-%m-%d)

create_user()
{
	prefix="create_user"
	i="0"
	obj_count=$(psql -h $SOURCE_PGHOST -p $SOURCE_PGPORT -d $SOURCE_PGDATABASE -U $SOURCE_PGUSER -t -A -c "SELECT COUNT(*) FROM svl_user_info WHERE usename not like '%_pseudouser'")
	echo "INFO: ${prefix}:creating ${obj_count}"
	for usename in $(psql -h $SOURCE_PGHOST -p $SOURCE_PGPORT -d $SOURCE_PGDATABASE -U $SOURCE_PGUSER -t -A -c "SELECT usename FROM svl_user_info WHERE usename not like '%_pseudouser' ORDER BY usename"); do
		i=$((i+1))
		exec_script="${exec_dir}/${prefix}_${i}.sh"
		echo -e "#!/bin/bash" > ${exec_script}
		echo -e "echo \"INFO: Creating user \\\"${usename}\\\"\"" >> ${exec_script}
		echo -e "count=\$(psql -h $TARGET_PGHOST -p $TARGET_PGPORT -d $TARGET_PGDATABASE -U $TARGET_PGUSER -t -A -c \"SELECT COUNT(*) FROM pg_user_info WHERE usename = '${usename}'\")" >> ${exec_script}
		echo -e "if [ \"\${count}\" -eq \"1\" ]; then" >> ${exec_script}
		echo -e "\techo \"INFO: User \\\"${usename}\\\" already exists.\"" >> ${exec_script}
		echo -e "else" >> ${exec_script}
		echo -e "\tfor i in \$(psql -h $SOURCE_PGHOST -p $SOURCE_PGPORT -d $SOURCE_PGDATABASE -U $SOURCE_PGUSER -t -A -c \"SELECT CASE WHEN usecreatedb THEN 'CREATEDB' ELSE 'NOCREATEDB' END, CASE WHEN usesuper THEN 'CREATEUSER' ELSE 'NOCREATEUSER' END, COALESCE(syslogaccess, 'RESTRICTED'), COALESCE(useconnlimit, '0'),  sessiontimeout FROM svl_user_info WHERE usename = '${usename}'\"); do" >> ${exec_script}
		echo -e "\t\tusecreatedb=\$(echo \$i | awk -F '|' '{print \$1}')" >> ${exec_script}
		echo -e "\t\tusesuper=\$(echo \$i | awk -F '|' '{print \$2}')" >> ${exec_script}
		echo -e "\t\tsyslogaccess=\$(echo \$i | awk -F '|' '{print \$3}')" >> ${exec_script}
		echo -e "\t\tuseconnlimit=\$(echo \$i | awk -F '|' '{print \$4}')" >> ${exec_script}
		echo -e "\t\tsessiontimeout=\$(echo \$i | awk -F '|' '{print \$5}')" >> ${exec_script}
		echo -e "\t\tif [ \"\${sessiontimeout}\" -eq \"0\" ]; then" >> ${exec_script}
		echo -e "\t\t\ttimeout=\"\"" >> ${exec_script}
		echo -e "\t\telse" >> ${exec_script}
		echo -e "\t\t\ttimeout=\"TIMEOUT ${sessiontimeout}\"" >> ${exec_script}
		echo -e "\t\tfi" >> ${exec_script}
		echo -e "\t\texec_sql=\"CREATE USER \\\"${usename}\\\" PASSWORD '${tmp_password}' \${usecreatedb} \${usesuper} SYSLOG ACCESS \${syslogaccess} CONNECTION LIMIT \${useconnlimit} \${timeout} VALID UNTIL '${expire_password}';\"" >> ${exec_script}
		echo -e "\t\tpsql -h $TARGET_PGHOST -p $TARGET_PGPORT -d $TARGET_PGDATABASE -U $TARGET_PGUSER -c \"\${exec_sql}\" -e" >> ${exec_script}
		echo -e "\tdone" >> ${exec_script}
		echo -e "fi" >> ${exec_script}
		echo -e "echo \"Set user config\"" >> ${exec_script}
		echo -e "for i in \$(psql -h $SOURCE_PGHOST -p $SOURCE_PGPORT -d $SOURCE_PGDATABASE -U $SOURCE_PGUSER -t -A -c \"SELECT split_part(array_to_string(sub.useconfig, ','), ',', i) AS useconfig FROM (SELECT generate_series(1, array_upper(useconfig, 1)) AS i, useconfig FROM pg_user WHERE usename = '${usename}' AND useconfig IS NOT NULL) AS sub ORDER BY 1;\"); do" >> ${exec_script}
		echo -e "\tuseconfig=\$(echo \${i} | awk -F '|' '{print \$1}')" >> ${exec_script}
		echo -e "\tcount=\$(psql -h $TARGET_PGHOST -p $TARGET_PGPORT -d $TARGET_PGDATABASE -U $TARGET_PGUSER -t -A -c \"SELECT COUNT(*) FROM (SELECT usename, generate_series(1, array_upper(useconfig, 1)) AS i, useconfig FROM pg_user WHERE useconfig IS NOT NULL) AS sub WHERE sub.usename = '${usename}' AND split_part(array_to_string(sub.useconfig, ','), ',', i) = '\${useconfig}'\")" >> ${exec_script}
		echo -e "\tif [ \"\${count}\" -eq \"1\" ]; then" >> ${exec_script}
		echo -e "\t\techo \"INFO: User ${usename} config \\\"\${useconfig}\\\" already set.\"" >> ${exec_script}
		echo -e "\telse" >> ${exec_script}
		echo -e "\t\tpsql -h $TARGET_PGHOST -p $TARGET_PGPORT -d $TARGET_PGDATABASE -U $TARGET_PGUSER -c \"ALTER USER \\\"${usename}\\\" SET \${useconfig};\" -e" >> ${exec_script}
		echo -e "\tfi" >> ${exec_script}
		echo -e "done" >> ${exec_script}
		chmod 755 ${exec_script}

		wait_for_threads "${exec_dir}"
		echo "INFO: ${prefix}:${i}:${obj_count}:${usename}"
		${exec_script} > $PWD/log/${prefix}_${i}.log 2>&1 &
	done
	wait_for_remaining "${exec_dir}" 
}
create_group()
{
	prefix="create_group"
	i="0"
	obj_count=$(psql -h $SOURCE_PGHOST -p $SOURCE_PGPORT -d $SOURCE_PGDATABASE -U $SOURCE_PGUSER -t -A -c "SELECT COUNT(*) FROM pg_group")
	echo "INFO: ${prefix}:creating ${obj_count}"
	for groname in $(psql -h $SOURCE_PGHOST -p $SOURCE_PGPORT -d $SOURCE_PGDATABASE -U $SOURCE_PGUSER -t -A -c "SELECT groname FROM pg_group ORDER BY groname"); do
		i=$((i+1))
		exec_script="${exec_dir}/${prefix}_${i}.sh"
		echo -e "#!/bin/bash" > ${exec_script}
		echo -e "echo \"INFO: Creating group \\\"${groname}\\\"\"" >> ${exec_script}
		echo -e "count=\$(psql -h $TARGET_PGHOST -p $TARGET_PGPORT -d $TARGET_PGDATABASE -U $TARGET_PGUSER -t -A -c \"SELECT COUNT(*) FROM pg_group WHERE groname = '${groname}'\")" >> ${exec_script}
		echo -e "if [ \"\${count}\" -eq \"1\" ]; then" >> ${exec_script}
		echo -e "\techo \"INFO: group \\\"${groname}\\\" already exists.\"" >> ${exec_script}
		echo -e "else" >> ${exec_script}
		echo -e "\tpsql -h $TARGET_PGHOST -p $TARGET_PGPORT -d $TARGET_PGDATABASE -U $TARGET_PGUSER -c \"CREATE GROUP \\\"${groname}\\\";\" -e" >> ${exec_script}
		echo -e "fi" >> ${exec_script}
		chmod 755 ${exec_script}

		wait_for_threads "${exec_dir}"
		echo "INFO: ${prefix}:${i}:${obj_count}:${groname}"
		${exec_script} > $PWD/log/${prefix}_${i}.log 2>&1 &
	done
	wait_for_remaining "${exec_dir}" 
}
add_user_to_group()
{
	prefix="add_user_to_group"
	i="0"
	OLDIFS=$IFS
	IFS=$'\n'
	obj_count=$(psql -h $SOURCE_PGHOST -p $SOURCE_PGPORT -d $SOURCE_PGDATABASE -U $SOURCE_PGUSER -t -A -c "SELECT COUNT(*) FROM (SELECT groname, grolist, generate_series(1, array_upper(grolist, 1)) AS i FROM pg_group) AS g JOIN pg_user u ON g.grolist[i] = u.usesysid")
	echo "INFO: ${prefix}:creating ${obj_count}"
	for x in $(psql -h $SOURCE_PGHOST -p $SOURCE_PGPORT -d $SOURCE_PGDATABASE -U $SOURCE_PGUSER -t -A -c "SELECT u.usename, g.groname FROM (SELECT groname, grolist, generate_series(1, array_upper(grolist, 1)) AS i FROM pg_group) AS g JOIN pg_user u ON g.grolist[i] = u.usesysid ORDER BY u.usename, g.groname"); do
		i=$((i+1))
		usename=$(echo ${x} | awk -F '|' '{print $1}')
		groname=$(echo ${x} | awk -F '|' '{print $2}')
		wait_for_threads "${tag}"
		echo "INFO: ${prefix}:${i}:${obj_count}:${groname}:${usename}"
		psql -h $TARGET_PGHOST -p $TARGET_PGPORT -d $TARGET_PGDATABASE -U $TARGET_PGUSER -c "ALTER GROUP \"${groname}\" ADD USER \"${usename}\"" -v tag=${tag} -e > $PWD/log/${prefix}_${i}.log 2>&1 &
	done
	wait_for_remaining "${tag}" 
	IFS=$OLDIFS
}

##Users
create_user

##Groups
create_group
add_user_to_group

echo "INFO: Migrate users and groups step complete"
