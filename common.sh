#!/bin/bash
set -e

LOCALPWD=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
source ${LOCALPWD}/config.sh
tag="7461670A"

#functions are executed in parallel either in a script in the exec_dir or individual psql commands identified with a tag.
wait_for_threads()
{
	thread_check="${1}"
	thread_count=$(ps -ef | grep "${thread_check}" | grep -v grep | wc -l)
	while [ "${thread_count}" -gt "${LOAD_THREADS}" ]; do
		sleep 1
		thread_count=$(ps -ef | grep "${thread_check}" | grep -v grep | wc -l)
	done
}
wait_for_remaining()
{
	thread_check="${1}"
	thread_count=$(ps -ef | grep "${thread_check}" | grep -v grep | wc -l)
	echo -ne "INFO: ${thread_count} remaining threads."
	while [ "${thread_count}" -gt "0" ]; do
		prev_thread_count="${thread_count}"
		echo -ne "."
		sleep 5
		thread_count=$(ps -ef | grep "${thread_check}" | grep -v grep | wc -l)
		if [  "${prev_thread_count}" -ne "${thread_count}" ]; then
			echo "."
			echo -ne "INFO: ${thread_count} remaining threads."
		fi
	done
	echo "."

	error_count=$(grep ERROR $LOCALPWD/log/${prefix}*.log 2> /dev/null | wc -l)
	if [ "${error_count}" -gt "0" ]; then
		echo "WARNING: ${error_count} Errors found! Check $LOCALPWD/log/${prefix}*.log for details."
	fi
	syntax_error_count=$(grep "syntax error" $LOCALPWD/log/${prefix}_*.log | wc -l)
	if [ "${syntax_error_count}" -gt "0" ]; then
		echo "WARNING: ${fatal_count} Syntax Errors found! Check $LOCALPWD/log/${prefix}*.log for details."
	fi
	fatal_count=$(grep FATAL $LOCALPWD/log/${prefix}*.log 2> /dev/null | wc -l)
	if [ "${fatal_count}" -gt "0" ]; then
		echo "WARNING: ${fatal_count} FATAL Errors found! Check $LOCALPWD/log/${prefix}*.log for details."
	fi
}
exec_fn()
{
	fn="${1}"
	${fn}
	fatal_count=$(grep FATAL $LOCALPWD/log/${fn}_*.log | wc -l)
	error_count=$(grep ERROR $LOCALPWD/log/${fn}_*.log | wc -l)
	syntax_error_count=$(grep "syntax error" $LOCALPWD/log/${fn}_*.log | wc -l)

	if [[ "${error_count}" -eq "0" && "${fatal_count}" -eq "0" && "${syntax_error_count}" -eq "0" ]]; then
		echo "INFO: No errors found with ${fn}."
	else
		echo "INFO: Errors found! Starting retries."
		for retry in $(seq 1 ${RETRY}); do
			#remove old logs for retry
			rm -f $LOCALPWD/log/${fn}_*.log
			${fn}
			fatal_count=$(grep FATAL $LOCALPWD/log/${fn}_*.log 2> /dev/null | wc -l)
			error_count=$(grep ERROR $LOCALPWD/log/${fn}_*.log 2> /dev/null | wc -l)
			syntax_error_count=$(grep "syntax error" $LOCALPWD/log/${fn}_*.log | wc -l)
			if [[ "${error_count}" -eq "0" && "${fatal_count}" -eq "0" && "${syntax_error_count}" -eq "0" ]]; then
				echo "INFO: No more errors found. Exiting after ${retry} retries."
				break
			fi
		done
	fi
	fatal_count=$(grep FATAL $LOCALPWD/log/${fn}_*.log 2> /dev/null | wc -l)
	error_count=$(grep ERROR $LOCALPWD/log/${fn}_*.log 2> /dev/null | wc -l)
	syntax_error_count=$(grep "syntax error" $LOCALPWD/log/${fn}_*.log | wc -l)
	if [[ "${error_count}" -gt "0" || "${fatal_count}" -gt "0" || "${syntax_error_count}" -gt "0" ]]; then
		echo "ERROR: Errors still found after ${RETRY} retries."
		exit 1
	fi
}
