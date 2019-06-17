# set -euxo pipefail

ex_dir(){
	LOG_DIR=$1
	echo $LOG_DIR
	for j in `ls $LOG_DIR`; do
		if [[ -d $LOG_DIR/$j ]]; then
			TMP_LOG=$LOG_DIR/$j/shuffle
		    for i in `ls $TMP_LOG/*.log`; do
		        tt=`cat $i| grep "End executing query" |rev|cut -d ' ' -f 1|rev`

		        name=`basename ${i}`
		        echo "${name}:  ${tt}" >> $LOG_DIR/re${j: -1}.txt
		    done

			# for i in `ls $TMP_LOG/query9/*.log`; do
			# 	nonl=`cat $i | grep "Read nonlocal" | wc -l`
			# 	name=`basename ${i}`
		    #     echo "${name}:  ${nonl}" >> $LOG_DIR/count${j: -1}.txt
			# done
		fi
	done
}


if [[ "$#" -lt 1 ]]; then
	echo "invalid input"
    exit 1
else
	UPPER_DIR=$1
	for sd in `ls $UPPER_DIR`; do
		if [[ -d $UPPER_DIR/$sd ]]; then
			ex_dir $UPPER_DIR/$sd
		fi
	done
fi