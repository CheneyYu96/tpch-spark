set -euxo pipefail
# This script is for one master HDFS, alluxio, spark cluster deployment.

# Prerequisites:
#     1. Install flintrock:
#         https://github.com/nchammas/flintrock#installation
#     2. After 1, make sure you have an AWS account, set your AWS Key Info in your shell and run "flintrock configure" to configure your cluster(Pls refer to https://heather.miller.am/blog/launching-a-spark-cluster-part-1.html#setting-up-flintrock-and-amazon-web-services)
manual_restart(){
	cluster_name=$1

	echo "stop all"
	flintrock run-command --master-only $cluster_name '/home/ec2-user/spark/sbin/stop-all.sh;/home/ec2-user/alluxio/bin/alluxio-stop.sh all;/home/ec2-user/hadoop/sbin/stop-dfs.sh;'

	echo "configure"
	flintrock run-command $cluster_name 'echo "export JAVA_HOME=/usr/lib/jvm/`ls /usr/lib/jvm/ | grep java-1.8.0-openjdk-1.8.0.`" >> /home/ec2-user/hadoop/conf/hadoop-env.sh;'

	flintrock run-command --master-only $cluster_name '/home/ec2-user/hadoop/bin/hdfs namenode -format -nonInteractive || true;'
	
	echo "restart all"
	flintrock run-command --master-only $cluster_name '/home/ec2-user/hadoop/sbin/start-dfs.sh; /home/ec2-user/alluxio/bin/alluxio format; /home/ec2-user/alluxio/bin/alluxio-start.sh all SudoMount; /home/ec2-user/spark/sbin/start-all.sh;'

	echo "manual restart finished"
}
configure_alluxio(){
	# configure alluxio
	cluster_name=$1
	echo "Configure alluxio"

	flintrock run-command $cluster_name 'cd /home/ec2-user; cp /home/ec2-user/alluxio/conf/alluxio-site.properties.template /home/ec2-user/alluxio/conf/alluxio-site.properties'

	# flintrock run-command $cluster_name 'echo "alluxio.user.file.copyfromlocal.write.location.policy.class=alluxio.client.file.policy.TimerPolicy" >> /home/ec2-user/alluxio/conf/alluxio-site.properties;'
	# flintrock run-command $cluster_name 'echo "alluxio.user.file.write.location.policy.class=alluxio.client.file.policy.TimerPolicy" >> /home/ec2-user/alluxio/conf/alluxio-site.properties;'
	# flintrock run-command $cluster_name 'echo "alluxio.worker.hostname=localhost" >> /home/ec2-user/alluxio/conf/alluxio-site.properties;'

	flintrock run-command $cluster_name 'echo "alluxio.user.file.delete.unchecked=true" >> /home/ec2-user/alluxio/conf/alluxio-site.properties;'
	flintrock run-command $cluster_name 'echo "alluxio.user.file.passive.cache.enabled=false" >> /home/ec2-user/alluxio/conf/alluxio-site.properties;'
	# flintrock run-command $cluster_name 'echo "alluxio.user.file.replication.min=2" >> /home/ec2-user/alluxio/conf/alluxio-site.properties;'
	flintrock run-command $cluster_name 'echo "alluxio.user.block.size.bytes.default=1GB" >> /home/ec2-user/alluxio/conf/alluxio-site.properties;'

	flintrock run-command $cluster_name 'echo "fr.repl.interval=600" >> /home/ec2-user/alluxio/conf/alluxio-site.properties; 
	echo "fr.parquet.info=true" >> /home/ec2-user/alluxio/conf/alluxio-site.properties; 
	echo "fr.repl.policy.class=alluxio.master.repl.policy.ColReplPolicy" >> /home/ec2-user/alluxio/conf/alluxio-site.properties;
	echo "fr.client.translation=true" >> /home/ec2-user/alluxio/conf/alluxio-site.properties;
	echo "fr.client.block.location=true" >> /home/ec2-user/alluxio/conf/alluxio-site.properties;
	echo "fr.client.block.finer=false" >> /home/ec2-user/alluxio/conf/alluxio-site.properties;
	echo "fr.record.interval=5000" >> /home/ec2-user/alluxio/conf/alluxio-site.properties;
	echo "fr.repl.global=true" >> /home/ec2-user/alluxio/conf/alluxio-site.properties;
	echo "fr.repl.repeat=false" >> /home/ec2-user/alluxio/conf/alluxio-site.properties;
	echo "fr.repl.budget=1" >> /home/ec2-user/alluxio/conf/alluxio-site.properties;
	echo "fr.repl.budget.access=true" >> /home/ec2-user/alluxio/conf/alluxio-site.properties;
	echo "fr.repl.delorigin=false" >> /home/ec2-user/alluxio/conf/alluxio-site.properties;
	echo "fr.repl.weight=1" >> /home/ec2-user/alluxio/conf/alluxio-site.properties;'

	flintrock run-command $cluster_name 'echo "alluxio.master.hostname=$(cat /home/ec2-user/hadoop/conf/masters)" >> /home/ec2-user/alluxio/conf/alluxio-site.properties;
	echo "alluxio.underfs.address=hdfs://$(cat /home/ec2-user/hadoop/conf/masters):9000/alluxio/root/" >> /home/ec2-user/alluxio/conf/alluxio-site.properties'


	flintrock run-command $cluster_name 'hadoop fs -mkdir -p /alluxio/root/'

	flintrock run-command $cluster_name 'cp /home/ec2-user/hadoop/conf/masters /home/ec2-user/alluxio/conf/masters;
	cp /home/ec2-user/hadoop/conf/slaves /home/ec2-user/alluxio/conf/workers'

}
launch() {
	cluster_name=$1

	echo "Launch cluster ${cluster_name}"
	# launch your specified cluster
	# flintrock launch $cluster_name

	# # # delete java1.8 installed by flintrock
	# # flintrock run-command $cluster_name "sudo yum -y remove java-1.8.0-openjdk.x86_64 java-1.8.0-openjdk-headless.x86_64"

	# # delete JAVA_HOME set by flintrock
	# flintrock run-command $cluster_name 'echo `sed -e '/JAVA_HOME/d' /etc/environment` | sudo tee /etc/environment; source /etc/environment'


	# # install open JDK 1.8 for Developer
	# flintrock run-command $cluster_name 'sudo yum -y install java-1.8.0-openjdk-devel'


	# flintrock run-command $cluster_name 'echo "export JAVA_HOME=/usr/lib/jvm/$(ls /usr/lib/jvm/ | grep java-1.8.0-openjdk-1.8.0.)" >> /home/ec2-user/.bashrc;
	# echo "export JRE_HOME=\$JAVA_HOME/jre" >> /home/ec2-user/.bashrc;
	# echo "export CLASSPATH=.:\$JAVA_HOME/lib:\$JRE_HOME/lib" >> /home/ec2-user/.bashrc;
	# echo "export PATH=\$JAVA_HOME/bin:\$PATH" >> /home/ec2-user/.bashrc'
	
	# # Install maven
	# echo "Install maven"

	# flintrock run-command $cluster_name 'wget http://mirrors.ocf.berkeley.edu/apache/maven/maven-3/3.5.4/binaries/apache-maven-3.5.4-bin.tar.gz;
	# tar zxvf apache-maven-3.5.4-bin.tar.gz;
	# rm apache-maven-3.5.4-bin.tar.gz
	# '

	# flintrock run-command $cluster_name 'echo "export MAVEN_HOME=\$HOME/apache-maven-3.5.4" >> /home/ec2-user/.bashrc;
	# echo "export PATH=\$MAVEN_HOME/bin:\$PATH" >> /home/ec2-user/.bashrc'

	# # Install git & setup
	# echo "Install git"

	# flintrock run-command $cluster_name 'sudo yum -y install git'

	# flintrock run-command $cluster_name "sudo yum -y install iperf3"

	# flintrock run-command $cluster_name 'mkdir -p /home/ec2-user/logs'


	# echo "Install tools for master"

	# flintrock run-command --master-only $cluster_name 'curl https://bintray.com/sbt/rpm/rpm > bintray-sbt-rpm.repo; 
	# sudo mv bintray-sbt-rpm.repo /etc/yum.repos.d/;
	# sudo yum -y install python-pip gcc make flex bison byacc sbt gcc-c++;
	# sudo pip install click paramiko numpy;
	# sudo pip install paramiko -U
	# '

	# flintrock run-command --master-only $cluster_name 'echo "export PYTHONPATH=\$SPARK_HOME/python:\$SPARK_HOME/python/lib/py4j-0.10.7-src.zip:\$PYTHONPATH" >> /home/ec2-user/.bashrc'

	# # Download alluxio source code
	# echo "Download & compile alluxio"

	# flintrock run-command $cluster_name 'git clone git://github.com/CheneyYu96/alluxio.git; cd /home/ec2-user/alluxio; git checkout io-ver;'

	# Compile alluxio source code
	flintrock run-command $cluster_name 'cd /home/ec2-user/alluxio; git pull; mvn -T 2C install -Phadoop-2 -Dhadoop.version=2.8.5 -Dmaven.javadoc.skip -DskipTests -Dlicense.skip=true -Dcheckstyle.skip=true -Dfindbugs.skip=true'
	# flintrock run-command --master-only $cluster_name 'cd /home/ec2-user/alluxio; mvn -T 2C install -Phadoop-2 -Dhadoop.version=2.8.5 -Dmaven.javadoc.skip -DskipTests -Dlicense.skip=true -Dcheckstyle.skip=true -Dfindbugs.skip=true'
	# flintrock run-command --master-only $cluster_name '/home/ec2-user/alluxio/bin/alluxio copyDir /home/ec2-user/alluxio/client'

	## build write parquet module
	flintrock run-command --master-only $cluster_name 'cd /home/ec2-user/alluxio/writeparquet; mvn package -DskipTests -Dlicense.skip=true -Dcheckstyle.skip=true -Dfindbugs.skip=true'
	flintrock run-command $cluster_name 'cd /home/ec2-user/alluxio; git pull; cd readparquet; mvn package -DskipTests -Dlicense.skip=true -Dcheckstyle.skip=true -Dfindbugs.skip=true'
	## build parquet-cli
	flintrock run-command --master-only $cluster_name 'git clone https://github.com/apache/parquet-mr.git; cd parquet-mr/parquet-cli; mvn clean install -DskipTests'
	## replace jars
	flintrock run-command $cluster_name '/home/ec2-user/alluxio/bin/move-par-jar.sh'

	configure_alluxio $cluster_name

	# flintrock run-command --master-only $cluster_name '/home/ec2-user/alluxio/bin/alluxio copyDir /home/ec2-user/alluxio/client'

	# Download workload & compile
	echo "Download & compile workload"
	flintrock run-command $cluster_name 'git clone git://github.com/CheneyYu96/tpch-spark.git'
	# flintrock run-command --master-only $cluster_name 'mv /home/ec2-user/SparkSQL-test /home/ec2-user/tpch-spark'
	flintrock run-command --master-only $cluster_name 'cd /home/ec2-user/tpch-spark/dbgen; make'
	# flintrock run-command $cluster_name 'cd /home/ec2-user/tpch-spark; git pull'
	flintrock run-command --master-only $cluster_name 'cd /home/ec2-user/tpch-spark/; sbt assembly'


	# configure spark
	echo "Configure spark"

	flintrock run-command $cluster_name 'cp /home/ec2-user/spark/conf/spark-defaults.conf.template /home/ec2-user/spark/conf/spark-defaults.conf'
	flintrock run-command $cluster_name 'cp /home/ec2-user/spark/conf/log4j.properties.template /home/ec2-user/spark/conf/log4j.properties'

	flintrock run-command $cluster_name 'echo "spark.driver.extraClassPath /home/ec2-user/alluxio/client/$(ls /home/ec2-user/alluxio/client)" >> /home/ec2-user/spark/conf/spark-defaults.conf;
	echo "spark.executor.extraClassPath /home/ec2-user/alluxio/client/$(ls /home/ec2-user/alluxio/client)" >> /home/ec2-user/spark/conf/spark-defaults.conf;
	echo "spark.sql.parquet.filterPushdown false" >> /home/ec2-user/spark/conf/spark-defaults.conf;
	echo "spark.locality.wait 0" >> /home/ec2-user/spark/conf/spark-defaults.conf'

	# set hadoop
	echo "Configure hadoop"

	flintrock run-command $cluster_name 'sed -i "/<\/configuration>/d" /home/ec2-user/hadoop/conf/core-site.xml;
	echo "
	  <property>
	    <name>fs.alluxio.impl</name>
	    <value>alluxio.hadoop.FileSystem</value>
	  </property>
	</configuration>" >> /home/ec2-user/hadoop/conf/core-site.xml
	'

	flintrock run-command $cluster_name 'echo "export HADOOP_CLASSPATH=/home/ec2-user/alluxio/client/$(ls /home/ec2-user/alluxio/client):\$HADOOP_CLASSPATH" >> /home/ec2-user/.bashrc; source /home/ec2-user/.bashrc'

	# flintrock run-command --master-only $cluster_name '/home/ec2-user/hadoop/sbin/stop-dfs.sh;/home/ec2-user/hadoop/sbin/start-dfs.sh;/home/ec2-user/alluxio/bin/alluxio format;/home/ec2-user/alluxio/bin/alluxio-start.sh all SudoMount'
	
	# echo "setup wondershaper for bandwidth limitation"
	flintrock run-command $cluster_name "sudo yum -y install tc; git clone  https://github.com/magnific0/wondershaper.git; cd wondershaper; sudo make install;" 
	# sudo systemctl enable wondershaper.service; sudo systemctl start wondershaper.service"

	# restart 
	echo "Restart"

	# flintrock stop --assume-yes $cluster_name
	# start $cluster_name
	manual_restart $cluster_name

	## conf spark worker name
	flintrock run-command --master-only $cluster_name '/home/ec2-user/alluxio/bin/conf-spark.sh add'
	flintrock run-command --master-only $cluster_name '/home/ec2-user/alluxio/bin/conf-spark.sh map'
	flintrock run-command --master-only $cluster_name '/home/ec2-user/alluxio/bin/move-readpar-jar.sh'

	flintrock run-command $cluster_name 'echo "ec2-user   soft    nproc     60000" | sudo tee -a /etc/security/limits.d/20-nproc.conf '
}

start() {
	cluster_name=$1

	echo "Start cluster ${cluster_name}"
	flintrock start $cluster_name

	echo "Configure alluxio"
	flintrock run-command $cluster_name 'cp /home/ec2-user/hadoop/conf/masters /home/ec2-user/alluxio/conf/masters;
	cp /home/ec2-user/hadoop/conf/slaves /home/ec2-user/alluxio/conf/workers'

	flintrock run-command $cluster_name 'sed -i "\$d" /home/ec2-user/alluxio/conf/alluxio-site.properties;sed -i "\$d" /home/ec2-user/alluxio/conf/alluxio-site.properties;'

	flintrock run-command $cluster_name 'echo "alluxio.master.hostname=$(cat /home/ec2-user/hadoop/conf/masters)" >> /home/ec2-user/alluxio/conf/alluxio-site.properties;
	echo "alluxio.underfs.address=hdfs://$(cat /home/ec2-user/hadoop/conf/masters):9000/alluxio/root/" >> /home/ec2-user/alluxio/conf/alluxio-site.properties'

	echo "Restart alluxio & hdfs"
	flintrock run-command --master-only $cluster_name '/home/ec2-user/alluxio/bin/alluxio-stop.sh all;/home/ec2-user/hadoop/sbin/stop-dfs.sh;/home/ec2-user/hadoop/sbin/start-dfs.sh;/home/ec2-user/alluxio/bin/alluxio format;/home/ec2-user/alluxio/bin/alluxio-start.sh all SudoMount'

	flintrock run-command --master-only $cluster_name '/home/ec2-user/alluxio/bin/conf-spark.sh add'
	flintrock run-command --master-only $cluster_name '/home/ec2-user/alluxio/bin/conf-spark.sh map'
}

stop() {
	cluster_name=$1
	echo "Stop cluster ${cluster_name}"

	flintrock stop --assume-yes $cluster_name
}

destroy() {
	cluster_name=$1
	echo "Destory cluster ${cluster_name}"

	flintrock destroy $cluster_name
}

updata_alluxio() {
	cluster_name=$1
	echo "Update & Recompile alluxio for cluster ${cluster_name}"

	echo "Pull alluxio for repo"
	flintrock run-command $cluster_name 'cd /home/ec2-user/alluxio; git checkout conf/threshold; git pull'

	echo "Compile ... "
	flintrock run-command $cluster_name 'cd /home/ec2-user/alluxio; mvn install -Phadoop-2 -Dhadoop.version=2.8.5 -DskipTests -Dlicense.skip=true -Dcheckstyle.skip=true'

	configure_alluxio $cluster_name
	
	echo "Restart alluxio & hdfs"
	flintrock run-command --master-only $cluster_name '/home/ec2-user/alluxio/bin/alluxio-stop.sh all;/home/ec2-user/hadoop/sbin/stop-dfs.sh;/home/ec2-user/hadoop/sbin/start-dfs.sh;/home/ec2-user/alluxio/bin/alluxio format;/home/ec2-user/alluxio/bin/alluxio-start.sh all SudoMount'
}

collect_log() {
	cluster_name=$1

	flintrock run-command --master-only $cluster_name 'cd /home/ec2-user; zip -r logs.zip logs/ alluxio/logs/master.log;' # zip -r info.zip info/'
	master_ip=$(flintrock describe $cluster_name | grep master | cut -c 11-)

	scp -o "StrictHostKeyChecking no" ec2-user@${master_ip}:/home/ec2-user/logs.zip ~/Desktop
	# scp -o "StrictHostKeyChecking no" ec2-user@${master_ip}:/home/ec2-user/info.zip ~/Desktop

	flintrock run-command --master-only $cluster_name 'rm /home/ec2-user/logs.zip;' # rm /home/ec2-user/info.zip'

}

usage() {
    echo "Usage: $0 start|stop|launch|destroy|update|conf_alluxio <cluster name>"
}

if [[ "$#" -lt 2 ]]; then
    usage
    exit 1
else
    case $1 in
        start)                  start $2
                                ;;
        stop)                   stop $2
                                ;;
        launch)                	launch $2
                                ;;
        destroy)				destroy $2
        						;;
        update)					updata_alluxio $2
        						;;
		conf_alluxio)			configure_alluxio $2
								;;             
		man_start)				manual_restart $2
								;;
				collect)			collect_log $2
											;;
        * )                     usage
    esac
fi
