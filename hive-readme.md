Another way to initialize metastore_db is by sourcing the hive-schema-3.1.0.mysql.sql script. Below are the steps to follow.

Login to MySQL using command 'mysql -u root -p'
Create the Metastore database (metastore_db in our case) using command 'create database metastore_db'
Use database metastore_db
Source hive-schema-3.1.0.mysql.sql into the MySQL Metastore database using command ‘source $HIVE_HOME/scripts/metastore/upgrade/mysql/hive-schema-3.1.0.mysql.sql’.
source $HIVE_HOME/scripts/metastore/upgrade/mysql/hive-schema-3.1.0.mysql.sql


Hive WebUI : 127.0.0.1:10002

schematool -initSchema -dbType mysql


$SPARK_HOME/sbin/start-thriftserver.sh
$SPARK_HOME/bin/beeline

$HIVE_HOME/bin/beeline -u jdbc:hive2:// -n APP -p APP

beeline> !connect jdbc:hive2://localhost:10000

beeline> !connect jdbc:hive2://spark-master:10000

$SPARK_HOME/bin/beeline.cmd -u jdbc:hive2://spark-master:10000/userdb

/opt/spark/bin/spark-submit --class org.apache.spark.sql.hive.thriftserver.HiveThriftServer2 --name Thrift JDBC/ODBC Server

## Insert some data.
### (id BIGINT, qty BIGINT, name STRING)
### PARTITIONED BY (rx_mth_cd STRING COMMENT 'Prescription Date YYYYMM aggregated')
INSERT INTO demo_sales PARTITION (rx_mth_cd="202002") VALUES (2, 2000, 'two');

SELECT * FROM demo_sales;

SHOW PARTITIONS demo_sales;

#
CREATE TABLE IF NOT EXISTS employee ( 
    eid int, 
    name String, 
    salary String, 
    designation String
) 
COMMENT 'Employee details'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE;

LOAD DATA LOCAL INPATH "/user/hive/in/employee.txt" OVERWRITE INTO TABLE employee;

LOAD DATA LOCAL INPATH "/e/apps/hostpath/spark/in/employee-part-1.txt" OVERWRITE INTO TABLE employee PARTITION("designation");

CREATE TABLE insert_partition_demo (
   id int,
   name varchar(10)
)
PARTITIONED BY (dept int)
CLUSTERED BY (id)
INTO 10 BUCKETS
STORED AS ORC TBLPROPERTIES ('orc.compress'='ZLIB','transactional'='true'); 
