@echo off
setlocal ENABLEDELAYEDEXPANSION

set SPARK_HOME=d:\apps\spark-3.0.1-hadoop3.2.0
set HADOOP_HOME=d:\apps\winutils-master\hadoop-3.2.0

set ALL_ARGS=%*
set CLS_NAME=%1
echo "All Params  : %ALL_ARGS%"
echo "Class Name  : %CLS_NAME%"

set params=
shift
:loop1
if "%1"=="" goto after_loop
set params=%params% %1
shift
goto loop1

:after_loop
echo "Input Parameters :: %params% "

if %EXE_TYPE%==LOCAL (
    
    echo "Running job with local spark and local files" 
    call :LocalRun srcdest tgtdest

    %SPARK_HOME%\bin\spark-submit.cmd ^
    --master local ^
    --class %CLS_NAME% ^
    file:///d:/apps/hostpath/spark/spark-scala-examples.jar %params% '%srcdest%' '%tgtdest%'

) else if %EXE_TYPE%==DOCKER_FS (
    
    echo "Running job in docker spark cluster with local files" 
    call :DockerFsRun srcdest tgtdest 
    
    echo "Parameters : %params% "
    echo "Parameters : !srcdest! !tgtdest! "

    docker exec spark-master /usr/local/spark/bin/spark-submit ^
    --master spark://spark-master:7077 ^
    --deploy-mode cluster ^
    --conf spark.eventLog.dir=file:///d/apps/hostpath/spark/logs ^
    --class %CLS_NAME% ^
    /usr/local/spark/work-dir/spark-scala-examples.jar %params%

) else if %EXE_TYPE%==DOCKER (
 
    echo "Running job in docker spark cluster."
    call :DockerRun srcdest tgtdest
    
    echo "Parameters : %params% "

    docker exec spark-master /usr/local/spark/bin/spark-submit ^
    --master spark://spark-master:7077 ^
    --deploy-mode cluster ^
    --conf spark.eventLog.dir=file:///d/apps/hostpath/spark/logs ^
    --class %CLS_NAME% ^
    /usr/local/spark/work-dir/spark-scala-examples.jar %params%
    
)
exit /B %ERRORLEVEL%

:LocalRun

    set "%~1=file:///d:/apps/hostpath/spark/"
    set "%~2=file:///d:/apps/hostpath/spark/outputs"
    cd %srcdest:~8%
    echo "Deleting output dir %tgtdest:~8%"
    if exist %tgtdest:~8% ( rm -r %tgtdest:~8% )    
 
EXIT /B %ERRORLEVEL%

:DockerFsRun

    set "%~1=/d/apps/hostpath/spark/"
    set "%~2=/d/apps/hostpath/spark/outputs"
    
    docker exec spark-master rm -Rf "%tgtdest%"

EXIT /B %ERRORLEVEL%

:DockerRun

    set "%~1=hdfs://namenode:9000/files/"
    set "%~2=hdfs://namenode:9000/user/root/outputs/*"

    echo "Delete existing output dir %tgtdest:~20%"
    docker exec namenode hdfs dfs -rm -f -R %tgtdest:~20%
    echo "Output directory %tgtdest:~20% successfully deleted from HDFS"

EXIT /B %ERRORLEVEL%
    
