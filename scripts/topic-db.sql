create database KAFKA_SENSORDB;
use KAFKA_SENSORDB;

show tables;

create table sensor_data(skey varchar(50), svalue varchar(50));
drop table if exists sensor_offsets;
commit;
create table sensor_offsets(topic_name varchar(50), `partition` int, `offset` BigInt);


insert into sensor_offsets values('SensorTopic', 0, 0);
insert into sensor_offsets values('SensorTopic', 1, 0);
insert into sensor_offsets values('SensorTopic', 2, 0);
insert into sensor_offsets values('SensorTopic', 3, 0);
insert into sensor_offsets values('SensorTopic', 4, 0);

delete from sensor_offsets where `partition`=2;

commit;
select count(*) from sensor_data;
select * from sensor_data;
select * from sensor_offsets;