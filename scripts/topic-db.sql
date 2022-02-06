create database KAFKA_SENSORDB;
use KAFKA_SENSORDB;

show tables;

create table sensor_data(skey varchar(50), svalue varchar(50));
drop table if exists sensor_offsets;
create table sensor_offsets(topic_name varchar(50), `partition` int, `offset` BigInt);
commit;


insert into sensor_offsets values('SensorTopic', 0, 0);
insert into sensor_offsets values('SensorTopic', 1, 0);
insert into sensor_offsets values('SensorTopic', 2, 0);
insert into sensor_offsets values('SensorTopic', 3, 0);
insert into sensor_offsets values('SensorTopic', 4, 0);

commit;

select count(*) from sensor_data;
select * from sensor_data;
select * from sensor_offsets;