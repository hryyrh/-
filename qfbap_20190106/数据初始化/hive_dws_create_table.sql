drop database if exists qfbap_dws cascade;
create database qfbap_dws;
drop table if exists qfbap_dws.dws_user_visit_month1;
create table if not exists qfbap_dws.dws_user_visit_month1
(
   user_id              bigint ,
   type                 string,
   content              string,
   cnt                  bigint,
   rn                   bigint,
   dw_date              string
)
partitioned by (dt string)
stored as orc
;
location '/qfbap/dws/dws_user_visit_month1'
;

userid	class score
1	eng	90
1	math 95
1	

userid	ipcnt iptop cookiecnt cookietop browsertop ostop 
















