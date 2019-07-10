create database if not exists qfbap_dws;

-- 用户30天访问情况表
drop table if exists qfbap_dws.dws_user_visit_month1;
create table qfbap_dws.dws_user_visit_month1 as
select 
	a.user_id,
	a.type,
	a.cnt,
	a.content,
	a.rn
from (
	select
		t.user_id,
		t.type,
		t.cnt,
		t.content,
		row_number() over(distribute by user_id,type sort by cnt desc) rn
	from (
			select
			  user_id,
			  'visit_ip' as type,-- 近30天访问ip
			  sum(pv) as cnt,
			  visit_ip as content
			from qfbap_dwd.dwd_user_pc_pv
			where dt >= date_add('2018-12-25',-29)
			group by 
			    user_id,
			    visit_ip
			union all
			select
			  user_id,
			  'cookie_id' as type, -- 近30天常用cookie
			  sum(pv) as cnt,
			  cookie_id as content
			from qfbap_dwd.dwd_user_pc_pv
			where dt >= date_add('2018-12-25',-29)
			group by 
			    user_id,
			    cookie_id
			union all 
			select
			  user_id,
			  'browser_name' as type,-- 近30天常用浏览器
			  sum(pv) as cnt,
			  browser_name as content
			from qfbap_dwd.dwd_user_pc_pv
			where dt >= date_add('2018-12-25',-29)
			group by 
			    user_id,
			    browser_name
			union all
			select
			  user_id,
			  'visit_os' as type, -- 近30天常用操作系统
			  sum(pv) as cnt,
			  visit_os as content
			from qfbap_dwd.dwd_user_pc_pv
			where dt >= date_add('2018-12-25',-29)
			group by 
			    user_id,
			    visit_os
		) t	
) a