-- PC端的统计指标
select 
   max(case when pc.rn_desc = 1 then in_time end) latest_pc_visit_date -- 最近一次访问时间
   ,max(case when pc.rn_desc = 1 then session_id end) latest_pc_visit_session -- 最近一次访问的session
   ,max(case when pc.rn_desc = 1 then cookie_id end ) latest_pc_cookies-- 最近一次的coookie
   ,max(case when pc.rn_desc = 1 then pv end) latest_pc_pv -- 最近一次的pc端的pv量
   ,max(case when pc.rn_desc = 1 then browser_name end ) latest_pc_browser_name -- 最近一次访问使用的浏览器
   ,max(case when pc.rn_desc = 1 then visit_os end ) latest_pc_visit_os-- 最近一次访问使用的操作系统
   ,max(case when pc.rn_asc = 1 then in_time end) first_pc_visit_date -- 最早pc端访问的日期
   ,max(case when pc.rn_asc = 1 then session_id end ) first_pc_visit_session--最早pc端访问的session
   ,max(case when pc.rn_asc = 1 then cookie_id end ) first_pc_cookies-- 最早pc端访问的cookie
   ,max(case when pc.rn_asc = 1 then pv end ) first_pc_pv-- 最早一次访问的pv
   ,max(case when pc.rn_asc = 1 then browser_name end) first_pc_browser_name -- 最早一次访问使用的浏览器
   ,max(case when pc.rn_asc = 1 then visit_os end) first_pc_visit_os-- 最早一次访问的os
   ,sum(dt7) day7_pc_cnt --连续7天访问次数
   ,sum(dt15) day15_pc_cnt-- 连续15天访问次数
   ,sum(dt30) month1_pc_cnt -- 连续30天访问次数
   ,sum(dt60) month2_pc_cnt-- 连续60天访问的次数
   ,sum(dt90) month3_pc_cnt -- 连续90天访问的次数,近30天pc访问的天数
   ,count(distinct (case when dt30=1 then substr(in_time,0,8) end) month1_pc_days --近30天pc端访问的次数
   ,sum(case when dt30=1 then pv end ) month1_pc_pv --近30天pc端的pv
   ,sum(case when dt30=1 then pv end ) 
      /count(distinct(case when dt30=1 then substr(in_time,0,8) end)) month1_pc_avg_pv --近30天pc端每天的平均pv
   ,count(case when hr025=1 then 1 end) month1_pc_hour025_cnt --0到5点的数量
   ,count(case when hr627=1 then 1 end ) month1_pc_hour627_cnt --6到7点的数量
   ,count(case when hr829=1 then 1 end ) month1_pc_hour829_cnt -- 8到9的数量
   ,count(case when hr10211=1 then 1 end ) month1_pc_hour10211_cnt -- 10到11的数量
   ,count(case when hr12213=1 then 1 end ) month1_pc_hour12213_cnt --12到13的数量
   ,count(case when hr14215=1 then 1 end ) month1_pc_hour14216_cnt -- 14到16点的数量
   ,count(case when hr16217=1 then 1 end ) month1_pc_hour17219_cnt -- 17到19点的数量
   ,count(case when hr18219=1 then 1 end ) month1_pc_hour18219_cnt -- 18到19点的数量
   ,count(case when hr20221=1 then 1 end ) month1_pc_hour20221_cnt -- 20到21点的数量
   ,count(case when hr22223=1 then 1 end ) month1_pc_hour22223_cnt -- 22到23点的数量
from (
   select 
      row_number() over(distribute by user_id sort by in_time asc) rn_asc,
      row_number() over(distribute by user_id sort by in_time desc) rn_desc,
      user_id,
      session_id,
      cookie_id,
      visit_os,
      browser_name,
      visit_ip,
      province,
      city,
      (case when in_time >=date_add('2018-12-25',-6) then 1 end ) dt7,
      (case when in_time>=date_sub('2018-12-25',14) then 1 end ) dt15,
      (case when in_time >= date_sub('2018-12-25',29) then 1 end) dt30,
      (case when in_time >= date_sub('2018-12-25',59) then 1 end) dt60,
       (case when in_time >= date_sub('2018-12-25',89) then 1 end) dt90,
      (case when hour(in_time) between 0 and 5 then 1 end) hr025,
      (case when hour(in_time) between 6 and 7 then 1 end ) hr627,
      (case when hour(in_time) between 8 and 9 then 1 end ) hr829,
      (case when hour(in_time) between 10 and 11 then 1 end ) hr10211,
      (case when hour(in_time) between 12 and 13 then 1 end ) hr12213,
      (case when hour(in_time) between 14 and 15 then 1 end ) hr14215,
      (case when hour(in_time) between 16 and 17 then 1 end ) hr16217,
      (case when hour(in_time) between 18 and 19 then 1 end ) hr18219,
      (case when hour(in_time) between 20 and 21 then 1 end ) hr20221,
      (case when hour(in_time) between 22 and 23 then 1 end ) hr22223,
      in_time,
      out_time,
      stay_time,
      pv
   from qfbap_dwd.dwd_user_pc_pv t
   where dt >= date_sub('2018-12-25',90) and dt<='2018-12-25'
) pc
group by user_id;



select 
user_id,
count(distinct 
   case 
      when type='visit_ip' 
      then content 
   end) month1_pc_diff_ip_cnt, --近30天访问使用的不同ip数量
max(case 
      when rn=1 
         and  type='visit_ip'
      then content
   end) month1_pc_common_ip, --近30天最常用的ip
count(
   distinct
   case 
      when rn=1
         and type = 'cookie_id'
      then content
   end
   ) month1_pc_diff_cookie_cnt, --近30天使用的cookie的数量
max(case 
      when rn=1
         and type='cookie_id'
      then content
   end) month1_pc_common_cookie, --近30使用最常用的cookie_id
max(case
      when rn=1
         and type='browser_name'
      then content
   end) month1_pc_common_browser_name,
max(case 
      when rn=1
         and type='visit_os'
      then content
   end) month1_pc_common_os, -- 近30天使用最常用系统
max()
from qfbap_dws.dws_user_visit_month1
group by user_id