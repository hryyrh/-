select 
   us.user_id
   ,r_pc.latest_pc_visit_date-- ���һ�η���ʱ��
   ,r_pc.latest_pc_visit_session -- ���һ�η��ʵ�session
   ,r_pc.latest_pc_cookies-- ���һ�ε�coookie
   ,r_pc.latest_pc_pv -- ���һ�ε�pc�˵�pv��
   ,r_pc.latest_pc_browser_name -- ���һ�η���ʹ�õ������
   ,r_pc.latest_pc_visit_os-- ���һ�η���ʹ�õĲ���ϵͳ
   ,r_pc.first_pc_visit_date -- ����pc�˷��ʵ�����
   ,r_pc.first_pc_visit_session--����pc�˷��ʵ�session
   ,r_pc.first_pc_cookies-- ����pc�˷��ʵ�cookie
   ,r_pc.first_pc_pv-- ����һ�η��ʵ�pv
   ,r_pc.first_pc_browser_name -- ����һ�η���ʹ�õ������
   ,r_pc.first_pc_visit_os-- ����һ�η��ʵ�os  
   ,r_pc.day7_pc_cnt --PC����7����ʴ���
   ,r_pc.day15_pc_cnt-- ����15����ʴ���
   ,r_pc.month1_pc_cnt -- ����30����ʴ���
   ,r_pc.month2_pc_cnt-- ����60����ʵĴ���
   ,r_pc.month3_pc_cnt -- ����90����ʵĴ���
   ,r_pc.month1_pc_days --��30��pc�˷��ʵĴ���
   ,r_pc.month1_pc_pv --��30��pc�˵�pv
   ,r_pc.month1_pc_avg_pv --��30��pc��ÿ���ƽ��pv
   ,r_pc.month1_pc_hour025_cnt --0��5�������
   ,r_pc.month1_pc_hour627_cnt --6��7�������
   ,r_pc.month1_pc_hour829_cnt -- 8��9������
   ,r_pc.month1_pc_hour10211_cnt -- 10��11������
   ,r_pc.month1_pc_hour12213_cnt --12��13������
   ,r_pc.month1_pc_hour14216_cnt -- 14��16�������
   ,r_pc.month1_pc_hour17219_cnt -- 17��19�������
   ,r_pc.month1_pc_hour18219_cnt -- 18��19�������
   ,r_pc.month1_pc_hour20221_cnt -- 20��21�������
   ,r_pc.month1_pc_hour22223_cnt -- 22��23�������
   ,pc_month1.month1_pc_diff_ip_cnt --��30�����ʹ�õĲ�ͬip����
   ,pc_month1.month1_pc_common_ip --��30����õ�ip
   ,pc_month1.month1_pc_diff_cookie_cnt --��30��ʹ�õ�cookie������
   ,pc_month1.month1_pc_common_cookie --��30ʹ����õ�cookie_id
   ,pc_month1.month1_pc_common_browser_name -- pc��������
   ,pc_month1.month1_pc_common_os -- ��30��ʹ�����ϵͳ
   ,r_app.latest_app_visit_date, --���һ��app���ʵ�����
   r_app.latest_app_name, -- ���һ�η���app������
  r_app.latest_app_visit_os, -- ���һ��app���ʵĲ���ϵͳ
  r_app.first_app_visit_date, -- ��һ��app��������
  r_app.first_app_name,-- ��һapp��������
  r_app.first_app_visit_os,-- ��һ��app����os
  r_app.first_visit_ip,--app��һ�η���ip
  r_app.day7_app_cnt,-- app ��7����ʴ���
  r_app.day15_app_cnt,-- app ��15����ʴ���
  r_app.month1_app_cnt, -- app ��30��ķ��ʴ���
  r_app.month2_app_cnt, -- app��60��ķ��ʴ���
  r_app.month3_app_cnt, -- app��90��ķ��ʴ���
  r_app.month1_app_hour025_cnt, -- app��30��0��5��ķ��ʴ���
  r_app.month1_app_hour627_cnt,-- app��30���6��7��ķ��ʴ���
  r_app.month1_app_hour829_cnt, -- app��30��8��9�ķ��ʴ���
  r_app.month1_app_hour10211_cnt,-- app��30��10��11���ʴ���
  r_app.month1_app_hour12213_cnt, -- app��30��12��13��ķ��ʴ���
  r_app.month1_app_hour14215_cnt  ,-- app��30��14��15��ķ��ʴ���
  r_app.month1_app_hour16217_cnt  ,-- app��30��16��17��ķ��ʴ���
  r_app.month1_app_hour18219_cnt  ,-- app��30��18��19��ķ��ʴ���
  r_app.month1_app_hour20221_cnt  ,-- app��30��20��21��ķ��ʴ���
  r_app.month1_app_hour22223_cnt  ,-- app��30��22��23��ķ��ʴ���
  (case 
      when  r_pc.latest_pc_visit_date>= r_app.latest_app_visit_date 
      then   r_pc.latest_visit_ip
      else   r_app.latest_visit_ip
    end )  latest_visit_ip  , --���һ�η��ʵ�ip
   (case 
      when r_pc.latest_pc_visit_date >= r_app.latest_app_visit_date
      then r_pc.latest_city
      else r_app.latest_city
    end )  latest_city          , --���һ�η��ʵĳ���
   (case
      when r_pc.latest_pc_visit_date >= r_app.latest_app_visit_date
      then r_pc.latest_city
      else r_app.latest_city
    end
    ) latest_province      ,-- ���һ�η��ʵ�ʡ��
   (case
      when r_pc.first_pc_visit_date <= r_app.first_app_visit_date
      then r_pc.first_visit_ip
      else r_app.first_visit_ip
    end
    ) first_visit_ip       , -- ��һ�η��ʵ�ip
   (case 
      when r_pc.first_pc_visit_date <= r_app.first_app_visit_date
      then r_pc.first_city
      else r_app.first_city
    end
    ) first_city           ,-- ��һ�η��ʵĳ���
   (case
      when r_pc.first_pc_visit_date <= r_app.first_app_visit_date
      then r_pc.first_province
      else r_app.first_province
    end
    ) first_province -- ��һ�η��ʵ�ʡ��
from qfbap_dwd.dws_user_basic  us
left join (

    -- PC�˵�ͳ��ָ��
    select 
      user_id
       ,max(case when pc.rn_desc = 1 then in_time end) latest_pc_visit_date -- ���һ�η���ʱ��
       ,max(case when pc.rn_desc=1 then visit_ip end) latest_visit_ip --���һ�εķ���ip
       ,max(case when pc.rn_desc=1 then province end) latest_province
       ,max(case when pc.rn_desc=1 then city end) latest_city
       ,max(case when pc.rn_desc = 1 then session_id end) latest_pc_visit_session -- ���һ�η��ʵ�session
       ,max(case when pc.rn_desc = 1 then cookie_id end ) latest_pc_cookies-- ���һ�ε�coookie
       ,max(case when pc.rn_desc = 1 then pv end) latest_pc_pv -- ���һ�ε�pc�˵�pv��
       ,max(case when pc.rn_desc = 1 then browser_name end ) latest_pc_browser_name -- ���һ�η���ʹ�õ������
       ,max(case when pc.rn_desc = 1 then visit_os end ) latest_pc_visit_os-- ���һ�η���ʹ�õĲ���ϵͳ
       ,max(case when pc.rn_asc = 1 then in_time end) first_pc_visit_date -- ����pc�˷��ʵ�����
       ,max(case when pc.rn_asc=1 then visit_ip end) first_visit_ip --���һ�εķ���ip
       ,max(case when pc.rn_asc=1 then province end) first_province
       ,max(case when pc.rn_asc=1 then city end) first_city
       ,max(case when pc.rn_asc = 1 then session_id end ) first_pc_visit_session--����pc�˷��ʵ�session
       ,max(case when pc.rn_asc = 1 then cookie_id end ) first_pc_cookies-- ����pc�˷��ʵ�cookie
       ,max(case when pc.rn_asc = 1 then pv end ) first_pc_pv-- ����һ�η��ʵ�pv
       ,max(case when pc.rn_asc = 1 then browser_name end) first_pc_browser_name -- ����һ�η���ʹ�õ������
       ,max(case when pc.rn_asc = 1 then visit_os end) first_pc_visit_os-- ����һ�η��ʵ�os
       ,sum(dt7) day7_pc_cnt --����7����ʴ���
       ,sum(dt15) day15_pc_cnt-- ����15����ʴ���
       ,sum(dt30) month1_pc_cnt -- ����30����ʴ���
       ,sum(dt60) month2_pc_cnt-- ����60����ʵĴ���
       ,sum(dt90) month3_pc_cnt -- ����90����ʵĴ���
       ,count(distinct (case when dt30=1 then substr(in_time,0,8) end)) month1_pc_days --��30��pc�˷��ʵĴ���
       ,sum(case when dt30=1 then pv end ) month1_pc_pv --��30��pc�˵�pv
       ,sum(case when dt30=1 then pv end ) 
          /count(distinct(case when dt30=1 then substr(in_time,0,8) end)) month1_pc_avg_pv --��30��pc��ÿ���ƽ��pv
       ,count(case when dt30=1 and hr025=1 then 1 end ) month1_pc_hour025_cnt --0��5�������
       ,count(case when dt30=1 and hr627=1 then 1 end ) month1_pc_hour627_cnt --6��7�������
       ,count(case when dt30=1 and hr829=1 then 1 end ) month1_pc_hour829_cnt -- 8��9������
       ,count(case when dt30=1 and hr10211=1 then 1 end ) month1_pc_hour10211_cnt -- 10��11������
       ,count(case when dt30=1 and hr12213=1 then 1 end ) month1_pc_hour12213_cnt --12��13������
       ,count(case when dt30=1 and hr14215=1 then 1 end ) month1_pc_hour14216_cnt -- 14��16�������
       ,count(case when dt30=1 and hr16217=1 then 1 end ) month1_pc_hour17219_cnt -- 17��19�������
       ,count(case when dt30=1 and hr18219=1 then 1 end ) month1_pc_hour18219_cnt -- 18��19�������
       ,count(case when dt30=1 and hr20221=1 then 1 end ) month1_pc_hour20221_cnt -- 20��21�������
       ,count(case when dt30=1 and hr22223=1 then 1 end ) month1_pc_hour22223_cnt -- 22��23�������
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
           (case when in_time >= date_sub('2018-12-25',179) then 1 end) dt90,
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
       where dt ='2018-12-25'
    )  pc
    group by user_id

) r_pc on us.user_id = r_pc.user_id
left join (
        -- pc�˽�30��ķ�������
        select 
        user_id,
        count(distinct 
           case 
              when type='visit_ip' 
              then content 
           end) month1_pc_diff_ip_cnt, --��30�����ʹ�õĲ�ͬip����
        max(case 
              when rn=1 
                 and  type='visit_ip'
              then content
           end) month1_pc_common_ip, --��30����õ�ip
        count(
           distinct
           case 
              when rn=1
                 and type = 'cookie_id'
              then content
           end
           ) month1_pc_diff_cookie_cnt, --��30��ʹ�õ�cookie������
        max(case 
              when rn=1
                 and type='cookie_id'
              then content
           end) month1_pc_common_cookie, --��30ʹ����õ�cookie_id
        max(case
              when rn=1
                 and type='browser_name'
              then content
           end) month1_pc_common_browser_name,
        max(case 
              when rn=1
                 and type='visit_os'
              then content
           end) month1_pc_common_os -- ��30��ʹ�����ϵͳ
    
        from qfbap_dws.dws_user_visit_month1
        group by user_id
    ) pc_month1 on us.user_id = pc_month1.user_id
left join (
  -- app�˵�ͳ��ָ��
   select 
     app.user_id,
     max(case when rn_desc = 1 then log_time end) latest_app_visit_date ,
     max(case when rn_desc=1 then app_name  end) latest_app_name,
     max(case when rn_desc=1 then visit_os end) latest_app_visit_os,
     max(case when rn_desc=1 then visit_ip end) latest_visit_ip,
     max(case when rn_desc=1 then city end) latest_city,
     max(case when rn_desc=1 then province end) latest_province,
     max(case when rn_asc=1 then log_time end ) first_app_visit_date,
     max(case when rn_asc=1 then app_name end) first_app_name,
     max(case when rn_asc=1 then visit_os end) first_app_visit_os,
     max(case when rn_asc=1 then visit_ip end) first_visit_ip,
     max(case when rn_asc=1 then city end) first_city,
     max(case when rn_asc=1 then province end) first_province,
     sum(app_dt7)  day7_app_cnt,
     sum(app_dt15) day15_app_cnt,
     sum(app_dt30) month1_app_cnt,
     sum(app_dt60) month2_app_cnt,
     sum(app_dt90) month3_app_cnt,
     sum(case when app_dt30 =1 then app_hr_025 end) month1_app_hour025_cnt,
     sum(case when app_dt30 =1 then app_hr_627 end) month1_app_hour627_cnt,
     sum(case when app_dt30 =1 then app_hr_829 end) month1_app_hour829_cnt,
     sum(case when app_dt30 =1 then app_hr_10211 end) month1_app_hour10211_cnt,
     sum(case when app_dt30 =1 then app_hr_12213 end) month1_app_hour12213_cnt,
     sum(case when app_dt30 =1 then app_hr_14215 end) month1_app_hour14215_cnt  ,
     sum(case when app_dt30 =1 then app_hr_16217 end) month1_app_hour16217_cnt  ,
     sum(case when app_dt30 =1 then app_hr_18219 end) month1_app_hour18219_cnt  ,
     sum(case when app_dt30 =1 then app_hr_20221 end) month1_app_hour20221_cnt  ,
     sum(case when app_dt30 =1 then app_hr_22223 end) month1_app_hour22223_cnt  
  from (
     select 
        user_id ,
        log_time ,
        log_hour ,
        phone_id ,
        visit_os ,
        os_version ,
        app_name   ,
        app_version,
        device_token,
        visit_ip,
        province,
        city,
        row_number() over(distribute by user_id sort by  log_time asc) rn_asc,
        row_number() over(distribute by user_id sort by  log_time desc) rn_desc,
        (case when log_time>=date_sub('2018-12-25',6) then 1 end) app_dt7,
        (case when log_time>=date_sub('2018-12-25',14) then 1 end) app_dt15,
        (case when log_time>=date_sub('2018-12-25',29) then 1 end) app_dt30,
        (case when log_time>=date_sub('2018-12-25',59) then 1 end) app_dt60,
        (case when log_time>=date_sub('2018-12-25',89) then 1 end) app_dt90,
        (case when hour(log_time) between 0 and 5 then 1 end) app_hr_025,
        (case when hour(log_time) between 6 and 7 then 1 end) app_hr_627,
        (case when hour(log_time) between 8 and 9 then 1 end) app_hr_829,
        (case when hour(log_time) between 10 and 11 then 1 end) app_hr_10211,
        (case when hour(log_time) between 12 and 13 then 1 end) app_hr_12213,
        (case when hour(log_time) between 14 and 15 then 1 end) app_hr_14215,
        (case when hour(log_time) between 16 and 17 then 1 end) app_hr_16217,
        (case when hour(log_time) between 18 and 19 then 1 end) app_hr_18219,
        (case when hour(log_time) between 20 and 21 then 1 end) app_hr_20221,
        (case when hour(log_time) between 22 and 23 then 1 end) app_hr_22223
     from qfbap_dwd.dwd_user_app_pv
     ) app
  group by user_id
) r_app on us.user_id = r_app.user_id;