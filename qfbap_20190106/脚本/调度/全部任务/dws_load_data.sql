-- load data dws_user_visit_month1
insert overwrite table qfbap_dws.dws_user_visit_month1 partition(dt=${hivevar:param_dt})
select 
    a.user_id,
    a.type,
    a.cnt,
    a.content,
    a.rn,
    current_timestamp() dw_date
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
            --where dt >= date_sub(from_unixtime(unix_timestamp('${hivevar:param_dt}','yyyymmdd'),'yyyy-mm-dd'),29)
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
            --where dt >= date_sub(from_unixtime(unix_timestamp('${hivevar:param_dt}','yyyymmdd'),'yyyy-mm-dd'),29)
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
            --where dt >= date_sub(from_unixtime(unix_timestamp('${hivevar:param_dt}','yyyymmdd'),'yyyy-mm-dd'),29)
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
            --where dt >= date_sub(from_unixtime(unix_timestamp('${hivevar:param_dt}','yyyymmdd'),'yyyy-mm-dd'),29)
            group by 
                user_id,
                visit_os
        ) t    
) a