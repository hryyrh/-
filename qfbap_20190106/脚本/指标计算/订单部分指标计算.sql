# 1、将多张表的数据汇聚到dw_order
create table if not exists qfbap_dwd.dwd_order(
order_id bigint,
user_id bigint,
order_source string,
order_date string,
order_money string,
pay_type string,
area_id bigint,
area_name string,
addr_id bigint,
user_order_flag string,
trade_amount float,
trade_type string,
trade_time string,
intime string
)
partitioned by(dt string)
;

#加载dwd_order中的数据到表中
from(
select
oo.order_id order_id,
oo.user_id user_id,
oo.order_source order_source,
oo.order_date order_date,
oo.order_money order_money,
oo.pay_type pay_type,
ood.area_id area_id,
ood.area_name area_name,
oua.addr_id addr_id,
oua.user_order_flag user_order_flag,
obt.amount trade_amount,
obt.trade_type trade_type,
obt.trade_time trade_time,
date(order_date,"yyyy-MM-dd") intime
from qfbap_ods.ods_order oo
left join qfbap_ods.ods_order_delivery ood
on oo.order_id = ood.order_id
left join qfbap_ods.ods_user_addr oua
on ood.addr_id = oua.addr_id
left join qfbap_ods.ods_biz_trade obt
on obt.order_id = oo.order_id
) tmp
insert into table qfbap_dwd.dwd_order partition(dt="2018-12-26")
select *
;

# 统计order的recent的dm表：
drop table qfbap_dm.dm_order_recent;
create table if not exists qfbap_dm.dm_order_recent(
userid bigint,
first_order_date string,
last_order_date string,
first_order_diff int,
last_order_diff int,
dt30rr_cnt int,
dt60rr_cnt int,
dt90rr_cnt int,
amount_dt30rr_cnt int,
amount_dt60rr_cnt int,
amount_dt90rr_cnt int,
dt30_cnt int,
dt60_cnt int,
dt90_cnt int,
amount_dt30_cnt int,
amount_dt60_cnt int,
amount_dt90_cnt int,
min_order_money int,
max_order_money int,
user_price_dt90 int
)
partitioned by(dt string)
;

# 聚合求近多少天数据
from(
select
user_id,
min((case when first_order_rn = 1 then order_date end)) first_order_date,--首次下单时间
max((case when last_order_rn = 1 then order_date end)) last_order_date,--最后下单时间
datediff("2018-12-26",min((case when first_order_rn = 1 then order_date end))) first_order_diff,--首单距今时间
datediff("2018-12-26",max((case when last_order_rn = 1 then order_date end))) last_order_diff,--尾单距今时间
count(case when dt30rr = 1 then 1 end) dt30rr_cnt,
count(case when dt60rr = 1 then 1 end) dt60rr_cnt,
count(case when dt90rr = 1 then 1 end) dt90rr_cnt,
sum(case when amount_dt30rr != 0 then amount_dt30rr end) amount_dt30rr_cnt,
sum(case when amount_dt60rr != 0 then amount_dt60rr end) amount_dt60rr_cnt,
sum(case when amount_dt90rr != 0 then amount_dt90rr end) amount_dt90rr_cnt,
count((case when dt30 = 1 then 1 end)) dt30_cnt,
count((case when dt60 = 1 then 1 end)) dt60_cnt,
count((case when dt90 = 1 then 1 end)) dt90_cnt,
sum(case when amount_dt30 != 0 then amount_dt30rr end) amount_dt30_cnt,
sum(case when amount_dt60 != 0 then amount_dt60rr end) amount_dt60_cnt,
sum(case when amount_dt90 != 0 then amount_dt90rr end) amount_dt90_cnt,
min(order_money) min_order_money,
max(order_money) max_order_money,
round(sum(case when amount_dt90rr != 0 then amount_dt90rr end)/count(case when dt90rr = 1 then 1 end),3) user_price_dt90
from(
select
do.user_id user_id,
do.order_date order_date,
row_number() over(partition by do.user_id order by unix_timestamp(do.order_date,"yyyy-MM-dd hh:mm:ss") asc) first_order_rn, --第一次下单时间
row_number() over(partition by do.user_id order by unix_timestamp(do.order_date,"yyyy-MM-dd hh:mm:ss") desc) last_order_rn, --最后一次下单
(case when do.intime >= date_sub('2018-12-26',29) then 1 end) dt30rr,--含退拒 rr:retreate reject
(case when do.intime >= date_sub('2018-12-26',59) then 1 end) dt60rr,--含退拒
(case when do.intime >= date_sub('2018-12-26',89) then 1 end) dt90rr,--含退拒
(case when do.intime >= date_sub('2018-12-26',29) then do.order_money else 0 end) amount_dt30rr,--含退拒
(case when do.intime >= date_sub('2018-12-26',59) then do.order_money else 0 end) amount_dt60rr,--含退拒
(case when do.intime >= date_sub('2018-12-26',89) then do.order_money else 0 end) amount_dt90rr,
(case when do.intime >= date_sub('2018-12-26',29) and do.trade_type = 1 then 1 end) dt30,--不含退拒 trade_type:1 成功付款，没有退和拒
(case when do.intime >= date_sub('2018-12-26',59) and do.trade_type = 1 then 1 end) dt60,--不含退拒
(case when do.intime >= date_sub('2018-12-26',89) and do.trade_type = 1 then 1 end) dt90,--不含退拒
(case when do.intime >= date_sub('2018-12-26',29) and do.trade_type = 1 then do.order_money else 0 end) amount_dt30,--不含退拒
(case when do.intime >= date_sub('2018-12-26',59) and do.trade_type = 1 then do.order_money else 0 end) amount_dt60,--不含退拒
(case when do.intime >= date_sub('2018-12-26',89) and do.trade_type = 1 then do.order_money else 0 end) amount_dt90,
do.addr_id addr_id, --收货地址id
do.pay_type pay_type, --支付类型
do.order_money order_money
from qfbap_dwd.dwd_order do
where do.dt>=date_sub('2018-12-26',89)
and do.dt<='2018-12-26'
) tmp
group by user_id
) tmp1
insert into qfbap_dm.dm_order_recent partition(dt="?")
select *;


# 求非近期的最终结果数据的qfbap_dm.dm_order_no_recent
drop table qfbap_dm.dm_order_no_recent;
create table if not exists qfbap_dm.dm_order_no_recent(
userid bigint,
paytype string, --当成int类型所以可以sum
addrid string,
no_rr_order_id_cnt int,
no_rr_order_id_sum int,
user_order_price float
)
partitioned by(dt string)
;


# 导出到qfbap_dm.dm_order_no_recent的结果
from(
select
user_id user_id,
pay_type pay_type,
0 addr_id,
0 no_rr_order_id_cnt,
0 no_rr_order_id_sum,
0 user_order_price
from(
select
user_id,
pay_type,
row_number() over(partition by user_id order by pay_type_cnt desc) rn_desc
from(
select
oo.user_id user_id,
oo.pay_type pay_type,
count(oo.pay_type) pay_type_cnt
from qfbap_dwd.dwd_order oo
where oo.dt = "2018-12-26"
group by oo.user_id,oo.pay_type
) tmp
group by user_id,pay_type,pay_type_cnt
) tmp1
where rn_desc = 1

union all
select
user_id user_id,
0 pay_type,
addr_id addr_id,
0 no_rr_order_id_cnt,
0 no_rr_order_id_sum,
0 user_order_price
from(
select
user_id,
addr_id,
row_number() over(partition by user_id order by addr_id_cnt desc) rn_desc
from(
select
oo.user_id user_id,
oo. addr_id addr_id,
count(oo.addr_id) addr_id_cnt
from qfbap_dwd.dwd_order oo
where oo.dt = "2018-12-26"
group by oo.user_id,oo.addr_id
) tmp
group by user_id,addr_id,addr_id_cnt
) tmp1
where rn_desc = 1


union all

select
user_id user_id,
0 pay_type,
0 addr_id,
(tmp2.no_rr_order_id_cnt + tmp3.no_rr_order_id_cnt) no_rr_order_id_cnt,
(tmp2.no_rr_order_id_sum + tmp3.no_rr_order_id_sum) no_rr_order_id_sum,
tmp2.user_order_price
from
(
SELECT
user_id user_id,
0 pay_type,
0 addr_id,
count(case when no_rr_order_id = 1 then 1 end) no_rr_order_id_cnt,
sum(case when no_rr_order_money != 0 then no_rr_order_money end) no_rr_order_id_sum,
sum(order_money)/count(order_id) user_order_price
from(
SELECT
do.user_id user_id,
(case WHEN do.trade_type = 1 then 1 end) no_rr_order_id,
(case WHEN do.trade_type = 1 then do.order_money ELSE 0 end) no_rr_order_money,
do.order_id order_id,
do.order_money order_money
FROM qfbap_dwd.dwd_order do
WHERE do.order_id IS NOT NULL
and do.dt  = "2018-12-26"
) tmp1
group by user_id

) tmp2
left join
(
select
dwso.userid,
dwso.no_rr_order_id_cnt,
dwso.no_rr_order_id_sum
from qfbap_dm.dm_order_no_recent dwso
where dwso.dt = "2018-12-25"
) tmp3
on tmp2.user_id = tmp3.userid

) tmp4
insert into qfbap_dm.dm_order_no_recent partition(dt="2018-12-26")
select
user_id,
sum(pay_type) pay_type, --当成int类型所以可以sum
sum(addr_id) addr_id,
sum(no_rr_order_id_cnt) no_rr_order_id_cnt,
sum(no_rr_order_id_sum) no_rr_order_id_sum,
sum(user_order_price) user_order_price
group by user_id
;


# 统计下单分布的结果表
create table if not exists qfbap_dm.dm_order_range(
user_id bigint,
school_order int,
company_order int,
home_order int,
am0_8_order int,
am9_12_order int,
am13_15_order int,
am16_20_order int,
am21_24_order int
)
partitioned by(dt string)
;


# 统计的下单分布
from(
SELECT
do.user_id,
do.order_id,
do.order_date,
(case WHEN do.user_order_flag = 1 then 1 end) school_order,
(case WHEN do.user_order_flag = 2 then 1 end) company_order,
(case WHEN do.user_order_flag = 3 then 1 end) home_order,
(case WHEN hour(do.order_date) between 0 and 5  then 1 end) am0_5_order,
(case WHEN hour(do.order_date) between 6 and 12  then 1 end) am6_12_order,
(case WHEN hour(do.order_date) between 13 and 15  then 1 end) am13_15_order,
(case WHEN hour(do.order_date) between 16 and 20  then 1 end) am16_20_order,
(case WHEN hour(do.order_date) between 21 and 24  then 1 end) am21_24_order
from dwd_order do
where do.dt = "2018-12-26"
) tmp
insert into qfbap_dm.dm_order_range partition(dt="2018-12-26")
SELECT
user_id,
count(case WHEN school_order = 1 then 1 end) school_order_cnt,
count(case WHEN company_order = 1 then 1 end) company_order_cnt,
count(case WHEN home_order = 1 then 1 end) home_order_cnt,
count(case WHEN am0_5_order = 1 then 1 end) am0_5_order_cnt,
count(case WHEN am6_12_order = 1 then 1 end) am6_12_order_cnt,
count(case WHEN am13_15_order = 1 then 1 end) am13_15_order_cnt,
count(case WHEN am16_20_order = 1 then 1 end) am16_20_order_cnt,
count(case WHEN am21_24_order = 1 then 1 end) am21_24_order_cnt
GROUP BY user_id
;

======================================================购物车=====================
# 2、将多张表的数据汇聚到dw_cart
create table if not exists qfbap_dwd.dwd_cart(
user_id bigint,
cart_id bigint,
session_id string,
goods_id bigint,
goods_num int,
add_time string,
cancle_time string,
sumbit_time string,
order_id bigint,
order_no string,
goods_no string,
goods_name string,
goods_amount int,
shop_id bigint,
shop_name string,
curr_price float,
market_price float,
discount float,
cost_price float,
first_cat bigint,
first_cat_name string,
second_cat bigint,
second_cat_name string,
third_cat bigint,
third_cat_name string,
goods_desc string
)
partitioned by(dt string)
;

#加载dwd_cart中的数据到表中
from(
SELECT
ooc.user_id,
ooc.cart_id,
ooc.session_id,
ooc.goods_id,
ooc.goods_num,
ooc.add_time,
ooc.cancle_time,
ooc.sumbit_time,
ooi.order_id,
ooi.order_no,
ooi.goods_no,
ooi.goods_name,
ooi.goods_amount,
ooi.shop_id,
ooi.shop_name,
ooi.curr_price,
ooi.market_price,
ooi.discount,
ooi.cost_price,
ooi.first_cat,
ooi.first_cat_name,
ooi.second_cat,
ooi.second_cat_name,
ooi.third_cat,
ooi.third_cat_name,
ooi.goods_desc
FROM qfbap_ods.ods_order_cart ooc
LEFT JOIN qfbap_ods.ods_order_item ooi
on ooc.goods_id = ooi.goods_id
WHERE ooc.dt = "2018-12-26"
) tmp
insert into qfbap_dwd.dwd_cart partition(dt="2018-12-26")
select *
;

# 统计购物车中近30天的指标


# 统计购物车中非近30天指标






报错信息：
union all报错中所有字段不一样：
SemanticException The abstract syntax tree is null

Hive中有两张表中拥有同一个字段名称，在这两个表做关联的时候会报这个错误:
SemanticException Column user_id Found in more than One Tables/Subqueries





