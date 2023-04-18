--------------1、建库-------------------

-- nohup /export/server/apache-hive-3.1.2-bin/bin/hive --service metastore &
-- nohup /export/server/apache-hive-3.1.2-bin/bin/hive --service hiveserver2 &

--如果数据库已存在就删除
drop database if exists db_hotel cascade;
--创建数据库
create database db_hotel;
--切换数据库
use db_hotel;

--------------2、建表-------------------
--如果表已存在就删除
drop table if exists db_hotel.hotel_bookings;
-- --建表
CREATE TABLE db_hotel.hotel_bookings (
  date_time TIMESTAMP,                   --  时间，格式为“年/月/日 时:分:秒”，表示查询记录生成的时间。
  site_name VARCHAR(50),                --  查询网站名称，这里为“Expedia.com”。
  user_location_country VARCHAR(50),    --  用户所在国家，这里为“美国”。
  user_location_region VARCHAR(50),     --  用户所在行政区划（省、州等），这里为“CA”（即加利福尼亚州）。
  user_location_city VARCHAR(50),       --  用户所在城市，这里为“BRENTWOOD”。
  user_location_latitude FLOAT,         --  用户所在地理位置的纬度。
  user_location_longitude FLOAT,        --  用户所在地理位置的经度。
  orig_destination_distance FLOAT,      --  目的地距离，单位为千米，表示查询时所输入目的地距离用户所在位置的距离。
  user_id INT,                          --  用户 ID。
  is_mobile INT,                        --  是否使用移动设备查询，1 表示是，0 表示否。
  is_package INT,                       --  是否查询套餐，1 表示是，0 表示否。
  channel INT,                          --  查询渠道。
  srch_ci DATE,                         --  入住时间，格式为“年/月/日”。
  srch_co DATE,                         --  退房时间，格式为“年/月/日”。
  srch_adults_cnt INT,                  --  入住成人数量。
  srch_children_cnt INT,                --  入住儿童数量。
  srch_rm_cnt INT,                      --  房间数量。
  srch_destination_id INT,              --  目的地 ID。
  hotel_country VARCHAR(50),            --  酒店所在国家。
  is_booking INT,                       --  是否预订，1 表示是，0 表示否。
  hotel_id INT,                         --  酒店 ID。
  prop_is_branded INT,                  --  是否品牌酒店，1 表示是，0 表示否。
  prop_starrating INT,                  --  酒店星级。
  distance_band CHAR(1),                --  距离带。
  hist_price_band CHAR(1),              --  历史价格带。
  popularity_band CHAR(1),              --  受欢迎程度带。
  cnt INT                               --  查询记录计数。
)
--指定分隔符为制表符
row format delimited fields terminated by '\t';


--------------3、加载数据-------------------
--上传数据文件到node1服务器本地文件系统（HS2服务所在机器）
--shell:  mkdir -p /root/hivedata

--加载数据到表中
load data local inpath '/root/hivedata/data.txt' into table db_hotel.hotel_bookings;


--查询表 验证数据文件是否映射成功
select * from db_hotel.hotel_bookings limit 1000;


--统计行数
select count(*) as cnt from db_hotel.hotel_bookings;--1628588

--ETL实现
--如果表已存在就删除
drop table if exists db_hotel.hotel_bookings_etl;
--将Select语句的结果保存到新表中
create table db_hotel.hotel_bookings_etl as
select * from hotel_bookings
WHERE user_location_latitude IS NOT NULL OR user_location_longitude IS NOT NULL OR orig_destination_distance IS NOt NULL;


--查询表 验证数据文件是否映射成功
select * from db_hotel.hotel_bookings_etl limit 1000;
--统计行数
select count(*) as cnt from db_hotel.hotel_bookings_etl;--1291375


--------------------------------------------------------------------------------------------------

--根据酒店所在国家计算预订的数量，并按预订数量从高到低排序。
create table if not exists result1 as
SELECT hotel_country, COUNT(*) as num_bookings
FROM hotel_bookings_etl
WHERE is_booking = 1
GROUP BY hotel_country
ORDER BY num_bookings DESC;

select * from result1;--结果验证
----------------------------------------------------

--计算预订酒店所在国家和星级的平均目的地距离。
create table if not exists result2 as
SELECT hotel_country, prop_starrating, AVG(orig_destination_distance) as avg_distance
FROM hotel_bookings_etl
WHERE is_booking = 1
GROUP BY hotel_country, prop_starrating;

select * from result2;--结果验证
---------------------------------------------------
create table if not exists result3temp as
select *, split(srch_ci,"-")[1] as srch_ci_months
FROM hotel_bookings_etl;

select * from result3temp;
select srch_ci,srch_ci_months from result3temp;

-- 根据搜索的目的地、月份和用户所在行政区划计算搜索次数，并筛选出搜索次数大于 100 的记录，以便确定最受欢迎的旅行目的地。
create table if not exists result3 as
SELECT srch_destination_id,  srch_ci_months,
       user_location_region, COUNT(*) as num_searches
FROM result3temp
WHERE is_booking = 0
GROUP BY srch_destination_id, srch_ci_months, user_location_region
HAVING num_searches > 100
ORDER BY num_searches DESC;

select * from result3;--结果验证
----------------------------------------------------

--计算预订的平均成人和儿童人数。
create table if not exists result4 as
SELECT AVG(srch_adults_cnt) as avg_adults, AVG(srch_children_cnt) as avg_children
FROM hotel_bookings_etl
WHERE is_booking = 1;

select * from result4;--结果验证
----------------------------------------------------

--根据用户所在行政区划和酒店品牌计算预订次数，并筛选出最受欢迎的酒店品牌。
create table if not exists result5 as
SELECT user_location_region, prop_is_branded, COUNT(*) as num_bookings
FROM hotel_bookings_etl
WHERE is_booking = 1
GROUP BY user_location_region, prop_is_branded
ORDER BY num_bookings DESC;

select * from result5;--结果验证
----------------------------------------------------

--根据预订酒店所在国家计算平均停留时间，并按平均停留时间从高到低排序。
create table if not exists result6 as
SELECT hotel_country, AVG(DATEDIFF(srch_co, srch_ci)) as avg_stay_length
FROM hotel_bookings_etl
WHERE is_booking = 1
GROUP BY hotel_country
ORDER BY avg_stay_length DESC;

select * from result6;--结果验证
----------------------------------------------------


-- 将根据用户所在行政区划、设备类型和预订渠道计算预订次数，并筛选出最受欢迎的预订渠道。
create table if not exists result7 as
SELECT user_location_region, is_mobile, channel, COUNT(*) as num_bookings
FROM hotel_bookings_etl
WHERE is_booking = 1
GROUP BY user_location_region, is_mobile, channel
ORDER BY num_bookings DESC;

select * from result7;--结果验证
----------------------------------------------------

drop table  if exists result8;
-- 将根据酒店距离带计算平均价格和平均距离。
create table if not exists result8 as
SELECT distance_band,  AVG(orig_destination_distance) as avg_distance
FROM hotel_bookings_etl
WHERE is_booking = 1
GROUP BY distance_band;

select * from result8;--结果验证
----------------------------------------------------


-- 计算每个预订渠道的总预订次数。
create table if not exists result9 as
SELECT channel, COUNT(*) as num_bookings
FROM hotel_bookings_etl
WHERE is_booking = 1
GROUP BY channel
ORDER BY num_bookings DESC;

select * from result9;--结果验证
----------------------------------------------------

-- 根据用户所在国家、行政区划和酒店星级计算预订次数，并筛选出最受欢迎的酒店。
create table if not exists result10 as
SELECT user_location_country, user_location_region, prop_starrating, hotel_id, COUNT(*) as num_bookings
FROM hotel_bookings_etl
WHERE is_booking = 1
GROUP BY user_location_country, user_location_region, prop_starrating, hotel_id
ORDER BY num_bookings DESC;


select * from result10;--结果验证
----------------------------------------------------

drop table  if exists result11;
--将根据用户所在国家、行政区划和预订酒店所在国家,目的地ID计算预订次数，并筛选出最受欢迎的目的地。
create table if not exists result11 as
SELECT user_location_country, user_location_region, hotel_country,srch_destination_id,COUNT(*) as num_bookings
FROM hotel_bookings_etl
WHERE is_booking = 1
GROUP BY user_location_country, user_location_region, hotel_country,srch_destination_id
ORDER BY num_bookings DESC;


select * from result11;--结果验证
----------------------------------------------------


--根据预订渠道计算平均预订房间数。
create table if not exists result12 as
SELECT channel, AVG(srch_rm_cnt) as avg_rooms_per_booking
FROM hotel_bookings_etl
WHERE is_booking = 1
GROUP BY channel;

select * from result12;--结果验证
----------------------------------------------------

--将根据酒店的星级计算预订酒店与用户位置的平均距离。
create table if not exists result13 as
SELECT prop_starrating, AVG(orig_destination_distance) AS avg_distance
FROM hotel_bookings_etl
WHERE is_booking = 1
GROUP BY prop_starrating;


select * from result13;--结果验证
----------------------------------------------------

--将根据预订次数筛选出最受欢迎的酒店目的地前10名。
create table if not exists result14 as
SELECT hotel_country, COUNT(*) as num_bookings
FROM hotel_bookings_etl
WHERE is_booking = 1
GROUP BY hotel_country
ORDER BY num_bookings DESC
LIMIT 10;


select * from result14;--结果验证
----------------------------------------------------


--将根据预订渠道计算打包销售的预订比例。
create table if not exists result15 as
SELECT channel, ROUND(100 * SUM(is_package) / COUNT(*), 2) AS package_percentage
FROM hotel_bookings_etl
WHERE is_booking = 1
GROUP BY channel;


select * from result15;--结果验证
----------------------------------------------------


drop table if exists result16;
--将根据用户所在地和酒店目的地计算预订次数，并筛选出最受欢迎的前5个目的地。
create table if not exists result16 as
SELECT user_location_country, user_location_region, hotel_country, COUNT(*) as num_bookings
FROM hotel_bookings_etl
WHERE is_booking = 1
GROUP BY user_location_country, user_location_region, hotel_country
ORDER BY num_bookings DESC
LIMIT 50;


select * from result16;--结果验证
----------------------------------------------------

--将根据酒店的星级计算平均每次预订的儿童数量。
create table if not exists result17 as
SELECT prop_starrating, AVG(srch_children_cnt) AS avg_children_per_booking
FROM hotel_bookings_etl
WHERE is_booking = 1
GROUP BY prop_starrating;


select * from result17;--结果验证
----------------------------------------------------



drop table  if exists  result18;
--将计算所有预订的提前天数，即预订日期和入住日期之间的天数。
create table if not exists result18 as
SELECT DATEDIFF(srch_ci, date_time) AS days_advance_booking
FROM hotel_bookings_etl
WHERE is_booking = 1;

create table if not exists result181 as
SELECT avg(DATEDIFF(srch_ci, date_time)) AS avg_days_advance_booking
FROM hotel_bookings_etl
WHERE is_booking = 1;

select * from result18 order by days_advance_booking desc limit 50;--结果验证
select * from result181;--结果验证
----------------------------------------------------

--将根据预订酒店所在国家计算平均停留时间，并按平均停留时间从高到低排序。
create table if not exists result19 as
SELECT hotel_country, AVG(DATEDIFF(srch_co, srch_ci)) as avg_stay_length
FROM hotel_bookings_etl
WHERE is_booking = 1
GROUP BY hotel_country
ORDER BY avg_stay_length DESC;


select * from result19;--结果验证
----------------------------------------------------

--将统计每个月的预订数和取消数，并按年份和月份分组。
create table if not exists result20 as
SELECT
  YEAR(srch_ci) AS year,
  MONTH(srch_ci) AS month,
  SUM(CASE WHEN is_booking = 1 THEN 1 ELSE 0 END) AS num_bookings,
  SUM(CASE WHEN is_booking = 0 THEN 1 ELSE 0 END) AS num_cancellations
FROM hotel_bookings_etl
GROUP BY YEAR(srch_ci), MONTH(srch_ci);


select * from result20;--结果验证
----------------------------------------------------

--将查找所有未预订的酒店中距离最近的前10个酒店，并列出它们的酒店ID和与用户位置的距离。
create table if not exists result21 as
SELECT hotel_id, MIN(orig_destination_distance) AS distance
FROM hotel_bookings_etl
WHERE is_booking = 0
GROUP BY hotel_id
ORDER BY distance
LIMIT 10;


select * from result21;--结果验证
----------------------------------------------------


--将根据酒店的星级和品牌信息计算预订的平均停留天数。
create table if not exists result22 as
SELECT prop_starrating, prop_is_branded, AVG(DATEDIFF(srch_co, srch_ci)) AS avg_length_of_stay
FROM hotel_bookings_etl
WHERE is_booking = 1
GROUP BY prop_starrating, prop_is_branded;


select * from result22;--结果验证
----------------------------------------------------

--将按照每个用户所属国家，统计预订数和取消数，并按照预订数从高到低排序。
create table if not exists result23 as
SELECT user_location_country,
  SUM(CASE WHEN is_booking = 1 THEN 1 ELSE 0 END) AS num_bookings,
  SUM(CASE WHEN is_booking = 0 THEN 1 ELSE 0 END) AS num_cancellations
FROM hotel_bookings_etl
GROUP BY user_location_country
ORDER BY num_bookings DESC;


select * from result23;--结果验证
----------------------------------------------------

--将显示酒店所在国家、预订数量、点击数量和预订比例。
create table if not exists result24 as
SELECT
  hotel_country,
  COUNT(CASE WHEN is_booking = 1 THEN 1 ELSE NULL END) AS bookings,
  COUNT(CASE WHEN is_booking = 0 THEN 1 ELSE NULL END) AS clicks,
  ROUND(COUNT(CASE WHEN is_booking = 1 THEN 1 ELSE NULL END) / COUNT(*) * 100, 2) AS booking_rate
FROM
  hotel_bookings_etl
GROUP BY hotel_country;


select * from result24;--结果验证
----------------------------------------------------

-- drop table  if exists result25;
-- --将根据月份计算平均每日价格
-- create table if not exists result25 as
-- SELECT
--   DATE_FORMAT(srch_ci, '%Y-%m') AS month,
--   AVG(hist_price_band) AS avg_daily_price
-- FROM
--   hotel_bookings_etl
-- WHERE
--   is_booking = 1
-- GROUP BY
--   DATE_FORMAT(srch_ci, '%Y-%m');
--
--
-- select * from result25;--结果验证
----------------------------------------------------

--将根据原始目的地距离段计算预订率
create table if not exists result26 as
SELECT
  CASE
    WHEN orig_destination_distance < 1000 THEN '0-1000'
    WHEN orig_destination_distance >= 1000 AND orig_destination_distance < 5000 THEN '1000-5000'
    WHEN orig_destination_distance >= 5000 AND orig_destination_distance < 10000 THEN '5000-10000'
    ELSE '10000+'
  END AS distance_range,
  COUNT(CASE WHEN is_booking = 1 THEN 1 ELSE NULL END) AS bookings,
  COUNT(*) AS clicks,
  ROUND(COUNT(CASE WHEN is_booking = 1 THEN 1 ELSE NULL END) / COUNT(*) * 100, 2) AS booking_rate
FROM
  hotel_bookings_etl
GROUP BY
  CASE
    WHEN orig_destination_distance < 1000 THEN '0-1000'
    WHEN orig_destination_distance >= 1000 AND orig_destination_distance < 5000 THEN '1000-5000'
    WHEN orig_destination_distance >= 5000 AND orig_destination_distance < 10000 THEN '5000-10000'
    ELSE '10000+'
  END;


select * from result26;--结果验证
----------------------------------------------------

--统计每个用户位置城市的酒店预订数量和平均酒店距离，并按预订数量降序排列
create table if not exists result27 as
SELECT
  user_location_city,
  COUNT(*) as booking_count,
  AVG(orig_destination_distance) as avg_distance
FROM
  hotel_bookings_etl
WHERE
  is_booking = 1
GROUP BY
  user_location_city
ORDER BY
  booking_count DESC;


select * from result27;--结果验证
----------------------------------------------------

--查询所有预订成功的用户位置和酒店位置之间的距离，并按距离升序排列
create table if not exists result28 as
SELECT
  user_location_latitude,
  user_location_longitude,
  hotel_country,
  AVG(orig_destination_distance) as avg_distance
FROM
  hotel_bookings_etl
WHERE
  is_booking = 1
GROUP BY
  user_location_latitude,
  user_location_longitude,
  hotel_country
ORDER BY
  avg_distance ASC;


select * from result28;--结果验证
----------------------------------------------------

--按照酒店星级分组，统计每个星级的酒店预订数和非预订数，并按照预订率从高到低排序，只显示预订率大于10%的星级。
create table if not exists result29 as
SELECT prop_starrating AS star_rating,
       COUNT(CASE WHEN is_booking=1 THEN 1 END) AS booking_count,
       COUNT(CASE WHEN is_booking=0 THEN 1 END) AS non_booking_count,
       COUNT(*) AS total_count
FROM hotel_bookings_etl
GROUP BY prop_starrating
HAVING (CAST(COUNT(CASE WHEN is_booking=1 THEN 1 END) AS FLOAT)/CAST(COUNT(*) AS FLOAT)) > 0.1
ORDER BY (CAST(COUNT(CASE WHEN is_booking=1 THEN 1 END) AS FLOAT)/CAST(COUNT(*) AS FLOAT)) DESC;


select * from result29;--结果验证
----------------------------------------------------

--计算每个星级和价格带的预订次数
create table if not exists result30 as
SELECT
    prop_starrating,
    distance_band,
    COUNT(*) as booking_count
FROM
    hotel_bookings_etl
WHERE
    is_booking = 1
GROUP BY
    prop_starrating,
    distance_band;


select * from result30;--结果验证
----------------------------------------------------

-- drop table if exists result31;
-- --计算每个目的地的平均价格并按照价格从高到低排序
-- create table if not exists result31 as
-- SELECT
--     srch_destination_id,
--     AVG(hist_price_band) as avg_price
-- FROM
--     hotel_bookings_etl
-- WHERE
--     is_booking = 1
-- GROUP BY
--     srch_destination_id
-- ORDER BY
--     avg_price DESC;
--
--
-- select * from result31;--结果验证
----------------------------------------------------

--统计每个用户在该网站上的活跃度并按照活跃度从高到低排序
create table if not exists result32 as
SELECT site_name,
    user_id,
    COUNT(*) as activity_count
FROM
    hotel_bookings_etl
GROUP BY
    site_name, user_id
ORDER BY
    activity_count DESC;


select * from result32;--结果验证
----------------------------------------------------

--统计每个用户的酒店预订数量，并按照数量降序排列
create table if not exists result33 as
SELECT user_id, COUNT(*) AS booking_count
FROM hotel_bookings_etl
GROUP BY user_id
ORDER BY booking_count DESC;


select * from result33;--结果验证
----------------------------------------------------

--按照酒店等级（prop_starrating）和酒店所在国家（hotel_country）统计酒店数量，并按照酒店数量降序排列
create table if not exists result34 as
SELECT prop_starrating, hotel_country, COUNT(*) AS hotel_count
FROM hotel_bookings_etl
GROUP BY prop_starrating, hotel_country
ORDER BY hotel_count DESC;


select * from result34;--结果验证
----------------------------------------------------

--统计每个渠道（channel）的酒店预订数量，并按照数量降序排列
create table if not exists result35 as
SELECT channel, COUNT(*) AS booking_count
FROM hotel_bookings_etl
GROUP BY channel
ORDER BY booking_count DESC;


select * from result35;--结果验证
----------------------------------------------------

--按照酒店所在国家（hotel_country）统计每个月的酒店预订数量，并将结果以矩阵形式展示
create table if not exists result36 as
SELECT hotel_country,
    COUNT(CASE WHEN MONTH(srch_ci) = 01 THEN 1 END) AS Jan,
    COUNT(CASE WHEN MONTH(srch_ci) = 02 THEN 1 END) AS Feb,
    COUNT(CASE WHEN MONTH(srch_ci) = 03 THEN 1 END) AS Mar,
    COUNT(CASE WHEN MONTH(srch_ci) = 04 THEN 1 END) AS Apr,
    COUNT(CASE WHEN MONTH(srch_ci) = 05 THEN 1 END) AS May,
    COUNT(CASE WHEN MONTH(srch_ci) = 06 THEN 1 END) AS Jun,
    COUNT(CASE WHEN MONTH(srch_ci) = 07 THEN 1 END) AS Jul,
    COUNT(CASE WHEN MONTH(srch_ci) = 08 THEN 1 END) AS Aug,
    COUNT(CASE WHEN MONTH(srch_ci) = 09 THEN 1 END) AS Sep,
    COUNT(CASE WHEN MONTH(srch_ci) = 10 THEN 1 END) AS Oct,
    COUNT(CASE WHEN MONTH(srch_ci) = 11 THEN 1 END) AS Nov,
    COUNT(CASE WHEN MONTH(srch_ci) = 12 THEN 1 END) AS Dece
FROM hotel_bookings_etl
GROUP BY hotel_country;


select * from result36;--结果验证
----------------------------------------------------

--按照酒店ID（hotel_id）分组，统计每个酒店的预订次数（is_booking = 1），并按照预订次数从高到低排序。
create table if not exists result37 as
SELECT hotel_id, COUNT(*) AS booking_count
FROM hotel_bookings_etl
WHERE is_booking = 1
GROUP BY hotel_id
ORDER BY booking_count DESC;


select * from result37;--结果验证
----------------------------------------------------

--按照用户所在国家（user_location_country）和酒店所在国家（hotel_country）进行分组，统计每个国家的预订次数（is_booking = 1），并按照预订次数从高到低排序。
create table if not exists result38 as
SELECT user_location_country, hotel_country, COUNT(*) AS booking_count
FROM hotel_bookings_etl
WHERE is_booking = 1
GROUP BY user_location_country, hotel_country
ORDER BY booking_count DESC;


select * from result38;--结果验证
----------------------------------------------------

--按照渠道（channel）进行分组，统计每个渠道的预订成功率，并按照预订成功率从高到低排序。
create table if not exists result39 as
SELECT channel,
       SUM(CASE WHEN is_booking = 1 THEN 1 ELSE 0 END) AS booking_count,
       COUNT(*) AS total_count,
       SUM(CASE WHEN is_booking = 1 THEN 1 ELSE 0 END) / COUNT(*) AS booking_rate
FROM hotel_bookings_etl
GROUP BY channel
ORDER BY booking_rate DESC;



select * from result39;--结果验证
----------------------------------------------------

--统计每个国家的预订数量并按降序排序
create table if not exists result40 as
SELECT hotel_country, COUNT(*) AS bookings
FROM hotel_bookings_etl
WHERE is_booking = 1
GROUP BY hotel_country
ORDER BY bookings DESC;


select * from result40;--结果验证
----------------------------------------------------

--统计每个星级酒店的平均原始目的地距离
create table if not exists result41 as
SELECT prop_starrating, AVG(orig_destination_distance) AS avg_distance
FROM hotel_bookings_etl
GROUP BY prop_starrating;


select * from result41;--结果验证
----------------------------------------------------

--
create table if not exists result42 as


select * from result42;--结果验证
----------------------------------------------------

--统计每个月的搜索次数和预订次数
create table if not exists result43 as
SELECT srch_ci_months,
       COUNT(*) AS searches,
       SUM(is_booking) AS bookings
FROM result3temp
GROUP BY srch_ci_months;


select * from result43;--结果验证
----------------------------------------------------

--统计每个用户所在国家的预订数量并按降序排序
create table if not exists result44 as
SELECT user_location_country, COUNT(*) AS bookings
FROM hotel_bookings_etl
WHERE is_booking = 1
GROUP BY user_location_country
ORDER BY bookings DESC;


select * from result44;--结果验证
----------------------------------------------------

--统计每个用户搜索时入住酒店的平均人数
create table if not exists result45 as
SELECT user_id, AVG(srch_adults_cnt + srch_children_cnt) AS avg_guests
FROM hotel_bookings_etl
GROUP BY user_id;


select * from result45;--结果验证
----------------------------------------------------

--按照酒店国家和用户位置国家统计预订次数
create table if not exists result46 as
SELECT hotel_country, user_location_country, COUNT(*) AS bookings
FROM hotel_bookings_etl
GROUP BY hotel_country, user_location_country
ORDER BY bookings DESC;


select * from result46;--结果验证
----------------------------------------------------

--统计每个酒店的平均住宿天数
create table if not exists result47 as
SELECT hotel_id, AVG(DATEDIFF(srch_co, srch_ci)) AS avg_stay_duration
FROM hotel_bookings_etl
GROUP BY hotel_id;


select * from result47;--结果验证
----------------------------------------------------

--统计每个用户位置城市的预订次数，并将结果限制为前十名
create table if not exists result48 as
SELECT user_location_city, COUNT(*) AS bookings
FROM hotel_bookings_etl
GROUP BY user_location_city
ORDER BY bookings DESC
LIMIT 10;


select * from result48;--结果验证
----------------------------------------------------
-- drop table if exists  result49;
-- --按照预订时间的月份统计预订次数
-- create table if not exists result49 as
-- SELECT srch_ci_months, COUNT(*) AS bookings
-- FROM result3temp
-- GROUP BY srch_ci_months
-- ORDER BY srch_ci_months ASC;
--
--
-- select * from result49;--结果验证
----------------------------------------------------

--按照用户是否使用移动设备进行预订统计预订次数
create table if not exists result50 as
SELECT is_mobile, COUNT(*) AS bookings
FROM hotel_bookings_etl
GROUP BY is_mobile;


select * from result50;--结果验证
----------------------------------------------------

--查询每个国家的订房数量，并按数量从大到小排序
create table if not exists result51 as
SELECT hotel_country, COUNT(*) AS bookings
FROM hotel_bookings_etl
GROUP BY hotel_country
ORDER BY bookings DESC;


select * from result51;--结果验证
----------------------------------------------------

--
-- --查询每个星级的酒店的平均价格
-- create table if not exists result52 as
-- SELECT prop_starrating, AVG(hist_price_band) AS avg_price
-- FROM hotel_bookings_etl
-- GROUP BY prop_starrating;
--
--
-- select * from result52;--结果验证
----------------------------------------------------
--查询每个渠道的订房数量
create table if not exists result53 as
SELECT channel, COUNT(*) AS bookings
FROM hotel_bookings_etl
GROUP BY channel;


select * from result53;--结果验证
----------------------------------------------------
--
-- --查询每个月的订房数量
-- create table if not exists result54 as
-- SELECT DATE_FORMAT(srch_ci, '%Y-%m') AS month, COUNT(*) AS bookings
-- FROM hotel_bookings_etl
-- GROUP BY month;
--
--
-- select * from result54;--结果验证
----------------------------------------------------

--计算不同国家的酒店数量
create table if not exists result55 as
SELECT hotel_country, COUNT(DISTINCT hotel_id) AS hotel_count
FROM hotel_bookings_etl
GROUP BY hotel_country
ORDER BY hotel_count DESC;


select * from result55;--结果验证
----------------------------------------------------

--分析不同酒店等级的预订数量
create table if not exists result56 as
SELECT prop_starrating, SUM(is_booking) AS booking_count
FROM hotel_bookings_etl
GROUP BY prop_starrating
ORDER BY prop_starrating ASC;


select * from result56;--结果验证
----------------------------------------------------

--统计各个州/省的预订数量
create table if not exists result57 as
SELECT user_location_region, SUM(is_booking) AS booking_count
FROM hotel_bookings_etl
GROUP BY user_location_region
ORDER BY booking_count DESC;


select * from result57;--结果验证
----------------------------------------------------

--计算酒店预订率前十的国家
create table if not exists result58 as
SELECT hotel_country, ROUND(SUM(is_booking)*100/COUNT(*),2) AS booking_rate
FROM hotel_bookings_etl
GROUP BY hotel_country
ORDER BY booking_rate DESC
LIMIT 10;


select * from result58;--结果验证
----------------------------------------------------


--
create table if not exists result59 as


select * from result59;--结果验证
----------------------------------------------------

--
create table if not exists result60 as


select * from result60;--结果验证
----------------------------------------------------

--
create table if not exists result61 as


select * from result61;--结果验证
----------------------------------------------------









