# SK C&C Big Data Knowledge Test (Part2)

### Problem 1
```
select id, status, amount, type
         from problem1.account 
        where status = "Active"
          and id in ('A212500000', 'A15761988', 'A22529838')
;

select A.id
       , A.type
       , A.status
       , A.amount
       , A.amount - B.avg_type
  from (
       select id, status, amount, type
         from problem1.account 
        where status = "Active"
       ) A
       LEFT OUTER JOIN (
        select type, AVG(amount) AS avg_type
         from problem1.account 
        where status = "Active" 
        group by type
       ) B
       ON A.type = B.type
;

-- 결과 확인
hive -f /home/training/problem1/solution.sql
A100000	Basic Checking	Active	44539	-5529.0
A100002	Basic Checking	Active	11483	-38585.0
A100003	Student Checking	Active	26132	-24127.631578947367
A100007	Student Checking	Active	71661	21401.368421052633
A100009	Savings	Active	65536	17498.36363636364
A100010	Savings	Active	40778	-7259.63636363636
A100011	Savings	Active	57953	9915.36363636364
A100013	Basic Checking	Active	45654	-4414.0
A100016	Basic Checking	Active	639	-49429.0
A100018	Basic Checking	Active	63563	13495.0
A100019	Basic Checking	Active	79771	29703.0
A100020	Basic Checking	Active	34328	-15740.0
A100021	Savings	Active	34405	-13632.63636363636
A100022	Student Checking	Active	51850	1590.3684210526335
A100023	Basic Checking	Active	85337	35269.0
A100025	Savings	Active	7265	-40772.63636363636
A100030	Student Checking	Active	7275	-42984.63157894737
A100032	Basic Checking	Active	51703	1635.0
A100033	Student Checking	Active	20139	-30120.631578947367
A100034	Savings	Active	66835	18797.36363636364
A100036	Savings	Active	1578	-46459.63636363636
A100037	Basic Checking	Active	98233	48165.0
A100038	Savings	Active	4042	-43995.63636363636
A100040	Student Checking	Active	73539	23279.368421052633
A100043	Student Checking	Active	86888	36628.36842105263
A100044	Basic Checking	Active	56530	6462.0
A100045	Student Checking	Active	44440	-5819.6315789473665
A100046	Savings	Active	17575	-30462.63636363636
A100047	Savings	Active	94291	46253.36363636364
A100049	Savings	Active	98628	50590.36363636364
A100050	Savings	Active	99035	50997.36363636364
A100054	Savings	Active	86903	38865.36363636364
A100055	Savings	Active	61255	13217.36363636364
A100056	Student Checking	Active	68404	18144.368421052633
A100057	Basic Checking	Active	31550	-18518.0
A100058	Student Checking	Active	1189	-49070.63157894737
A100060	Student Checking	Active	35259	-15000.631578947367
A100061	Basic Checking	Active	87322	37254.0
A100062	Student Checking	Active	34025	-16234.631578947367
A100067	Student Checking	Active	64871	14611.368421052633
A100069	Basic Checking	Active	44071	-5997.0
A100071	Student Checking	Active	28357	-21902.631578947367
A100073	Savings	Active	23589	-24448.63636363636
A100074	Student Checking	Active	77169	26909.368421052633
A100075	Basic Checking	Active	19327	-30741.0
A100076	Savings	Active	41747	-6290.63636363636
A100078	Basic Checking	Active	78031	27963.0
A100079	Savings	Active	95810	47772.36363636364
A100082	Savings	Active	27664	-20373.63636363636
A100083	Student Checking	Active	61306	11046.368421052633
A100084	Savings	Active	69928	21890.36363636364
A100086	Student Checking	Active	65928	15668.368421052633
A100087	Savings	Active	19786	-28251.63636363636
A100088	Basic Checking	Active	48954	-1114.0
A100089	Savings	Active	38086	-9951.63636363636
A100091	Basic Checking	Active	66045	15977.0
A100094	Student Checking	Active	54444	4184.3684210526335
A100095	Basic Checking	Active	4212	-45856.0
A100096	Student Checking	Active	82057	31797.368421052633
A100098	Savings	Active	4139	-43898.63636363636

```

### Problem 2
```
hdfs dfs -ls /user/training/problem2/data/employee/

CREATE DATABASE problem2;

CREATE EXTERNAL TABLE `problem2.employees`(
  `id` int, 
  `fname` string, 
  `lname` string, 
  `address` string, 
  `city` string, 
  `state` string, 
  `zip` string, 
  `birthday` string, 
  `hireday` string
  )
STORED AS PARQUET
LOCATION
  '/user/training/problem2/data/employee'
;

-- 결과 확인
select * from problem2.employees limit 10;
10000	Rigel	Shaw	Ap #124-4664 Vulputate, Rd.	Cannole	OH	83380	07/12/80	05/30/18
10001	Chancellor	Bond	Ap #702-9298 Pretium Street	Piana degli Albanesi	AZ	38128	03/02/95	04/12/18
10002	Aurora	Franco	5717 Mattis. Street	Casalvieri	CA	12296	01/29/77	07/19/18
10003	Armando	Aguilar	Ap #189-3372 Nam St.	Durgapur	TX	50289	05/10/50	04/14/18
10004	Neve	Sharp	2748 Risus. Rd.	Poole	NV	23113	07/07/76	06/04/17
10005	Kenyon	Whitfield	Ap #863-352 Blandit St.	Ayr	CA	62678	12/23/66	04/30/18
10006	Germane	Pruitt	P.O. Box 266, 1496 Mus. St.	Opole	DE	21311	02/20/63	02/14/18
10007	Alyssa	Lindsey	989-4040 Habitant Av.	Lexington	TX	62013	09/03/95	05/17/17
10008	Driscoll	Bishop	P.O. Box 428, 449 Cum Ave	La Reina	NV	61197	05/18/84	12/20/16
10009	Tanisha	Rosales	5388 Vitae Street	Caramanico Terme	TX	14932	07/03/64	09/01/16

```

### Problem 3
```
desc problem3.customer;
show create table problem3.customer;

CREATE TABLE `problem3.solution`(
  `id` string, 
  `fname` string, 
  `lname` string, 
  `hphone` string)
;

INSERT INTO TABLE problem3.solution
SELECT *
  FROM problem3.customer
  ;

-- 결과 확인
hive> select * from problem3.solution limit 10;
OK
10000	Cullen	Johnson	(504) 159-1501
10001	Sybil	Wiley	(504) 780-0366
10002	Victoria	Mcpherson	(990) 307-7932
10003	Deanna	Pollard	(178) 455-7090
10004	Dominic	Aguilar	(647) 503-9092
10005	Jana	Patrick	(751) 459-6046
10006	Sylvia	Shelton	(781) 597-8748
10007	Amethyst	Horne	(112) 121-5962
10008	Kylie	Hutchinson	(975) 530-3622
10009	Xavier	Merritt	(387) 583-1356

hive> show create table problem3.solution;
OK
CREATE TABLE `problem3.solution`(
  `id` string, 
  `fname` string, 
  `lname` string, 
  `hphone` string)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  'hdfs://localhost:8020/user/hive/warehouse/problem3.db/solution'
TBLPROPERTIES (
  'COLUMN_STATS_ACCURATE'='true', 
  'numFiles'='1', 
  'numRows'='100', 
  'rawDataSize'='3381', 
  'totalSize'='3481', 
  'transient_lastDdlTime'='1558500417')

```

### Problem 4
```
hdfs dfs -cat /user/training/problem4/data/employee1/part-m-00000

10000000	Olga	Booker	Ap #643-2741 Proin Street	Gresham	OR	42593-0000
10000001	Raja	Spence	P.O. Box 765, 7700 Eros Rd.	Duluth	MN	67110-0000
10000002	Meredith	Schwartz	3414 At Road	San Antonio	TX	35713-0000
10000003	Thor	Lloyd	2703 Amet, Road	Rock Springs	WY	73861-0000
10000004	Arden	Mooney	Ap #888-1187 Aliquam Road	Rockville	MD	82478-0000
10000005	Kenneth	Lucas	Ap #637-8746 Feugiat Av.	Boston	MA	84047-0000
10000006	Leonard	Goodwin	Ap #465-7707 Aliquam Avenue	Rock Springs	WY	91337-0000
10000007	Elton	Conrad	P.O. Box 787, 8889 Nam Street	Auburn	ME	28280-0000

CREATE DATABASE problem4;

CREATE EXTERNAL TABLE `problem4.employee1`(
  `id` int, 
  `fname` string, 
  `lname` string, 
  `address` string, 
  `city` string, 
  `state` string, 
  `zip` string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'  
LOCATION
  '/user/training/problem4/data/employee1'
;

select * from problem4.employee1 where state = "CA" limit 10;
0000063	Burton	Hayes	Ap #720-4012 Vivamus Avenue	San Diego	CA	96066-0000
10000068	Ria	Herman	2974 Cras St.	San Francisco	CA	95310-0000
10000073	Daquan	Roy	7636 Et Rd.	Los Angeles	CA	96606-0000

hdfs dfs -cat /user/training/problem4/data/employee2/part-m-00000
10010000,656,HULL,WARREN,7593 Pede. Rd.,Kansas City,MO,55725
10010001,142,HAYES,KASPER,425-3365 Feugiat Rd.,Springfield,MO,27927
10010002,417,BARRETT,SYBILL,Ap #367-7227 Eu Street,Hartford,CT,83690
10010003,925,VALDEZ,THOR,"212-6890 Risus, St.",Springfield,MO
10010004,832,BALLARD,WYNNE,748-4292 Vel Road,Hillsboro,OR,53903
10010005,536,GLASS,ZOE,Ap #565-7061 Massa. Rd.,Bear,DE,83658
10010006,987,BEARD,HIRAM,"P.O. Box 997, 6136 Ut Ave",Lakewood,CO

CREATE EXTERNAL TABLE `problem4.employee2`(
  `id` int, 
  `unknow` string, 
  `lname` string, 
  `fname` string, 
  `address` string, 
  `city` string, 
  `state` string, 
  `zip` string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','  
LOCATION
  '/user/training/problem4/data/employee2'
;

select * from problem4.employee2 where state = "CA" limit 10;
10010024	822	WOOD	PRESTON	2782 Amet Street	San Francisco	CA	92124
10010032	495	ROJAS	BO	Ap #445-6043 Massa Av.	San Diego	CA	92363
10010037	945	BANKS	ZANE	4778 Interdum. St.	San Diego	CA	92120
10010079	354	JUSTICE	URSA	3321 Risus. Ave	San Jose	CA	96850
10010088	151	BOOTH	DECLAN	4048 Nunc Rd.	San Francisco	CA	96324

select *
  from (
       select id
              , LOWER(fname)
              , LOWER(lname)
              , address
              , city
              , state
              , SUBSTR(zip, 1, 5)
         from problem4.employee1 
        where state = "CA"
       UNION ALL
       select id
              , LOWER(fname)
              , LOWER(lname)
              , address
              , city
              , state
              , SUBSTR(zip, 1, 5)
         from problem4.employee2 
        where state = "CA"
       ) A
;
10010024	preston	wood	2782 Amet Street	San Francisco	CA	92124
10010032	bo	rojas	Ap #445-6043 Massa Av.	San Diego	CA	92363
10010037	zane	banks	4778 Interdum. St.	San Diego	CA	92120
10010079	ursa	justice	3321 Risus. Ave	San Jose	CA	96850
10010088	declan	booth	4048 Nunc Rd.	San Francisco	CA	96324

INSERT OVERWRITE DIRECTORY 'hdfs://localhost:8020/user/training/problem4/solution'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
select *
  from (
       select id
              , LOWER(fname)
              , LOWER(lname)
              , address
              , city
              , state
              , SUBSTR(zip, 1, 5)
         from problem4.employee1 
        where state = "CA"
       UNION ALL
       select id
              , LOWER(fname)
              , LOWER(lname)
              , address
              , city
              , state
              , SUBSTR(zip, 1, 5)
         from problem4.employee2 
        where state = "CA"
       ) A
;

-- 결과 확인
hdfs dfs -ls /user/training/problem4/solution

hdfs dfs -cat /user/training/problem4/solution/000000_0
10000063	burton	hayes	Ap #720-4012 Vivamus Avenue	San Diego	CA	96066
10000068	ria	herman	2974 Cras St.	San Francisco	CA	95310
10000073	daquan	roy	7636 Et Rd.	Los Angeles	CA	96606
10010024	preston	wood	2782 Amet Street	San Francisco	CA	92124
10010032	bo	rojas	Ap #445-6043 Massa Av.	San Diego	CA	92363
10010037	zane	banks	4778 Interdum. St.	San Diego	CA	92120
10010079	ursa	justice	3321 Risus. Ave	San Jose	CA	96850
10010088	declan	booth	4048 Nunc Rd.	San Francisco	CA	96324

```

### Problem 5
```
DROP TABLE problem5.solution;

CREATE TABLE `problem5.solution`(
  `fname` string, 
  `lname` string, 
  `city` string, 
  `state` string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'  
;

insert into table problem5.solution
select fname, lname, city, state
  from (
       select fname, lname, city, state 
         from problem5.employee 
        where state = "CA" 
          and city = "Palo Alto"
       UNION ALL
       select fname, lname, city, state
         from problem5.customer 
        where state = "CA" 
          and city = "Palo Alto"
       ) A
;

-- 결과 확인
hive -f /home/training/problem5/solution.sql
select * from problem5.solution limit 10;
Farrah	Preston	Palo Alto	CA
Brielle	Hudson	Palo Alto	CA
Nelle	Kim	Palo Alto	CA
Tatum	Jacobs	Palo Alto	CA
Ivan	Gentry	Palo Alto	CA
Naida	Tran	Palo Alto	CA
Alden	Daniels	Palo Alto	CA
Maggy	Mcdaniel	Palo Alto	CA
Griffin	Pate	Palo Alto	CA
Burton	Hayes	Palo Alto	CA

select count(*) from problem5.solution;
12

```

### Problem 6
```
show create table problem6.employee;

select * from problem6.employee limit 10;

CREATE TABLE `problem6.solution`(
  `id` int, 
  `fname` string, 
  `lname` string, 
  `address` string, 
  `city` string, 
  `state` string, 
  `zip` string, 
  `birthday` string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' 
;

insert into table problem6.solution
select  id
        , fname
        , lname
        , address
        , city
        , state
        , zip
        , substr(birthday, 1, 5) 
from problem6.employee
;

-- 결과 확인
select * from problem6.solution limit 10;
10000000	Deanna	Lane	900-1514 Vitae, Rd.	Lafayette	LA	97827	08/31
10000001	Hall	Garrett	9656 Urna Avenue	Tucson	AZ	86511	08/24
10000002	Lucian	Dotson	P.O. Box 277, 4808 Fusce St.	Kearney	NE	57731	08/12
10000003	Yuri	Sherman	Ap #399-8275 Molestie Road	Kapolei	HI	16943	08/26
10000004	Jaime	Griffin	Ap #647-2123 Quis Rd.	Madison	WI	51394	08/13
10000005	Zorita	Weber	747-9424 Orci, Av.	Hattiesburg	MS	90262	08/09
10000006	Mara	Meadows	517-4594 Ac, Rd.	Huntsville	AL	35374	08/11
10000007	Evan	Richard	P.O. Box 223, 8182 Non, Av.	College	AK	99682	08/25
10000008	Briar	Anderson	Ap #548-6452 Nunc Road	Cleveland	OH	90704	08/18
10000009	Cole	Odom	P.O. Box 962, 2496 Sodales St.	Boston	MA	27282	08/21

```

### Problem 7
```
desc problem7.employee;

select concat(concat(fname,' '), lname) as name
  from problem7.employee 
 where city = 'Seattle'
 order by name
 ;

-- 결과 확인
hive -f /home/training/problem7/solution.sql
Lucian Dotson
Zeph Horn
Zeph Mcclain

```

### Problem 8
```
hdfs dfs -ls /user/training/problem8/data/customer/;

hdfs dfs -cat /user/training/problem8/data/customer/000000_0;

CREATE DATABASE problem8;

CREATE EXTERNAL TABLE `problem8.customer`(
  `id` int, 
  `fname` string, 
  `lname` string, 
  `address` string, 
  `city` string, 
  `state` string, 
  `zip` string, 
  `birthday` string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' 
LOCATION
  '/user/training/problem8/data/customer'
;

sqoop export \
--connect jdbc:mysql://localhost/problem8 \
--username cloudera --password cloudera \
--table solution \
--export-dir /user/training/problem8/data/customer \
--fields-terminated-by "\t" \

-- 결과 확인
mysql -u cloudera -p
mysql> select * from problem8.solution limit 10;
+----------+--------+----------+--------------------------------+-------------+-------+-------+------------+
| id       | fname  | lname    | address                        | city        | state | zip   | birthday   |
+----------+--------+----------+--------------------------------+-------------+-------+-------+------------+
| 10000000 | Deanna | Lane     | 900-1514 Vitae, Rd.            | Lafayette   | LA    | 97827 | 08/31/2016 |
| 10000001 | Hall   | Garrett  | 9656 Urna Avenue               | Tucson      | AZ    | 86511 | 08/24/2016 |
| 10000002 | Lucian | Dotson   | P.O. Box 277, 4808 Fusce St.   | Seattle     | WA    | 57731 | 08/12/2016 |
| 10000003 | Yuri   | Sherman  | Ap #399-8275 Molestie Road     | Kapolei     | HI    | 16943 | 08/26/2016 |
| 10000004 | Jaime  | Griffin  | Ap #647-2123 Quis Rd.          | Madison     | WI    | 51394 | 08/13/2016 |
| 10000005 | Zorita | Weber    | 747-9424 Orci, Av.             | Hattiesburg | MS    | 90262 | 08/09/2016 |
| 10000006 | Mara   | Meadows  | 517-4594 Ac, Rd.               | Huntsville  | AL    | 35374 | 08/11/2016 |
| 10000007 | Evan   | Richard  | P.O. Box 223, 8182 Non, Av.    | College     | AK    | 99682 | 08/25/2016 |
| 10000008 | Briar  | Anderson | Ap #548-6452 Nunc Road         | Cleveland   | OH    | 90704 | 08/18/2016 |
| 10000009 | Cole   | Odom     | P.O. Box 962, 2496 Sodales St. | Boston      | MA    | 27282 | 08/21/2016 |
+----------+--------+----------+--------------------------------+-------------+-------+-------+------------+
10 rows in set (0.00 sec)

mysql> select count(*) from problem8.solution;
+----------+
| count(*) |
+----------+
|      100 |
+----------+
1 row in set (0.00 sec)

```

### Problem 9
```
desc problem9.customer;

select * from problem9.customer limit 10;
1000000	Medge	Roach	P.O. Box 799, 6865 Nec Rd.	Racine	WI	56336	08/10/2016
1000001	Nasim	Stone	P.O. Box 975, 759 Scelerisque Street	Tuscaloosa	AL	36696	08/16/2016
1000002	Jolie	Schneider	P.O. Box 829, 9011 Vulputate St.	Kapolei	HI	59913	08/22/2016
1000003	Lacota	Molina	Ap #831-2124 Pharetra. Avenue	Philadelphia	PA	84716	08/24/2016
1000004	Blaine	Sweet	P.O. Box 151, 7847 Pede. St.	Topeka	KS	66452	08/21/2016
1000005	Lesley	Bird	P.O. Box 569, 6635 Maecenas St.	Dallas	TX	87918	08/26/2016
1000006	Sydnee	Howell	P.O. Box 147, 3714 Dignissim Street	Dallas	TX	94072	08/11/2016
1000007	Jermaine	Griffin	Ap #859-2722 Donec Rd.	Baltimore	MD	13392	08/10/2016
1000008	Brynn	Pennington	Ap #968-9936 Eleifend Avenue	Lincoln	NE	89536	08/23/2016
1000009	Ava	Noble	P.O. Box 955, 1459 Urna St.	Baton Rouge	LA	34822	08/08/2016

show create table problem9.customer;

CREATE TABLE `problem9.solution`(
  `id` string, 
  `fname` string, 
  `lname` string, 
  `address` string, 
  `city` string, 
  `state` string, 
  `zip` string)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' 
;

insert into table problem9.solution
select concat("A", id)
       , fname
       , lname
       , address
       , city
       , state
       , zip
  from problem9.customer 
;

-- 결과 확인
hive> select * from problem9.solution limit 10;
OK
A1000000	Medge	Roach	P.O. Box 799, 6865 Nec Rd.	Racine	WI	56336
A1000001	Nasim	Stone	P.O. Box 975, 759 Scelerisque Street	Tuscaloosa	AL	36696
A1000002	Jolie	Schneider	P.O. Box 829, 9011 Vulputate St.	Kapolei	HI	59913
A1000003	Lacota	Molina	Ap #831-2124 Pharetra. Avenue	Philadelphia	PA	84716
A1000004	Blaine	Sweet	P.O. Box 151, 7847 Pede. St.	Topeka	KS	66452
A1000005	Lesley	Bird	P.O. Box 569, 6635 Maecenas St.	Dallas	TX	87918
A1000006	Sydnee	Howell	P.O. Box 147, 3714 Dignissim Street	Dallas	TX	94072
A1000007	Jermaine	Griffin	Ap #859-2722 Donec Rd.	Baltimore	MD	13392
A1000008	Brynn	Pennington	Ap #968-9936 Eleifend Avenue	Lincoln	NE	89536
A1000009	Ava	Noble	P.O. Box 955, 1459 Urna St.	Baton Rouge	LA	34822

```

### Problem 10
```
select A.id as id
       , A.fname as fname
       , A.lname as lname
       , A.city as city
       , A.state as state
       , B.charge as charge
       , substr(B.tstamp, 1, 10) as billdata
  from (
       select * 
         from problem10.customer 
       ) A
       left outer join (
       select * 
         from problem10.billing 
       ) B
       on A.id = B.id
 where B.id is not NULL
limit 10
;
1000000	Medge	Roach	Racine	WI	15.79	2017-03-05
1000001	Nasim	Stone	Tuscaloosa	AL	57.73	2016-09-05
1000002	Jolie	Schneider	Kapolei	HI	556.04	2017-02-06
1000003	Lacota	Molina	Philadelphia	PA	654.63	2016-12-01
1000004	Blaine	Sweet	Topeka	KS	287.12	2017-05-30
1000005	Lesley	Bird	Dallas	TX	900.14	2016-10-26
1000006	Sydnee	Howell	Dallas	TX	347.88	2016-10-04
1000007	Jermaine	Griffin	Baltimore	MD	754.84	2017-08-01
1000008	Brynn	Pennington	Lincoln	NE	289.66	2017-03-26
1000009	Ava	Noble	Baton Rouge	LA	928.68	2016-12-12

create view problem10.solution 
AS 
select A.id as id
       , A.fname as fname
       , A.lname as lname
       , A.city as city
       , A.state as state
       , B.charge as charge
       , substr(B.tstamp, 1, 10) as billdata
  from (
       select * 
         from problem10.customer 
       ) A
       left outer join (
       select * 
         from problem10.billing 
       ) B
       on A.id = B.id
 where B.id is not NULL
; 

-- 결과 확인
select * from problem10.solution limit 10;
1000000	Medge	Roach	Racine	WI	15.79	2017-03-05
1000001	Nasim	Stone	Tuscaloosa	AL	57.73	2016-09-05
1000002	Jolie	Schneider	Kapolei	HI	556.04	2017-02-06
1000003	Lacota	Molina	Philadelphia	PA	654.63	2016-12-01
1000004	Blaine	Sweet	Topeka	KS	287.12	2017-05-30
1000005	Lesley	Bird	Dallas	TX	900.14	2016-10-26
1000006	Sydnee	Howell	Dallas	TX	347.88	2016-10-04
1000007	Jermaine	Griffin	Baltimore	MD	754.84	2017-08-01
1000008	Brynn	Pennington	Lincoln	NE	289.66	2017-03-26
1000009	Ava	Noble	Baton Rouge	LA	928.68	2016-12-12

```