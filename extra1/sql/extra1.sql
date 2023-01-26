-- Дополнительное задание

drop table if exists table1;
drop table if exists table2;


create table if not exists table1
(
    id integer,
    var1 varchar(20) not null,
    valid_from_dttm date not null,
    valid_to_dttm date not null
);

insert into table1
values (1, 'A', '2018-09-01', '2018-09-15');

insert into table1
values (1, 'B', '2018-09-16', '5999-12-31');


create table if not exists table2
(
    id integer,
    var2 varchar(20) not null,
    valid_from_dttm date not null,
    valid_to_dttm date not null
);

insert into table2
values (1, 'A', '2018-09-01', '2018-09-18');

insert into table2
values (1, 'B', '2018-09-19', '5999-12-31');


select * 
from (select table1.id, var1, var2,
             greatest(table1.valid_from_dttm, table2.valid_from_dttm) as valid_from_dttm,
             least(table1.valid_to_dttm, table2.valid_to_dttm) as valid_to_dttm
      from table1 full outer join table2 on table1.id = table2.id) as result
where valid_from_dttm <= valid_to_dttm 
order by id, valid_from_dttm;