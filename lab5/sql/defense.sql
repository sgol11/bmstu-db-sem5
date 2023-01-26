drop table if exists dd;

create table dd(doc jsonb);

insert into dd values ('{"detective_id":4010, "detective_name":"Sherlock Holmes", "solved_crimes_num":100, 
                         "department": {"department_id":1090, "department_size":298}}');
insert into dd values ('{"detective_id":4011, "detective_name":"Hercule Poirot", "solved_crimes_num":87, 
                         "department": {"department_id":1080, "department_size":600}}');
insert into dd values ('{"detective_id":4012, "detective_name":"Auguste Dupin", "solved_crimes_num":16, 
                         "department": {"department_id":1097, "department_size":226}}');
                     

select * from dd;