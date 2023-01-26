-- Функция, возвращающая самый продуктивный департамент 
-- (показатель: количество решенных дел / кол-во детективов)

drop function if exists count_metric(int);
drop function if exists most_productive();

create or replace function count_metric(dep int)
returns numeric
as 
$$	     
res = plpy.execute(f"select * from departments join detectives on department_id = department;")
				     
detectives_num = 0
solved_crimes_sum = 0
				     
for i in res:
    if i["department_id"] == dep:
        detectives_num += 1
        solved_crimes_sum += i["solved_crimes_num"]
		
if detectives_num > 0:
    return solved_crimes_sum / detectives_num
$$ 
language plpython3u;

create or replace function most_productive()
returns int
as 
$$      
res = plpy.execute(f"select department_id, count_metric(department_id) as metric from departments;")

max_metric = 0
most_productive_dep = 0
				     
for i in res:
    if i["metric"] and i["metric"] > max_metric:
        max_metric = i["metric"]
        most_productive_dep = i["department_id"]
		
if most_productive_dep:
    return most_productive_dep
$$ 
language plpython3u;

select count_metric(1824);

select most_productive();


select department_id, sum(solved_crimes_num), count(1), 1.0 * sum(solved_crimes_num) / count(1) as metric
from detectives join departments on department_id = department
group by department_id
order by metric desc;
