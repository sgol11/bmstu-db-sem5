-- Защита: ввести id детектива, вывести всех подозреваемых, с которыми он взаимодействовал

drop function if exists get_detective_suspects(int);

create or replace function get_detective_suspects(detective_arg int = 3000) 
returns table (suspect_id int, suspect_name varchar) as '
    select s.suspect_id, s.full_name
    from detectives d join crimes c on d.detective_id = c.detective 
                      join suspects_crimes sc on sc.crime_id = c.crime_id 
                      join suspects s on sc.suspect_id = s.suspect_id
    where d.detective_id = detective_arg;
'
language sql;

select *
from get_detective_suspects(3001);