alter table places
add constraint pk_place_id primary key (place_id),
add constraint check_latitude check (latitude between -90.000000 and 90.000000),
add constraint check_longitude check (longitude between -180.000000 and 180.000000);

alter table suspects
add constraint pk_suspect_id primary key (suspect_id),
add constraint check_age check (age >= 14),
add constraint check_crimes_num check (crimes_num >= 0);

alter table departments
add constraint pk_department_id primary key (department_id),
add constraint check_size check (department_size between 1 and 1000);

alter table detectives
add constraint pk_detective_id primary key (detective_id),
add constraint check_experience check (experience between 0 and 50),
add constraint check_solved_crimes_num check (solved_crimes_num >= 0),
add constraint fk_department foreign key (department) references departments(department_id);

alter table crimes
add constraint pk_crime_id primary key (crime_id),
add constraint fk_place foreign key (crime_place) references places(place_id),
add constraint fk_detective foreign key (detective) references detectives(detective_id);

alter table suspects_crimes
add constraint fk_suspect foreign key (suspect_id) references suspects(suspect_id),
add constraint fk_crime foreign key (crime_id) references crimes(crime_id);