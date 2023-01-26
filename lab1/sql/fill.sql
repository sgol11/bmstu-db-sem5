copy places (place_id, latitude, longitude, place_type, place_object) from 'crimes_data/places.csv' delimiter ',' csv;
copy suspects (suspect_id, full_name, gender, age, home_town, crimes_num) from 'crimes_data/suspects.csv' delimiter ',' csv;
copy departments (department_id, address, department_size, phone_number, email) from 'crimes_data/departments.csv' delimiter ',' csv;
copy detectives (detective_id, full_name, experience, solved_crimes_num, department) from 'crimes_data/detectives.csv' delimiter ',' csv;
copy crimes (crime_id, crime_date, crime_place, crime_type, detective) from 'crimes_data/crimes.csv' delimiter ',' csv;
copy suspects_crimes (suspect_id, crime_id) from 'crimes_data/suspects_crimes.csv' delimiter ',' csv;