from mimesis import Generic
from mimesis.locales import Locale
from mimesis.enums import Gender
from random import choice, randint, shuffle

import pandas as pd

NCrimes = 1000
NPlaces = 1000
NSuspects = 1000
NDetectives = 1000
NDepartments = 1000

crimes_filename = 'crimes.csv'
places_filename = 'places.csv'
suspects_filename = 'suspects.csv'
detectives_filename = 'detectives.csv'
departments_filename = 'departments.csv'
suspects_crimes_filename = 'suspects_crimes.csv'

crimes_columns = ['CrimeID', 'Date', 'Place', 'Type', 'Detective']
places_columns = ['PlaceID', 'Latitude', 'Longitude', 'Type', 'Object']
suspects_columns = ['SuspectID', 'FullName', 'Gender', 'Age', 'CityOfResidence', 'CrimesNum']
detectives_columns = ['DetectiveID', 'FullName', 'Experience', 'SolvedCrimesNum', 'Department']
departments_columns = ['DepartmentID', 'Address', 'Size', 'PhoneNumber', 'Email']
suspects_crimes_columns = ['SuspectID', 'CrimeID']

CrimeIDList = list(range(1, NCrimes + 1))
PlaceIDList = list(range(8000, NPlaces + 8001))
SuspectIDList = list(range(5000, NSuspects + 5001))
DetectiveIDList = list(range(3000, NDetectives + 3001))
DepartmentIDList = list(range(1000, NDepartments + 1001))


def generate_crimes():
    """
        Crimes
    """
    crimes_df = pd.DataFrame(columns=crimes_columns)

    generic = Generic(locale=Locale.EN)

    crime_types = ['Burglary', 'Arson', 'Domestic abuse', 'Murder', 'Robbery', 'Sexual harassment']

    for i in range(len(CrimeIDList)):
        date = generic.datetime.formatted_datetime()
        place = choice(PlaceIDList)
        crime_type = choice(crime_types)
        detective = choice(DetectiveIDList)

        crime = [None, date, place, crime_type, detective]
        crimes_df = crimes_df.append({crimes_columns[i]: crime[i]
                                      for i in range(len(crimes_columns))}, ignore_index=True)

    crimes_df['Date'] = pd.to_datetime(crimes_df['Date'])
    crimes_df = crimes_df.sort_values(by='Date')
    crimes_df['CrimeID'] = CrimeIDList
    crimes_df.to_csv(crimes_filename, sep=',', header=False, index=False)


def generate_places():
    """
        Places
    """
    places_df = pd.DataFrame(columns=places_columns)

    generic = Generic(locale=Locale.EN)

    outdoors_objects = ['River', 'Lake', 'Pond', 'Park', 'Field', 'Forest', 'Garden', 'Street', 'Square']
    indoors_objects = ['Flat', 'House', 'Bank', 'Office', 'University', 'Shop', 'Museum']
    transport_objects = ['Bus', 'Taxi', 'Car', 'Underground']

    for place_id in PlaceIDList:
        latitude = generic.address.latitude()
        longitude = generic.address.longitude()
        weight_random = randint(0, 100)
        if weight_random <= 35:
            place_type = 'Outdoors'
            place_object = choice(outdoors_objects)
        elif weight_random <= 80:
            place_type = 'Indoors'
            place_object = choice(indoors_objects)
        else:
            place_type = 'Transport'
            place_object = choice(transport_objects)

        place = [place_id, latitude, longitude, place_type, place_object]
        places_df = places_df.append({places_columns[i]: place[i]
                                      for i in range(len(places_columns))}, ignore_index=True)

    places_df.to_csv(places_filename, sep=',', header=False, index=False)


def generate_suspects():
    """
        Suspects
    """
    suspects_df = pd.DataFrame(columns=suspects_columns)

    generic = Generic(locale=Locale.EN)

    cities = ['New York City', 'Los Angeles', 'Chicago', 'Houston', 'Philadelphia', 'Dallas',
              'San Diego', 'Columbus', 'Boston', 'Kansas City', 'Omaha', 'Atlanta', 'Orlando',
              'Riverside', 'San Juan', 'Madison', 'Vancouver', 'Roseville', 'Savannah', 'Rockford',
              'Springfield', 'Bayamon', 'Aurora', 'Mobile', 'Glendale', 'Montgomery', 'Oxnard']

    for suspect_id in SuspectIDList:
        gender_id = randint(0, 1)
        gender = [Gender.FEMALE, Gender.MALE][gender_id]

        name = generic.person.full_name(gender=gender)
        gender = 'f' if gender_id == 0 else 'm'
        age = randint(18, 60)
        city = choice(cities)
        crimes_num = randint(0, 10)

        suspect = [suspect_id, name, gender, age, city, crimes_num]
        suspects_df = suspects_df.append({suspects_columns[i]: suspect[i]
                                          for i in range(len(suspects_columns))}, ignore_index=True)

    suspects_df.to_csv(suspects_filename, sep=',', header=False, index=False)


def generate_detectives():
    """
        Detectives
    """
    detectives_df = pd.DataFrame(columns=detectives_columns)

    generic = Generic(locale=Locale.EN)

    for detective_id in DetectiveIDList:
        name = generic.person.full_name()
        experience = randint(0, 30)
        solved_crimes = randint(0, 50)
        department = choice(DepartmentIDList)

        detective = [detective_id, name, experience, solved_crimes, department]
        detectives_df = detectives_df.append({detectives_columns[i]: detective[i]
                                              for i in range(len(detectives_columns))}, ignore_index=True)

    detectives_df.to_csv(detectives_filename, sep=',', header=False, index=False)


def generate_departments():
    """
        Departments
    """
    departments_df = pd.DataFrame(columns=departments_columns)

    generic = Generic(locale=Locale.EN)

    email_domains = ['police.org', 'police.com', 'detective.org', 'detective.com']

    for department_id in DepartmentIDList:
        address = generic.address.address()
        size = randint(50, 600)
        phone_number = generic.person.telephone(mask='1-(800)-###-##-##')
        email = 'dep' + str(department_id) + '@' + choice(email_domains)

        department = [department_id, address, size, phone_number, email]
        departments_df = departments_df.append({departments_columns[i]: department[i]
                                                for i in range(len(departments_columns))}, ignore_index=True)

    departments_df.to_csv(departments_filename, sep=',', header=False, index=False)


def generate_suspects_crimes():
    """
        SuspectsCrimes
    """
    suspects_crimes_df = pd.DataFrame(columns=suspects_crimes_columns)

    generic = Generic(locale=Locale.EN)

    for i in range(2000):
        suspect = choice(SuspectIDList)
        crime = choice(CrimeIDList)

        suspect_crime = [suspect, crime]
        suspects_crimes_df = suspects_crimes_df.append({suspects_crimes_columns[i]: suspect_crime[i]
                                                        for i in range(len(suspects_crimes_columns))}, ignore_index=True)

    suspects_crimes_df.to_csv(suspects_crimes_filename, sep=',', header=False, index=False)


if __name__ == "__main__":
    generate_crimes()
    generate_places()
    generate_suspects()
    generate_detectives()
    generate_departments()
    generate_suspects_crimes()
