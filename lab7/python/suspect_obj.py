class Suspect:
    suspect_id = int()
    full_name = str()
    gender = str()
    age = int()
    home_town = str()
    crimes_num = int()

    def __init__(self, suspect_id, full_name, gender, age, home_town, crimes_num):
        self.suspect_id = suspect_id
        self.full_name = full_name
        self.gender = gender
        self.age = age
        self.home_town = home_town
        self.crimes_num = crimes_num

    def get(self):
        return {'suspect_id': self.suspect_id,
                'full_name': self.full_name,
                'gender': self.gender,
                'age': self.age,
                'home_town': self.home_town,
                'crimes_num': self.crimes_num}

    def __str__(self):
        return f"{self.suspect_id:<5} {self.full_name:<20} " \
               f"{self.gender:<2} {self.age:<3} {self.home_town:<20} {self.crimes_num:<3}"


def create_suspects(file_name):
    file = open(file_name, 'r')
    suspects = []

    for i, line in enumerate(file):
        arr = line.split(',')
        arr[0], arr[3], arr[5] = int(arr[0]), int(arr[3]), int(arr[5])
        suspects.append(Suspect(*arr).get())

    return suspects
