from pymongo import MongoClient
from datetime import datetime
import pprint

mongodb_client = MongoClient("localhost", 27017)
database = mongodb_client["university"]
collection = database["courses"]

document = {
    "course_name": "Повышение квалификации преподавателей по математике",
    "teacher_name": "Иванов Иван Петрович",
    "start_date": datetime(2024, 11, 1),
    "end_date":  datetime(2024, 11, 7),
    "hour_amount": 20,
    "university_workers": ["Золотарева Алина Руслановна", "Антонов Даниил Иванович"]
}

documents =[
    {
        "course_name": "Курс по postgresql",
        "teacher_name": "Петров Петр Иванович",
        "start_date": datetime(2024, 11, 5),
        "end_date":  datetime(2024, 11, 10),
        "hour_amount": 15,
        "university_workers": ["Потапова Ольга Артёмовна", "Антонов Даниил Иванович"]
    },
    {
        "course_name": "Курс улучшения коммуникации со студентами",
        "teacher_name": "Семенова Стефания Александровна",
        "start_date": datetime(2024, 12, 3),
        "end_date":  datetime(2024, 12, 10),
        "hour_amount": 40,
        "university_workers": ["Румянцев Михаил Михайлович", "Поляков Алексей Ильич", "Казакова Александра Дамировна"]
    },
    {
        "course_name": "Курс повышения навыков работы с офисным пакетом Micrisoft office",
        "teacher_name": "Алексеев Дмитрий Захарович",
        "start_date": datetime(2024, 10, 15),
        "end_date":  datetime(2024, 10, 23),
        "hour_amount": 40,
        "university_workers": ["Игнатова Алиса Николаевна", "Румянцев Михаил Михайлович", "Антонов Даниил Иванович"]
    },
    {
        "course_name": "Курс работы с Matlab",
        "teacher_name": "Иванов Иван Петрович",
        "start_date": datetime(2024, 9, 11),
        "end_date":  datetime(2024, 9, 20),
        "hour_amount": 40,
        "university_workers": ["Никифоров Анатолий Тихонович"]
    }
]

#collection.insert_one(document)
#collection.insert_many(documents)

#pprint.pprint(collection.find_one())     #Чтобы вывести первый документ

#for document in collection.find():
#    pprint.pprint(document)
#    print()

#for document in collection.find({"hour_amount": {"$gte": 25}}).sort("course_name"):
#    pprint.pprint(document)
#    print()

for document in collection.find({"$where": "this.university_workers.length > 2"}).sort("course_name"):
    pprint.pprint(document)
    print()
