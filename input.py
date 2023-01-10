import pymongo 
from datetime import datetime, timedelta
import random
import pandas as pd
if __name__ == '__main__':
    #connecting to the local server
    client=pymongo.MongoClient("mongodb://localhost:27017")
    print('done')
    #creating database 
    db = client['wysa']

    
    insert_appointment=[
        {"user_id": "A","start_time":datetime.strptime("2022-04-03T09:00:11.553Z", "%Y-%m-%dT%H:%M:%S.%fZ").timestamp(),"end_time":datetime.strptime("2022-04-03T10:00:11.553Z", "%Y-%m-%dT%H:%M:%S.%fZ").timestamp() },
        {"user_id": "B","start_time":datetime.strptime("2022-04-03T11:00:11.553Z", "%Y-%m-%dT%H:%M:%S.%fZ").timestamp(),"end_time":datetime.strptime("2022-04-03T12:00:11.553Z", "%Y-%m-%dT%H:%M:%S.%fZ").timestamp() },
        {"user_id": "C","start_time":datetime.strptime("2022-04-03T13:00:11.553Z", "%Y-%m-%dT%H:%M:%S.%fZ").timestamp(),"end_time":datetime.strptime("2022-04-03T14:00:11.553Z", "%Y-%m-%dT%H:%M:%S.%fZ").timestamp() },
        {"user_id": "A","start_time":datetime.strptime("2022-04-04T10:00:11.553Z", "%Y-%m-%dT%H:%M:%S.%fZ").timestamp(),"end_time":datetime.strptime("2022-04-04T11:00:11.553Z", "%Y-%m-%dT%H:%M:%S.%fZ").timestamp() },
        {"user_id": "B","start_time":datetime.strptime("2022-04-04T12:00:11.553Z", "%Y-%m-%dT%H:%M:%S.%fZ").timestamp(),"end_time":datetime.strptime("2022-04-04T13:00:11.553Z", "%Y-%m-%dT%H:%M:%S.%fZ").timestamp() },
        {"user_id": "C","start_time":datetime.strptime("2022-04-04T14:00:11.553Z", "%Y-%m-%dT%H:%M:%S.%fZ").timestamp(),"end_time":datetime.strptime("2022-04-04T15:00:11.553Z", "%Y-%m-%dT%H:%M:%S.%fZ").timestamp() },
        {"user_id": "A","start_time":datetime.strptime("2022-04-05T15:00:11.553Z", "%Y-%m-%dT%H:%M:%S.%fZ").timestamp(),"end_time":datetime.strptime("2022-04-05T16:00:11.553Z", "%Y-%m-%dT%H:%M:%S.%fZ").timestamp() },
        {"user_id": "B","start_time":datetime.strptime("2022-04-05T17:00:11.553Z", "%Y-%m-%dT%H:%M:%S.%fZ").timestamp(),"end_time":datetime.strptime("2022-04-05T18:00:11.553Z", "%Y-%m-%dT%H:%M:%S.%fZ").timestamp() },
        {"user_id": "C","start_time":datetime.strptime("2022-04-05T19:00:11.553Z", "%Y-%m-%dT%H:%M:%S.%fZ").timestamp(),"end_time":datetime.strptime("2022-04-05T20:00:11.553Z", "%Y-%m-%dT%H:%M:%S.%fZ").timestamp() }
    ]
    
    def rand_time(start, end):
        timings=[]
        curr_time = start
        increase_chat_time = random.randint(-10,20)
        while curr_time <= end+timedelta(minutes = increase_chat_time):
            r1 = random.randint(1, 10)
            curr_time += timedelta(minutes = r1)
            timings.append(curr_time)
        return timings
    
    insert_chats = []
    for appointment in insert_appointment:
        collected_chat_time = rand_time(datetime.fromtimestamp(appointment['start_time']), datetime.fromtimestamp(appointment['end_time']))
        for chat_time in collected_chat_time:
            chat = {
                "user_id": appointment['user_id'],
                "message": f"message of {chat_time}",
                "time": str(chat_time.timestamp())
            }
            insert_chats.append(chat)

    for appointment in insert_appointment:
        appointment['start_time']=str(appointment['start_time'])
        appointment['end_time']=str(appointment['end_time'])


    print(insert_appointment) 
    collection=db['Appointment']
    collection.insert_many(insert_appointment)

    collection=db['Chats']
    collection.insert_many(insert_chats)