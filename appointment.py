import pymongo 
import json



if __name__ == '__main__':

    #connecting to the local server
    client=pymongo.MongoClient("mongodb://localhost:27017")

    #creating database 
    db = client['wysa']
    appointment_collection=db['Appointment']
    chats_collection=db['Chats']
    appointment_chats=db['appointment_chats']

    # create collection with chat between particular appointment interval...
    appointment_chats_collection = list(chats_collection.aggregate([
        {
            "$lookup":{
                "from":"Appointment",
                "localField":"user_id",
                "foreignField":"user_id",
                "as":"appointment_data",
            }
        },
        {
            "$unwind" : "$appointment_data"
        },
        {
            "$match":{
                "$and": [
                    {
                        "$expr": {
                            "$gte": [
                                "$time",
                                "$appointment_data.start_time"
                            ]
                        }
                    },
                    {
                        "$expr": {
                            "$lte": [
                                "$time",
                                "$appointment_data.end_time"
                            ]
                        }
                    }
                ]  
            }
        },
        {
            "$out": "appointment_chats"
        }
    ]))

    extra_chat_count = list(chats_collection.aggregate([
        {
            "$lookup":{
                "from":"appointment_chats",
                "localField":"_id",
                "foreignField":"_id",
                "as":"res_chats",
            }       
       },
       {
            "$match":{
                "res_chats": {
                    "$size": 0
                }
            }
       },
       {
            "$group": {
                "_id": {"user_id":"$user_id"},
                "extra_message_count": { "$count" : {} },
            }     
       },
       {
            "$project": {
                "_id":0,
                "user_id": "$_id.user_id",
                "extra_message_count": "$extra_message_count"
            }
       }

     ]))
    
with open("extra_chat_count.json", 'w') as fp:
    json.dump(extra_chat_count, fp, indent=4, default=str)  


