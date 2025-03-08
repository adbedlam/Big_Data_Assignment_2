db = connect("mongodb://localhost:27017/mongo_task")
db.createCollection("campaigns", {
    "capped": false,
    "validator": {
        "$jsonSchema": {
            "bsonType": "object",
            "title": "campaigns",
            "properties": {
                "_id": {
                    "bsonType": "objectId"
                },
                "id": {
                    "bsonType": "number"
                },
                "campaign_type": {
                    "bsonType": "string"
                },
                "channel": {
                    "bsonType": "string"
                },
                "topic": {
                    "bsonType": "string"
                },
                "started_at": {
                    "bsonType": "date"
                },
                "finished_at": {
                    "bsonType": "date"
                },
                "total_count": {
                    "bsonType": "number"
                },
                "ab_test": {
                    "bsonType": "bool"
                },
                "warmup_mode": {
                    "bsonType": "bool"
                },
                "hour_limit": {
                    "bsonType": "number"
                },
                "subject_length": {
                    "bsonType": "int"
                },
                "subject_with_personalization": {
                    "bsonType": "bool"
                },
                "subject_with_deadline": {
                    "bsonType": "bool"
                },
                "subject_with_emoji": {
                    "bsonType": "bool"
                },
                "subject_with_bonuses": {
                    "bsonType": "bool"
                },
                "subject_with_discount": {
                    "bsonType": "bool"
                },
                "subject_with_saleout": {
                    "bsonType": "bool"
                },
                "is_test": {
                    "bsonType": "bool"
                },
                "position": {
                    "bsonType": "number"
                }
            },
            "additionalProperties": false
        }
    },
    "validationLevel": "off",
    "validationAction": "warn"
});




db.createCollection("events", {
    "capped": false,
    "validator": {
        "$jsonSchema": {
            "bsonType": "object",
            "title": "events",
            "properties": {
                "_id": {
                    "bsonType": "objectId"
                },
                "event_time": {
                    "bsonType": "date"
                },
                "event_type": {
                    "bsonType": "string"
                },
                "product_id": {
                    "bsonType": "number"
                },
                "category_id": {
                    "bsonType": "number"
                },
                "category_code": {
                    "bsonType": "string"
                },
                "brand": {
                    "bsonType": "string"
                },
                "price": {
                    "bsonType": "number"
                },
                "user_id": {
                    "bsonType": "number"
                },
                "user_session": {
                    "bsonType": "string"
                }
            },
            "additionalProperties": false
        }
    },
    "validationLevel": "off",
    "validationAction": "warn"
});




db.createCollection("friends", {
    "capped": false,
    "validator": {
        "$jsonSchema": {
            "bsonType": "object",
            "title": "friends",
            "properties": {
                "_id": {
                    "bsonType": "objectId"
                },
                "friend1": {
                    "bsonType": "number"
                },
                "friend2": {
                    "bsonType": "number"
                }
            },
            "additionalProperties": false
        }
    },
    "validationLevel": "off",
    "validationAction": "warn"
});

db.friends.createIndex({
    "friend1": 1,
    "friend2": 1
},
{
    "name": "unique_friend",
    "unique": true
});




db.createCollection("client_first_purchase_date", {
    "capped": false,
    "validator": {
        "$jsonSchema": {
            "bsonType": "object",
            "title": "client_first_purchase_date",
            "properties": {
                "_id": {
                    "bsonType": "objectId"
                },
                "client_id": {
                    "bsonType": "number"
                },
                "first_purchase_date": {
                    "bsonType": "date"
                },
                "user_id": {
                    "bsonType": "number"
                },
                "user_device_id": {
                    "bsonType": "number"
                }
            },
            "additionalProperties": false
        }
    },
    "validationLevel": "off",
    "validationAction": "warn"
});

db.client_first_purchase_date.createIndex({
    "client_id": 1
},
{
    "name": "unique_client",
    "unique": true
});




db.createCollection("messages", {
    "capped": false,
    "validator": {
        "$jsonSchema": {
            "bsonType": "object",
            "title": "messages",
            "properties": {
                "_id": {
                    "bsonType": "objectId"
                },
                "id": {
                    "bsonType": "number"
                },
                "message_id": {
                    "bsonType": "string"
                },
                "campaign_id": {
                    "bsonType": "number"
                },
                "message_type": {
                    "bsonType": "string"
                },
                "client_id": {
                    "bsonType": "number"
                },
                "channel": {
                    "bsonType": "string"
                },
                "category": {
                    "bsonType": "string"
                },
                "platform": {
                    "bsonType": "string"
                },
                "email_provider": {
                    "bsonType": "string"
                },
                "stream": {
                    "bsonType": "string"
                },
                "date": {
                    "bsonType": "date"
                },
                "sent_at": {
                    "bsonType": "date"
                },
                "is_opened": {
                    "bsonType": "bool"
                },
                "opened_first_time_at": {
                    "bsonType": "date"
                },
                "opened_last_time_at": {
                    "bsonType": "date"
                },
                "is_clicked": {
                    "bsonType": "bool"
                },
                "clicked_first_time_at": {
                    "bsonType": "date"
                },
                "clicked_last_time_at": {
                    "bsonType": "date"
                },
                "is_unsubscribe": {
                    "bsonType": "bool"
                },
                "unsubscribed_at": {
                    "bsonType": "date"
                },
                "is_hard_bounced": {
                    "bsonType": "bool"
                },
                "hard_bounced_at": {
                    "bsonType": "date"
                },
                "is_soft_bounced": {
                    "bsonType": "bool"
                },
                "soft_bounced_at": {
                    "bsonType": "date"
                },
                "is_complained": {
                    "bsonType": "bool"
                },
                "is_blocked": {
                    "bsonType": "bool"
                },
                "blocked_at": {
                    "bsonType": "date"
                },
                "is_purchased": {
                    "bsonType": "bool"
                },
                "purchased_at": {
                    "bsonType": "date"
                },
                "created_at": {
                    "bsonType": "date"
                },
                "updated_at": {
                    "bsonType": "date"
                },
                "user_device_id": {
                    "bsonType": "number"
                },
                "user_id": {
                    "bsonType": "number"
                }
            },
            "additionalProperties": false
        }
    },
    "validationLevel": "off",
    "validationAction": "warn"
});

var exec = require("child_process").exec;


exec(`mongoimport -d mongo_task -c campaigns --type csv --file D:/datasets/assigment/cleaned/Clean_Campaings.csv --headerline`);
exec(`mongoimport -d mongo_task -c client_first_purchase_date --type csv --file D:/datasets/assigment/cleaned/Clean_client_first_purchase_date.csv --headerline`);
exec(`mongoimport -d mongo_task -c events --type csv --file D:/datasets/assigment/cleaned/Clean_events.csv --headerline`);
exec(`mongoimport -d mongo_task -c friends --type csv --file D:/datasets/assigment/cleaned/Clean_friends.csv --headerline`);
exec(`mongoimport -d mongo_task -c messages --type csv --file D:/datasets/assigment/cleaned/Clean_messages.csv --headerline`);