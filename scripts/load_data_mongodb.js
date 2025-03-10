db = connect("mongodb://localhost:27017/ecommerce")
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
                "subject": {
                    "bsonType": "object",
                    "properties": {
                        "length": {
                            "bsonType": "int"
                        },
                        "with_personalization": {
                            "bsonType": "bool"
                        },
                        "with_deadline": {
                            "bsonType": "bool"
                        },
                        "with_emoji": {
                            "bsonType": "bool"
                        },
                        "with_bonuses": {
                            "bsonType": "bool"
                        },
                        "with_discount": {
                            "bsonType": "bool"
                        },
                        "with_saleout": {
                            "bsonType": "bool"
                        }
                    },
                    "additionalProperties": false
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
                "product": {
                    "bsonType": "object",
                    "properties": {
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
                        }
                    },
                    "additionalProperties": false
                }
            },
            "additionalProperties": false,
            "patternProperties": {
                "user": {
                    "bsonType": "object",
                    "properties": {
                        "user_id": {
                            "bsonType": "number"
                        },
                        "user_session": {
                            "bsonType": "string"
                        }
                    },
                    "additionalProperties": false
                }
            }
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
                "platform": {
                    "bsonType": "string"
                },
                "email_provider": {
                    "bsonType": "string"
                },
                "stream": {
                    "bsonType": "string"
                },
                "sent_at": {
                    "bsonType": "date"
                },
                "status": {
                    "bsonType": "object",
                    "properties": {
                        "is_opened": {
                            "bsonType": "bool"
                        },
                        "opened_at": {
                            "bsonType": "object",
                            "properties": {
                                "first": {
                                    "bsonType": "date"
                                }
                            },
                            "additionalProperties": false,
                            "patternProperties": {
                                "last": {
                                    "bsonType": "date"
                                }
                            }
                        },
                        "is_clicked": {
                            "bsonType": "bool"
                        },
                        "clicked_at": {
                            "bsonType": "object",
                            "properties": {
                                "first": {
                                    "bsonType": "date"
                                },
                                "last": {
                                    "bsonType": "date"
                                }
                            },
                            "additionalProperties": false
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
                        "complained_at": {
                            "bsonType": "date"
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
                        }
                    },
                    "additionalProperties": false
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

var exec = require("child_process").execSync;

exec(`mongoimport -d ecommerce -c campaigns --jsonArray --type json --file ./data/cleaned_MongoDB/campaigns_fixed.json`);
exec(`mongoimport -d ecommerce -c client_first_purchase_date --jsonArray --type json --file ./data/cleaned_MongoDB/cfp_date_fixed.json`);
exec(`mongoimport -d ecommerce -c events --jsonArray --type json --file ./data/cleaned_MongoDB/events_fixed.json `);
exec(`mongoimport -d ecommerce -c friends --jsonArray --type json --file ./data/cleaned_MongoDB/friends.json `);
exec(`mongoimport -d ecommerce -c messages --jsonArray --type json --file ./data/cleaned_MongoDB/messages_date1_fixed.json `);
exec(`mongoimport -d ecommerce -c messages --jsonArray --type json --file ./data/cleaned_MongoDB/messages_date2_fixed.json `);
exec(`mongoimport -d ecommerce -c messages --jsonArray --type json --file ./data/cleaned_MongoDB/messages_date3_fixed.json `);
exec(`mongoimport -d ecommerce -c messages --jsonArray --type json --file ./data/cleaned_MongoDB/messages_date4_fixed.json `);
exec(`mongoimport -d ecommerce -c messages --jsonArray --type json --file ./data/cleaned_MongoDB/messages_date5_fixed.json `);