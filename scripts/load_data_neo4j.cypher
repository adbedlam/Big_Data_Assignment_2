
CREATE INDEX campaign_id_type IF NOT EXISTS FOR (c:campaign) ON (c.id, c.campaign_type);
CREATE INDEX client_user_id IF NOT EXISTS FOR (cl:client) ON (cl.user_id);
CREATE INDEX product_id_code IF NOT EXISTS FOR (p:product) ON (p.product_id, p.category_code);
CREATE INDEX message_campaign_id IF NOT EXISTS FOR (m:message) ON (m.campaign_id);
CREATE INDEX event_user_id IF NOT EXISTS FOR (e:event) ON (e.user_id);

:auto

LOAD CSV WITH HEADERS FROM "file:///campaings.csv" AS row FIELDTERMINATOR ','
CALL {
    WITH row
    CREATE (`campaign`:`campaign` {
	`id`: toInteger(row.id),
	`campaign_type`: row.campaign_type,
	`channel`: row.channel,
	`topic`: row.topic,
	`started_at`: datetime(REPLACE(row.started_at, ' ', 'T')),
	`finished_at`: datetime(REPLACE(row.finished_at, ' ', 'T')),
	`total_count`: toInteger(row.total_count),
	`ab_test`: toBoolean(row.ab_test),
	`warmup_mode`: toBoolean(row.warmup_mode),
	`hour_limit`: toInteger(row.hour_limit),
	`subject_length`: toInteger(row.subject_length),
	`subject_with_personalization`: toBoolean(row.subject_with_personalization),
	`subject_with_deadline`: toBoolean(row.subject_with_deadline),
	`subject_with_emoji`: toBoolean(row.subject_with_emoji),
	`subject_with_bonuses`: toBoolean(row.subject_with_bonuses),
	`subject_with_discount`: toBoolean(row.subject_with_discount),
	`subject_with_saleout`: toBoolean(row.subject_with_saleout),
	`is_test`: toBoolean(row.is_test),
	`position`: toInteger(row.position)
	})
} IN TRANSACTIONS OF 50000 ROWS;


:auto

LOAD CSV WITH HEADERS FROM "file:///messages.csv" AS row FIELDTERMINATOR ','
CALL {
    WITH row
    MATCH (c:campaign {id: toInteger(row.campaign_id), campaign_type: row.message_type})
    CREATE (`message`:`message` {
	`id`: toInteger(row.id),
	`message_id`: toInteger(row.message_id),
	`campaign_id`: toInteger(row.campaign_id),
	`message_type`: row.message_type,
	`client_id`: toInteger(row.client_id),
	`channel`: row.channel,
	`category`: row.category,
	`platform`: row.platform,
	`email_provider`: row.email_provider,
	`stream`: row.stream,
	`date`: datetime(REPLACE(row.date, ' ', 'T')),
	`sent_at`: datetime(REPLACE(row.sent_at, ' ', 'T')),
	`is_opened`: toBoolean(row.is_opened),
	`opened_first_time_at`: datetime(REPLACE(row.opened_first_time_at, ' ', 'T')),
	`opened_last_time_at`: datetime(REPLACE(row.opened_last_time_at, ' ', 'T')),
	`is_clicked`: toBoolean(row.is_clicked),
	`clicked_first_time_at`: datetime(REPLACE(row.clicked_first_time_at, ' ', 'T')),
	`clicked_last_time_at`: datetime(REPLACE(row.clicked_last_time_at, ' ', 'T')),
	`is_unsubscribed`: toBoolean(row.is_unsubscribed),
	`unsubscribed_at`: datetime(REPLACE(row.unsubscribed_at, ' ', 'T')),
	`is_hard_bounced`: toBoolean(row.is_hard_bounced),
	`hard_bounced_at`: datetime(REPLACE(row.hard_bounced_at, ' ', 'T')),
	`is_soft_bounced`: toBoolean(row.is_soft_bounced),
	`soft_bounced_at`: datetime(REPLACE(row.soft_bounced_at, ' ', 'T')),
	`is_complained`: toBoolean(row.is_complained),
	`complained_at`: datetime(REPLACE(row.complained_at, ' ', 'T')),
	`is_blocked`: toBoolean(row.is_blocked),
	`blocked_at`: datetime(REPLACE(row.blocked_at, ' ', 'T')),
	`is_purchased`: toBoolean(row.is_purchased),
	`purchased_at`: datetime(REPLACE(row.purchased_at, ' ', 'T')),
	`created_at`: datetime(REPLACE(row.created_at, ' ', 'T')),
	`updated_at`: datetime(REPLACE(row.updated_at, ' ', 'T'))
})
    CREATE (m)-[:RELATED_TO]->(c)
} IN TRANSACTIONS OF 50000 ROWS;


:auto

LOAD CSV WITH HEADERS FROM "file:///client.csv" AS row FIELDTERMINATOR ','
CALL {
    WITH row
    CREATE (`client`:`client` {
	`client_id`: toInteger(row.client_id),
	`first_purchase_date`: datetime(row.first_purchase_date),
	`user_id`: toInteger(row.user_id),
	`device_id`: toInteger(row.device_id)
})

} IN TRANSACTIONS OF 50000 ROWS;

:auto

LOAD CSV WITH HEADERS FROM "file:///friends.csv" AS row FIELDTERMINATOR ','
CALL {
    WITH row
    MATCH (cl:client {user_id: toInteger(row.friend1)})
    MATCH (ci:client {user_id: toInteger(row.friend2)})
    MERGE (cl)-[:FRIEND]->(ci)
    MERGE (ci)-[:FRIEND]->(cl)
} IN TRANSACTIONS OF 50000 ROWS;


:auto
LOAD CSV WITH HEADERS FROM "file:///events.csv" AS row FIELDTERMINATOR ','
CALL {
    WITH row
    CREATE (`event`:`event` {
		`event_time`: datetime(REPLACE(row.event_time, ' ', 'T')),
		`event_type`: row.event_type,
		`product_id`: toInteger(row.product_id),
		`category_code`: row.category_code,
		`user_id`: toInteger(row.user_id),
		`user_session`: row.user_session
		})
} IN TRANSACTIONS OF 50000 ROWS;


:auto

LOAD CSV WITH HEADERS FROM "file:///products.csv" AS row FIELDTERMINATOR ','
CALL {
    WITH row
    CREATE (`product`:`product` {
			`product_id`: toInteger(row.product_id),
			`category_id`: row.category_id,
			`brand`: row.brand,
			`category_code`: row.category_code,
			`price`: toFloat(row.price)
		})
} IN TRANSACTIONS OF 50000 ROWS;


MATCH (m:message), (c:campaign)
WHERE m.campaign_id = c.id AND m.message_type = c.campaign_type
MERGE (m)-[:RELATED_TO]->(c)



MATCH (m:message), (cl:client)
WHERE m.client_id = cl.client_id
MERGE (m)-[:RELATED_TO]->(cl)




MATCH (cl:client), (e:event)
WHERE cl.user_id = e.user_id
MERGE (cl)-[:RELATED_TO]->(e)


MATCH (p:product), (e:event)
WHERE p.product_id = e.product_id AND p.category_code = e.category_code
MERGE (p)-[:RELATED_TO]->(e)
