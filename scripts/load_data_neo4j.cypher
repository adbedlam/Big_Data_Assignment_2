:begin
// cat <path to cypher file> | ./bin/cypher-shell -a <address> -u <user> -p <password>
USING PERIODIC COMMIT 10000
LOAD CSV WITH HEADERS FROM "file:///processed_events.csv" AS 	row
CREATE
(`user`:`user` {
	`user_id`: row.user_id
});
USING PERIODIC COMMIT 10000
LOAD CSV WITH HEADERS FROM "file:///processed_campaigns.csv" AS 	row
CREATE (`campaign`:`campaign` {
	`id`:  toInteger(row.id),
	`campaign_type`: row.campaign_type,
	`channel`: row.channel,
	`started_at`: datetime(replace(row.started_at, " ", "T")),
	`finished_at`: datetime(replace(row.finished_at, " ", "T"))
});

USING PERIODIC COMMIT 10000
LOAD CSV WITH HEADERS FROM "file:///processed_events.csv" AS 	row
CREATE
(`product`:`product` {
	`prduct_id`: toInteger(row.id),
	`price`: toFloat(row.price),
	`brand`: row.brand,
	`category_code`: toInteger(row.category_code)
});
USING PERIODIC COMMIT 10000
LOAD CSV WITH HEADERS FROM "file:///processed_clients.csv" AS 	row
CREATE
(`client`:`client` {
	`client_id`: toInteger(row.client_id),
	`first_purchase_date`: datetime(replace(row.first_purchase_date, " ", "T"))
});
USING PERIODIC COMMIT 10000
LOAD CSV WITH HEADERS FROM "file:///processed_messages.csv" AS 	row
CREATE
(`message`:`message` {
	`message_id`: toInteger(row.id),
	`sent_at`: datetime(replace(row.sent_at, " ", "T")),
	`is_opened`: toBoolean(row.is_opened),
	`is_clicked`: toBoolean(row.is_cliked)
});

USING PERIODIC COMMIT 10000
LOAD CSV WITH HEADERS FROM "file:///processed_clients.csv" AS 	row
CREATE
(`device`:`device` {
	`user_device_id`: toInteger(row.user_device_id)
});
USING PERIODIC COMMIT 10000
LOAD CSV WITH HEADERS FROM "file:///processed_clients.csv" AS 	row
CREATE
(`event`:`event` {
	`event_time`: datetime(replace(row.event_time, " ", "T")),
	`event_type`: row.event_type
});
USING PERIODIC COMMIT 10000
CREATE
(`campaign`)-[:`SENT_MESSAGE` {}]->(`message`),

(`user`)-[:`PERFORMED` {}]->(`event`),

(`user`)-[:`FRIENDS_WITH` {}]->(`user`),

(`client`)-[:`HAS_USER` {}]->(`user`),

(`client`)-[:`USES_DEVICE` {}]->(`device`),

(`message`)-[:`SENT_TO` {}]->(`client`),

(`event`)-[:`INVOLVES` {}]->(`product`)
 RETURN `campaign`,`product`,`user`,`client`,`message`,`device`,`event`;
:commit
