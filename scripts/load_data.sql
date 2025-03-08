CREATE TABLE IF NOT EXISTS friends (
	friend bigint,
	friend2 bigint
) TABLESPACE pg_default;

CREATE TABLE IF NOT EXISTS campaigns (
	id integer NOT NULL,
	campaign_type varchar(15) NOT NULL,
	channel varchar(15),
	topic varchar(30),
	started_at timestamp WITHOUT TIME ZONE,
	finished_at timestamp WITHOUT TIME ZONE,
	total_count integer,
	ab_test boolean,
	warmup_mode boolean,
	hour_limit integer,
	subject_length integer,
	subject_with_personalization boolean,
	subject_with_deadline boolean,
	subject_with_emoji boolean,
	subject_with_bonuses boolean,
	subject_with_discount boolean,
	subject_with_saleout boolean,
	is_test boolean,
	position integer,
	PRIMARY KEY (id, campaign_type)
) TABLESPACE pg_default;

CREATE TABLE IF NOT EXISTS events (
	event_time timestamp,
	event_type varchar(10),
	product_id bigint,
	category_id bigint,
	category_code varchar(40),
	brand varchar(30),
	price real,
	user_id bigint,
	user_session varchar(36)
) TABLESPACE pg_default;

CREATE TABLE IF NOT EXISTS client_first_purchase_date (
	client_id bigint PRIMARY KEY,
	first_purchase_date date,
	user_id bigint,
	user_device_id smallint
) TABLESPACE pg_default;

CREATE TABLE IF NOT EXISTS messages (
	id integer PRIMARY KEY,
	message_id varchar(36),
	campaign_id smallint,
	message_type varchar(13),
	client_id bigint,
	channel varchar(20),
	category varchar(10),
	platform varchar(10),
	email_provider varchar(16),
	stream varchar(7),
	date timestamp,
	sent_at timestamp,
	is_opened boolean,
	opened_first_time_at timestamp,
	opened_last_time_at timestamp,
	is_clicked boolean,
	clicked_first_time_at timestamp,
	clicked_last_time_at timestamp,
	is_unsubscribed boolean,
	unsubscribed_at varchar(19),
	is_hard_bounced boolean,
	hard_bounced_at varchar(19),
	is_soft_bounced boolean,
	soft_bounced_at varchar(19),
	is_complained boolean,
	complained_at varchar(19),
	is_blocked boolean,
	blocked_at varchar(19),
	is_purchased boolean,
	purchased_at varchar(19),
	created_at timestamp,
	updated_at timestamp,
	user_device_id smallint,
	user_id bigint
) TABLESPACE pg_default;


COPY public.campaigns FROM 'D:\datasets\assigment\cleaned\Clean_Campaings.csv' DELIMITER ',' CSV HEADER ENCODING 'UTF8' null as '';
COPY public.messages FROM 'D:\datasets\assigment\cleaned\Clean_Messages.csv' DELIMITER ',' CSV HEADER ENCODING 'UTF8';
COPY public.client_first_purchase_date FROM 'D:\datasets\assigment\cleaned\Clean_client_first_purchase_date.csv' DELIMITER ',' CSV HEADER ENCODING 'UTF8';
COPY public.events FROM 'D:\datasets\assigment\cleaned\Clean_Events.csv' DELIMITER ',' CSV HEADER ENCODING 'UTF8';
COPY public.friends FROM 'D:\datasets\assigment\cleaned\Clean_Friends.csv' DELIMITER ',' CSV HEADER ENCODING 'UTF8';

COMMIT