CREATE TABLE IF NOT EXISTS friends (
	friend bigint,
	friend2 bigint
) ;

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
);

CREATE TABLE IF NOT EXISTS products (
	product_id integer NOT NULL,
	category_id bigint NOT NULL,
	brand varchar,
	category_code varchar,
	PRIMARY KEY (product_id, category_id)
);

CREATE TABLE IF NOT EXISTS events (
	event_time timestamp,
	event_type varchar(10),
	product_id integer,
	category_id bigint,
	price real,
	user_id bigint,
	user_session varchar(36),
	CONSTRAINT "New Relationship" FOREIGN KEY (product_id, category_id) REFERENCES products (product_id, category_id)
);

CREATE TABLE IF NOT EXISTS client_first_purchase_date (
	client_id bigint PRIMARY KEY,
	first_purchase_date date,
	user_id bigint,
	user_device_id smallint
) ;
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
);


\COPY campaigns FROM '.\data\cleaned_data_psql\Clean_Campaings.csv' DELIMITER ',' CSV HEADER ENCODING 'UTF8' null as '';
\COPY messages FROM '.\data\cleaned_data_psql\Clean_Messages.csv' DELIMITER ',' CSV HEADER ENCODING 'UTF8';
\COPY client_first_purchase_date FROM '.\data\cleaned_data_psql\Clean_client_first_purchase_date.csv' DELIMITER ',' CSV HEADER ENCODING 'UTF8';
\COPY products FROM '.\data\cleaned_data_psql\Clean_Products.csv' DELIMITER ',' CSV HEADER ENCODING 'UTF8';
\COPY events FROM '.\data\cleaned_data_psql\Clean_Events.csv' DELIMITER ',' CSV HEADER ENCODING 'UTF8';
\COPY friends FROM '.\data\cleaned_data_psql\Clean_Friends.csv' DELIMITER ',' CSV HEADER ENCODING 'UTF8';

COMMIT