CREATE TABLE IF NOT EXISTS shards (
	shard_id INT NOT NULL,
	owner VARCHAR(255) NOT NULL,
	range_id BIGINT NOT NULL,
	stolen_since_renew INT NOT NULL,
	updated_at TIMESTAMP(3) NOT NULL,
	replication_ack_level BIGINT NOT NULL,
	transfer_ack_level BIGINT NOT NULL,
	timer_ack_level TIMESTAMP(3) NOT NULL,
	cluster_transfer_ack_level BLOB NOT NULL,
	cluster_timer_ack_level BLOB NOT NULL,
	domain_notification_version BIGINT NOT NULL,
	PRIMARY KEY (shard_id)
);
