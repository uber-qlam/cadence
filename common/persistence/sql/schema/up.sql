CREATE TABLE domains(
/* domain */
  id CHAR(36) PRIMARY KEY NOT NULL,
  name VARCHAR(255) UNIQUE NOT NULL,
  status INT NOT NULL,
  description VARCHAR(255) NOT NULL,
  owner_email VARCHAR(255) NOT NULL,
  data BLOB NOT NULL,
/* end domain */
  retention_days INT NOT NULL,
  emit_metric TINYINT(1) NOT NULL,
/* end domain_config */
  config_version BIGINT NOT NULL,
  notification_version BIGINT NOT NULL,
  failover_notification_version BIGINT NOT NULL,
  failover_version BIGINT NOT NULL,
  is_global_domain TINYINT(1) NOT NULL,
/* domain_replication_config */
  active_cluster_name VARCHAR(255) NOT NULL,
  clusters BLOB NOT NULL
/* end domain_replication_config */
) DEFAULT CHARACTER SET utf8 COLLATE utf8_unicode_ci;

CREATE TABLE domain_metadata (
  notification_version BIGINT NOT NULL
);

INSERT INTO domain_metadata (notification_version) VALUES (0);

CREATE TABLE shards (
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

CREATE TABLE transfer_tasks(
	domain_id CHAR(64) NOT NULL,
	workflow_id VARCHAR(255) NOT NULL,
	run_id CHAR(64) NOT NULL,
	task_id BIGINT NOT NULL,
	type TINYINT NOT NULL,
	target_domain_id CHAR(64) NOT NULL,
	target_workflow_id CHAR(64) NOT NULL,
	target_run_id CHAR(64) NOT NULL,
	target_child_workflow_only TINYINT(1) NOT NULL,
	task_list VARCHAR(255) NOT NULL,
	schedule_id BIGINT NOT NULL,
	version BIGINT NOT NULL,
	-- fields specific to the former transfer_task type end here
	shard_id INT NOT NULL,
	PRIMARY KEY (shard_id, task_id)
);

CREATE TABLE executions(
	domain_id CHAR(64) NOT NULL,
	workflow_id VARCHAR(255) NOT NULL,
	run_id CHAR(64) NOT NULL,
	parent_domain_id CHAR(64), -- 1.
	parent_workflow_id VARCHAR(255), -- 2.
	parent_run_id CHAR(64), -- 3.
	initiated_id BIGINT, -- 4. these (parent-related fields) are nullable as their default values are not checked by tests
	completion_event BLOB, -- 5.
	task_list VARCHAR(255) NOT NULL,
	workflow_type_name VARCHAR(255) NOT NULL,
	workflow_timeout_seconds INT UNSIGNED NOT NULL,
	decision_task_timeout_minutes INT UNSIGNED NOT NULL,
	execution_context BLOB, -- nullable because test passes in a null blob.
	state INT NOT NULL,
	close_status INT NOT NULL,
  start_version BIGINT,
  -- other replication_state members omitted due to lack of use
	last_first_event_id BIGINT NOT NULL,
	next_event_id BIGINT NOT NULL, -- very important! for conditional updates of all the dependent tables.
	last_processed_event BIGINT NOT NULL,
	start_time TIMESTAMP NOT NULL,
	last_updated_time TIMESTAMP NOT NULL,
	create_request_id CHAR(64) NOT NULL,
	decision_version BIGINT NOT NULL, -- 1.
	decision_schedule_id BIGINT NOT NULL, -- 2.
	decision_started_id BIGINT NOT NULL, -- 3. cannot be nullable as common.EmptyEventID is checked
	decision_request_id VARCHAR(255), -- not checked
	decision_timeout INT NOT NULL, -- 4.
	decision_attempt BIGINT NOT NULL, -- 5.
	decision_timestamp BIGINT NOT NULL, -- 6.
	cancel_requested TINYINT(1), -- a.
	cancel_request_id VARCHAR(255), -- b. default values not checked
	sticky_task_list VARCHAR(255) NOT NULL, -- 1. defualt value is checked
	sticky_schedule_to_start_timeout INT NOT NULL, -- 2.
	client_library_version VARCHAR(255) NOT NULL, -- 3.
	client_feature_version VARCHAR(255) NOT NULL, -- 4.
	client_impl VARCHAR(255) NOT NULL, -- 5.
--
	shard_id INT NOT NULL,
	PRIMARY KEY (shard_id, domain_id, workflow_id, run_id)
);

CREATE TABLE current_executions(
  shard_id INT NOT NULL,
  domain_id CHAR(64) NOT NULL,
  workflow_id VARCHAR(255) NOT NULL,
  --
  run_id CHAR(64) NOT NULL,
  create_request_id CHAR(64) NOT NULL,
	state INT NOT NULL,
	close_status INT NOT NULL,
  start_version BIGINT,
  PRIMARY KEY (shard_id, domain_id, workflow_id)
);

CREATE TABLE tasks (
  domain_id CHAR(64) NOT NULL,
  workflow_id VARCHAR(255) NOT NULL,
  run_id CHAR(64) NOT NULL,
  schedule_id BIGINT NOT NULL,
  task_list_name VARCHAR(255) NOT NULL,
  task_list_type TINYINT NOT NULL,
  task_id BIGINT NOT NULL,
  expiry_ts TIMESTAMP NOT NULL,
  PRIMARY KEY (domain_id, task_list_name, task_list_type, task_id)
);

CREATE TABLE task_lists (
	domain_id CHAR(64) NOT NULL,
	range_id BIGINT NOT NULL,
	name VARCHAR(255) NOT NULL,
	type TINYINT NOT NULL, -- {Activity, Decision}
	ack_level BIGINT NOT NULL DEFAULT 0,
	kind TINYINT NOT NULL, -- {Normal, Sticky}
	expiry_ts TIMESTAMP NOT NULL,
	PRIMARY KEY (domain_id, name, type)
);

CREATE TABLE replication_tasks (
	domain_id CHAR(64) NOT NULL,
	workflow_id VARCHAR(255) NOT NULL,
	run_id CHAR(64) NOT NULL,
	task_id BIGINT NOT NULL,
	type TINYINT NOT NULL,
	first_event_id BIGINT NOT NULL,
	next_event_id BIGINT NOT NULL,
	version BIGINT NOT NULL,
  last_replication_info BLOB NOT NULL,
--
shard_id INT NOT NULL,
PRIMARY KEY (shard_id, task_id)
);

CREATE TABLE timer_tasks (
	domain_id CHAR(64) NOT NULL,
	workflow_id VARCHAR(255) NOT NULL,
	run_id CHAR(64) NOT NULL,
	visibility_ts TIMESTAMP(3) NOT NULL,
	task_id BIGINT NOT NULL,
	type TINYINT NOT NULL,
	timeout_type TINYINT NOT NULL,
	event_id BIGINT NOT NULL,
	schedule_attempt BIGINT NOT NULL,
	version BIGINT NOT NULL,
	--
	shard_id INT NOT NULL,
	PRIMARY KEY (shard_id, visibility_ts, task_id)
);

CREATE TABLE activity_info_maps (
-- each row corresponds to one key of one map<string, ActivityInfo>
	shard_id INT NOT NULL,
	domain_id CHAR(64) NOT NULL,
	workflow_id VARCHAR(255) NOT NULL,
  run_id CHAR(64) NOT NULL,
	schedule_id BIGINT NOT NULL, -- the key.
-- fields of activity_info type follow
version                   BIGINT NOT NULL,
scheduled_event           BLOB NOT NULL,
scheduled_time            TIMESTAMP NOT NULL,
started_id                BIGINT NOT NULL,
started_event             BLOB NOT NULL,
started_time              TIMESTAMP NOT NULL,
activity_id               VARCHAR(255) NOT NULL,
request_id                VARCHAR(255) NOT NULL,
details                   BLOB,
schedule_to_start_timeout INT NOT NULL,
schedule_to_close_timeout INT NOT NULL,
start_to_close_timeout    INT NOT NULL,
heartbeat_timeout        INT NOT NULL,
cancel_requested          TINYINT(1),
cancel_request_id         BIGINT NOT NULL,
last_heartbeat_updated_time      TIMESTAMP NOT NULL,
timer_task_status         INT NOT NULL,
attempt                   INT NOT NULL,
task_list                 VARCHAR(255) NOT NULL,
started_identity          VARCHAR(255) NOT NULL,
has_retry_policy          BOOLEAN NOT NULL,
init_interval             INT NOT NULL,
backoff_coefficient       DOUBLE NOT NULL,
max_interval              INT NOT NULL,
expiration_time           TIMESTAMP NOT NULL,
max_attempts              INT NOT NULL,
non_retriable_errors      BLOB, -- this was a list<text>. The use pattern is to replace, no modifications.
	PRIMARY KEY (shard_id, domain_id, workflow_id, run_id, schedule_id)
);

CREATE TABLE timer_info_maps (
shard_id INT NOT NULL,
domain_id CHAR(64) NOT NULL,
workflow_id VARCHAR(255) NOT NULL,
run_id CHAR(64) NOT NULL,
timer_id VARCHAR(255) NOT NULL, -- what string type should this be?
--
  version BIGINT NOT NULL,
  started_id BIGINT NOT NULL,
  expiry_time TIMESTAMP NOT NULL,
  task_id BIGINT NOT NULL,
  PRIMARY KEY (shard_id, domain_id, workflow_id, run_id, timer_id)
);

CREATE TABLE child_execution_info_maps (
  shard_id INT NOT NULL,
domain_id CHAR(64) NOT NULL,
workflow_id VARCHAR(255) NOT NULL,
run_id CHAR(64) NOT NULL,
initiated_id BIGINT NOT NULL,
--
version BIGINT NOT NULL,
initiated_event BLOB,
started_id BIGINT NOT NULL,
started_event BLOB,
create_request_id CHAR(64),
PRIMARY KEY (shard_id, domain_id, workflow_id, run_id, initiated_id)
);

CREATE TABLE request_cancel_info_maps (
 shard_id INT NOT NULL,
domain_id CHAR(64) NOT NULL,
workflow_id VARCHAR(255) NOT NULL,
run_id CHAR(64) NOT NULL,
initiated_id BIGINT NOT NULL,
--
version BIGINT NOT NULL,
cancel_request_id CHAR(64) NOT NULL, -- a uuid
PRIMARY KEY (shard_id, domain_id, workflow_id, run_id, initiated_id)
);


CREATE TABLE signal_info_maps (
 shard_id INT NOT NULL,
domain_id CHAR(64) NOT NULL,
workflow_id VARCHAR(255) NOT NULL,
run_id CHAR(64) NOT NULL,
initiated_id BIGINT NOT NULL,
--
version BIGINT NOT NULL,
signal_request_id CHAR(64) NOT NULL, -- uuid
signal_name VARCHAR(255) NOT NULL,
input BLOB,
control BLOB,
PRIMARY KEY (shard_id, domain_id, workflow_id, run_id, initiated_id)
);
