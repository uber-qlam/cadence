CREATE TABLE IF NOT EXISTS transfer_tasks(
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

CREATE TABLE IF NOT EXISTS executions(
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
  start_version BIGINT NOT NULL,
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

CREATE TABLE IF NOT EXISTS current_executions(
  shard_id INT NOT NULL,
  domain_id CHAR(64) NOT NULL,
  workflow_id VARCHAR(255) NOT NULL,
  current_run_id CHAR(64) NOT NULL,
  PRIMARY KEY (shard_id, domain_id, workflow_id)
);