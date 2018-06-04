CREATE TABLE domains(
  id CHAR(36) UNIQUE,
  retention INT,
  emit_metric TINYINT, /* we are using TINYINT for boolean... should we be? */
/* end domain_config */
  config_version BIGINT,
  db_version BIGINT,
/* domain */
  name CHAR(100) PRIMARY KEY,
  status INT,
  description CHAR(200),
  owner_email CHAR(100),
/* end domain */
  failover_version BIGINT,
  is_global_domain TINYINT,
/* domain_replication_config */
  active_cluster_name CHAR(100),
  clusters TINYINT /* this should be list<string> but we'll ignore this for now. */
/* end domain_replication_config */
)
