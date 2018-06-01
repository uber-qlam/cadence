CREATE TABLE domains(
  id CHAR(16) UNIQUE, /* a uuid */
/* domain_config */
  retention INT,
  emit_metric BIT,
/* end domain_config */
  config_version BIGINT,
/* domain */
  name CHAR(100) UNIQUE,
  status INT,
  description CHAR(200),
  owner_email CHAR(100),
/* end domain */
  failover_version BIGINT,
  is_global_domain BIT,
/* domain_replication_config */
  active_cluster_name CHAR(100),
  clusters BIT /* this should be list<string> but we'll ignore this for now. */
/* end domain_replication_config */
)