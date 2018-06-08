CREATE TABLE IF NOT EXISTS domains(
  id VARCHAR(64) UNIQUE NOT NULL,
  retention_days INT NOT NULL,
  emit_metric TINYINT(1) NOT NULL,
/* end domain_config */
  config_version BIGINT NOT NULL,
  db_version BIGINT,
/* domain */
  name VARCHAR(255) PRIMARY KEY NOT NULL,
  status INT,
  description VARCHAR(255),
  owner_email VARCHAR(255),
/* end domain */
  failover_version BIGINT NOT NULL,
  is_global_domain TINYINT(1) NOT NULL,
/* domain_replication_config */
  active_cluster_name VARCHAR(255) NOT NULL,
  clusters BLOB NOT NULL
/* end domain_replication_config */
) DEFAULT CHARACTER SET utf8 COLLATE utf8_unicode_ci;

