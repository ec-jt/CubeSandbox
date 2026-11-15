-- Copyright (c) 2026 Tencent Inc.
-- SPDX-License-Identifier: Apache-2.0
--
-- Template S3 distribution (PostgreSQL counterpart of
-- mysql/20261115120000_template_base.sql).

-- +goose NO TRANSACTION
-- +goose Up

SELECT cubemaster_acquire_migration_lock('cubemaster_migration_20261115120000_template_base', 60);

CREATE TABLE IF NOT EXISTS t_cube_template_base (
  id bigserial NOT NULL,
  created_at timestamp DEFAULT NULL,
  updated_at timestamp DEFAULT NULL,
  template_id varchar(128) NOT NULL DEFAULT '',
  role varchar(16) NOT NULL DEFAULT '',
  base_uuid varchar(64) NOT NULL DEFAULT '',
  artifact_id varchar(128) NOT NULL DEFAULT '',
  builder_node_id varchar(128) NOT NULL DEFAULT '',
  builder_node_ip varchar(64) NOT NULL DEFAULT '',
  status varchar(32) NOT NULL DEFAULT '',
  PRIMARY KEY (id)
);
CREATE UNIQUE INDEX IF NOT EXISTS uniq_template_base_uuid ON t_cube_template_base (base_uuid);
CREATE INDEX IF NOT EXISTS idx_template_base_template ON t_cube_template_base (template_id);
CREATE INDEX IF NOT EXISTS idx_template_base_status ON t_cube_template_base (status);

CREATE TABLE IF NOT EXISTS t_cube_template_base_replica (
  id bigserial NOT NULL,
  created_at timestamp DEFAULT NULL,
  updated_at timestamp DEFAULT NULL,
  base_uuid varchar(64) NOT NULL DEFAULT '',
  node_id varchar(128) NOT NULL DEFAULT '',
  node_ip varchar(64) NOT NULL DEFAULT '',
  status varchar(32) NOT NULL DEFAULT '',
  last_error text,
  PRIMARY KEY (id)
);
CREATE UNIQUE INDEX IF NOT EXISTS uniq_template_base_replica ON t_cube_template_base_replica (base_uuid, node_id);
CREATE INDEX IF NOT EXISTS idx_template_base_replica_node ON t_cube_template_base_replica (node_id);

SELECT pg_advisory_unlock(hashtext('cubemaster_migration_20261115120000_template_base'));

-- +goose Down

SELECT cubemaster_acquire_migration_lock('cubemaster_migration_20261115120000_template_base', 60);

DROP TABLE IF EXISTS t_cube_template_base_replica;
DROP TABLE IF EXISTS t_cube_template_base;

SELECT pg_advisory_unlock(hashtext('cubemaster_migration_20261115120000_template_base'));
