-- Copyright (c) 2026 Tencent Inc.
-- SPDX-License-Identifier: Apache-2.0
--
-- Template S3 distribution (CUBE_TEMPLATE_S3_DISTRIBUTION): published
-- template bases (one row per base uuid; several per template may coexist)
-- and the nodes that hold a verified copy of each.

-- +goose NO TRANSACTION
-- +goose Up

CALL cubemaster_acquire_migration_lock('cubemaster_migration_20261115120000_template_base', 60);

CREATE TABLE IF NOT EXISTS `t_cube_template_base` (
  `id`              bigint unsigned NOT NULL AUTO_INCREMENT,
  `created_at`      datetime(3)     DEFAULT NULL,
  `updated_at`      datetime(3)     DEFAULT NULL,
  `template_id`     varchar(128)    NOT NULL DEFAULT '',
  `role`            varchar(16)     NOT NULL DEFAULT '' COMMENT 'rootfs or memory',
  `base_uuid`       varchar(64)     NOT NULL DEFAULT '' COMMENT 's3lvol export uuid of the base',
  `artifact_id`     varchar(128)    NOT NULL DEFAULT '' COMMENT 'template build the base was published from',
  `builder_node_id` varchar(128)    NOT NULL DEFAULT '',
  `builder_node_ip` varchar(64)     NOT NULL DEFAULT '',
  `status`          varchar(32)     NOT NULL DEFAULT '' COMMENT 'READY or RETIRED',
  PRIMARY KEY (`id`),
  UNIQUE KEY `uniq_template_base_uuid` (`base_uuid`),
  KEY `idx_template_base_template` (`template_id`),
  KEY `idx_template_base_status` (`status`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='Published template bases';

CREATE TABLE IF NOT EXISTS `t_cube_template_base_replica` (
  `id`         bigint unsigned NOT NULL AUTO_INCREMENT,
  `created_at` datetime(3)     DEFAULT NULL,
  `updated_at` datetime(3)     DEFAULT NULL,
  `base_uuid`  varchar(64)     NOT NULL DEFAULT '',
  `node_id`    varchar(128)    NOT NULL DEFAULT '',
  `node_ip`    varchar(64)     NOT NULL DEFAULT '',
  `status`     varchar(32)     NOT NULL DEFAULT '' COMMENT 'READY or FAILED',
  `last_error` text,
  PRIMARY KEY (`id`),
  UNIQUE KEY `uniq_template_base_replica` (`base_uuid`, `node_id`),
  KEY `idx_template_base_replica_node` (`node_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='Nodes holding a verified template base';

SELECT RELEASE_LOCK('cubemaster_migration_20261115120000_template_base');

-- +goose Down

CALL cubemaster_acquire_migration_lock('cubemaster_migration_20261115120000_template_base', 60);

DROP TABLE IF EXISTS `t_cube_template_base_replica`;
DROP TABLE IF EXISTS `t_cube_template_base`;

SELECT RELEASE_LOCK('cubemaster_migration_20261115120000_template_base');
