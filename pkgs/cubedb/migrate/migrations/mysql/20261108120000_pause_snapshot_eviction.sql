-- Copyright (c) 2026 Tencent Inc.
-- SPDX-License-Identifier: Apache-2.0
--
-- LRU paused eviction: record that a paused sandbox's node-local pause state
-- was dropped (its S3 checkpoint stays) and the committed checkpoint digest
-- the cross-node source-quiesced gate binds to. Both default to "not evicted".

-- +goose NO TRANSACTION
-- +goose Up

CALL cubemaster_acquire_migration_lock('cubemaster_migration_20261108120000_pause_eviction', 60);

CALL cubemaster_add_column_if_missing(
  't_cube_pause_snapshot',
  'evicted_at',
  "datetime(3) DEFAULT NULL COMMENT 'LRU eviction time; NULL means local pause state is present'"
);
CALL cubemaster_add_column_if_missing(
  't_cube_pause_snapshot',
  'checkpoint_digest',
  "varchar(64) NOT NULL DEFAULT '' COMMENT 'sha256 of the committed native checkpoint record at eviction'"
);

SELECT RELEASE_LOCK('cubemaster_migration_20261108120000_pause_eviction');

-- +goose Down
CALL cubemaster_acquire_migration_lock('cubemaster_migration_20261108120000_pause_eviction', 60);

CALL cubemaster_drop_column_if_exists('t_cube_pause_snapshot', 'checkpoint_digest');
CALL cubemaster_drop_column_if_exists('t_cube_pause_snapshot', 'evicted_at');

SELECT RELEASE_LOCK('cubemaster_migration_20261108120000_pause_eviction');
