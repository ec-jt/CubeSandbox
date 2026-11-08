-- Copyright (c) 2026 Tencent Inc.
-- SPDX-License-Identifier: Apache-2.0
--
-- LRU paused eviction proof: the exact local-quiesced record the origin
-- Cubelet published at eviction. The cross-node source-quiesced gate validates
-- it strictly, so resume of an evicted binding does not need the origin to be
-- reachable. NULL means no persisted proof (fall back to the origin read).

-- +goose NO TRANSACTION
-- +goose Up

CALL cubemaster_acquire_migration_lock('cubemaster_migration_20261108130000_pause_eviction_record', 60);

CALL cubemaster_add_column_if_missing(
  't_cube_pause_snapshot',
  'eviction_record',
  "mediumtext COMMENT 'origin local-quiesced record bytes published at LRU eviction'"
);

SELECT RELEASE_LOCK('cubemaster_migration_20261108130000_pause_eviction_record');

-- +goose Down
CALL cubemaster_acquire_migration_lock('cubemaster_migration_20261108130000_pause_eviction_record', 60);

CALL cubemaster_drop_column_if_exists('t_cube_pause_snapshot', 'eviction_record');

SELECT RELEASE_LOCK('cubemaster_migration_20261108130000_pause_eviction_record');
