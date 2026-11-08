-- Copyright (c) 2026 Tencent Inc.
-- SPDX-License-Identifier: Apache-2.0
--
-- LRU paused eviction: record that a paused sandbox's node-local pause state
-- was dropped (its S3 checkpoint stays) and the committed checkpoint digest
-- the cross-node source-quiesced gate binds to. Both default to "not evicted".

-- +goose Up

ALTER TABLE t_cube_pause_snapshot ADD COLUMN IF NOT EXISTS evicted_at timestamp DEFAULT NULL;
ALTER TABLE t_cube_pause_snapshot ADD COLUMN IF NOT EXISTS checkpoint_digest varchar(64) NOT NULL DEFAULT '';

-- +goose Down

ALTER TABLE t_cube_pause_snapshot DROP COLUMN IF EXISTS checkpoint_digest;
ALTER TABLE t_cube_pause_snapshot DROP COLUMN IF EXISTS evicted_at;
