-- Copyright (c) 2026 Tencent Inc.
-- SPDX-License-Identifier: Apache-2.0
--
-- LRU paused eviction proof: the exact local-quiesced record the origin
-- Cubelet published at eviction. The cross-node source-quiesced gate validates
-- it strictly, so resume of an evicted binding does not need the origin to be
-- reachable. NULL means no persisted proof (fall back to the origin read).

-- +goose Up

ALTER TABLE t_cube_pause_snapshot ADD COLUMN IF NOT EXISTS eviction_record text;

-- +goose Down

ALTER TABLE t_cube_pause_snapshot DROP COLUMN IF EXISTS eviction_record;
