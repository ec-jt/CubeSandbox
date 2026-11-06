/* Copyright (c) 2026 Tencent Inc.
 * SPDX-License-Identifier: Apache-2.0 */
/* Reuse the real AIO framework and payload checker, not a mock WAL. */
#define main wal_existing_suite_main
#include "s3_wal_test.c"
#undef main
#include "spdk/crc32.h"

static void io_done(struct spdk_bdev_io *io, bool success, void *arg)
{
	spdk_bdev_free_io(io);
	async_int_cb(arg, success ? 0 : -EIO);
}

static struct s3_local_dev *format_geometry(uint64_t bytes)
{
	struct open_dev_ctx c = {0};
	struct s3_local_dev_format_opts o = {
		.wal_bdev_name = AIO_BDEV_NAME, .lvs_name = "wal_geometry",
		.capacity_bytes = 256ULL << 20, .chunk_size = TEST_CHUNK_SIZE,
		.journal_size = TEST_JOURNAL_SIZE, .wal_size = bytes,
	};
	s3_local_dev_format(&o, local_dev_open_cb, &c);
	if (!poll_until(&c.done) || c.status) { g_fail++; return NULL; }
	return c.dev;
}

/* Construct an old-format one-segment log with one acknowledged payload.
 * This exercises open/replay without requiring the new writer to create it. */
static void legacy_test(struct s3_local_dev *dev)
{
	const struct s3_region *r = s3_local_dev_get_region(dev, S3_REGION_WAL);
	uint8_t *buf = spdk_dma_zmalloc(4 * S3_WAL_BLOCK_SIZE, S3_WAL_BLOCK_SIZE, NULL);
	struct s3_wal_super *sb = (void *)buf;
	struct s3_wal_entry_hdr *h = (void *)(buf + S3_WAL_SUPER_SIZE);
	struct async_ctx c = {0};
	struct open_wal_ctx w = {0};
	struct replay_state replay = {0};
	assert(buf);
	sb->magic = S3_WAL_SUPER_MAGIC; sb->version = S3_WAL_SUPER_VERSION;
	sb->seg_size = S3_WAL_DEFAULT_SEG_SIZE; sb->seg_count = 1;
	sb->epoch = 1; sb->seq_next = 2; sb->head = 8192; sb->slot_gen = 1;
	sb->crc = spdk_crc32c_update(sb, sizeof(*sb), ~0u);
	h->magic = S3_WAL_ENTRY_MAGIC; h->type = S3_WAL_WRITE;
	h->flags = S3_WAL_F_LAST_IN_BATCH; h->hdr_len = sizeof(*h);
	h->seq = (1ULL << 48) | 1; h->batch_id = 1;
	h->lba = 7; h->nblocks = 1; h->payload_len = 4096;
	fill_payload(h + 1, h->lba, h->payload_len);
	h->crc_payload = spdk_crc32c_update(h + 1, h->payload_len, ~0u);
	h->crc_hdr = spdk_crc32c_update(h, sizeof(*h), ~0u);
	int rc = spdk_bdev_write(s3_local_dev_get_desc(dev, S3_REGION_WAL),
		s3_local_dev_get_channel(dev, S3_REGION_WAL), buf, r->offset,
		4 * S3_WAL_BLOCK_SIZE, io_done, &c);
	assert(rc == 0 && poll_until(&c.done) && c.status == 0);
	s3_wal_open(dev, wal_open_cb, &w);
	assert(poll_until(&w.done)); check("legacy open", w.status, 0);
	if (w.wal) {
		check("legacy replay", sync_replay(w.wal, &replay), 0);
		check_true("legacy payload recovered", replay.writes == 1 &&
			replay.payload_ok == 1 && replay.payload_bad == 0, NULL);
		check("legacy append fails without retry loop",
			sync_append_write(w.wal, 7, h + 1), -ENOSPC);
		check("legacy close", sync_close(w.wal), 0);
	}
	spdk_dma_free(buf);
}

int main(int argc, char **argv)
{
	struct spdk_env_opts env;
	const char *path = argc > 1 ? argv[1] : "/tmp/wal-geometry.aio";
	struct s3_local_dev *dev;
	struct open_wal_ctx w = {0};
	struct async_ctx c = {0};
	spdk_env_opts_init(&env); env.opts_size = sizeof(env);
	env.name = "wal_geometry"; env.no_huge = true; env.no_pci = true; env.mem_size = 512;
	assert(spdk_env_init(&env) == 0);
	assert(spdk_thread_lib_init(NULL, 0) == 0);
	g_thread = spdk_thread_create("wal_geometry", NULL); assert(g_thread);
	spdk_set_thread(g_thread); assert(framework_start() == 0);
	/* Refuse an existing path, unlike the older suite's unlink-first helper. */
	assert(access(path, F_OK) != 0 && errno == ENOENT);
	assert(make_aio_file(path, 256ULL << 20) == 0);
	assert(create_aio_bdev(AIO_BDEV_NAME, path, 4096, false, false, NULL, false) == 0);
	dev = format_geometry(64ULL << 20); assert(dev);
	s3_wal_create(dev, NULL, wal_open_cb, &w); assert(poll_until(&w.done));
	check("64 MiB/default one-segment create rejected", w.status, -ENOSPC);
	assert(!w.wal);
	legacy_test(dev); s3_local_dev_close(dev);
	/* Exact minimum: two usable default segments, two slack, super. */
	dev = format_geometry(4ULL * S3_WAL_DEFAULT_SEG_SIZE + S3_WAL_SUPER_SIZE); assert(dev);
	memset(&w, 0, sizeof(w)); s3_wal_create(dev, NULL, wal_open_cb, &w);
	assert(poll_until(&w.done)); check("two-segment minimum accepted", w.status, 0); assert(w.wal);
	uint32_t len = 512 * 1024;
	void *payload = spdk_dma_zmalloc(len, 4096, NULL); assert(payload);
	unsigned pressure = 0;
	for (unsigned i = 0; i < 160; i++) {
		fill_payload(payload, i, len); memset(&c, 0, sizeof(c));
		s3_wal_append_write(w.wal, i, len / 4096, payload, 0, NULL, async_int_cb, &c);
		assert(poll_until(&c.done));
		if (c.status) { check("progress append", c.status, 0); break; }
		if (s3_wal_is_backpressured(w.wal)) {
			pressure++;
			s3_wal_truncate_to_seq(w.wal, s3_wal_get_next_seq(w.wal) - 1);
			assert(!s3_wal_is_backpressured(w.wal));
			assert(sync_super(w.wal, s3_wal_get_next_seq(w.wal) - 1) == 0);
		}
	}
	check_true("multiple backpressure/reclaim cycles", pressure >= 3, NULL);
	struct s3_wal_stats stats; s3_wal_get_stats(w.wal, &stats);
	printf("progress: appends=%" PRIu64 " released=%" PRIu64 " pressure=%u\n",
		stats.appends, stats.segments_released, pressure);
	check_true("all 160 appends completed", stats.appends == 160, NULL);
	/* Mark consumed prefix, then require exactly one new payload on replay. */
	assert(sync_super(w.wal, s3_wal_get_next_seq(w.wal) - 1) == 0);
	fill_payload(payload, 999, 4096); assert(sync_append_write(w.wal, 999, payload) == 0);
	assert(sync_close(w.wal) == 0); memset(&w, 0, sizeof(w));
	s3_wal_open(dev, wal_open_cb, &w); assert(poll_until(&w.done) && w.wal);
	struct replay_state replay = {0}; check("multi-lap replay", sync_replay(w.wal, &replay), 0);
	check_true("only uncheckpointed payload replayed", replay.writes == 1 &&
		replay.payload_ok == 1 && replay.payload_bad == 0, NULL);
	assert(sync_close(w.wal) == 0); spdk_dma_free(payload); s3_local_dev_close(dev);
	memset(&c, 0, sizeof(c)); bdev_aio_delete(AIO_BDEV_NAME, async_int_cb, &c);
	assert(poll_until(&c.done)); framework_stop();
	spdk_thread_exit(g_thread); while (!spdk_thread_is_exited(g_thread)) spdk_thread_poll(g_thread, 0, 0);
	spdk_thread_destroy(g_thread); spdk_set_thread(NULL); spdk_thread_lib_fini(); spdk_env_fini();
	printf("geometry result: %d failures\n", g_fail); return g_fail != 0;
}
