/* SPDX-License-Identifier: Apache-2.0 */
/* Production entrypoints with deterministic asynchronous dependencies. No SPDK
 * environment, PCI, block device or network initialization. Not a real-S3 test.
 * Unused production paths are removed by --gc-sections, not replaced by copies.
 */
#include "../../module/bdev/s3lvol/vbdev_s3lvol_lvstore.c"
#include "../../module/bdev/s3lvol/vbdev_s3lvol_lvol.c"

static uint64_t ticks;
static struct s3_target target = {.endpoint="local-test", .bucket="test"};
static bool lower_busy, export_busy, registry_busy, pending_busy, decouple_busy;
static spdk_blob_op_complete write_cb;
static void *write_arg;
static s3_bs_dev_cb drain_cb, checkpoint_cb;
static void *drain_arg, *checkpoint_arg;
static unsigned writes, reads, completions, holds, drains, checkpoints;
static int last_io_status;
static struct s3_producer_gate *callback_gate;
#ifdef SCOPED_PATH_TEST
static _Thread_local bool race_unrelated;
static _Atomic unsigned unrelated_accepted, unrelated_rejected;
static void (*upper_hook)(struct spdk_bdev_io *);
static bool import_busy, publication_pending, export_present;
static struct spdk_thread *current_thread = (void *)1;
#endif
static unsigned callback_count;
static int callback_status;
static _Atomic bool race_mode;
static _Atomic unsigned race_accepted, race_rejected;

struct spdk_thread *spdk_get_thread(void) {
#ifdef SCOPED_PATH_TEST
return current_thread;
#else
return (void *)1;
#endif
}
uint64_t spdk_get_ticks(void) { return ticks; }
uint64_t spdk_get_ticks_hz(void) { return 1000000; }
struct spdk_poller *spdk_poller_register_named(spdk_poller_fn fn, void *arg,
	uint64_t us, const char *name) { return (void *)2; }
void spdk_poller_unregister(struct spdk_poller **p) { *p = NULL; }
#ifndef SCOPED_PATH_TEST
int spdk_uuid_compare(const struct spdk_uuid *a, const struct spdk_uuid *b)
{ return memcmp(a,b,sizeof(*a)); }
bool spdk_uuid_is_null(const struct spdk_uuid *u)
{ struct spdk_uuid z = {}; return !memcmp(u,&z,sizeof(z)); }
void spdk_uuid_copy(struct spdk_uuid *a, const struct spdk_uuid *b) { *a = *b; }
#endif
const struct s3_target *rcow_namespace_to_target(const char *n) { return &target; }
const char *s3_client_endpoint(const struct s3_client *c) { return target.endpoint; }
const char *s3_client_bucket(const struct s3_client *c) { return target.bucket; }
int s3lvol_pending_foreach(s3lvol_pending_cb cb, void *arg) { return 0; }
bool s3lvol_export_work_busy(struct s3lvol_lvstore *lvs) { return export_busy; }
bool s3lvol_export_registry_busy(struct s3lvol_lvstore *lvs) { return registry_busy; }
bool s3lvol_pending_persist_busy(const struct spdk_uuid *u) { return pending_busy; }
bool s3lvol_lvol_decouple_pending(const struct spdk_lvol *v) { return decouple_busy; }
bool s3lvol_snapshot_pending_test(const struct spdk_uuid *a, const struct spdk_uuid *b)
{ return pending_busy; }
struct s3lvol_import *s3lvol_import_first(struct s3lvol_lvstore *lvs) {
#ifdef SCOPED_PATH_TEST
return import_busy ? (void *)9 : NULL;
#else
return NULL;
#endif
}
struct s3lvol_export *s3lvol_export_first(struct s3lvol_lvstore *lvs) {
#ifdef SCOPED_PATH_TEST
return export_present ? (void *)10 : NULL;
#else
return NULL;
#endif
}
struct s3lvol_export *s3lvol_export_next(struct s3lvol_export *p) { return NULL; }
void s3lvol_export_get(const struct s3lvol_export *e, struct s3lvol_export_entry *o)
{ memset(o,0,sizeof(*o));
#ifdef SCOPED_PATH_TEST
o->publication_pending=publication_pending;
#endif
}
void s3_bs_dev_scoped_source_hold(struct spdk_bs_dev *dev) { holds++; }
void s3_bs_dev_source_hold(struct spdk_bs_dev *dev) { holds++; }
bool s3_bs_dev_source_busy(struct spdk_bs_dev *dev) { return lower_busy; }
void s3_bs_dev_drain(struct spdk_bs_dev *dev, s3_bs_dev_cb cb, void *arg)
{
#ifdef SCOPED_PATH_TEST
assert(spdk_get_thread()==(void *)1);
if(drain_cb) { cb(arg,-EBUSY); return; }
#endif
drains++; drain_cb=cb; drain_arg=arg; }
void s3_bs_dev_checkpoint(struct spdk_bs_dev *dev, s3_bs_dev_cb cb, void *arg)
{
#ifdef SCOPED_PATH_TEST
assert(spdk_get_thread()==(void *)1);
if(checkpoint_cb) { cb(arg,-EBUSY); return; }
#endif
checkpoints++; checkpoint_cb=cb; checkpoint_arg=arg; }
void spdk_log_register_flag(const char *name, struct spdk_log_flag *flag) {}
void spdk_log(enum spdk_log_level level, const char *file, int line,
	const char *func, const char *fmt, ...) {}

void spdk_bdev_io_complete(struct spdk_bdev_io *io, enum spdk_bdev_io_status status)
{
	if (atomic_load(&race_mode)) {
		if (status == SPDK_BDEV_IO_STATUS_FAILED) atomic_fetch_add(&race_rejected,1);
#ifdef SCOPED_PATH_TEST
		if(race_unrelated) {
			if(status==SPDK_BDEV_IO_STATUS_SUCCESS)atomic_fetch_add(&unrelated_accepted,1);
			else atomic_fetch_add(&unrelated_rejected,1);
		}
#endif
		return;
	}
	completions++; last_io_status=status;
	if (callback_gate) assert(s3_producer_count(callback_gate) > 0);
#ifdef SCOPED_PATH_TEST
if(upper_hook) upper_hook(io);
#endif
}
void spdk_bdev_io_get_buf(struct spdk_bdev_io *io, spdk_bdev_io_get_buf_cb cb,
	uint64_t len) { cb(NULL,io,true); }
void spdk_blob_io_writev_ext(struct spdk_blob *b, struct spdk_io_channel *ch,
	struct iovec *iov, int n, uint64_t off, uint64_t len,
	spdk_blob_op_complete cb, void *arg, struct spdk_blob_ext_io_opts *opts)
{
	if (atomic_load(&race_mode)) { atomic_fetch_add(&race_accepted,1); cb(arg,0); return; }
	writes++; write_cb=cb; write_arg=arg;
}
void spdk_blob_io_readv_ext(struct spdk_blob *b, struct spdk_io_channel *ch,
	struct iovec *iov, int n, uint64_t off, uint64_t len,
	spdk_blob_op_complete cb, void *arg, struct spdk_blob_ext_io_opts *opts)
{ reads++; cb(arg,0); }
void spdk_blob_io_unmap(struct spdk_blob *b, struct spdk_io_channel *ch,
	uint64_t off, uint64_t len, spdk_blob_op_complete cb, void *arg)
{ writes++; write_cb=cb; write_arg=arg; }
void spdk_blob_io_write_zeroes(struct spdk_blob *b, struct spdk_io_channel *ch,
	uint64_t off, uint64_t len, spdk_blob_op_complete cb, void *arg)
{ writes++; write_cb=cb; write_arg=arg; }

struct spdk_log_flag SPDK_LOG_vbdev_s3lvol;
void spdk_lvol_close(struct spdk_lvol *v, spdk_lvol_op_complete cb, void *a) { abort(); }
void spdk_bdev_destruct_done(struct spdk_bdev *b, int status) { abort(); }
struct spdk_io_channel *spdk_lvol_get_io_channel(struct spdk_lvol *v) { abort(); }
bool spdk_blob_is_read_only(struct spdk_blob *b) { return false; }
bool spdk_blob_is_snapshot(struct spdk_blob *b) { return false; }
bool spdk_blob_is_clone(struct spdk_blob *b) { return false; }
bool spdk_blob_is_thin_provisioned(struct spdk_blob *b) { return false; }
#ifndef SCOPED_PATH_TEST
int spdk_json_write_object_end(struct spdk_json_write_ctx *w) { abort(); }
int spdk_json_write_named_object_begin(struct spdk_json_write_ctx *w,const char *n) { abort(); }
int spdk_json_write_named_bool(struct spdk_json_write_ctx *w,const char *n,bool b) { abort(); }
int spdk_json_write_named_string(struct spdk_json_write_ctx *w,const char *n,const char *v) { abort(); }
int spdk_json_write_named_uuid(struct spdk_json_write_ctx *w,const char *n,const struct spdk_uuid *v) { abort(); }

#endif
static void result(void *arg, int status)
{ callback_count++; callback_status=status; if (arg) free(arg); }
static void uuid(struct spdk_uuid *u, unsigned char n) { memset(u,n,sizeof(*u)); }

static void run_case(int type, bool timeout, bool error)
{
	struct spdk_lvol_store store = {};
	struct spdk_lvol v = {};
	struct s3lvol_lvstore lvs = {.lvs=&store,.wal=(void *)3,.local_dev=(void *)4,
		.bs_dev=(void *)5,.name="source",.ns_name="ns",.owner_held=true,
		.owner_mode=S3_OWNER_MODE_STRICT,.dependency_view_complete=true,
		.exports_ready=true,.source_pending_loaded=true,.source_thread=(void *)1};
	struct s3_producer_gate volume_gate = {};
	struct s3lvol_gated_bdev disk = {.gate=&lvs.producers,.source_gate=&volume_gate};
	struct spdk_bdev_io *io=calloc(1,sizeof(*io)+sizeof(struct spdk_blob_ext_io_opts));
	struct spdk_bdev_io *later=calloc(1,sizeof(*later)+sizeof(struct spdk_blob_ext_io_opts));
	struct s3lvol_source_binding b = {.endpoint="local-test",.bucket="test",
		.prefix="source",.volumes=&v.uuid,.volume_count=1,.exclusive_store=true,
		.source_generation=7};
	unsigned old_drains=drains;
	atomic_init(&lvs.producers.state,0); atomic_init(&lvs.producers.error,0);
	uuid(&lvs.uuid,1);uuid(&lvs.owner_claim.info.nonce,2);lvs.owner_claim.valid=true;
	uuid(&v.uuid,3); uuid(&b.source,4);uuid(&b.operation,5);
	b.store_uuid=lvs.uuid;b.owner_nonce=lvs.owner_claim.info.nonce;
	v.lvol_store=&store;v.blob=(void *)6;v.bdev=&disk.bdev;disk.bdev.ctxt=&v;disk.bdev.blocklen=4096;
	TAILQ_INIT(&store.lvols);TAILQ_INSERT_TAIL(&store.lvols,&v,link);
	io->bdev=&disk.bdev;io->type=type;later->bdev=&disk.bdev;later->type=type;
	callback_count=0; ticks=0; lower_busy=false;
	unsetenv("S3LVOL_INTERNAL_SOURCE_DRAIN");
	assert(s3lvol_source_drain(&lvs,&b,100,result,NULL)==-ENOTSUP);
	setenv("S3LVOL_INTERNAL_SOURCE_DRAIN","1",1);
	lvs.source_pending_loaded=false;
	assert(s3lvol_source_drain(&lvs,&b,100,result,NULL)==-EAGAIN);
	lvs.source_pending_loaded=true;
	b.owner_nonce = b.source;
	assert(s3lvol_source_drain(&lvs,&b,100,result,NULL)==-ESTALE);
	b.owner_nonce=lvs.owner_claim.info.nonce;
	b.bucket="other";assert(s3lvol_source_drain(&lvs,&b,100,result,NULL)==-ESTALE);b.bucket="test";
	b.volumes=&b.source;assert(s3lvol_source_drain(&lvs,&b,100,result,NULL)==-ESTALE);b.volumes=&v.uuid;
	((unsigned char *)&b.store_uuid)[0] ^= 1;
	assert(s3lvol_source_drain(&lvs,&b,100,result,NULL)==-ESTALE);
	b.store_uuid=lvs.uuid;
	assert(!s3lvol_control_enter(&lvs));
	assert(s3lvol_source_drain(&lvs,&b,100,result,NULL)==-EBUSY);
	s3lvol_control_leave(&lvs,0);
	export_busy=true;assert(s3lvol_source_drain(&lvs,&b,100,result,NULL)==-EBUSY);export_busy=false;
	registry_busy=true;assert(s3lvol_source_drain(&lvs,&b,100,result,NULL)==-EBUSY);registry_busy=false;
	pending_busy=true;assert(s3lvol_source_drain(&lvs,&b,100,result,NULL)==-EBUSY);pending_busy=false;
	decouple_busy=true;assert(s3lvol_source_drain(&lvs,&b,100,result,NULL)==-EBUSY);decouple_busy=false;
	s3lvol_submit_request(NULL,io);
	assert(s3_producer_count(&lvs.producers)==1);
	assert(!s3lvol_source_drain(&lvs,&b,100,result,timeout?malloc(8):NULL));
	assert(!s3lvol_lvstore_xfer_allowed(&lvs));
	v.bdev=&disk.bdev;assert(s3lvol_destruct(&v)==-EBUSY);
	assert(s3lvol_control_enter(&lvs)==-ESHUTDOWN);
	source_drain_poll(&lvs);assert(drains==old_drains);
	s3lvol_submit_request(NULL,later);assert(last_io_status==SPDK_BDEV_IO_STATUS_FAILED);
	assert(s3_producer_count(&lvs.producers)==1);
	((unsigned char *)&b.operation)[0]^=1;assert(s3lvol_source_drain(&lvs,&b,100,result,NULL)==-EEXIST);((unsigned char *)&b.operation)[0]^=1;
	if(timeout) {ticks=101;source_drain_poll(&lvs);assert(callback_count==1&&callback_status==-ETIMEDOUT);}
	callback_gate=&lvs.producers;write_cb(write_arg,error?-EIO:0);callback_gate=NULL;
	assert(!s3_producer_count(&lvs.producers));
	lower_busy=true;source_drain_poll(&lvs);assert(drains==old_drains);
	lower_busy=false;source_drain_poll(&lvs);
	if (!error) {
		assert(drains==old_drains+1);drain_cb(drain_arg,0);
		assert(lvs.source_phase==2);checkpoint_cb(checkpoint_arg,0);source_drain_poll(&lvs);
	} else assert(drains==old_drains);
	assert(callback_count==1);
	assert(s3lvol_lvstore_source_closed(&lvs));
	assert(!s3lvol_source_drain(&lvs,&b,100,result,NULL));
	assert(callback_count==2&&callback_status==(error?-EIO:0));
	later->type=SPDK_BDEV_IO_TYPE_READ;s3lvol_submit_request(NULL,later);
	assert(last_io_status==SPDK_BDEV_IO_STATUS_SUCCESS);
	free(io);free(later);
}

struct race_writer { struct spdk_bdev_io *io; pthread_barrier_t *start; };
static void *race_write(void *arg)
{
	struct race_writer *w=arg;
	pthread_barrier_wait(w->start);
	for (unsigned i=0;i<1000;i++) s3lvol_submit_request(NULL,w->io);
	return NULL;
}
static void race_test(void)
{
	for(unsigned round=0;round<200;round++) {
		struct s3_producer_gate gate={}, volume_gate={};
		struct spdk_lvol v={.blob=(void *)6};
		struct s3lvol_gated_bdev disk={.gate=&gate,.source_gate=&volume_gate};
		pthread_t threads[4];pthread_barrier_t start;
		struct race_writer writers[4];
		disk.bdev.ctxt=&v;v.bdev=&disk.bdev;
		atomic_init(&gate.state,0);atomic_init(&gate.error,0);
		pthread_barrier_init(&start,NULL,5);
		atomic_store(&race_mode,true);
		for(unsigned i=0;i<4;i++) {
			writers[i].io=calloc(1,sizeof(struct spdk_bdev_io)+sizeof(struct spdk_blob_ext_io_opts));
			writers[i].io->bdev=&disk.bdev;writers[i].io->type=SPDK_BDEV_IO_TYPE_WRITE;
			writers[i].start=&start;assert(!pthread_create(&threads[i],NULL,race_write,&writers[i]));
		}
		pthread_barrier_wait(&start);s3_producer_close(&gate);
		for(unsigned i=0;i<4;i++) { pthread_join(threads[i],NULL);free(writers[i].io); }
		assert(!s3_producer_count(&gate));assert(!atomic_load(&gate.error));
		assert(s3_producer_enter(&gate)==-ESHUTDOWN);
		pthread_barrier_destroy(&start);
	}
	atomic_store(&race_mode,false);
	assert(atomic_load(&race_accepted)+atomic_load(&race_rejected)==800000);
	printf("PASS concurrent production submit: accepted=%u rejected=%u rounds=200\n",
		atomic_load(&race_accepted),atomic_load(&race_rejected));
}

int main(void)
{
	run_case(SPDK_BDEV_IO_TYPE_WRITE,false,false);
	run_case(SPDK_BDEV_IO_TYPE_UNMAP,true,false);
	run_case(SPDK_BDEV_IO_TYPE_WRITE_ZEROES,false,true);
	race_test();
	printf("PASS production submit/barrier: writes=%u reads=%u completions=%u holds=%u drains=%u checkpoints=%u\n",
		writes,reads,completions,holds,drains,checkpoints);
	return 0;
}
