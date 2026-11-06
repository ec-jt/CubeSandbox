/* SPDX-License-Identifier: Apache-2.0
 * Real lvstore unload/destroy callbacks; only external boundaries are mocked.
 * No SPDK environment, PCI, DPDK, devices or network is initialized. */
#include "../../module/bdev/s3lvol/vbdev_s3lvol_lvstore.c"

static unsigned checks;
#define CHECK(x) do { checks++; if (!(x)) { fprintf(stderr,"FAIL %d: %s\n",__LINE__,#x); abort(); } } while (0)
struct spdk_poller { spdk_poller_fn fn; void *arg; };
static struct spdk_poller *timer;
static uint64_t ticks;
static bool fail_timer, registry_busy, inline_bs, hold_release, hold_close, hold_delete;
static s3_op_cb delete_cb;
static void *delete_arg;
static spdk_bdev_unregister_cb close_cb;
static void *close_arg;
static int pre_error, submit_error, reports, result, closes, releases, deletes;
static spdk_lvs_op_complete bs_cb, dev_cb, release_cb;
static void *bs_arg, *dev_arg, *release_arg;
static s3_bs_dev_reap_cb reap_cb;
static void *reap_arg;
static struct spdk_lvol_store store;
static struct spdk_bdev disk;
static struct s3_target target;

uint64_t spdk_get_ticks(void) { return ticks; }
uint64_t spdk_get_ticks_hz(void) { return 1000000; }
struct spdk_poller *spdk_poller_register_named(spdk_poller_fn f, void *a, uint64_t us, const char *n) {
 if (fail_timer) return NULL;
 CHECK(!timer); timer=calloc(1,sizeof(*timer)); timer->fn=f; timer->arg=a; return timer;
}
void spdk_poller_unregister(struct spdk_poller **p) { if (*p) { CHECK(*p==timer); free(*p); *p=NULL; timer=NULL; } }
void spdk_log(enum spdk_log_level l,const char *f,int n,const char *fn,const char *fmt,...) {}
void spdk_log_register_flag(const char *n,struct spdk_log_flag *f) {}
const char *spdk_strerror(int e) { return strerror(e); }
bool spdk_uuid_is_null(const struct spdk_uuid *u) { struct spdk_uuid z={0}; return !memcmp(u,&z,sizeof z); }
void spdk_uuid_copy(struct spdk_uuid *d,const struct spdk_uuid *s) { memcpy(d,s,sizeof(*d)); }
int spdk_uuid_compare(const struct spdk_uuid *a,const struct spdk_uuid *b) { return memcmp(a,b,sizeof(*a)); }
void s3_bs_dev_set_destroy_cb(struct spdk_bs_dev *d,s3_bs_dev_cb f,void *a) { dev_cb=f; dev_arg=a; }
void s3_bs_dev_set_reap_cb(struct spdk_bs_dev *d,s3_bs_dev_reap_cb f,void *a) { reap_cb=f; reap_arg=a; }
void s3_bs_dev_drain(struct spdk_bs_dev *d,s3_bs_dev_cb f,void *a) { f(a,pre_error); }
static void finish_dev(int status) {
 spdk_lvs_op_complete f=dev_cb; void *a=dev_arg; dev_cb=NULL; dev_arg=NULL;
 if (reap_cb) { reap_cb(reap_arg,NULL); reap_cb=NULL; reap_arg=NULL; }
 f(a,status);
}
int spdk_lvs_unload(struct spdk_lvol_store *s,spdk_lvs_op_complete f,void *a) {
 if (submit_error) { f(a,submit_error); return submit_error; }
 if (inline_bs) { finish_dev(0); f(a,0); } else { bs_cb=f; bs_arg=a; }
 return 0;
}
void spdk_bdev_unregister(struct spdk_bdev *d,spdk_bdev_unregister_cb f,void *a) {
 if (hold_close) { close_cb=f; close_arg=a; } else f(a,0);
}
void spdk_lvol_close(struct spdk_lvol *l,spdk_lvol_op_complete f,void *a) { f(a,0); }
bool s3lvol_pending_persist_busy(const struct spdk_uuid *u) { return registry_busy; }
void s3lvol_snapshot_pending_clear_lvs(const struct spdk_uuid *u) {}
void s3lvol_xfer_lvstore_fini(struct s3lvol_lvstore *l) {}
void s3lvol_xfer_exports_fini(struct s3lvol_lvstore *l) {}
void s3_journal_destroy(struct s3_journal *j) {}
void s3_local_dev_close(struct s3_local_dev *d) { CHECK(!dev_cb); CHECK(!registry_busy); closes++; }
void s3_client_put(struct s3_client *c) {}
const char *s3_client_endpoint(const struct s3_client *c) { return "local"; }
const char *s3_client_bucket(const struct s3_client *c) { return "test"; }
struct spdk_bdev_desc *s3_local_dev_get_desc(struct s3_local_dev *d,enum s3_region_id r) { return (void *)&disk; }
struct spdk_bdev *spdk_bdev_desc_get_bdev(struct spdk_bdev_desc *d) { return &disk; }
const char *spdk_bdev_get_name(const struct spdk_bdev *d) { return "wal"; }
const struct spdk_uuid *spdk_bdev_get_uuid(const struct spdk_bdev *d) { return &disk.uuid; }
void s3_owner_release(struct s3_client *c,const char *n,s3_owner_cb f,void *a) {
 releases++; if (hold_release) { release_cb=f; release_arg=a; } else f(a,0);
}
void s3_owner_release_strict(struct s3_client *c,const char *n,const struct s3_owner_claim *cl,const struct s3_owner_caps *cap,s3_owner_cb f,void *a) { s3_owner_release(c,n,f,a); }
int bstore_remove_entry(const char *n) { return 0; }
int s3_delete(struct s3_client *c,const char *k,s3_op_cb f,void *a) { deletes++; if (hold_delete) { hold_delete=false; delete_cb=f; delete_arg=a; } else f(a,0); return 0; }
int s3_head(struct s3_client *c,const char *k,uint64_t *sz,s3_op_cb f,void *a) { f(a,-ENOENT); return 0; }
const struct s3_target *rcow_namespace_to_target(const char *n) { return &target; }
int s3_client_get_or_create(const struct s3_target *t,struct s3_client **c) { *c=(void *)&target; return 0; }
struct s3lvol_export *s3lvol_export_first(struct s3lvol_lvstore *l) { return NULL; }
struct s3lvol_export *s3lvol_export_next(struct s3lvol_export *e) { return NULL; }
void s3lvol_export_get(const struct s3lvol_export *e,struct s3lvol_export_entry *out) {}
void s3_chunk_data_key(const char *p,const struct spdk_uuid *u,char *b,size_t n) { snprintf(b,n,"%s/data/test",p); }
void s3_chunk_map_foreach(struct s3_chunk_map *m,s3_chunk_map_iter_cb f,void *a) {}

static void caller(void *arg,int status) { CHECK(arg); free(arg); reports++; result=status; }
static void rejected(void *arg,int status) { *(int *)arg=status; }
static struct s3lvol_lvstore *setup(void) {
 CHECK(TAILQ_EMPTY(&g_lvstores)); CHECK(!timer);
 lvs_retained_clear_idle(&g_retained);
 reports=result=closes=releases=deletes=pre_error=submit_error=0;
 fail_timer=registry_busy=inline_bs=hold_release=hold_close=hold_delete=false;
 bs_cb=dev_cb=release_cb=NULL; reap_cb=NULL;
 memset(&store,0,sizeof store); TAILQ_INIT(&store.lvols);
 struct s3lvol_lvstore *l=calloc(1,sizeof(*l));
 l->name=strdup("deadline"); l->ns_name=strdup("test"); l->lvs=&store;
 l->bs_dev=(void *)&disk; l->local_dev=(void *)&disk; l->client=(void *)&target;
 ((unsigned char *)&l->uuid)[0]=1; ((unsigned char *)&disk.uuid)[0]=2;
 l->owner_held=true; l->owner_mode=S3_OWNER_MODE_STRICT;
 l->owner_claim.valid=true; strcpy(l->owner_claim.etag,"etag");
 ((unsigned char *)&l->owner_claim.info.nonce)[0]=3;
 TAILQ_INSERT_TAIL(&g_lvstores,l,link); return l;
}
static void tick(void) { CHECK(timer); spdk_poller_fn f=timer->fn; void *a=timer->arg; f(a); }
static void timeout_now(void) { ticks+=LVS_UNLOAD_POSTDRAIN_TIMEOUT_US; tick(); CHECK(reports==1); CHECK(result==-ETIMEDOUT); }
static void finish_bs(void) { spdk_lvs_op_complete f=bs_cb; void *a=bs_arg; bs_cb=NULL; bs_arg=NULL; f(a,0); }
static void clean_loaded(struct s3lvol_lvstore *l) { dev_cb=NULL; TAILQ_REMOVE(&g_lvstores,l,link); s3lvol_lvstore_free(l); }
int main(void) {
 /* Timeout while a real bsdev callback is held forever; then late success/error.
  * RPC argument is freed at notification, so sanitizer detects any reuse. */
 for (int destroy=0;destroy<2;destroy++) for (int late=0;late<2;late++) {
  struct s3lvol_lvstore *l=setup();
  if (destroy) s3lvol_lvstore_destroy(l,caller,malloc(1)); else s3lvol_lvstore_unload(l,caller,malloc(1));
  finish_bs(); timeout_now(); CHECK(!lvs_retained_find(&g_retained,"deadline"));
  for (int n=0;n<100;n++) tick();
  CHECK(reports==1 && closes==0 && releases==0 && deletes==0);
  CHECK(l->unload_in_progress && l->owner_held && l->local_dev);
  int rc=0; s3lvol_lvstore_unload(l,rejected,&rc); CHECK(rc<0);
  rc=0; s3lvol_lvstore_destroy(l,rejected,&rc); CHECK(rc<0);
  finish_dev(late ? -EIO : 0);
  CHECK(reports==1 && closes==1 && !timer && deletes==0 && releases==0);
  struct lvs_retained *r=lvs_retained_find(&g_retained,"deadline");
  CHECK(r && !r->busy && !strcmp(r->claim.etag,"etag"));
 }
 /* Inline success in both rendezvous callbacks, timer unregistered before free. */
 struct s3lvol_lvstore *l=setup(); inline_bs=true;
 s3lvol_lvstore_unload(l,caller,malloc(1)); CHECK(reports==1 && result==0 && closes==1 && releases==1 && !timer);
 /* Poller allocation fails before destructive transitions. */
 l=setup(); fail_timer=true; s3lvol_lvstore_destroy(l,caller,malloc(1));
 CHECK(reports==1 && result==-ENOMEM && !l->unload_in_progress && !dev_cb && !reap_cb && !deletes); clean_loaded(l);
 /* Predrain failure remains retryable. */
 l=setup(); pre_error=-ETIMEDOUT; s3lvol_lvstore_destroy(l,caller,malloc(1));
 CHECK(reports==1 && !l->unload_in_progress && !reap_cb && !timer); clean_loaded(l);
 /* SPDK inline error plus error return must not double free. */
 l=setup(); submit_error=-EBUSY; s3lvol_lvstore_unload(l,caller,malloc(1));
 CHECK(reports==1 && result==-EBUSY && !timer && !l->unload_in_progress); clean_loaded(l);
 /* Registry may never complete: keep device/exclusion, then finish safely. */
 l=setup(); registry_busy=true; s3lvol_lvstore_destroy(l,caller,malloc(1)); finish_bs(); finish_dev(0);
 timeout_now(); for(int i=0;i<10;i++) tick(); CHECK(!closes && !deletes && l->unload_in_progress);
 registry_busy=false; tick(); CHECK(closes==1 && reports==1 && !timer && !deletes && !releases);
 /* Owner release already issued cannot be cancelled; wait for actual callback. */
 for(int late=0;late<2;late++) {
  l=setup(); hold_release=true; s3lvol_lvstore_unload(l,caller,malloc(1)); finish_bs(); finish_dev(0);
  CHECK(releases==1 && !closes); timeout_now(); tick(); CHECK(!closes);
  release_cb(release_arg,late ? -EIO : 0); release_cb=NULL;
  CHECK(closes==1 && reports==1 && !timer);
 }
 /* Completion on deadline tick checks expiry even before the timer runs. */
 l=setup(); s3lvol_lvstore_destroy(l,caller,malloc(1)); finish_bs(); ticks+=LVS_UNLOAD_POSTDRAIN_TIMEOUT_US; finish_dev(0);
 CHECK(reports==1 && result==-ETIMEDOUT && !deletes && !releases && closes==1);
 /* Held lvol close callback keeps the context after user notification. */
 for (int error=0;error<2;error++) {
  l=setup(); struct spdk_lvol vol={0}; vol.bdev=&disk;
  TAILQ_INSERT_TAIL(&store.lvols,&vol,link); hold_close=true;
  s3lvol_lvstore_destroy(l,caller,malloc(1)); timeout_now();
  CHECK(!closes && !bs_cb && l->unload_in_progress && reap_cb);
  close_cb(close_arg,error ? -EBUSY : 0); close_cb=NULL;
  if (error) {
   CHECK(!l->unload_in_progress && !reap_cb && reports==1); clean_loaded(l);
  } else { finish_dev(0); CHECK(!closes); finish_bs(); CHECK(closes==1 && reports==1 && !deletes); }
 }
 /* Clean destroy is unchanged and only begins deletes after quiescence. */
 l=setup(); s3lvol_lvstore_destroy(l,caller,malloc(1)); finish_bs(); CHECK(!deletes); finish_dev(0);
 CHECK(reports==1 && !result && deletes==5 && releases==1 && closes==1 && !timer);
 /* Deletion phase is explicitly outside the unload bound. An issued DELETE
  * owns destroy state until its callback, not until an unload timer tick. */
 l=setup(); hold_delete=true; s3lvol_lvstore_destroy(l,caller,malloc(1)); finish_bs(); finish_dev(0);
 CHECK(!timer && !reports && deletes==1 && !releases);
 ticks+=10*LVS_UNLOAD_POSTDRAIN_TIMEOUT_US; CHECK(!reports);
 delete_cb(delete_arg,0); delete_cb=NULL;
 CHECK(reports==1 && !result && deletes==5 && releases==1);
 /* Legacy mode retains remotely without manufacturing a strict record. */
 l=setup(); l->owner_mode=S3_OWNER_MODE_LEGACY;
 s3lvol_lvstore_unload(l,caller,malloc(1)); finish_bs(); timeout_now(); finish_dev(0);
 CHECK(!releases && reports==1 && !lvs_retained_find(&g_retained,"deadline"));
 CHECK(TAILQ_EMPTY(&g_lvstores)); lvs_retained_clear_idle(&g_retained);
 printf("PASS %u checks: real lvstore unload/destroy callback paths\n",checks);
 return 0;
}
