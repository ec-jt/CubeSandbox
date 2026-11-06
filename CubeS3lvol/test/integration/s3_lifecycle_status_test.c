/* SPDX-License-Identifier: Apache-2.0
 * Actual lvstore callbacks + actual RPC TU + real SPDK JSON writer.
 * External storage/transport boundaries are mocked; no environment init. */
#define main deadline_tests_main
#include "s3_lvstore_deadline_test.c"
#undef main
#include "spdk/rpc.h"
#undef SPDK_RPC_REGISTER
#define SPDK_RPC_REGISTER(method, fn, state)
#undef SPDK_RPC_REGISTER_ALIAS_DEPRECATED
#define SPDK_RPC_REGISTER_ALIAS_DEPRECATED(method, alias)
#include "../../module/bdev/s3lvol/vbdev_s3lvol_rpc.c"
#include <uuid/uuid.h>

static char json[32768];
static size_t json_len;
static unsigned native_reads, stats_reads, errors;
static int capture(void *arg, const void *data, size_t n) {
 CHECK(json_len+n < sizeof(json)); memcpy(json+json_len,data,n);
 json_len+=n; json[json_len]=0; return 0;
}
struct spdk_json_write_ctx *spdk_jsonrpc_begin_result(struct spdk_jsonrpc_request *r) {
 json_len=0; json[0]=0; return spdk_json_write_begin(capture,NULL,0);
}
void spdk_jsonrpc_end_result(struct spdk_jsonrpc_request *r,struct spdk_json_write_ctx *w) { CHECK(spdk_json_write_end(w)==0); }
void spdk_jsonrpc_send_error_response(struct spdk_jsonrpc_request *r,int code,const char *msg) { CHECK(code==SPDK_JSONRPC_ERROR_INVALID_PARAMS); errors++; }
int spdk_uuid_fmt_lower(char *s,size_t n,const struct spdk_uuid *u) { CHECK(n>=37); uuid_unparse_lower((const unsigned char *)u,s); return 0; }
uint64_t spdk_bs_get_cluster_size(struct spdk_blob_store *bs) { CHECK(bs==(void *)&disk); native_reads++; return 1048576; }
uint64_t spdk_bs_total_data_cluster_count(struct spdk_blob_store *bs) { CHECK(bs==(void *)&disk); native_reads++; return 64; }
uint64_t spdk_bs_free_cluster_count(struct spdk_blob_store *bs) { CHECK(bs==(void *)&disk); native_reads++; return 37; }
uint64_t spdk_blob_get_num_clusters(struct spdk_blob *b) { return 12; }
uint64_t spdk_blob_get_num_allocated_clusters(struct spdk_blob *b) { return 3; }
bool spdk_blob_is_read_only(struct spdk_blob *b) { return true; }
int s3lvol_snapshot_query_lvol(struct spdk_lvol *l,enum s3lvol_export_state *s,bool *d,bool *p) { *s=S3LVOL_EXPORT_STATE_NONE; *d=true; *p=false; return 0; }
uint32_t s3lvol_lvol_chain_depth(struct s3lvol_lvstore *l,struct spdk_lvol *v) { return 2; }
void s3_bs_dev_get_stats(struct spdk_bs_dev *d,struct s3_bs_dev_stats *s) { CHECK(d==(void *)&disk); stats_reads++; memset(s,0,sizeof(*s)); s->wal_attached=true; s->wal_writes=17; s->chunks_flushed=7; s->journal_capacity_bytes=4096; }

static void query(const char *label, bool retained) {
 int old_closes=closes, old_releases=releases, old_deletes=deletes, old_reports=reports;
 struct spdk_poller *old_timer=timer;
 if (retained) rpc_rcow_get_retained_lvstores(NULL,NULL); else rpc_rcow_get_lvstores(NULL,NULL);
 CHECK(closes==old_closes && releases==old_releases && deletes==old_deletes && reports==old_reports && timer==old_timer);
 CHECK(!strstr(json,"SECRET") && !strstr(json,"etag") && !strstr(json,"nonce") && !strstr(json,"endpoint") && !strstr(json,"bucket"));
 if (label) printf("JSON %s %s\n",label,json);
}
static void unavailable(const char *label) {
 unsigned nr=native_reads, sr=stats_reads;
 query(label,false);
 CHECK(strstr(json,"\"native_available\":false") && strstr(json,"\"cluster_size\":null") && strstr(json,"\"write_path\":null") && strstr(json,"\"lvols\":null"));
 CHECK(native_reads==nr && stats_reads==sr);
}
static void retained_roundtrip(void) {
 struct lvs_retained *r=lvs_retained_find(&g_retained,"deadline"), *taken=NULL;
 CHECK(r); struct lvs_retained saved=*r;
 query("retained-ready",true); CHECK(strstr(json,"retained_attach_ready"));
 for(int i=0;i<100;i++) query(NULL,true);
 CHECK(!memcmp(r,&saved,sizeof saved));
 CHECK(lvs_retained_take(&g_retained,"deadline",&taken)==0);
 query("retained-busy",true); CHECK(strstr(json,"retained_attach_busy"));
 CHECK(lvs_retained_put_back(taken)==0); query("retained-retry",true);
 CHECK(lvs_retained_take(&g_retained,"deadline",&taken)==0);
 CHECK(lvs_retained_consume(&g_retained,taken)==0); query("retained-consumed",true);
 CHECK(strstr(json,"\"records\":[]"));
}
int main(void) {
 CHECK(deadline_tests_main()==0);
 struct s3lvol_lvstore *l=setup(); store.blobstore=(void *)&disk; store.uuid=l->uuid;
 struct spdk_lvol vol={0}; strcpy(vol.name,"snapshot"); vol.bdev=&disk; disk.name="volume"; vol.blob=(void *)&disk; vol.uuid=l->uuid;
 TAILQ_INSERT_TAIL(&store.lvols,&vol,link);
 query("loaded",false); CHECK(strstr(json,"\"state\":\"loaded\""));
 l->bs_dev=NULL; query("no-write-stats",false); CHECK(strstr(json,"\"write_path\":null")); l->bs_dev=(void *)&disk;
 store.blobstore=NULL; unavailable("no-blobstore"); store.blobstore=(void *)&disk;
 l->lvs=NULL; unavailable("no-native-store"); l->lvs=&store;
 TAILQ_REMOVE(&store.lvols,&vol,link);
 s3lvol_lvstore_unload(l,caller,malloc(1)); unavailable("unloading");
 finish_bs(); CHECK(!l->lvs); unavailable("retiring-null-store"); timeout_now(); unavailable("timeout-null-store");
 struct s3lvol_lvstore saved=*l;
 for(int i=0;i<100;i++) unavailable(NULL);
 CHECK(!memcmp(l,&saved,sizeof saved));
 finish_dev(0); query("retired-registry",false); CHECK(!strcmp(json,"[]")); retained_roundtrip();
 l=setup(); hold_release=true; s3lvol_lvstore_unload(l,caller,malloc(1)); finish_bs(); finish_dev(0);
 unavailable("release-pending"); CHECK(strstr(json,"\"release_pending\":true") && strstr(json,"\"owner_claim_local\":false"));
 timeout_now(); unavailable("release-pending-timeout"); release_cb(release_arg,-EIO); release_cb=NULL;
 query("release-error-absent",true);
 l=setup(); struct spdk_lvol held={0}; held.bdev=&disk; TAILQ_INSERT_TAIL(&store.lvols,&held,link); hold_close=true;
 s3lvol_lvstore_unload(l,caller,malloc(1)); timeout_now(); close_cb(close_arg,-EBUSY); close_cb=NULL;
 store.blobstore=(void *)&disk; query("loaded-after-timeout-error",false); CHECK(strstr(json,"\"timeout_reported\":true"));
 TAILQ_REMOVE(&store.lvols,&held,link); hold_close=false;
 s3lvol_lvstore_unload(l,caller,malloc(1)); unavailable("retry-reset-timeout"); CHECK(strstr(json,"\"timeout_reported\":false")); finish_bs(); finish_dev(0);
 l=setup(); hold_delete=true; s3lvol_lvstore_destroy(l,caller,malloc(1)); finish_bs(); finish_dev(0);
 CHECK(!s3lvol_lvstore_first() && !reports && deletes==1); query("destroy-after-handoff",true);
 CHECK(strstr(json,"\"includes_destroy_after_handoff\":false") && strstr(json,"\"absence_proves_completion\":false"));
 delete_cb(delete_arg,0); delete_cb=NULL;
 struct spdk_json_val param={0}; rpc_rcow_get_lvstores(NULL,&param); rpc_rcow_get_retained_lvstores(NULL,&param); CHECK(errors==2);
 CHECK(TAILQ_EMPTY(&g_lvstores) && TAILQ_EMPTY(&g_retained) && !timer);
 printf("PASS %u checks: lifecycle serializer and real callbacks\n",checks); return 0;
}
