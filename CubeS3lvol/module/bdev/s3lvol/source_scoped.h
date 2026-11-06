/* SPDX-License-Identifier: Apache-2.0
 * Included only by lvstore.c. Private Cubelet control channel. The node resolves
 * ownership from persisted storage records; native validates the COMPLETE live
 * inventory and incarnation. Never accept an exclusive-store assertion.
 * Prepared state is process-local. Restart retains the strict owner claim and
 * cannot attach. The node's durable pending record requires reconciliation.
 */
#include "spdk/json.h"
#define SCOPED_MAX 256
struct scoped_member {
	char *name, *uuid;
	uint64_t size;
	bool readonly, source;
};
struct scoped_doc {
	char *token, *sandbox, *incarnation, *store, *owner, *endpoint, *bucket, *prefix;
	uint64_t generation;
	struct scoped_member members[SCOPED_MAX];
	size_t count;
};
struct scoped_source {
	struct scoped_source *next;
	struct s3lvol_lvstore *lvs;
	char *body, *token, *sandbox;
	struct spdk_lvol *volumes[SCOPED_MAX];
	size_t count;
	struct spdk_poller *poller;
	spdk_lvs_op_complete cb;
	void *arg;
	uint64_t deadline;
	int phase, status;
};
static int scoped_member_decode(const struct spdk_json_val *v, void *out)
{
	const struct spdk_json_object_decoder d[] = {
		{"name", offsetof(struct scoped_member,name),spdk_json_decode_string,false},
		{"uuid", offsetof(struct scoped_member,uuid),spdk_json_decode_string,false},
		{"size", offsetof(struct scoped_member,size),spdk_json_decode_uint64,false},
		{"readonly", offsetof(struct scoped_member,readonly),spdk_json_decode_bool,false},
		{"source", offsetof(struct scoped_member,source),spdk_json_decode_bool,false},
	};
	return spdk_json_decode_object(v,d,SPDK_COUNTOF(d),out);
}
static int scoped_members_decode(const struct spdk_json_val *v, void *out)
{
	struct scoped_doc *d = out;
	return spdk_json_decode_array(v, scoped_member_decode, d->members, SCOPED_MAX,
		&d->count, sizeof(d->members[0]));
}
static void scoped_doc_free(struct scoped_doc *d)
{
	free(d->token);free(d->sandbox);free(d->incarnation);free(d->store);free(d->owner);
	free(d->endpoint);free(d->bucket);free(d->prefix);
	for(size_t i=0;i<SCOPED_MAX;i++){free(d->members[i].name);free(d->members[i].uuid);}
}
static int scoped_decode(const char *body, struct scoped_doc *d)
{
	const struct spdk_json_object_decoder dec[] = {
#define SD(n, f) {n,offsetof(struct scoped_doc,f),spdk_json_decode_string,false}
		SD("token",token),SD("sandbox",sandbox),SD("incarnation",incarnation),
		SD("store_uuid",store),SD("owner_nonce",owner),SD("endpoint",endpoint),
		SD("bucket",bucket),SD("prefix",prefix),
#undef SD
		{"generation",offsetof(struct scoped_doc,generation),spdk_json_decode_uint64,false},
		{"members",0,scoped_members_decode,false},
	};
	struct spdk_json_val *v;
	int rc=-EINVAL;
	if(!body || strlen(body)>120*1024) return rc;
	v=calloc(8192,sizeof(*v)); if(!v)return -ENOMEM;
	if(spdk_json_parse((void *)body,strlen(body),v,8192,NULL,0)>0)
		rc=spdk_json_decode_object(v,dec,SPDK_COUNTOF(dec),d);
	free(v);
	if(!rc && (!d->token || strlen(d->token)!=64 || strspn(d->token,"0123456789abcdef")!=64 ||
	   !d->sandbox || !*d->sandbox || strlen(d->sandbox)>256 ||
	   !d->incarnation || strlen(d->incarnation)!=64 ||
	   strspn(d->incarnation,"0123456789abcdef")!=64 || !d->count))rc=-EINVAL;
	return rc;
}
static int scoped_ready(struct s3lvol_lvstore *lvs)
{
	const char *e=getenv("S3LVOL_INTERNAL_SCOPED_SOURCE_DRAIN");
	if(!e || strcmp(e,"1"))return -ENOTSUP;
	if(!lvs || !lvs->lvs || spdk_get_thread()!=lvs->source_thread)return -EXDEV;
	if(!lvs->wal || !lvs->local_dev || !lvs->owner_held ||
	   lvs->owner_mode!=S3_OWNER_MODE_STRICT || !lvs->owner_claim.valid ||
	   !lvs->dependency_view_complete || !lvs->exports_ready ||
	   !lvs->source_pending_loaded || s3lvol_import_first(lvs))return -ENOTSUP;
	if(lvs->unload_in_progress || s3lvol_lvstore_source_closed(lvs))return -EBUSY;
	return 0;
}
bool s3lvol_scoped_source_held(struct s3lvol_lvstore *lvs)
{
	return lvs && lvs->scoped_sources;
}
bool s3lvol_lvol_source_held(struct spdk_lvol *v)
{
	struct s3lvol_lvstore *lvs=v?s3lvol_lvstore_find_by_lvs(v->lvol_store):NULL;
	if(!lvs)return false;
	for(struct scoped_source *s=lvs->scoped_sources;s;s=s->next)
		for(size_t i=0;i<s->count;i++)if(s->volumes[i]==v)return true;
	return false;
}
int s3lvol_source_inventory(struct s3lvol_lvstore *lvs,struct spdk_json_write_ctx *w)
{
	struct spdk_lvol *v;char uuid[SPDK_UUID_STRING_LEN];size_t count=0;
	int rc=scoped_ready(lvs);if(rc)return rc;
	if(lvs->control_inflight)return -EBUSY;
	TAILQ_FOREACH(v,&lvs->lvs->lvols,link)if(++count>SCOPED_MAX)return -E2BIG;
	spdk_json_write_object_begin(w);
	spdk_uuid_fmt_lower(uuid,sizeof(uuid),&lvs->uuid);spdk_json_write_named_string(w,"store_uuid",uuid);
	spdk_uuid_fmt_lower(uuid,sizeof(uuid),&lvs->owner_claim.info.nonce);spdk_json_write_named_string(w,"owner_nonce",uuid);
	spdk_json_write_named_string(w,"endpoint",s3_client_endpoint(lvs->client));
	spdk_json_write_named_string(w,"bucket",s3_client_bucket(lvs->client));
	spdk_json_write_named_string(w,"prefix",lvs->name);
	spdk_json_write_named_array_begin(w,"members");
	TAILQ_FOREACH(v,&lvs->lvs->lvols,link){
		if(!v->blob || v->action_in_progress || s3lvol_lvol_decouple_pending(v))return -EBUSY;
		spdk_json_write_object_begin(w);
		spdk_json_write_named_string(w,"name",v->name);spdk_json_write_named_string(w,"uuid",v->uuid_str);
		spdk_json_write_named_uint64(w,"size",spdk_blob_get_num_clusters(v->blob)*spdk_bs_get_cluster_size(lvs->lvs->blobstore));
		spdk_json_write_named_bool(w,"readonly",spdk_blob_is_read_only(v->blob));
		spdk_json_write_named_bool(w,"source",false);
		spdk_json_write_object_end(w);
	}
	spdk_json_write_array_end(w);spdk_json_write_object_end(w);return 0;
}
static int scoped_validate(struct s3lvol_lvstore *lvs,struct scoped_doc *d,struct scoped_source *s)
{
	struct spdk_uuid u;struct spdk_lvol *v;size_t n=0;int rc=scoped_ready(lvs);
	struct source_pending_check pending={.uuid=&lvs->uuid};
	if(rc)return rc;
	/* A failed metadata operation may have left an uncertain inventory.
	 * The store error is sticky; do not publish a successful new binding. */
	rc=atomic_load(&lvs->producers.error);if(rc)return rc;
	if(!d->store || spdk_uuid_parse(&u,d->store) || spdk_uuid_compare(&u,&lvs->uuid) ||
	   !d->owner || spdk_uuid_parse(&u,d->owner) || spdk_uuid_compare(&u,&lvs->owner_claim.info.nonce) ||
	   !d->endpoint || strcmp(d->endpoint,s3_client_endpoint(lvs->client)) ||
	   !d->bucket || strcmp(d->bucket,s3_client_bucket(lvs->client)) ||
	   !d->prefix || strcmp(d->prefix,lvs->name))return -ESTALE;
	if(lvs->control_inflight || s3lvol_export_work_busy(lvs) || s3lvol_export_registry_busy(lvs) ||
	   s3lvol_pending_persist_busy(&lvs->uuid))return -EBUSY;
	for(struct s3lvol_export *exp=s3lvol_export_first(lvs);exp;exp=s3lvol_export_next(exp)) {
		struct s3lvol_export_entry info;
		s3lvol_export_get(exp,&info);
		if(info.publication_pending || info.reaping)return -EBUSY;
	}
	s3lvol_pending_foreach(source_pending_check,&pending);if(pending.found)return -EBUSY;
	TAILQ_FOREACH(v,&lvs->lvs->lvols,link)n++;
	if(n!=d->count)return -ESTALE;
	for(size_t i=0;i<d->count;i++){
		struct scoped_member *m=&d->members[i];
		if(!m->name || !m->uuid || (i && strcmp(d->members[i-1].name,m->name)>=0))return -EINVAL;
		v=s3lvol_lvol_find(lvs,m->name);
		if(!v || !v->blob || strcmp(v->uuid_str,m->uuid) || m->readonly!=spdk_blob_is_read_only(v->blob) ||
		   m->size!=spdk_blob_get_num_clusters(v->blob)*spdk_bs_get_cluster_size(lvs->lvs->blobstore))return -ESTALE;
		if(v->action_in_progress || s3lvol_lvol_decouple_pending(v))return -EBUSY;
		if(m->source){
			if(s3lvol_lvol_source_held(v) || !s3lvol_lvol_producer_gate(v))return -EBUSY;
			s->volumes[s->count++]=v;
		}
	}
	return s->count?0:-EINVAL;
}
int s3lvol_scoped_source_prepare(struct s3lvol_lvstore *lvs,const char *body)
{
	struct scoped_doc d={0};struct scoped_source *s=NULL;int rc=scoped_ready(lvs);
	if(rc)return rc;
	if((rc=scoped_decode(body,&d)))goto out;
	for(s=lvs->scoped_sources;s;s=s->next){
		if(!strcmp(s->token,d.token) || !strcmp(s->sandbox,d.sandbox)){
			rc=strcmp(s->body,body)?-EEXIST:0;goto out;
		}
	}
	s=calloc(1,sizeof(*s));if(!s){rc=-ENOMEM;goto out;}
	rc=scoped_validate(lvs,&d,s);if(rc){free(s);goto out;}
	s->body=strdup(body);s->token=strdup(d.token);s->sandbox=strdup(d.sandbox);
	if(!s->body || !s->token || !s->sandbox){free(s->body);free(s->token);free(s->sandbox);free(s);rc=-ENOMEM;goto out;}
	s->lvs=lvs;s->phase=-1;s->next=lvs->scoped_sources;lvs->scoped_sources=s;
	/* Membership pin only. Source writes continue until qualified runtime stop.
	 * No automatic expiry, unregister, cleanup or owner release exists. */
out:scoped_doc_free(&d);return rc;
}
static void scoped_checkpoint_done(void *arg,int status)
{
	struct scoped_source *s=arg;s->status=status;s->phase=3;
}
static void scoped_wal_done(void *arg,int status)
{
	struct scoped_source *s=arg;
	if(status){s->status=status;s->phase=3;return;}
	s->phase=2;s3_bs_dev_checkpoint(s->lvs->bs_dev,scoped_checkpoint_done,s);
}
static int scoped_poll(void *arg)
{
	struct scoped_source *s=arg;spdk_lvs_op_complete cb;void *a;
	if(s->cb && spdk_get_ticks()>=s->deadline){cb=s->cb;a=s->arg;s->cb=NULL;s->arg=NULL;cb(a,-ETIMEDOUT);}
	if(s->phase==0){
		for(size_t i=0;i<s->count;i++){
			struct s3_producer_gate *g=s3lvol_lvol_producer_gate(s->volumes[i]);
			if(s3_producer_count(g))return SPDK_POLLER_IDLE;
			if(atomic_load(&g->error)){s->status=atomic_load(&g->error);s->phase=3;break;}
		}
		if(s->phase==0){
			/* One native drain/checkpoint slot is shared by the store. Keep
			 * other scoped sources queued, not permanently failed with EBUSY.
			 * Only the owner thread reads or changes these phases. */
			for(struct scoped_source *other=s->lvs->scoped_sources;other;other=other->next)
				if(other!=s && (other->phase==1 || other->phase==2))return SPDK_POLLER_IDLE;
			s->phase=1;s3_bs_dev_drain(s->lvs->bs_dev,scoped_wal_done,s);
		}
	}
	if(s->phase!=3)return SPDK_POLLER_IDLE;
	spdk_poller_unregister(&s->poller);cb=s->cb;a=s->arg;s->cb=NULL;s->arg=NULL;
	if(cb)cb(a,s->status);
	return SPDK_POLLER_BUSY;
}
int s3lvol_scoped_source_drain(struct s3lvol_lvstore *lvs,const char *body,spdk_lvs_op_complete cb,void *arg)
{
	struct scoped_source *s;int rc=scoped_ready(lvs);if(rc)return rc;
	if(!body || !cb)return -EINVAL;
	for(s=lvs->scoped_sources;s;s=s->next)if(!strcmp(s->body,body))break;
	if(!s)return -ESTALE;
	if(s->cb)return -EBUSY;
	if(s->phase==3){cb(arg,s->status);return 0;}
	if(!s->poller){s->poller=SPDK_POLLER_REGISTER(scoped_poll,s,1000);if(!s->poller)return -ENOMEM;}
	if(s->phase==-1){
		for(size_t i=0;i<s->count;i++)s3_producer_close(s3lvol_lvol_producer_gate(s->volumes[i]));
		s3_bs_dev_scoped_source_hold(lvs->bs_dev);s->phase=0;
	}
	s->cb=cb;s->arg=arg;s->deadline=spdk_get_ticks()+30*spdk_get_ticks_hz();return 0;
}
