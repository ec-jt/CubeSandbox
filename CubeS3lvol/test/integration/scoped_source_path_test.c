/* SPDX-License-Identifier: Apache-2.0 */
/* Current production lvstore/lvol TUs, real SPDK JSON parser and UUID utility.
 * Deterministic external asynchronous dependencies, no env/PCI/network init.
 * Lower drain/checkpoint stubs enforce the production single-caller contract.
 * Metadata APIs/pollers run only on the owner thread; pthreads exercise only
 * the immutable-pointer production submit/completion path. */
#define SCOPED_PATH_TEST 1
#define main legacy_main
#include "source_drain_path_test.c"
#undef main

uint64_t spdk_blob_get_num_clusters(struct spdk_blob *b) { return 2; }
uint64_t spdk_bs_get_cluster_size(struct spdk_blob_store *b) { return 4096; }

struct fixture {
	struct spdk_lvol_store store;
	struct s3lvol_lvstore lvs;
	struct spdk_lvol vols[3];
	struct s3lvol_gated_bdev disks[3];
};
static void init(struct fixture *f)
{
	memset(f,0,sizeof(*f));
	f->lvs=(struct s3lvol_lvstore){.lvs=&f->store,.wal=(void *)3,
		.local_dev=(void *)4,.bs_dev=(void *)5,.client=(void *)6,
		.name="source",.ns_name="ns",.owner_held=true,.owner_claim.valid=true,
		.owner_mode=S3_OWNER_MODE_STRICT,.dependency_view_complete=true,
		.exports_ready=true,.source_pending_loaded=true,.source_thread=(void *)1};
	assert(!spdk_uuid_parse(&f->lvs.uuid,"11111111-1111-1111-1111-111111111111"));
	assert(!spdk_uuid_parse(&f->lvs.owner_claim.info.nonce,"22222222-2222-2222-2222-222222222222"));
	TAILQ_INIT(&f->store.lvols);
	TAILQ_INSERT_TAIL(&g_lvstores,&f->lvs,link);
	for(unsigned i=0;i<3;i++) {
		struct spdk_lvol *v=&f->vols[i];
		snprintf(v->name,sizeof(v->name),"vol%u",i);
		snprintf(v->uuid_str,sizeof(v->uuid_str),"33333333-3333-3333-3333-%012u",i);
		assert(!spdk_uuid_parse(&v->uuid,v->uuid_str));
		v->lvol_store=&f->store;v->blob=(void *)(uintptr_t)(10+i);
		v->bdev=&f->disks[i].bdev;v->bdev->ctxt=v;v->bdev->blocklen=4096;
		f->disks[i].gate=&f->lvs.producers;
		f->disks[i].source_gate=s3lvol_source_gate_alloc(v,&f->lvs.producers);
		assert(f->disks[i].source_gate);
		assert(f->disks[i].source_gate==s3lvol_source_gate_alloc(v,&f->lvs.producers));
		TAILQ_INSERT_TAIL(&f->store.lvols,v,link);
	}
	ticks=0;callback_count=0;callback_status=0;current_thread=(void *)1;
	drain_cb=checkpoint_cb=NULL;drain_arg=checkpoint_arg=NULL;
	setenv("S3LVOL_INTERNAL_SCOPED_SOURCE_DRAIN","1",1);
}
static void fini(struct fixture *f)
{
	assert(!drain_cb && !checkpoint_cb);
	assert(!s3_producer_count(&f->lvs.producers));
	/* Test teardown only. Production pins have deliberately no cleanup API. */
	while(f->lvs.scoped_sources) {
		struct scoped_source *s=f->lvs.scoped_sources;
		assert(!s->cb && !s->poller);
		f->lvs.scoped_sources=s->next;free(s->body);free(s->token);free(s->sandbox);free(s);
	}
	while(f->lvs.source_gates) {
		struct source_gate_entry *e=f->lvs.source_gates;
		assert(!s3_producer_count(&e->gate));f->lvs.source_gates=e->next;free(e);
	}
	TAILQ_REMOVE(&g_lvstores,&f->lvs,link);
}
static char *body(struct fixture *f,unsigned selected,unsigned token,unsigned generation)
{
	char *b=calloc(1,4096);size_t n;
	assert(b);
	n=(size_t)snprintf(b,4096,"{\"token\":\"%064x\",\"sandbox\":\"sandbox%u\","
		"\"incarnation\":\"%064x\",\"generation\":%u,"
		"\"store_uuid\":\"11111111-1111-1111-1111-111111111111\","
		"\"owner_nonce\":\"22222222-2222-2222-2222-222222222222\","
		"\"endpoint\":\"local-test\",\"bucket\":\"test\",\"prefix\":\"source\",\"members\":[",
		token,token,token,generation);
	for(unsigned i=0;i<3;i++) n+=(size_t)snprintf(b+n,4096-n,
		"%s{\"name\":\"%s\",\"uuid\":\"%s\",\"size\":8192,\"readonly\":false,\"source\":%s}",
		i?",":"",f->vols[i].name,f->vols[i].uuid_str,selected==i?"true":"false");
	assert(n+3<4096);strcat(b,"]}");return b;
}
static struct spdk_bdev_io *new_io(struct fixture *f,unsigned volume,int type)
{
	struct spdk_bdev_io *io=calloc(1,sizeof(*io)+sizeof(struct spdk_blob_ext_io_opts));
	assert(io);io->bdev=&f->disks[volume].bdev;io->type=type;return io;
}
static void write_done(int status)
{
	spdk_blob_op_complete cb=write_cb;void *arg=write_arg;
	assert(cb);write_cb=NULL;write_arg=NULL;cb(arg,status);
}
static void wal_done(int status)
{
	s3_bs_dev_cb cb=drain_cb;void *arg=drain_arg;
	assert(cb);drain_cb=NULL;drain_arg=NULL;cb(arg,status);
}
static void ckpt_done(int status)
{
	s3_bs_dev_cb cb=checkpoint_cb;void *arg=checkpoint_arg;
	assert(cb);checkpoint_cb=NULL;checkpoint_arg=NULL;cb(arg,status);
}
static void immediate_io(struct fixture *f,unsigned vol,bool accepted)
{
	unsigned before=writes;struct spdk_bdev_io *io=new_io(f,vol,SPDK_BDEV_IO_TYPE_WRITE);
	s3lvol_submit_request(NULL,io);
	assert(writes==before+(accepted?1:0));
	if(accepted)write_done(0);
	else assert(last_io_status==SPDK_BDEV_IO_STATUS_FAILED);
	free(io);
}
static void validation(void)
{
	struct fixture f;init(&f);char *b=body(&f,0,1,7);
	current_thread=(void *)2;
	assert(s3lvol_scoped_source_prepare(&f.lvs,b)==-EXDEV);
	assert(s3lvol_control_enter(&f.lvs)==-EXDEV);current_thread=(void *)1;
	import_busy=true;assert(s3lvol_scoped_source_prepare(&f.lvs,b)==-ENOTSUP);import_busy=false;
	export_present=publication_pending=true;
	assert(s3lvol_scoped_source_prepare(&f.lvs,b)==-EBUSY);
	export_present=publication_pending=false;
	assert(!s3lvol_control_enter(&f.lvs));
	assert(s3lvol_scoped_source_prepare(&f.lvs,b)==-EBUSY);s3lvol_control_leave(&f.lvs,0);
	f.vols[0].uuid_str[0]='4';assert(s3lvol_scoped_source_prepare(&f.lvs,b)==-ESTALE);f.vols[0].uuid_str[0]='3';
	char *p=strstr(b,"\"readonly\":false");memcpy(p+11,"true ",5);
	assert(s3lvol_scoped_source_prepare(&f.lvs,b)==-ESTALE);memcpy(p+11,"false",5);
	p=strstr(b,"8192");memcpy(p,"8193",4);
	assert(s3lvol_scoped_source_prepare(&f.lvs,b)==-ESTALE);memcpy(p,"8192",4);
	((unsigned char *)&f.lvs.owner_claim.info.nonce)[0]^=1;
	assert(s3lvol_scoped_source_prepare(&f.lvs,b)==-ESTALE);((unsigned char *)&f.lvs.owner_claim.info.nonce)[0]^=1;
	assert(!s3lvol_scoped_source_prepare(&f.lvs,b));assert(!s3lvol_scoped_source_prepare(&f.lvs,b));
	char *changed=body(&f,0,1,8),*foreign=body(&f,0,2,7);
	assert(s3lvol_scoped_source_prepare(&f.lvs,changed)==-EEXIST);
	assert(s3lvol_scoped_source_drain(&f.lvs,changed,result,NULL)==-ESTALE);
	assert(s3lvol_scoped_source_drain(&f.lvs,foreign,result,NULL)==-ESTALE);
	assert(s3lvol_scoped_source_prepare(&f.lvs,foreign)==-EBUSY);
	assert(s3lvol_lvol_source_held(&f.vols[0]));assert(!s3lvol_lvol_source_held(&f.vols[1]));
	assert(s3lvol_destruct(&f.vols[0])==-EBUSY);
	immediate_io(&f,0,true);immediate_io(&f,1,true);
	fini(&f);free(b);free(changed);free(foreign);
	puts("PASS scoped validation, affinity, pending-export guard, held destructor, prepared writes");
}
static struct scoped_source *upper_source;
static void free_upper(struct spdk_bdev_io *io)
{
	struct s3lvol_gated_bdev *d=SPDK_CONTAINEROF(io->bdev,struct s3lvol_gated_bdev,bdev);
	unsigned before=drains;
	assert(s3_producer_count(d->gate)==1);assert(s3_producer_count(d->source_gate)==1);
	scoped_poll(upper_source);assert(drains==before);
	free(io); /* Real production completion must not touch io after this. */
}
static void lifetime(int stage,int error)
{
	struct fixture f;init(&f);char *b=body(&f,0,1,7);
	assert(!s3lvol_scoped_source_prepare(&f.lvs,b));
	struct scoped_source *s=f.lvs.scoped_sources;
	struct spdk_bdev_io *io=new_io(&f,0,stage%2?SPDK_BDEV_IO_TYPE_UNMAP:SPDK_BDEV_IO_TYPE_WRITE_ZEROES);
	s3lvol_submit_request(NULL,io);spdk_blob_op_complete saved=write_cb;void *saved_arg=write_arg;
	assert(!s3lvol_scoped_source_drain(&f.lvs,b,result,malloc(8)));
	assert(s3lvol_scoped_source_drain(&f.lvs,b,result,NULL)==-EBUSY);
	immediate_io(&f,0,false);immediate_io(&f,2,true);
	unsigned before=drains;scoped_poll(s);assert(drains==before);
	if(stage==0){ticks=30000001;scoped_poll(s);assert(callback_status==-ETIMEDOUT);}
	upper_source=s;upper_hook=free_upper;saved(saved_arg,error==1?-EIO:0);upper_hook=NULL;
	assert(!s3_producer_count(&f.lvs.producers));
	scoped_poll(s);
	if(error!=1) {
		assert(s->phase==1);
		if(stage==1){ticks=30000001;scoped_poll(s);assert(callback_status==-ETIMEDOUT);}
		wal_done(error==2?-EIO:(error==4?-ETIMEDOUT:0));
		if(error!=2 && error!=4) {
			assert(s->phase==2);
			if(stage==2){ticks=30000001;scoped_poll(s);assert(callback_status==-ETIMEDOUT);}
			ckpt_done(error==3?-EIO:(error==5?-ETIMEDOUT:0));
		}
		scoped_poll(s);
	}
	assert(callback_count==1);assert(s->phase==3);
	assert(!s3lvol_scoped_source_drain(&f.lvs,b,result,NULL));
	assert(callback_count==2 && callback_status==(error>=4?-ETIMEDOUT:(error?-EIO:0)));
	assert(s3lvol_lvol_source_closed(&f.vols[0]));
	assert(!s3lvol_lvstore_source_closed(&f.lvs));
	immediate_io(&f,2,true);
	fini(&f);free(b);
	printf("PASS lifetime timeout-stage=%d failure-stage=%d sticky replay\n",stage,error);
}
static void control_failure(void)
{
	struct fixture f;init(&f);char *b=body(&f,0,1,7);
	s3lvol_lvstore_flush(&f.lvs,result,malloc(8));
	assert(f.lvs.control_inflight==1);
	wal_done(-EIO);
	assert(callback_count==1 && callback_status==-EIO);
	assert(!f.lvs.control_inflight);
	assert(s3lvol_scoped_source_prepare(&f.lvs,b)==-EIO);
	fini(&f);free(b);puts("PASS production control completion error prevents false prepare success");
}
static pthread_mutex_t upper_mutex=PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t upper_cond=PTHREAD_COND_INITIALIZER;
static bool upper_entered,upper_release;
static void blocking_upper(struct spdk_bdev_io *io)
{
	pthread_mutex_lock(&upper_mutex);upper_entered=true;
	pthread_cond_broadcast(&upper_cond);
	while(!upper_release)pthread_cond_wait(&upper_cond,&upper_mutex);
	pthread_mutex_unlock(&upper_mutex);
	free(io);
}
static void *complete_on_io_thread(void *arg)
{
	s3lvol_io_complete(arg,0);return NULL;
}
static void delayed_upper(void)
{
	struct fixture f;init(&f);char *b=body(&f,0,1,7);pthread_t worker;
	assert(!s3lvol_scoped_source_prepare(&f.lvs,b));
	struct spdk_bdev_io *io=new_io(&f,0,SPDK_BDEV_IO_TYPE_WRITE);
	s3lvol_submit_request(NULL,io);
	assert(write_cb==s3lvol_io_complete && write_arg==io);
	assert(!s3lvol_scoped_source_drain(&f.lvs,b,result,NULL));
	upper_entered=upper_release=false;upper_hook=blocking_upper;
	assert(!pthread_create(&worker,NULL,complete_on_io_thread,io));
	pthread_mutex_lock(&upper_mutex);
	while(!upper_entered)pthread_cond_wait(&upper_cond,&upper_mutex);
	pthread_mutex_unlock(&upper_mutex);
	unsigned before=drains;
	scoped_poll(f.lvs.scoped_sources);
	assert(drains==before && s3_producer_count(f.disks[0].source_gate)==1);
	assert(s3_producer_count(&f.lvs.producers)==1);
	pthread_mutex_lock(&upper_mutex);upper_release=true;
	pthread_cond_broadcast(&upper_cond);pthread_mutex_unlock(&upper_mutex);
	pthread_join(worker,NULL);upper_hook=NULL;
	assert(!s3_producer_count(f.disks[0].source_gate));
	scoped_poll(f.lvs.scoped_sources);wal_done(0);ckpt_done(0);scoped_poll(f.lvs.scoped_sources);
	assert(callback_count==1 && callback_status==0);
	fini(&f);free(b);puts("PASS owner poll cannot drain while I/O-thread upper callback is blocked");
}
static void controls(void)
{
	struct fixture f;init(&f);char *b=body(&f,0,1,7);
	assert(!s3lvol_scoped_source_prepare(&f.lvs,b));
	/* Common production admission used by import/create/snapshot/clone/resize.
	 * Conservative store-wide metadata refusal is intentional. Data is scoped. */
	assert(s3lvol_control_enter(&f.lvs)==-EBUSY);
	assert(!s3lvol_lvstore_xfer_allowed(&f.lvs));
	unsigned before=drains,cp=checkpoints;
	s3lvol_lvstore_flush(&f.lvs,result,NULL);
	assert(callback_count==1 && callback_status==-EBUSY && drains==before);
	s3lvol_lvstore_checkpoint(&f.lvs,result,NULL);
	assert(callback_count==2 && callback_status==-EBUSY && checkpoints==cp);
	assert(!f.lvs.control_inflight);
	immediate_io(&f,0,true);immediate_io(&f,2,true);
	fini(&f);free(b);puts("PASS post-prepare metadata/import admission fails closed, data remains open");
}
static void multi(void)
{
	struct fixture f;init(&f);char *a=body(&f,0,1,7),*b=body(&f,1,2,7);
	assert(!s3lvol_scoped_source_prepare(&f.lvs,a));struct scoped_source *sa=f.lvs.scoped_sources;
	assert(!s3lvol_scoped_source_prepare(&f.lvs,b));struct scoped_source *sb=f.lvs.scoped_sources;
	assert(!s3lvol_scoped_source_drain(&f.lvs,a,result,NULL));
	assert(!s3lvol_scoped_source_drain(&f.lvs,b,result,NULL));
	scoped_poll(sa);assert(sa->phase==1);scoped_poll(sb);
	assert(sb->phase==0 && !callback_count); /* Must not consume another source's drain slot. */
	wal_done(0);scoped_poll(sb);assert(sb->phase==0);
	ckpt_done(0);scoped_poll(sa);assert(callback_count==1);
	scoped_poll(sb);assert(sb->phase==1);wal_done(0);ckpt_done(0);scoped_poll(sb);
	assert(callback_count==2 && callback_status==0);
	immediate_io(&f,0,false);immediate_io(&f,1,false);immediate_io(&f,2,true);
	fini(&f);free(a);free(b);puts("PASS two scoped sources serialize shared WAL/checkpoint without callback overwrite");
}
static struct fixture *reentrant_fixture;
static char *reentrant_body;
static void replay_cb(void *arg,int status)
{
	assert(status==0);assert(!s3lvol_scoped_source_drain(&reentrant_fixture->lvs,reentrant_body,result,NULL));
}
static void reentrant(void)
{
	struct fixture f;init(&f);char *b=body(&f,0,1,7);
	assert(!s3lvol_scoped_source_prepare(&f.lvs,b));
	reentrant_fixture=&f;reentrant_body=b;
	assert(!s3lvol_scoped_source_drain(&f.lvs,b,replay_cb,NULL));
	scoped_poll(f.lvs.scoped_sources);wal_done(0);ckpt_done(0);scoped_poll(f.lvs.scoped_sources);
	assert(callback_count==1);fini(&f);free(b);puts("PASS reentrant terminal replay");
}
static void *scoped_writer(void *arg)
{
	struct race_writer *w=arg;
	struct spdk_lvol *v=w->io->bdev->ctxt;
	race_unrelated=!strcmp(v->name,"vol2");
	return race_write(arg);
}
static void scoped_race(void)
{
	unsigned accepted_before=atomic_load(&race_accepted),rejected_before=atomic_load(&race_rejected);
	for(unsigned round=0;round<100;round++) {
		struct fixture f;init(&f);char *b=body(&f,0,1,7);
		assert(!s3lvol_scoped_source_prepare(&f.lvs,b));
		pthread_t threads[4];pthread_barrier_t start;struct race_writer w[4];
		pthread_barrier_init(&start,NULL,5);atomic_store(&race_mode,true);
		for(unsigned i=0;i<4;i++) {
			w[i]=(struct race_writer){.io=new_io(&f,i<2?0:2,SPDK_BDEV_IO_TYPE_WRITE),.start=&start};
			assert(!pthread_create(&threads[i],NULL,scoped_writer,&w[i]));
		}
		pthread_barrier_wait(&start);
		assert(!s3lvol_scoped_source_drain(&f.lvs,b,result,NULL));
		for(unsigned i=0;i<4;i++){pthread_join(threads[i],NULL);free(w[i].io);}
		atomic_store(&race_mode,false);pthread_barrier_destroy(&start);
		assert(!s3_producer_count(&f.lvs.producers));
		for(unsigned i=0;i<3;i++)assert(!s3_producer_count(f.disks[i].source_gate));
		immediate_io(&f,0,false);immediate_io(&f,2,true);
		scoped_poll(f.lvs.scoped_sources);wal_done(0);ckpt_done(0);scoped_poll(f.lvs.scoped_sources);
		fini(&f);free(b);
	}
	unsigned a=atomic_load(&race_accepted)-accepted_before,r=atomic_load(&race_rejected)-rejected_before;
	assert(a+r==400000);assert(a>=200000);
	assert(atomic_load(&unrelated_accepted)==200000);assert(!atomic_load(&unrelated_rejected));
	printf("PASS scoped production submit race: accepted=%u rejected=%u total=400000 unrelated=200000\n",a,r);
}
int main(int argc,char **argv)
{
	assert(argc==2);
	if(!strcmp(argv[1],"null")){assert(!s3lvol_source_gate_alloc(NULL,NULL));puts("PASS NULL gate allocation");}
	else if(!strcmp(argv[1],"validation"))validation();
	else if(!strcmp(argv[1],"control-failure"))control_failure();
	else if(!strcmp(argv[1],"controls"))controls();
	else if(!strcmp(argv[1],"multi"))multi();
	else if(!strcmp(argv[1],"race"))scoped_race();
	else if(!strcmp(argv[1],"lifetime")){
		for(int i=0;i<3;i++)lifetime(i,0);
		for(int i=1;i<4;i++)lifetime(3,i);
		lifetime(1,2);lifetime(2,3);
		lifetime(1,4);lifetime(2,5);
		reentrant();delayed_upper();
	}else abort();
	return 0;
}
