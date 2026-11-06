/* SPDX-License-Identifier: Apache-2.0 */
/* Actual lower-device hold/busy functions, no device or network initialization. */
#include "../../lib/s3bsdev/s3_bs_dev.c"

struct spdk_thread *spdk_get_thread(void) { return (void *)1; }
void spdk_poller_unregister(struct spdk_poller **p) { *p = NULL; }

int main(void)
{
	struct s3_ctx ctx = {.owner_thread=(void *)1,.ckpt_poller=(void *)2};
	s3_bs_dev_source_hold(&ctx.bs_dev);
	assert(ctx.source_hold && !ctx.ckpt_poller && !ctx.destroying);
	assert(!s3_bs_dev_source_busy(&ctx.bs_dev));
	ctx.inflight=1;assert(s3_bs_dev_source_busy(&ctx.bs_dev));ctx.inflight=0;
	ctx.retry_count=1;assert(s3_bs_dev_source_busy(&ctx.bs_dev));ctx.retry_count=0;
	ctx.ckpt_in_flight=true;assert(s3_bs_dev_source_busy(&ctx.bs_dev));ctx.ckpt_in_flight=false;
	ctx.deferred_inflight=1;assert(s3_bs_dev_source_busy(&ctx.bs_dev));ctx.deferred_inflight=0;
	assert(!s3_bs_dev_source_busy(&ctx.bs_dev));
	assert(ctx.source_hold && !ctx.destroying);
	puts("PASS production lower source hold/busy: I/O, retry, checkpoint, delete callback");
	return 0;
}
