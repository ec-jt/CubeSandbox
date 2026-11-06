/* SPDX-License-Identifier: Apache-2.0 */
#include <assert.h>
#include <stdio.h>
#include "export_admission_ctl.h"
static struct ea_doc initial(void)
{
	struct ea_doc d = {0};
	memcpy(d.magic, "S3EA0001", 8); d.nonce[0] = 1;
	d.export_id[0] = 2; d.owner_id[0] = 3;
	strcpy(d.prefix, "source"); d.chunks_be[7] = 1; d.state = EA_OPEN;
	return d;
}
int main(void)
{
	struct ea_doc d = initial(), before, add, close;
	uint8_t id[16] = {4};
	assert(!ea_validate(&d, sizeof(d)));
	assert(ea_validate(&d, sizeof(d)-1));
	assert(ea_validate(&d, sizeof(d)+1));
	assert(ea_change(&d, EA_BEGIN_CLEANUP, NULL) == -EBUSY);
	assert(!ea_change(&d, EA_ADD, id));
	assert(!ea_change(&d, EA_ADD, id) && d.count == 1);
	assert(!ea_change(&d, EA_CLOSE, NULL));
	assert(ea_change(&d, EA_ADD, id) == -EBUSY);
	assert(ea_change(&d, EA_BEGIN_CLEANUP, NULL) == -EBUSY);
	assert(!ea_change(&d, EA_DROP, id));
	assert(!ea_change(&d, EA_DROP, id));
	assert(!ea_change(&d, EA_BEGIN_CLEANUP, NULL));
	assert(!ea_change(&d, EA_CLOSE, NULL) && d.state == EA_CLEANUP);
	assert(!ea_change(&d, EA_FINISH, NULL));
	assert(!ea_change(&d, EA_CLOSE, NULL) && d.state == EA_DONE);
	assert(ea_change(&d, EA_ADD, id) == -EBUSY);
	d = initial(); d.count = EA_READERS+1; assert(ea_validate(&d,sizeof(d)));
	d = initial(); d.reserved[0] = 1; assert(ea_validate(&d,sizeof(d)));
	d = initial(); d.state = 99; assert(ea_validate(&d,sizeof(d)));
	d = initial(); memset(d.prefix,'x',sizeof(d.prefix)); assert(ea_validate(&d,sizeof(d)));
	d = initial(); memset(d.nonce,0,16); assert(ea_validate(&d,sizeof(d)));
	d = initial(); d.readers[1][0]=5; assert(ea_validate(&d,sizeof(d)));
	d = initial(); d.count=2; memcpy(d.readers[0],id,16);memcpy(d.readers[1],id,16);
	assert(ea_validate(&d,sizeof(d)));
	d = initial();
	for (unsigned i=0;i<EA_READERS;i++) { id[0]=i+1;assert(!ea_change(&d,EA_ADD,id)); }
	id[0]=100; assert(ea_change(&d,EA_ADD,id)==-ENOSPC);
	/* Simulated CAS loser must re-read. Both linearization orders are safe. */
	before=initial(); add=close=before;
	assert(!ea_change(&add,EA_ADD,id));assert(!ea_change(&close,EA_CLOSE,NULL));
	d=add;assert(!ea_change(&d,EA_CLOSE,NULL));assert(d.count==1);
	d=close;assert(ea_change(&d,EA_ADD,id)==-EBUSY);
	puts("admission state, malformed records, capacity, retry and both race orders: PASS");
	return 0;
}
