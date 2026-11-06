/* SPDX-License-Identifier: Apache-2.0 */
#ifndef SOURCE_CONTROL_TICKET_H
#define SOURCE_CONTROL_TICKET_H
#include "vbdev_s3lvol.h"

struct source_control_ticket {
	struct s3lvol_lvstore *lvs;
	s3lvol_lvol_op_cb lvol_cb;
	spdk_lvol_op_complete cb;
	void *arg;
};

static inline void
source_control_lvol_done(void *arg, struct spdk_lvol *lvol, int status)
{
	struct source_control_ticket *t = arg;
	if (t->lvol_cb) t->lvol_cb(t->arg, lvol, status);
	s3lvol_control_leave(t->lvs, status);
	free(t);
}

static inline void
source_control_done(void *arg, int status)
{
	struct source_control_ticket *t = arg;
	if (t->cb) t->cb(t->arg, status);
	s3lvol_control_leave(t->lvs, status);
	free(t);
}
#endif
