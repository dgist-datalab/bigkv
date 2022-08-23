#include "platform/dev_spdk.h"
#include "platform/handler.h"
#include "platform/request.h"

#include <stdlib.h>
#include <stdio.h>
#include <sys/queue.h>


static struct spdk_pci_addr allowed_pci_addr[MAX_ALLOWED_PCI_DEVICE_NUM];

static uint32_t g_io_unit_size = (UINT32_MAX & (~0x03));

static int g_outstanding_commands;


static bool g_latency_ssd_tracking_enable;
static uint32_t g_io_align = 0x200;
static uint32_t g_io_size_bytes = 0x200;
static int g_nr_unused_io_queues = 0;
static int g_nr_io_queues_per_ns = 1;
static bool g_warn;
static int g_queue_depth = SPDK_QD;
static bool g_no_shn_notification;
static uint32_t g_disable_sq_cmb;
static uint32_t g_max_io_md_size;
static uint32_t g_max_io_size_blocks;
static uint32_t g_metacfg_pract_flag;
static uint32_t g_metacfg_prchk_flags;


static int
nvme_init_ns_worker_ctx(struct ns_worker_ctx *ns_ctx)
{
	struct spdk_nvme_io_qpair_opts opts;
	struct ns_entry *entry = ns_ctx->entry;
	struct spdk_nvme_poll_group *group;
	struct spdk_nvme_qpair *qpair;
	int i;

	ns_ctx->u.nvme.num_active_qpairs = g_nr_io_queues_per_ns;
	ns_ctx->u.nvme.num_all_qpairs = g_nr_io_queues_per_ns + g_nr_unused_io_queues;
	ns_ctx->u.nvme.qpair = (struct spdk_nvme_qpair **)calloc(ns_ctx->u.nvme.num_all_qpairs, sizeof(struct spdk_nvme_qpair *));
	if (!ns_ctx->u.nvme.qpair) {
		return -1;
	}

	spdk_nvme_ctrlr_get_default_io_qpair_opts(entry->u.nvme.ctrlr, &opts, sizeof(opts));
	if (opts.io_queue_requests < entry->num_io_requests) {
		opts.io_queue_requests = entry->num_io_requests;
	}
	opts.io_queue_requests = 65536;
	opts.delay_cmd_submit = true;
	opts.create_only = true;

	ns_ctx->u.nvme.group = spdk_nvme_poll_group_create(NULL, NULL);
	if (ns_ctx->u.nvme.group == NULL) {
		goto poll_group_failed;
	}

	group = ns_ctx->u.nvme.group;
	for (i = 0; i < ns_ctx->u.nvme.num_all_qpairs; i++) {
		ns_ctx->u.nvme.qpair[i] = spdk_nvme_ctrlr_alloc_io_qpair(entry->u.nvme.ctrlr, &opts,
					  sizeof(opts));
		qpair = ns_ctx->u.nvme.qpair[i];
		if (!qpair) {
			printf("ERROR: spdk_nvme_ctrlr_alloc_io_qpair failed\n");
			goto qpair_failed;
		}

		if (spdk_nvme_poll_group_add(group, qpair)) {
			printf("ERROR: unable to add I/O qpair to poll group.\n");
			spdk_nvme_ctrlr_free_io_qpair(qpair);
			goto qpair_failed;
		}

		if (spdk_nvme_ctrlr_connect_io_qpair(entry->u.nvme.ctrlr, qpair)) {
			printf("ERROR: unable to connect I/O qpair.\n");
			spdk_nvme_poll_group_remove(group, qpair);
			spdk_nvme_ctrlr_free_io_qpair(qpair);
			goto qpair_failed;
		}
	}

	return 0;

qpair_failed:
	for (; i > 0; --i) {
		spdk_nvme_poll_group_remove(ns_ctx->u.nvme.group, ns_ctx->u.nvme.qpair[i - 1]);
		spdk_nvme_ctrlr_free_io_qpair(ns_ctx->u.nvme.qpair[i - 1]);
	}

	spdk_nvme_poll_group_destroy(ns_ctx->u.nvme.group);
poll_group_failed:
	free(ns_ctx->u.nvme.qpair);
	return -1;
}

static void
nvme_cleanup_ns_worker_ctx(struct ns_worker_ctx *ns_ctx)
{
	int i;

	for (i = 0; i < ns_ctx->u.nvme.num_all_qpairs; i++) {
		spdk_nvme_poll_group_remove(ns_ctx->u.nvme.group, ns_ctx->u.nvme.qpair[i]);
		spdk_nvme_ctrlr_free_io_qpair(ns_ctx->u.nvme.qpair[i]);
	}

	spdk_nvme_poll_group_destroy(ns_ctx->u.nvme.group);
	free(ns_ctx->u.nvme.qpair);
}

static void
io_data_complete(struct io_data *io_data) {
	struct callback *cb = (struct callback *)io_data->data;
#ifdef BREAKDOWN
			if (io_data->read == 1) {
				struct request *req = (struct request *)(cb->arg);
				if (req->type == REQ_TYPE_GET) {
					sw_end(req->sw_bd[4]);
					sw_start(req->sw_bd[5]);
				}
			}
#endif

	cb->func(cb->arg);
	q_enqueue((void *)cb, cb->hlr->cb_pool);
	q_enqueue((void *)io_data, cb->hlr->io_data_pool);
}

static void
io_complete(void *ctx, const struct spdk_nvme_cpl *cpl)
{
	struct io_data *io_data = (struct io_data *)ctx;

	if (spdk_unlikely(spdk_nvme_cpl_is_error(cpl))) {
		if (io_data->read) {
			printf("Read completed with error (sct=%d, sc=%d)\n",
				      cpl->status.sct, cpl->status.sc);
		} else {
			printf("Write completed with error (sct=%d, sc=%d)\n",
				      cpl->status.sct, cpl->status.sc);
		}
		if (cpl->status.sct == SPDK_NVME_SCT_GENERIC &&
		    cpl->status.sc == SPDK_NVME_SC_INVALID_NAMESPACE_OR_FORMAT) {
			/* The namespace was hotplugged.  Stop trying to send I/O to it. */
		}
	}

	io_data_complete(io_data);
}

static int
nvme_submit_io(struct io_data *io_data, struct ns_worker_ctx *ns_ctx,
	       struct ns_entry *entry)
{
	int rc;
	int qp_num;

	qp_num = ns_ctx->u.nvme.last_qpair;
	ns_ctx->u.nvme.last_qpair++;
	if (ns_ctx->u.nvme.last_qpair == ns_ctx->u.nvme.num_active_qpairs) {
		ns_ctx->u.nvme.last_qpair = 0;
	}
	
	static int num = 0;
	if (io_data->read) {
		return spdk_nvme_ns_cmd_read(entry->u.nvme.ns, ns_ctx->u.nvme.qpair[qp_num],
						    io_data->iovec.iov_base,
						    io_data->offset / entry->block_size,
							io_data->iovec.iov_len / entry->block_size,
						    io_complete,
							io_data,
						    entry->io_flags);
	} else {
		return spdk_nvme_ns_cmd_write(entry->u.nvme.ns, ns_ctx->u.nvme.qpair[qp_num],
							io_data->iovec.iov_base,
						    io_data->offset / entry->block_size,
							io_data->iovec.iov_len / entry->block_size,
						    io_complete,
							io_data,
						    entry->io_flags);
	}
}



static int
build_nvme_name(char *name, size_t length, struct spdk_nvme_ctrlr *ctrlr)
{
	const struct spdk_nvme_transport_id *trid;
	int res = 0;

	trid = spdk_nvme_ctrlr_get_transport_id(ctrlr);

	switch (trid->trtype) {
	case SPDK_NVME_TRANSPORT_PCIE:
		res = snprintf(name, length, "PCIE (%s)", trid->traddr);
		break;
	case SPDK_NVME_TRANSPORT_RDMA:
		res = snprintf(name, length, "RDMA (addr:%s subnqn:%s)", trid->traddr, trid->subnqn);
		break;
	case SPDK_NVME_TRANSPORT_TCP:
		res = snprintf(name, length, "TCP (addr:%s subnqn:%s)", trid->traddr, trid->subnqn);
		break;
	case SPDK_NVME_TRANSPORT_VFIOUSER:
		res = snprintf(name, length, "VFIOUSER (%s)", trid->traddr);
		break;
	case SPDK_NVME_TRANSPORT_CUSTOM:
		res = snprintf(name, length, "CUSTOM (%s)", trid->traddr);
		break;

	default:
		fprintf(stderr, "Unknown transport type %d\n", trid->trtype);
		break;
	}
	return res;
}

static void
build_nvme_ns_name(char *name, size_t length, struct spdk_nvme_ctrlr *ctrlr, uint32_t nsid)
{
	int res = 0;

	res = build_nvme_name(name, length, ctrlr);
	if (res > 0) {
		snprintf(name + res, length - res, " NSID %u", nsid);
	}

}

static void
register_ns(struct spdk_ctx *sctx, struct spdk_nvme_ctrlr *ctrlr, struct spdk_nvme_ns *ns)
{
	struct ns_entry *entry;
	const struct spdk_nvme_ctrlr_data *cdata;
	uint32_t max_xfer_size, entries, sector_size;
	uint64_t ns_size;
	struct spdk_nvme_io_qpair_opts opts;

	cdata = spdk_nvme_ctrlr_get_data(ctrlr);

	if (!spdk_nvme_ns_is_active(ns)) {
		printf("Controller %-20.20s (%-20.20s): Skipping inactive NS %u\n",
		       cdata->mn, cdata->sn,
		       spdk_nvme_ns_get_id(ns));
		g_warn = true;
		return;
	}

	ns_size = spdk_nvme_ns_get_size(ns);
	sector_size = spdk_nvme_ns_get_sector_size(ns);

	if (ns_size < g_io_size_bytes || sector_size > g_io_size_bytes) {
		printf("WARNING: controller %-20.20s (%-20.20s) ns %u has invalid "
		       "ns size %" PRIu64 " / block size %u for I/O size %u\n",
		       cdata->mn, cdata->sn, spdk_nvme_ns_get_id(ns),
		       ns_size, spdk_nvme_ns_get_sector_size(ns), g_io_size_bytes);
		g_warn = true;
		return;
	}

	if (g_io_size_bytes % sector_size != 0) {
		printf("WARNING: IO size %u (-o) is not a multiple of nsid %u sector size %u."
		       " Removing this ns from test\n", g_io_size_bytes, spdk_nvme_ns_get_id(ns), sector_size);
		g_warn = true;
		return;
	}

	max_xfer_size = spdk_nvme_ns_get_max_io_xfer_size(ns);
	spdk_nvme_ctrlr_get_default_io_qpair_opts(ctrlr, &opts, sizeof(opts));
	/* NVMe driver may add additional entries based on
	 * stripe size and maximum transfer size, we assume
	 * 1 more entry be used for stripe.
	 */
	entries = (g_io_size_bytes - 1) / max_xfer_size + 2;
	if ((g_queue_depth * entries) > opts.io_queue_size) {
		printf("controller IO queue size %u less than required\n",
		       opts.io_queue_size);
		printf("Consider using lower queue depth or small IO size because "
		       "IO requests may be queued at the NVMe driver.\n");
	}
	/* For requests which have children requests, parent request itself
	 * will also occupy 1 entry.
	 */
	entries += 1;

	entry = (struct ns_entry *)calloc(1, sizeof(struct ns_entry));
	if (entry == NULL) {
		perror("ns_entry malloc");
		exit(1);
	}

	entry->type = ENTRY_TYPE_NVME_NS;
	//entry->fn_table = &nvme_fn_table;
	entry->u.nvme.ctrlr = ctrlr;
	entry->u.nvme.ns = ns;
	entry->num_io_requests = g_queue_depth * entries;

	entry->size_in_ios = ns_size / g_io_size_bytes;
	entry->io_size_blocks = g_io_size_bytes / sector_size;

	entry->block_size = spdk_nvme_ns_get_extended_sector_size(ns);
	entry->md_size = spdk_nvme_ns_get_md_size(ns);
	entry->md_interleave = spdk_nvme_ns_supports_extended_lba(ns);
	entry->pi_loc = spdk_nvme_ns_get_data(ns)->dps.md_start;
	entry->pi_type = spdk_nvme_ns_get_pi_type(ns);

	if (spdk_nvme_ns_get_flags(ns) & SPDK_NVME_NS_DPS_PI_SUPPORTED) {
		entry->io_flags = g_metacfg_pract_flag | g_metacfg_prchk_flags;
	}

	/* If metadata size = 8 bytes, PI is stripped (read) or inserted (write),
	 *  and so reduce metadata size from block size.  (If metadata size > 8 bytes,
	 *  PI is passed (read) or replaced (write).  So block size is not necessary
	 *  to change.)
	 */
	if ((entry->io_flags & SPDK_NVME_IO_FLAGS_PRACT) && (entry->md_size == 8)) {
		entry->block_size = spdk_nvme_ns_get_sector_size(ns);
	}

	if (g_max_io_md_size < entry->md_size) {
		g_max_io_md_size = entry->md_size;
	}

	if (g_max_io_size_blocks < entry->io_size_blocks) {
		g_max_io_size_blocks = entry->io_size_blocks;
	}

	build_nvme_ns_name(entry->name, sizeof(entry->name), ctrlr, spdk_nvme_ns_get_id(ns));

	sctx->num_namespaces++;
	TAILQ_INSERT_TAIL(&sctx->namespaces, entry, link);
}

static void
unregister_namespaces(struct spdk_ctx *sctx)
{
	struct ns_entry *entry, *tmp;

	TAILQ_FOREACH_SAFE(entry, &sctx->namespaces, link, tmp) {
		TAILQ_REMOVE(&sctx->namespaces, entry, link);
		free(entry);
	}
}

static void
enable_latency_tracking_complete(void *cb_arg, const struct spdk_nvme_cpl *cpl)
{
	if (spdk_nvme_cpl_is_error(cpl)) {
		printf("enable_latency_tracking_complete failed\n");
	}
	g_outstanding_commands--;
}


static void
set_latency_tracking_feature(struct spdk_nvme_ctrlr *ctrlr, bool enable)
{
	int res;
	union spdk_nvme_intel_feat_latency_tracking latency_tracking;

	if (enable) {
		latency_tracking.bits.enable = 0x01;
	} else {
		latency_tracking.bits.enable = 0x00;
	}

	res = spdk_nvme_ctrlr_cmd_set_feature(ctrlr, SPDK_NVME_INTEL_FEAT_LATENCY_TRACKING,
					      latency_tracking.raw, 0, NULL, 0, enable_latency_tracking_complete, NULL);
	if (res) {
		printf("fail to allocate nvme request.\n");
		return;
	}
	g_outstanding_commands++;

	while (g_outstanding_commands) {
		spdk_nvme_ctrlr_process_admin_completions(ctrlr);
	}
}

static void
register_ctrlr(struct spdk_nvme_ctrlr *ctrlr, struct trid_entry *trid_entry)
{
	struct spdk_nvme_ns *ns;
	struct ctrlr_entry *entry = (struct ctrlr_entry *)malloc(sizeof(struct ctrlr_entry));
	uint32_t nsid;

	if (entry == NULL) {
		perror("ctrlr_entry malloc");
		exit(1);
	}

	entry->latency_page = (struct spdk_nvme_intel_rw_latency_page *)spdk_dma_zmalloc(sizeof(struct spdk_nvme_intel_rw_latency_page),
					       4096, NULL);
	if (entry->latency_page == NULL) {
		printf("Allocation error (latency page)\n");
		exit(1);
	}

	build_nvme_name(entry->name, sizeof(entry->name), ctrlr);

	entry->ctrlr = ctrlr;
	entry->trtype = trid_entry->trid.trtype;
	TAILQ_INSERT_TAIL(&trid_entry->sctx->controllers, entry, link);

	if (g_latency_ssd_tracking_enable &&
	    spdk_nvme_ctrlr_is_feature_supported(ctrlr, SPDK_NVME_INTEL_FEAT_LATENCY_TRACKING)) {
		set_latency_tracking_feature(ctrlr, true);
	}

	if (trid_entry->nsid == 0) {
		for (nsid = spdk_nvme_ctrlr_get_first_active_ns(ctrlr);
		     nsid != 0; nsid = spdk_nvme_ctrlr_get_next_active_ns(ctrlr, nsid)) {
			ns = spdk_nvme_ctrlr_get_ns(ctrlr, nsid);
			if (ns == NULL) {
				continue;
			}
			register_ns(trid_entry->sctx, ctrlr, ns);
		}
	} else {
		ns = spdk_nvme_ctrlr_get_ns(ctrlr, trid_entry->nsid);
		if (!ns) {
			perror("Namespace does not exist.");
			exit(1);
		}

		register_ns(trid_entry->sctx, ctrlr, ns);
	}
}


static bool
probe_cb(void *cb_ctx, const struct spdk_nvme_transport_id *trid,
	 struct spdk_nvme_ctrlr_opts *opts)
{
	struct trid_entry *trid_entry = (struct trid_entry *)cb_ctx;

	if (trid->trtype == SPDK_NVME_TRANSPORT_PCIE) {
		if (g_disable_sq_cmb) {
			opts->use_cmb_sqs = false;
		}
		if (g_no_shn_notification) {
			opts->no_shn_notification = true;
		}
	}

	if (trid->trtype != trid_entry->trid.trtype &&
	    strcasecmp(trid->trstring, trid_entry->trid.trstring)) {
		return false;
	}

	/* Set io_queue_size to UINT16_MAX, NVMe driver
	 * will then reduce this to MQES to maximize
	 * the io_queue_size as much as possible.
	 */
	opts->io_queue_size = UINT16_MAX;

	/* Set the header and data_digest */
	opts->header_digest = 0;
	opts->data_digest = 0;
	opts->keep_alive_timeout_ms = 10000;
	memcpy(opts->hostnqn, trid_entry->hostnqn, sizeof(opts->hostnqn));

	return true;
}

static void
attach_cb(void *cb_ctx, const struct spdk_nvme_transport_id *trid,
	  struct spdk_nvme_ctrlr *ctrlr, const struct spdk_nvme_ctrlr_opts *opts)
{
	struct trid_entry	*trid_entry = (struct trid_entry *)cb_ctx;
	struct spdk_pci_addr	pci_addr;
	struct spdk_pci_device	*pci_dev;
	struct spdk_pci_id	pci_id;

	if (trid->trtype != SPDK_NVME_TRANSPORT_PCIE) {
		printf("Attached to NVMe over Fabrics controller at %s:%s: %s\n",
		       trid->traddr, trid->trsvcid,
		       trid->subnqn);
	} else {
		if (spdk_pci_addr_parse(&pci_addr, trid->traddr)) {
			return;
		}

		pci_dev = spdk_nvme_ctrlr_get_pci_device(ctrlr);
		if (!pci_dev) {
			return;
		}

		pci_id = spdk_pci_device_get_id(pci_dev);

		printf("Attached to NVMe Controller at %s [%04x:%04x]\n",
		       trid->traddr,
		       pci_id.vendor_id, pci_id.device_id);
	}

	register_ctrlr(ctrlr, trid_entry);
}





static int
register_controllers(struct spdk_ctx *sctx)
{
	struct trid_entry *trid_entry;

	printf("Initializing NVMe Controllers\n");

	TAILQ_FOREACH(trid_entry, &sctx->trid_list, tailq) {
		printf("probe\n");
		if (spdk_nvme_probe(&trid_entry->trid, trid_entry, probe_cb, attach_cb, NULL) != 0) {
			fprintf(stderr, "spdk_nvme_probe() failed for transport address '%s'\n",
				trid_entry->trid.traddr);
			return -1;
		}
	}

	return 0;
}

static void
unregister_controllers(struct spdk_ctx *sctx)
{
	struct ctrlr_entry *entry, *tmp;
	struct spdk_nvme_detach_ctx *detach_ctx = NULL;

	TAILQ_FOREACH_SAFE(entry, &sctx->controllers, link, tmp) {
		TAILQ_REMOVE(&sctx->controllers, entry, link);

		spdk_dma_free(entry->latency_page);
		if (g_latency_ssd_tracking_enable &&
		    spdk_nvme_ctrlr_is_feature_supported(entry->ctrlr, SPDK_NVME_INTEL_FEAT_LATENCY_TRACKING)) {
			set_latency_tracking_feature(entry->ctrlr, false);
		}

		if (g_nr_unused_io_queues) {
			int i;

			for (i = 0; i < g_nr_unused_io_queues; i++) {
				spdk_nvme_ctrlr_free_io_qpair(entry->unused_qpairs[i]);
			}

			free(entry->unused_qpairs);
		}

		spdk_nvme_detach_async(entry->ctrlr, &detach_ctx);
		free(entry);
	}

	if (detach_ctx) {
		spdk_nvme_detach_poll(detach_ctx);
	}
}

static void
unregister_trids(struct spdk_ctx *sctx)
{
	struct trid_entry *trid_entry, *tmp;

	TAILQ_FOREACH_SAFE(trid_entry, &sctx->trid_list, tailq, tmp) {
		TAILQ_REMOVE(&sctx->trid_list, trid_entry, tailq);
		free(trid_entry);
	}
}

static int
add_trid(struct spdk_ctx *sctx, const char *trid_str)
{
	struct trid_entry *trid_entry;
	struct spdk_nvme_transport_id *trid;
	char *ns;
	char *hostnqn;

	trid_entry = (struct trid_entry *)calloc(1, sizeof(*trid_entry));
	if (trid_entry == NULL) {
		return -1;
	}

	trid = &trid_entry->trid;
	trid->trtype = SPDK_NVME_TRANSPORT_PCIE;
	snprintf(trid->subnqn, sizeof(trid->subnqn), "%s", SPDK_NVMF_DISCOVERY_NQN);

	printf("%s\n", trid_str);

	if (spdk_nvme_transport_id_parse(trid, trid_str) != 0) {
		fprintf(stderr, "Invalid transport ID format '%s'\n", trid_str);
		free(trid_entry);
		return 1;
	}

	spdk_nvme_transport_id_populate_trstring(trid,
			spdk_nvme_transport_id_trtype_str(trid->trtype));

	ns = strcasestr((char *)trid_str, "ns:");
	if (ns) {
		char nsid_str[6]; /* 5 digits maximum in an nsid */
		int len;
		int nsid;

		ns += 3;

		len = strcspn(ns, " \t\n");
		if (len > 5) {
			fprintf(stderr, "NVMe namespace IDs must be 5 digits or less\n");
			free(trid_entry);
			return 1;
		}

		memcpy(nsid_str, ns, len);
		nsid_str[len] = '\0';

		nsid = spdk_strtol(nsid_str, 10);
		if (nsid <= 0 || nsid > 65535) {
			fprintf(stderr, "NVMe namespace IDs must be less than 65536 and greater than 0\n");
			free(trid_entry);
			return 1;
		}

		trid_entry->nsid = (uint16_t)nsid;
	}

	hostnqn = strcasestr((char *)trid_str, "hostnqn:");
	if (hostnqn) {
		size_t len;

		hostnqn += strlen("hostnqn:");

		len = strcspn(hostnqn, " \t\n");
		if (len > (sizeof(trid_entry->hostnqn) - 1)) {
			fprintf(stderr, "Host NQN is too long\n");
			free(trid_entry);
			return 1;
		}

		memcpy(trid_entry->hostnqn, hostnqn, len);
		trid_entry->hostnqn[len] = '\0';
	}

	trid_entry->sctx = sctx;
	TAILQ_INSERT_TAIL(&sctx->trid_list, trid_entry, tailq);
	return 0;
}


static int
add_allowed_pci_device(const char *bdf_str, struct spdk_env_opts *env_opts)
{
	int rc;

	if (env_opts->num_pci_addr >= MAX_ALLOWED_PCI_DEVICE_NUM) {
		fprintf(stderr, "Currently we only support allowed PCI device num=%d\n",
			MAX_ALLOWED_PCI_DEVICE_NUM);
		return -1;
	}

	rc = spdk_pci_addr_parse(&env_opts->pci_allowed[env_opts->num_pci_addr], bdf_str);
	if (rc < 0) {
		fprintf(stderr, "Failed to parse the given bdf_str=%s\n", bdf_str);
		return -1;
	}

	env_opts->num_pci_addr++;
	return 0;
}

static int
associate_workers_with_ns(struct spdk_ctx *sctx)
{
	struct ns_entry		*entry = TAILQ_FIRST(&sctx->namespaces);
	struct ns_worker_ctx	*ns_ctx;
	int			i, count;

	count = sctx->num_namespaces;

	for (i = 0; i < count; i++) {
		if (entry == NULL) {
			break;
		}

		ns_ctx = (struct ns_worker_ctx *)calloc(1, sizeof(struct ns_worker_ctx));
		if (!ns_ctx) {
			return -1;
		}
		ns_ctx->stats.min_tsc = UINT64_MAX;
		ns_ctx->entry = entry;
		TAILQ_INSERT_TAIL(&sctx->ns_ctx, ns_ctx, link);

		entry = TAILQ_NEXT(entry, link);
		if (entry == NULL) {
			entry = TAILQ_FIRST(&sctx->namespaces);
		}

	}

	return 0;
}

int dev_spdk_open (struct spdk_ctx *sctx, char *dev_name)
{
	int rc;
	char str_trid[128] = {0,};
	struct ns_worker_ctx *ns_ctx;

	sctx->trid_list = TAILQ_HEAD_INITIALIZER(sctx->trid_list);
	sctx->controllers = TAILQ_HEAD_INITIALIZER(sctx->controllers);
	sctx->namespaces = TAILQ_HEAD_INITIALIZER(sctx->namespaces);
	sctx->ns_ctx = TAILQ_HEAD_INITIALIZER(sctx->ns_ctx);
	sctx->num_namespaces = 0;

	snprintf(str_trid, 128, "trtype:PCIe traddr:%s", dev_name);
	add_trid(sctx, str_trid);

	if (register_controllers(sctx) != 0) {
		rc = -1;
		goto cleanup;
	}

	if (g_warn) {
		printf("WARNING: Some requested NVMe devices were skipped\n");
	}

	if (sctx->num_namespaces == 0) {
		fprintf(stderr, "No valid NVMe controllers or AIO or URING devices found\n");
		goto cleanup;
	}
	
	if (associate_workers_with_ns(sctx) != 0) {
		rc = -1;
		goto cleanup;
	}

	TAILQ_FOREACH(ns_ctx, &sctx->ns_ctx, link) {
		nvme_init_ns_worker_ctx(ns_ctx);
	}




cleanup:

	return rc;
}

int dev_spdk_env_init (char *core_mask)
{
	int rc;
	struct spdk_env_opts opts;
	char str_mask[16] = {0,};

	snprintf(str_mask, 16, "%s", core_mask);

	spdk_env_opts_init(&opts);
	opts.name = "bigkv_spdk";
	opts.core_mask = str_mask;
	if (spdk_env_init(&opts) < 0) {
		fprintf(stderr, "Unable to initialize SPDK env\n");
		rc = -1;
	}
	uint32_t i;
	SPDK_ENV_FOREACH_CORE(i) {
		printf("core %d\n", i);
	}

	return rc;
}

int dev_spdk_close (struct spdk_ctx *sctx) {
	struct ns_worker_ctx *ns_ctx;

	TAILQ_FOREACH(ns_ctx, &sctx->ns_ctx, link) {
		nvme_cleanup_ns_worker_ctx(ns_ctx);
	}

	unregister_trids(sctx);
	unregister_namespaces(sctx);
	unregister_controllers(sctx);

	return 0;
}


int
dev_spdk_read(struct handler *hlr, struct dev_abs *dev, uint64_t addr_in_byte, uint32_t size,
	 char *buf, struct callback *cb) {

	int rc = 0;

	struct ns_worker_ctx *ns_ctx;
	struct spdk_ctx *sctx = dev->sctx;
	struct io_data *io_data;

	TAILQ_FOREACH(ns_ctx, &sctx->ns_ctx, link) {
		while ((io_data = (struct io_data *)q_dequeue(cb->hlr->io_data_pool)) == NULL);

		io_data->iovec.iov_base = buf;
		io_data->iovec.iov_len = size;
		io_data->data = cb;
		io_data->read = 1;
		io_data->offset = addr_in_byte;

		if((rc = nvme_submit_io(io_data, ns_ctx, ns_ctx->entry)) < 0) {
			perror("nvme_sumbit");
			rc = -1;
			abort();
		}
	}

	return rc;
}

int
dev_spdk_write(struct handler *hlr, struct dev_abs *dev, uint64_t addr_in_byte, uint32_t size,
	  char *buf, struct callback *cb) {

	int rc = 0;

	struct ns_worker_ctx *ns_ctx;
	struct spdk_ctx *sctx = dev->sctx;
	struct io_data *io_data;

	TAILQ_FOREACH(ns_ctx, &sctx->ns_ctx, link) {
		while ((io_data = (struct io_data *)q_dequeue(cb->hlr->io_data_pool)) == NULL);

		io_data->iovec.iov_base = buf;
		io_data->iovec.iov_len = size;
		io_data->data = cb;
		io_data->read = 0;
		io_data->offset = addr_in_byte;

		if((rc = nvme_submit_io(io_data, ns_ctx, ns_ctx->entry)) < 0) {
			rc = -1;
			abort();
		}

	}

	return rc;
}
