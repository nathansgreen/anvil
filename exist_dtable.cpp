/* This file is part of Anvil. Anvil is copyright 2007-2009 The Regents
 * of the University of California. It is distributed under the terms of
 * version 2 of the GNU GPL. See the file LICENSE for details. */

#define _ATFILE_SOURCE

#include <errno.h>
#include <unistd.h>
#include <assert.h>
#include <string.h>

#include "openat.h"

#include "util.h"
#include "overlay_dtable.h"
#include "dtable_skip_iter.h"
#include "exist_dtable.h"

dtable * exist_dtable_factory::open(int dfd, const char * name, const params & config, sys_journal * sysj) const
{
	int e_dfd;
	dtable * base;
	dtable * dnebase;
	overlay_dtable * exist;
	const dtable_factory * base_factory;
	const dtable_factory * dnebase_factory;
	params base_config, dnebase_config;
	base_factory = dtable_factory::lookup(config, "base");
	dnebase_factory = dtable_factory::lookup(config, "dnebase");
	if(!base_factory || !dnebase_factory)
		return NULL;
	if(!config.get("base_config", &base_config, params()))
		return NULL;
	if(!config.get("dnebase_config", &dnebase_config, params()))
		return NULL;
	e_dfd = openat(dfd, name, O_RDONLY);
	if(e_dfd < 0)
		return NULL;
	base = base_factory->open(e_dfd, "base", base_config, sysj);
	if(!base)
		goto fail_base;
	dnebase = dnebase_factory->open(e_dfd, "dnebase", dnebase_config, sysj);
	if(!dnebase)
		goto fail_dnebase;
	
	if(base->key_type() != dnebase->key_type())
		goto fail_exist;
	exist = new overlay_dtable;
	if(!exist)
		goto fail_exist;
	if(exist->init(base, dnebase, NULL) < 0)
		goto fail_init;
	
	close(e_dfd);
	return exist;
	
fail_init:
	delete exist;
fail_exist:
	dnebase->destroy();
fail_dnebase:
	base->destroy();
fail_base:
	close(e_dfd);
	return NULL;
}

int exist_dtable_factory::create(int dfd, const char * name, const params & config, dtable::iter * source, const ktable * shadow) const
{
	return exist_dtable::create(dfd, name, config, source, shadow);
}

int exist_dtable::create(int dfd, const char * file, const params & config, dtable::iter * source, const ktable * shadow)
{
	int e_dfd, r;
	params base_config, dnebase_config;
	const dtable_factory * base = dtable_factory::lookup(config, "base");
	const dtable_factory * dnebase = dtable_factory::lookup(config, "dnebase");
	if(!base || !dnebase)
		return -ENOENT;
	if(!config.get("base_config", &base_config, params()))
		return -EINVAL;
	if(!config.get("dnebase_config", &dnebase_config, params()))
		return -EINVAL;
	
	if(!source_shadow_ok(source, shadow))
		return -EINVAL;
	
	r = mkdirat(dfd, file, 0755);
	if(r < 0)
		return r;
	e_dfd = openat(dfd, file, O_RDONLY);
	if(e_dfd < 0)
		goto fail_open;
	
	/* just to be sure */
	source->first();
	{
		dtable_skip_iter<dne_skip_test> base_source(source);
		r = base->create(e_dfd, "base", base_config, &base_source, NULL);
		if(r < 0)
			goto fail_base;
	}
	
	source->first();
	{
		full_ktable full_shadow(source);
		nonshadow_skip_test skip_test(shadow);
		dtable_skip_iter<nonshadow_skip_test> dnebase_source(source, skip_test);
		r = dnebase->create(e_dfd, "dnebase", dnebase_config, &dnebase_source, &full_shadow);
		if(r < 0)
			goto fail_dnebase;
	}
	
	close(e_dfd);
	return 0;
	
fail_dnebase:
	util::rm_r(e_dfd, "base");
fail_base:
	close(e_dfd);
fail_open:
	unlinkat(dfd, file, AT_REMOVEDIR);
	return (r < 0) ? r : -1;
}

const exist_dtable_factory exist_dtable::factory;
