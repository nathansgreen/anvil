/* This file is part of Anvil. Anvil is copyright 2007-2009 The Regents
 * of the University of California. It is distributed under the terms of
 * version 2 of the GNU GPL. See the file LICENSE for details. */

#include "dtable.h"

/* that's it, that's all this file is here for */
atomic<abortable_tx> dtable::atx_handle(NO_ABORTABLE_TX);
