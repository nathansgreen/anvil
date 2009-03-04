/* This file is part of Toilet. Toilet is copyright 2007-2009 The Regents
 * of the University of California. It is distributed under the terms of
 * version 2 of the GNU GPL. See the file LICENSE for details. */

#ifndef __TPCH_CONFIG_H
#define __TPCH_CONFIG_H

#ifndef __cplusplus
#error tpch_config.h is a C++ header file
#endif

#include "params.h"

/* part:
 *  p_partkey,
 *  p_name (55v),
 *  p_mfgr (25f),
 *  p_brand (10f),
 *  p_type (25v),
 *  p_size (int),
 *  p_container (10f),
 *  p_retailprice (float),
 *  p_comment (23v) */
static const char * tpch_part_column_config = LITERAL(
	config [
		"columns" int 8
		"base" class(dt) managed_dtable
		"base_config" config [
			"base" class(dt) simple_dtable
			"digest_on_close" bool true
		]
		"column0_name" string "p_name"
		"column1_name" string "p_mfgr"
		"column2_name" string "p_brand"
		"column3_name" string "p_type"
		"column4_name" string "p_size"
		"column5_name" string "p_container"
		"column6_name" string "p_retailprice"
		"column7_name" string "p_comment"
		"column1_config" config [
			"base" class(dt) exception_dtable
			"base_config" config [
				"base" class(dt) array_dtable
				"alt" class(dt) simple_dtable
				"reject_value" string "______________"
			]
			"digest_on_close" bool true
		]
		"column2_config" config [
			"base" class(dt) exception_dtable
			"base_config" config [
				"base" class(dt) array_dtable
				"alt" class(dt) simple_dtable
				"reject_value" string "________"
			]
			"digest_on_close" bool true
		]
		"column4_config" config [
			"base" class(dt) exception_dtable
			"base_config" config [
				"base" class(dt) smallint_dtable
				"base_config" config [
					"base" class(dt) array_dtable
					"bytes" int 1
				]
				"alt" class(dt) simple_dtable
				"reject_value" blob 00000000
			]
			"digest_on_close" bool true
		]
		"column6_config" config [
			"base" class(dt) array_dtable
			"base_config" config [
				"value_size" int 4
			]
			"digest_on_close" bool true
		]
	]);

/* customer:
 *  c_custkey,
 *  c_name (25v),
 *  c_address (40v),
 *  c_nationkey (id),
 *  c_phone (15f),
 *  c_acctbal (float),
 *  c_mktsegment (10f),
 *  c_comment (117v) */
static const char * tpch_customer_column_config = LITERAL(
	config [
		"columns" int 7
		"base" class(dt) managed_dtable
		"base_config" config [
			"base" class(dt) simple_dtable
			"digest_on_close" bool true
		]
		"column0_name" string "c_name"
		"column1_name" string "c_address"
		"column2_name" string "c_nationkey"
		"column3_name" string "c_phone"
		"column4_name" string "c_acctbal"
		"column5_name" string "c_mktsegment"
		"column6_name" string "c_comment"
		"column2_config" config [
			"base" class(dt) exception_dtable
			"base_config" config [
				"base" class(dt) smallint_dtable
				"base_config" config [
					"base" class(dt) array_dtable
					"bytes" int 1
				]
				"alt" class(dt) simple_dtable
				"reject_value" blob 00000000
			]
			"digest_on_close" bool true
		]
		"column4_config" config [
			"base" class(dt) array_dtable
			"base_config" config [
				"value_size" int 4
			]
			"digest_on_close" bool true
		]
	]);

/* orders:
 *  o_orderkey,
 *  o_custkey (id),
 *  o_orderstatus (1f),
 *  o_totalprice (float),
 *  o_orderdate (date),
 *  o_orderpriority (15f),
 *  o_clerk (15f),
 *  o_shippriority (int),
 *  o_comment(79v) */
static const char * tpch_orders_column_config = LITERAL(
	config [
		"columns" int 8
		"base" class(dt) managed_dtable
		"base_config" config [
			"base" class(dt) simple_dtable
			"digest_on_close" bool true
		]
		"column0_name" string "o_custkey"
		"column1_name" string "o_orderstatus"
		"column2_name" string "o_totalprice"
		"column3_name" string "o_orderdate"
		"column4_name" string "o_orderpriority"
		"column5_name" string "o_clerk"
		"column6_name" string "o_shippriority"
		"column7_name" string "o_comment"
		"column0_config" config [
			"base" class(dt) exception_dtable
			"base_config" config [
				"base" class(dt) smallint_dtable
				"base_config" config [
					"base" class(dt) fixed_dtable
					"bytes" int 3
				]
				"alt" class(dt) simple_dtable
				"reject_value" blob 00FFFFFF
			]
			"digest_on_close" bool true
		]
		"column1_config" config [
			"base" class(dt) exception_dtable
			"base_config" config [
				"base" class(dt) fixed_dtable
				"alt" class(dt) simple_dtable
				"reject_value" string "_"
			]
			"digest_on_close" bool true
		]
		"column2_config" config [
			"base" class(dt) fixed_dtable
			"base_config" config [
				"value_size" int 4
			]
			"digest_on_close" bool true
		]
		"column6_config" config [
			"base" class(dt) exception_dtable
			"base_config" config [
				"base" class(dt) smallint_dtable
				"base_config" config [
					"base" class(dt) fixed_dtable
					"bytes" int 1
				]
				"alt" class(dt) simple_dtable
				"reject_value" blob 000000FF
			]
			"digest_on_close" bool true
		]
	]);

/* lineitem:
 *  (no key)
 *  l_orderkey (id),
 *  l_partkey (id),
 *  l_suppkey (id),
 *  l_linenumber (int),
 *  l_quantity (float),
 *  l_extendedprice (float),
 *  l_discount (float),
 *  l_tax (float),
 *  l_returnflag (1f),
 *  l_linestatus (1f),
 *  l_shipdate (date),
 *  l_commitdate (date),
 *  l_receiptdate (date),
 *  l_shipinstruct (25f),
 *  l_shipmode (10f),
 *  l_comment (44v) */
static const char * tpch_lineitem_column_config = LITERAL(
	config [
		"columns" int 16
		"base" class(dt) managed_dtable
		"base_config" config [
			"base" class(dt) simple_dtable
			"digest_on_close" bool true
		]
		"column0_name" string "l_orderkey"
		"column1_name" string "l_partkey"
		"column2_name" string "l_suppkey"
		"column3_name" string "l_linenumber"
		"column4_name" string "l_quantity"
		"column5_name" string "l_extendedprice"
		"column6_name" string "l_discount"
		"column7_name" string "l_tax"
		"column8_name" string "l_returnflag"
		"column9_name" string "l_linestatus"
		"column10_name" string "l_shipdate"
		"column11_name" string "l_commitdate"
		"column12_name" string "l_receiptdate"
		"column13_name" string "l_shipinstruct"
		"column14_name" string "l_shipmode"
		"column15_name" string "l_comment"
		"column0_config" config [
			"base" class(dt) exception_dtable
			"base_config" config [
				"base" class(dt) smallint_dtable
				"base_config" config [
					"base" class(dt) array_dtable
					"bytes" int 3
				]
				"alt" class(dt) simple_dtable
				"reject_value" blob 00FFFFFF
			]
			"digest_on_close" bool true
		]
		"column1_config" config [
			"base" class(dt) exception_dtable
			"base_config" config [
				"base" class(dt) smallint_dtable
				"base_config" config [
					"base" class(dt) array_dtable
					"bytes" int 3
				]
				"alt" class(dt) simple_dtable
				"reject_value" blob 00FFFFFF
			]
			"digest_on_close" bool true
		]
		"column2_config" config [
			"base" class(dt) exception_dtable
			"base_config" config [
				"base" class(dt) smallint_dtable
				"base_config" config [
					"base" class(dt) array_dtable
					"bytes" int 3
				]
				"alt" class(dt) simple_dtable
				"reject_value" blob 00FFFFFF
			]
			"digest_on_close" bool true
		]
		"column3_config" config [
			"base" class(dt) exception_dtable
			"base_config" config [
				"base" class(dt) smallint_dtable
				"base_config" config [
					"base" class(dt) array_dtable
					"bytes" int 1
				]
				"alt" class(dt) simple_dtable
				"reject_value" blob 00000000
			]
			"digest_on_close" bool true
		]
		"column4_config" config [
			"base" class(dt) array_dtable
			"base_config" config [
				"value_size" int 4
			]
			"digest_on_close" bool true
		]
		"column5_config" config [
			"base" class(dt) array_dtable
			"base_config" config [
				"value_size" int 4
			]
			"digest_on_close" bool true
		]
		"column6_config" config [
			"base" class(dt) array_dtable
			"base_config" config [
				"value_size" int 4
			]
			"digest_on_close" bool true
		]
		"column7_config" config [
			"base" class(dt) array_dtable
			"base_config" config [
				"value_size" int 4
			]
			"digest_on_close" bool true
		]
		"column8_config" config [
			"base" class(dt) exception_dtable
			"base_config" config [
				"base" class(dt) array_dtable
				"alt" class(dt) simple_dtable
				"reject_value" string "_"
			]
			"digest_on_close" bool true
		]
		"column9_config" config [
			"base" class(dt) exception_dtable
			"base_config" config [
				"base" class(dt) array_dtable
				"alt" class(dt) simple_dtable
				"reject_value" string "_"
			]
			"digest_on_close" bool true
		]
	]);

/* the configurations for the row store version are much simpler... */

static const char * tpch_part_row_config = LITERAL(
	config [
		"columns" int 8
		"base" class(dt) managed_dtable
		"base_config" config [
			"base" class(dt) simple_dtable
			"digest_on_close" bool true
		]
		"column0_name" string "p_name"
		"column1_name" string "p_mfgr"
		"column2_name" string "p_brand"
		"column3_name" string "p_type"
		"column4_name" string "p_size"
		"column5_name" string "p_container"
		"column6_name" string "p_retailprice"
		"column7_name" string "p_comment"
	]);

static const char * tpch_customer_row_config = LITERAL(
	config [
		"columns" int 7
		"base" class(dt) managed_dtable
		"base_config" config [
			"base" class(dt) simple_dtable
			"digest_on_close" bool true
		]
		"column0_name" string "c_name"
		"column1_name" string "c_address"
		"column2_name" string "c_nationkey"
		"column3_name" string "c_phone"
		"column4_name" string "c_acctbal"
		"column5_name" string "c_mktsegment"
		"column6_name" string "c_comment"
	]);

static const char * tpch_orders_row_config = LITERAL(
	config [
		"columns" int 8
		"base" class(dt) managed_dtable
		"base_config" config [
			"base" class(dt) simple_dtable
			"digest_on_close" bool true
		]
		"column0_name" string "o_custkey"
		"column1_name" string "o_orderstatus"
		"column2_name" string "o_totalprice"
		"column3_name" string "o_orderdate"
		"column4_name" string "o_orderpriority"
		"column5_name" string "o_clerk"
		"column6_name" string "o_shippriority"
		"column7_name" string "o_comment"
	]);

static const char * tpch_lineitem_row_config = LITERAL(
	config [
		"columns" int 16
		"base" class(dt) managed_dtable
		"base_config" config [
			"base" class(dt) simple_dtable
			"digest_on_close" bool true
		]
		"column0_name" string "l_orderkey"
		"column1_name" string "l_partkey"
		"column2_name" string "l_suppkey"
		"column3_name" string "l_linenumber"
		"column4_name" string "l_quantity"
		"column5_name" string "l_extendedprice"
		"column6_name" string "l_discount"
		"column7_name" string "l_tax"
		"column8_name" string "l_returnflag"
		"column9_name" string "l_linestatus"
		"column10_name" string "l_shipdate"
		"column11_name" string "l_commitdate"
		"column12_name" string "l_receiptdate"
		"column13_name" string "l_shipinstruct"
		"column14_name" string "l_shipmode"
		"column15_name" string "l_comment"
	]);

#endif /* __TPCH_CONFIG_H */
