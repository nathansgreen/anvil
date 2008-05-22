<?php

$toilet_init_path = "/mnt/test";
$toilet_db_path = "/mnt/test/invite";

/* takes an array of rows, each of which is an associative array of key => value */
function sort_rows_by($rows, $column, $desc = false)
{
	$sort_column = array();
	foreach($rows as $index => $row)
		$sort_column[$index] = $row[$column];
	array_multisort($sort_column, $desc ? SORT_DESC : SORT_ASC, $rows);
}

/* takes an array of rowids and optionally an array of column names, returns an array of associative row arrays */
function rowids_get_rows($rowids, $columns = null)
{
	if($columns)
	{
		$columns = array_fill(0, count($rowids), $columns);
		return array_map("rowid_get_row", $rowids, $columns);
	}
	return array_map("rowid_get_row", $rowids);
}

/* takes an array of rowids, a column name to sort by, and optionally whether to sort descending
   and an array of column names, returns a sorted array of associative row arrays */
function rowids_get_sorted_rows($rowids, $sort, $desc = false, $columns = null)
{
	$rows = rowids_get_rows($rowids, $columns);
	sort_rows_by($rows, $sort, $desc);
	return $rows;
}

?>
