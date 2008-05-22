<HTML>
<HEAD>
<LINK REL="StyleSheet" TYPE="text/css" HREF="style.css">
<?php

include "util.php";

function count_replies($guests, $event, $reply, $org = null)
{
	$rows = gtable_query($guests, "event", $event);
	$rows = rowids_get_rows($rows, array("id", "reply", "heads", "emails"));
	$result = 0;
	if($reply == "Y" || $reply == "M")
		foreach($rows as $row)
		{
			if($row["reply"] != $reply)
				continue;
			$result += $row["heads"];
		}
	else
		foreach($rows as $row)
		{
			if($row["reply"] != $reply)
				continue;
			if(!$reply && !$row["emails"] && !rowid_equal($row["id"], $org))
				continue;
			$result++;
		}
	echo $result;
}

function show_replies($guests, $event, $reply, $org)
{
	$rows = gtable_query($guests, "event", $event);
	$rows = rowids_get_sorted_rows($rows, "name", false, array("id", "name", "email", "emails", "reply", "heads", "comments"));
	foreach($rows as $row)
	{
		if($row["reply"] != $reply)
			continue;
		$is_org = rowid_equal($row["id"], $org);
		if(!$row["reply"] && !$row["emails"] && !$is_org)
			continue;
		$name = $row["name"];
		$email = htmlspecialchars($row["email"]);
		$heads = $row["heads"];
		$comments = htmlspecialchars($row["comments"]);
		echo "<B><A HREF=\"mailto:$email\">$name</A>";
		if($is_org)
			echo " (the organizer)";
		if($heads > 1)
			echo " + " . ($heads - 1) . (($heads > 2) ? " people" : " person");
		echo "</B><BR>\n";
		if($comments)
			echo "<DIV CLASS=\"comments\">$comments</DIV>\n";
	}
}

function get_reply($guest)
{
	global $reply, $heads, $comments;
	$row = rowid_get_row($guest, array("reply", "heads", "comments"));
	if($row && $row["reply"])
	{
		$reply = $row["reply"];
		$heads = $row["heads"];
		$comments = htmlspecialchars($row["comments"]);
	}
	else
	{
		$reply = null;
		$heads = 0;
		$comments = "";
	}
}

function process_reply($guest)
{
	$reply = $_REQUEST["reply"];
	$heads = $_REQUEST["heads"];
	$comments = $_REQUEST["comments"];
	if($reply == "N")
		$heads = 0;
	else if($heads < 1)
		$heads = 1;
	else if($heads > 50)
		$heads = 50;
	rowid_set_values($guest, array("reply" => $reply, "heads" => $heads, "comments" => $comments));
	get_reply($guest);
	return 0;
}

toilet_init($toilet_init_path);
$db = toilet_open($toilet_db_path);
$events = toilet_gtable($db, "events");
$guests = toilet_gtable($db, "guests");

$error = null;
if(isset($_REQUEST["invite"]))
{
	$invite = rtrim($_REQUEST["invite"], ".");
	
	$rows = gtable_query($guests, "hash", $invite);
	if($rows[0])
	{
		$guest = $rows[0];
		$row = rowid_get_row($guest, array("event", "name", "email"));
		$event = $row["event"];
		$name = htmlspecialchars($row["name"]);
		$email = htmlspecialchars($row["email"]);
		$row = rowid_get_row($event);
		if($row)
		{
			$hash = $row["hash"];
			$title = htmlspecialchars($row["name"]);
			$desc = str_replace("\n", "<BR>\n", htmlspecialchars($row["description"]));
			$desc = preg_replace('/\[http([^]]*)]/', '<A HREF="http\1">http\1</A>', $desc);
			$where = htmlspecialchars($row["location"]);
			$where = preg_replace('/\[http([^]]*)]/', '<A HREF="http\1">http\1</A>', $where);
			$org_id = $row["organizer"];
			$time = date("g:i A", $row["time"]);
			$day = date("l\, F jS Y", $row["time"]);
			if(isset($_REQUEST["unreply"]))
				rowid_set_values($guest, array("reply" => null, "heads" => null, "comments" => null));
			get_reply($guest);
			$row = rowid_get_row($org_id, array("name"));
			if($row)
				$org = $row["name"];
			else
				$error = "Database error.";
			
			if(isset($_REQUEST["reply"]) && !isset($_REQUEST["unreply"]))
			{
				if(process_reply($guest) < 0)
					$error = "Error processing reply.";
				else
					$message = "Your reply has been received.";
			}
		}
		else
			$error = "Database error.";
	}
	else
		$error = "Invitation not found.";
}
else
	$error = "No invitation code.<BR><A HREF=\"new.php\">New event</A>";

if(!$error)
{
?>
<TITLE>Invite-o-toilet: <? echo $title; ?></TITLE>
</HEAD>
<BODY>
<TABLE CELLPADDING=5 CELLSPACING=5>
<TR>
	<TD WIDTH=1 HEIGHT=1><IMG SRC="toilet.png"></TD>
	<TD WIDTH="100%" VALIGN=TOP>
	<H1><? echo $title; ?></H1>
	<H3><? echo $desc; ?></H3>
	Organizer: <? echo $org; ?><BR>
	Date: <? echo $day; ?><BR>
	Time: <? echo $time; ?><BR>
	Location: <? echo $where; ?><BR>
	</TD>
</TR>
<TR><TD COLSPAN=2>
	<H2><? echo $message ? $message : "$name, you're invited!"; ?></H2>
	<DIV ID="status">
	<TABLE CELLPADDING=2 CELLSPACING=0 CLASS="yes"><TR><TH>Yes (<? count_replies($guests, $event, "Y"); ?>)</TH></TR><TR><TD>
	<? show_replies($guests, $event, "Y", $org_id); ?>
	</TD></TR></TABLE>
	<TABLE CELLPADDING=2 CELLSPACING=0 CLASS="maybe"><TR><TH>Maybe (<? count_replies($guests, $event, "M"); ?>)</TH></TR><TR><TD>
	<? show_replies($guests, $event, "M", $org_id); ?>
	</TD></TR></TABLE>
	<TABLE CELLPADDING=2 CELLSPACING=0 CLASS="no"><TR><TH>No (<? count_replies($guests, $event, "N"); ?>)</TH></TR><TR><TD>
	<? show_replies($guests, $event, "N", $org_id); ?>
	</TD></TR></TABLE>
	<TABLE CELLPADDING=2 CELLSPACING=0 CLASS="noreply"><TR><TH>Not yet replied (<? count_replies($guests, $event, null, $org_id); ?>)</TH></TR><TR><TD>
	<? show_replies($guests, $event, null, $org_id); ?>
	</TD></TR></TABLE>
	</DIV>
<?php
	if($reply)
		echo "<DIV CLASS=\"note\">You've already replied. To change your reply, use the form below.</DIV>\n";
?>
	<FORM ACTION="?invite=<? echo $invite ?>" METHOD="POST">
	Will you attend? &nbsp;
	<SELECT NAME="reply">
<?php
	echo "<OPTION VALUE=\"Y\"";
	if($reply == "Y")
		echo " SELECTED";
	echo ">Yes</OPTION>\n";
	echo "<OPTION VALUE=\"M\"";
	if($reply == "M")
		echo " SELECTED";
	echo ">Maybe</OPTION>\n";
	echo "<OPTION VALUE=\"N\"";
	if($reply == "N")
		echo " SELECTED";
	echo ">No</OPTION>\n";
?>
	</SELECT><BR>
	Number of guests (including you): &nbsp;
	<INPUT TYPE="TEXT" NAME="heads" VALUE="<? echo $heads ? $heads : 1; ?>" SIZE=4><BR>
	Comments:<BR>
	<TEXTAREA NAME="comments" COLS=40 ROWS=5><? echo $comments; ?></TEXTAREA><BR>
	<INPUT TYPE="submit" VALUE="Reply">
	<? if($reply) echo "<INPUT TYPE=\"submit\" VALUE=\"Unreply\" NAME=\"unreply\">\n"; ?>
	</FORM>
	<BR ID="clear">
</TD></TR>
</TABLE>
<?php
	if(rowid_equal($guest, $org_id))
	{
?>
<DIV CLASS="comments"><A HREF="manage.php?event=<? echo $hash; ?>">Manage this event</A></DIV>
<?php
	}
}
else
{
?>
<TITLE>Invite-o-toilet</TITLE>
</HEAD>
<BODY>
<DIV ALIGN=CENTER>
<TABLE CELLPADDING=0 CELLSPACING=0 HEIGHT="100%">
<TR><TD WIDTH="100%" ALIGN=CENTER VALIGN=MIDDLE>
<IMG SRC="toilet.png">
<BR>
<? echo "$error\n"; ?>
</TD></TR>
</TABLE>
</DIV>
<?php
}

unset($guests);
unset($events);
toilet_close($db);

?>
</BODY>
</HTML>
