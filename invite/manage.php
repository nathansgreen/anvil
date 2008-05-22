<HTML>
<HEAD>
<LINK REL="StyleSheet" TYPE="text/css" HREF="style.css">
<TITLE>Invite-o-toilet</TITLE>
<SCRIPT LANGUAGE="JavaScript" TYPE="text/javascript"><!--
var changed = false;
function notifyChange()
{
	changed = true;
}
function resetChange()
{
	changed = false;
}
function bypassCheck()
{
	changed = false;
}
function trySubmit()
{
	if(changed)
		if(!confirm("Event and/or guest details have been modified, but taking this action will discard any changes.\nContinue?"))
			return false;
	return true;
}
--></SCRIPT>
</HEAD>
<BODY>
<?php

include "util.php";

function hash_id($type, $id)
{
	return md5("secret_prefix:$type:$id=-");
}

function email_invite_url($event, $email, $url, $guest, $host, $from, $comments)
{
	return true;
	$message = "Dear $guest,\n\n";
	$message .= "$host has invited you to $event!\n\n";
	$message .= $comments;
	$message .= "\n\nTo view the invitation, visit $url.";
	return mail($email, "Invite-o-toilet invitation from $host: $event", $message, "From: $host <$from>", "-f$from");
}

function count_replies($guests, $event, $reply)
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
			$result++;
		}
	echo $result;
}

function show_replies($guests, $event, $reply, $org)
{
	$rows = gtable_query($guests, "event", $event);
	$rows = rowids_get_sorted_rows($rows, "name", false, array("id", "name", "hash", "email", "emails", "reply", "heads", "comments"));
	foreach($rows as $row)
	{
		if($row["reply"] != $reply)
			continue;
		$id = $row["id"];
		$id_s = rowid_format($id);
		$is_org = rowid_equal($id, $org);
		$name = $row["name"];
		$hash = $row["hash"];
		$email = htmlspecialchars($row["email"]);
		$emails = $row["emails"];
		$heads = $row["heads"];
		$comments = htmlspecialchars($row["comments"]);
		echo "<DIV STYLE=\"float: right;\"><INPUT TYPE=\"CHECKBOX\" NAME=\"check_$id_s\"></DIV>\n";
		echo "<B><INPUT TYPE=\"TEXT\" SIZE=20 NAME=\"name_$id_s\" VALUE=\"$name\" onChange='notifyChange()'>";
		if($is_org)
			echo " (the organizer)";
		if($heads > 1)
			echo " + " . ($heads - 1) . (($heads > 2) ? " people" : " person");
		echo "</B><BR>\n";
		if($comments)
			echo "<DIV CLASS=\"comments\">$comments</DIV>\n";
		$s = ($emails == 1) ? "" : "s";
		echo "<DIV CLASS=\"info\">[ $emails email$s to ";
		if($is_org)
			echo "$email";
		else
			echo "<INPUT TYPE=\"TEXT\" SIZE=30 NAME=\"email_$id_s\" VALUE=\"$email\" onChange='notifyChange()'>";
		echo " | <A HREF=\"index.php?invite=$hash\" TARGET=\"_new\">edit reply</A>]</DIV>\n";
	}
}

toilet_init($toilet_init_path);
$db = toilet_open($toilet_db_path);
$events = toilet_gtable($db, "events");
$guests = toilet_gtable($db, "guests");

$error = null;
if(isset($_REQUEST["event"]))
{
	$event = $_REQUEST["event"];
	
	$rows = gtable_query($events, "hash", $event);
	if($rows[0])
	{
		$id = $rows[0];
		$row = rowid_get_row($id, array("name", "description", "location", "organizer", "time", "comments"));
		$title = htmlspecialchars($row["name"]);
		$desc = htmlspecialchars($row["description"]);
		$where = htmlspecialchars($row["location"]);
		$org_id = $row["organizer"];
		$time = $row["time"];
		$comments = $row["comments"];
		$rows = gtable_query($guests, "id", $org_id);
		if($rows[0])
		{
			$row = rowid_get_row($rows[0], array("name", "email"));
			$org = $row["name"];
			$org_email = $row["email"];
		}
		if(!$comments)
			$comments = "Please check out the details on the Invite-o-toilet invitation online.";
		$action = $_REQUEST["action"];
		if($action == "Invite" || $action == "Invite & Email message below")
		{
			$guest = $_REQUEST["guest"];
			$email = $_REQUEST["email"];
			if($guest && $email)
			{
				$guest_id = gtable_new_row($guests);
				rowid_set_values($guest_id, array("event" => $id, "name" => $guest, "email" => $email, "emails" => 0));
				$hash = hash_id("guest", rowid_format($guest_id));
				rowid_set_values($guest_id, array("hash" => $hash));
				if($action != "Invite")
				{
					$email = stripslashes($email);
					$server = $HTTP_SERVER_VARS["SERVER_NAME"];
					$n_comments = stripslashes($_REQUEST["comments"]);
					email_invite_url(stripslashes($title), $email, "http://$server/?invite=$hash", $guest, $org, $org_email, $n_comments);
					rowid_set_values($guest_id, array("emails" => 1));
					if(isset($_REQUEST["save"]))
					{
						rowid_set_values($id, array("comments" => $n_comments));
						$comments = stripslashes($n_comments);
					}
				}
			}
		}
		else if($action == "Send message to")
		{
			$recipients = $_REQUEST["recipients"];
			$s_title = stripslashes($title);
			$server = $HTTP_SERVER_VARS["SERVER_NAME"];
			$n_comments = stripslashes($_REQUEST["comments"]);
			$rows = gtable_query($guests, "event", $id);
			foreach($rows as $guest_id)
			{
				$row = rowid_get_row($guest_id, array("name", "hash", "email", "emails", "reply"));
				if($recipients == "check" && !isset($_REQUEST["check_" . rowid_format($guest_id)]))
					continue;
				$reply = $row["reply"];
				if($recipients == "yes" && $reply != "Y")
					continue;
				if($recipients == "yesmaybe" && $reply != "Y" && $reply != "M")
					continue;
				if($recipients == "reply" && !$reply)
					continue;
				if($recipients == "noreply" && $reply)
					continue;
				$emails = $row["emails"];
				if($recipients == "nomail" && $emails != 0)
					continue;
				$guest = $row["name"];
				$hash = $row["hash"];
				$email = $row["email"];
				email_invite_url($s_title, $email, "http://$server/?invite=$hash", $guest, $org, $org_email, $n_comments);
				rowid_set_values($guest_id, array("emails" => ($emails + 1)));
			}
			if(isset($_REQUEST["save"]))
			{
				$n_comments = addslashes($n_comments);
				rowid_set_values($id, array("comments" => $n_comments));
				$comments = stripslashes($n_comments);
			}
		}
		else if($action == "Remove checked guests" && isset($_REQUEST["confirm"]))
		{
			$rows = gtable_query($guests, "event", $id);
			foreach($rows as $guest)
			{
				if(rowid_equal($guest, $org_id))
					continue;
				$format = rowid_format($guest);
				if(isset($_REQUEST["check_$format"]))
					rowid_drop($guest);
			}
		}
		else if($action == "Update event/guest info")
		{
			$name = $_REQUEST["name"];
			$n_desc = $_REQUEST["desc"];
			$month = $_REQUEST["month"];
			$day = $_REQUEST["day"];
			$year = $_REQUEST["year"];
			$hour = $_REQUEST["hour"];
			$min = $_REQUEST["min"];
			$ampm = $_REQUEST["ampm"];
			$n_where = $_REQUEST["where"];
			if($month && $day && $year && $ampm)
			{
				if($ampm == "PM")
					$hour += 12;
				$event_time = mktime($hour, $min, 0, $month, $day, $year);
				
				rowid_set_values($id, array("name" => $name, "description" => $n_desc, "location" => $n_where, "time" => $event_time));
				
				$row = rowid_get_row($id, array("name", "description", "location", "time"));
				if($row)
				{
					$title = htmlspecialchars($row["name"]);
					$desc = htmlspecialchars($row["description"]);
					$where = htmlspecialchars($row["location"]);
					$time = $row["time"];
				}
				else
					$error = "Database error.";
			}
			
			if(!$error)
			{
				$rows = gtable_query($guests, "event", $id);
				foreach($rows as $guest)
				{
					$format = rowid_format($guest);
					$name = $_REQUEST["name_$format"];
					$email = $_REQUEST["email_$format"];
					$row = rowid_get_row($guest, array("name", "email"));
					if($row["name"] != $name)
						rowid_set_values($guest, array("name" => $name));
					/* can't change the organizer email address */
					if(!rowid_equal($guest, $org_id))
						if($row["email"] != $email)
							rowid_set_values($guest, array("email" => $email, "emails" => 0));
				}
				$row = rowid_get_row($org_id, array("name", "email"));
				if($row)
				{
					$org = $row["name"];
					$org_email = $row["email"];
				}
				else
					$error = "Database error.";
			}
		}
	}
	else
		$error = "Event not found.";
}
else
	$error = "No event to manage.";

if(!$error)
{
?>
<FORM ACTION="manage.php?event=<? echo $event; ?>" METHOD="POST" NAME="form" onSubmit='return trySubmit()'>
<TABLE CELLPADDING=5 CELLSPACING=5>
<TR>
	<TD WIDTH=1 HEIGHT=1><IMG SRC="toilet.png"></TD>
	<TD WIDTH="100%" VALIGN=TOP>
	<INPUT TYPE="TEXT" NAME="name" SIZE=35 CLASS="subtle" STYLE="font-size: xx-large;" VALUE="<? echo $title; ?>" onChange='notifyChange()'>
	<BR><BR>
	<TEXTAREA NAME="desc" COLS=60 ROWS=5 CLASS="subtle" STYLE="font-size: large;" onChange='notifyChange()'><? echo $desc; ?></TEXTAREA>
	<BR><BR>
	Date:
	<SELECT NAME="month" onChange='notifyChange()'>
<?php
	$months = array("January", "February", "March", "April", "May", "June", "July",
	                "August", "September", "October", "November", "December");
	$current = date("n", $time);
	for($i = 1; $i <= 12; $i++)
	{
		$month = $months[$i - 1];
		$selected = ($i == $current) ? " SELECTED" : "";
		echo "\t<OPTION VALUE=\"$i\"$selected>$month</OPTION\n";
	}
?>
	</SELECT>
	<SELECT NAME="day" onChange='notifyChange()'>
<?php
	$current = date("j", $time);
	for($i = 1; $i <= 31; $i++)
	{
		$selected = ($i == $current) ? " SELECTED" : "";
		echo "\t<OPTION VALUE=\"$i\"$selected>$i</OPTION\n";
	}
?>
	</SELECT>
	<SELECT NAME="year" onChange='notifyChange()'>
<?php
	$current = date("Y", $time);
	for($i = 2008; $i <= 2009; $i++)
	{
		$selected = ($i == $current) ? " SELECTED" : "";
		echo "\t<OPTION VALUE=\"$i\"$selected>$i</OPTION\n";
	}
?>
	</SELECT>
	<BR>
	Time:
	<SELECT NAME="hour" onChange='notifyChange()'>
<?php
	$current = date("g", $time);
	for($i = 1; $i <= 12; $i++)
	{
		$selected = ($i == $current) ? " SELECTED" : "";
		echo "\t<OPTION VALUE=\"$i\"$selected>$i</OPTION\n";
	}
?>
	</SELECT>
	<SELECT NAME="min" onChange='notifyChange()'>
<?php
	$current = date("i", $time);
	for($i = 0; $i < 60; $i += 5)
	{
		$min = ($i < 10) ? "0$i" : "$i";
		$selected = ($min == $current) ? " SELECTED" : "";
		echo "\t<OPTION VALUE=\"$i\"$selected>$min</OPTION\n";
	}
?>
	</SELECT>
	<SELECT NAME="ampm" onChange='notifyChange()'>
<?php
	$current = date("G", $time);
	$am = ($current < 12) ? " SELECTED" : "";
	$pm = ($current >= 12) ? " SELECTED" : "";
	echo "\t<OPTION VALUE=\"AM\"$am>AM</OPTION>\n";
	echo "\t<OPTION VALUE=\"PM\"$pm>PM</OPTION>\n";
?>
	</SELECT>
	<BR>
	Location: <INPUT TYPE="TEXT" NAME="where" SIZE=64 VALUE="<? echo $where; ?>" onChange='notifyChange()'><BR>
	</TD>
</TR>
<TR><TD COLSPAN=2>
	<DIV ID="status">
	<TABLE CELLPADDING=2 CELLSPACING=0 CLASS="yes"><TR><TH>Yes (<? count_replies($guests, $id, "Y"); ?>)</TH></TR><TR><TD>
	<? show_replies($guests, $id, "Y", $org_id); ?>
	</TD></TR></TABLE>
	<TABLE CELLPADDING=2 CELLSPACING=0 CLASS="maybe"><TR><TH>Maybe (<? count_replies($guests, $id, "M"); ?>)</TH></TR><TR><TD>
	<? show_replies($guests, $id, "M", $org_id); ?>
	</TD></TR></TABLE>
	<TABLE CELLPADDING=2 CELLSPACING=0 CLASS="no"><TR><TH>No (<? count_replies($guests, $id, "N"); ?>)</TH></TR><TR><TD>
	<? show_replies($guests, $id, "N", $org_id); ?>
	</TD></TR></TABLE>
	<TABLE CELLPADDING=2 CELLSPACING=0 CLASS="noreply"><TR><TH>Not yet replied (<? count_replies($guests, $id, null); ?>)</TH></TR><TR><TD>
	<? show_replies($guests, $id, null, $org_id); ?>
	</TD></TR></TABLE>
	<BR>
	<INPUT TYPE="SUBMIT" NAME="action" VALUE="Remove checked guests"> &nbsp;
	<INPUT TYPE="CHECKBOX" NAME="confirm"> Yes, really.
	</DIV>
	<H2>Invite another guest:</H2>
	Name: <INPUT TYPE="TEXT" NAME="guest" SIZE=40><BR>
	Email: <INPUT TYPE="TEXT" NAME="email" SIZE=40><BR>
	<INPUT TYPE="SUBMIT" NAME="action" VALUE="Invite" STYLE="margin-top: 2px;">
	<INPUT TYPE="SUBMIT" NAME="action" VALUE="Invite & Email message below"><BR>
	<BR>
	Send email (italic sections will be filled in automatically):<BR>
	<TABLE CELLPADDING=0 CELLSPACING=0 ID="email"><TR><TD>
		Dear <I>guest</I>,<BR><BR>
		
		<? echo $org; ?> has invited you to <? echo $title; ?>!<BR>
		<TEXTAREA NAME="comments" COLS=45 ROWS=10 STYLE="margin-top: 1ex; margin-bottom: 1ex;"><? echo htmlspecialchars($comments); ?></TEXTAREA>
		<BR>
		To view the invitation, visit <SPAN STYLE="font-size: smaller;"><I>Invite-o-toilet URL</I></SPAN>.<BR>
	</TD></TR></TABLE>
	<INPUT TYPE="SUBMIT" NAME="action" VALUE="Send message to" STYLE="margin-top: 2px;">
	<SELECT NAME="recipients">
	<OPTION VALUE="all">all guests</OPTION>
	<OPTION VALUE="yes">yes replies</OPTION>
	<OPTION VALUE="yesmaybe">yes &amp; maybe replies</OPTION>
	<OPTION VALUE="reply">all replied guests</OPTION>
	<OPTION VALUE="noreply">unreplied guests</OPTION>
	<OPTION VALUE="check" SELECTED>checked guests</OPTION>
	<OPTION VALUE="nomail">non-emailed guests</OPTION>
	</SELECT>
	<INPUT TYPE="CHECKBOX" NAME="save">
	<SPAN STYLE="font-size: smaller;">Save this message</SPAN>
	<BR><BR>
	<INPUT TYPE="SUBMIT" NAME="action" VALUE="Update event/guest info" onClick='bypassCheck()'>
	<INPUT TYPE="RESET" VALUE="Reset info changes" onClick='resetChange()'><BR>
	<DIV STYLE="font-size: small;">
		(Changes you have made on this page will not take effect unless you click "Update".)
	</DIV>
	<BR ID="clear">
</TD></TR>
</TABLE>
</FORM>
<?php
}
else
{
?>
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
