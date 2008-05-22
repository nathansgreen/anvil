<HTML>
<HEAD>
<LINK REL="StyleSheet" TYPE="text/css" HREF="style.css">
<?php

include "util.php";

function hash_id($type, $id)
{
	return md5("-=invite-o-toilet:$type:$id=-");
}

function email_manage_url($event, $email, $server, $url)
{
	return true;
	$message = "A new event, $event, has been created on Invite-o-toilet.\n\n";
	$message .= "You can manage it using this URL:\n$url\n\n";
	$message .= "Don't lose or share this link, as there are no passwords.";
	return mail($email, "New event created: $event", $message, "From: Invite-o-toilet <invite-o-toilet@$server>", "-finvite-o-toilet@$server");
}

$error = null;
$message = null;
if(isset($_REQUEST["name"]))
{
	$name = $_REQUEST["name"];
	$desc = $_REQUEST["desc"];
	$month = $_REQUEST["month"];
	$day = $_REQUEST["day"];
	$year = $_REQUEST["year"];
	$hour = $_REQUEST["hour"];
	$min = $_REQUEST["min"];
	$ampm = $_REQUEST["ampm"];
	$where = $_REQUEST["where"];
	$org = $_REQUEST["org"];
	$email = $_REQUEST["email"];
	$reply = $_REQUEST["reply"];
	$heads = $_REQUEST["heads"];
	$comments = $_REQUEST["comments"];
	
	if(!$error && $month && $day && $year && $ampm && $org && $email)
	{
		if($ampm == "PM")
			$hour += 12;
		$event_time = mktime($hour, $min, 0, $month, $day, $year);
		
		toilet_init($toilet_init_path);
		$db = toilet_open($toilet_db_path);
		$events = toilet_gtable($db, "events");
		$guests = toilet_gtable($db, "guests");
		
		$event = gtable_new_row($events);
		if($event)
		{
			rowid_set_values($event, array("name" => $name, "description" => $desc, "location" => $where));
			$hash = hash_id("event", rowid_format($event));
			rowid_set_values($event, array("hash" => $hash));
			$redirect = "manage.php?event=$hash";
			$guest = gtable_new_row($guests);
			if($guest > 0)
			{
				rowid_set_values($guest, array("event" => $event, "name" => $org, "email" => $email));
				rowid_set_values($event, array("organizer" => $guest, "time" => $event_time));
				$hash = hash_id("guest", rowid_format($guest));
				rowid_set_values($guest, array("hash" => $hash));
				if($reply == "N")
					$heads = 0;
				else if($heads < 1)
					$heads = 1;
				else if($heads > 50)
					$heads = 50;
				rowid_set_values($guest, array("reply" => $reply, "heads" => $heads, "comments" => $comments));
				$server = $HTTP_SERVER_VARS["SERVER_NAME"];
				email_manage_url(stripslashes($name), stripslashes($email), $server, "http://$server$redirect");
				$message = "Event created.<BR><SPAN STYLE=\"font-size: smaller;\">A message has been sent to your email address with a URL to manage your event.</SPAN>";
			}
			else
				$error = "Database error.";
		}
		else
			$error = "Database error.";
		
		unset($guest);
		unset($event);
		unset($guests);
		unset($events);
		toilet_close($db);
	}
	else if(!$error)
		$error = "Missing required information.";
}

if($error)
{
?>
<TITLE>Invite-o-toilet error</TITLE>
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
else if($message)
{
	echo "<TITLE>Invite-o-toilet</TITLE>\n";
	/* convenient, but without verification of validity of organizer email
	 * address, we don't want to be sending email from it to guests */
	if($redirect)
		echo "<META HTTP-EQUIV=\"Refresh\" CONTENT=\"2; $redirect\">\n";
?>
</HEAD>
<BODY>
<DIV ALIGN=CENTER>
<TABLE CELLPADDING=0 CELLSPACING=0 HEIGHT="100%">
<TR><TD WIDTH="100%" ALIGN=CENTER VALIGN=MIDDLE>
<IMG SRC="toilet.png">
<BR>
<? echo "$message\n"; ?>
</TD></TR>
</TABLE>
</DIV>
<?php
}
else
{
?>
<TITLE>New Invite-o-toilet event</TITLE>
</HEAD>
<BODY>
<FORM ACTION="new.php" METHOD="POST">
<TABLE CELLPADDING=5 CELLSPACING=5>
<TR>
	<TD WIDTH=1 HEIGHT=1><IMG SRC="toilet.png"></TD>
	<TD WIDTH="100%" VALIGN=TOP>
	<INPUT TYPE="TEXT" NAME="name" SIZE=35 CLASS="subtle" STYLE="font-size: xx-large;" VALUE="Event title">
	<BR><BR>
	<TEXTAREA NAME="desc" COLS=60 ROWS=5 CLASS="subtle" STYLE="font-size: large;">Event description</TEXTAREA>
	<BR><BR>
	Date:
	<SELECT NAME="month">
	<OPTION SELECTED>Month</OPTION>
	<OPTION VALUE="1">January</OPTION> <OPTION VALUE="2">February</OPTION> <OPTION VALUE="3">March</OPTION>
	<OPTION VALUE="4">April</OPTION> <OPTION VALUE="5">May</OPTION> <OPTION VALUE="6">June</OPTION>
	<OPTION VALUE="7">July</OPTION> <OPTION VALUE="8">August</OPTION> <OPTION VALUE="9">September</OPTION>
	<OPTION VALUE="10">October</OPTION> <OPTION VALUE="11">November</OPTION> <OPTION VALUE="12">December</OPTION>
	</SELECT>
	<SELECT NAME="day">
	<OPTION SELECTED>Day</OPTION>
	<OPTION VALUE="1">1</OPTION> <OPTION VALUE="2">2</OPTION> <OPTION VALUE="3">3</OPTION>
	<OPTION VALUE="4">4</OPTION> <OPTION VALUE="5">5</OPTION> <OPTION VALUE="6">6</OPTION>
	<OPTION VALUE="7">7</OPTION> <OPTION VALUE="8">8</OPTION> <OPTION VALUE="9">9</OPTION>
	<OPTION VALUE="10">10</OPTION> <OPTION VALUE="11">11</OPTION> <OPTION VALUE="12">12</OPTION>
	<OPTION VALUE="13">13</OPTION> <OPTION VALUE="14">14</OPTION> <OPTION VALUE="15">15</OPTION>
	<OPTION VALUE="16">16</OPTION> <OPTION VALUE="17">17</OPTION> <OPTION VALUE="18">18</OPTION>
	<OPTION VALUE="19">19</OPTION> <OPTION VALUE="20">20</OPTION> <OPTION VALUE="21">21</OPTION>
	<OPTION VALUE="22">22</OPTION> <OPTION VALUE="23">23</OPTION> <OPTION VALUE="24">24</OPTION>
	<OPTION VALUE="25">25</OPTION> <OPTION VALUE="26">26</OPTION> <OPTION VALUE="27">27</OPTION>
	<OPTION VALUE="28">28</OPTION> <OPTION VALUE="29">29</OPTION> <OPTION VALUE="30">30</OPTION>
	<OPTION VALUE="31">31</OPTION>
	</SELECT>
	<SELECT NAME="year">
	<OPTION VALUE="2008" SELECTED>2008</OPTION> <OPTION VALUE="2009">2009</OPTION>
	</SELECT>
	<BR>
	Time:
	<SELECT NAME="hour">
	<OPTION SELECTED>Hour</OPTION>
	<OPTION VALUE="1">1</OPTION> <OPTION VALUE="2">2</OPTION> <OPTION VALUE="3">3</OPTION>
	<OPTION VALUE="4">4</OPTION> <OPTION VALUE="5">5</OPTION> <OPTION VALUE="6">6</OPTION>
	<OPTION VALUE="7">7</OPTION> <OPTION VALUE="8">8</OPTION> <OPTION VALUE="9">9</OPTION>
	<OPTION VALUE="10">10</OPTION> <OPTION VALUE="11">11</OPTION> <OPTION VALUE="0">12</OPTION>
	</SELECT> :
	<SELECT NAME="min">
	<OPTION SELECTED>Min</OPTION>
	<OPTION VALUE="0">00</OPTION> <OPTION VALUE="5">05</OPTION> <OPTION VALUE="10">10</OPTION>
	<OPTION VALUE="15">15</OPTION> <OPTION VALUE="20">20</OPTION> <OPTION VALUE="25">25</OPTION>
	<OPTION VALUE="30">30</OPTION> <OPTION VALUE="35">35</OPTION> <OPTION VALUE="40">40</OPTION>
	<OPTION VALUE="45">45</OPTION> <OPTION VALUE="50">50</OPTION> <OPTION VALUE="55">55</OPTION>
	</SELECT>
	<SELECT NAME="ampm">
	<OPTION VALUE="AM">AM</OPTION> <OPTION VALUE="PM" SELECTED>PM</OPTION>
	</SELECT>
	<BR>
	Location: <INPUT TYPE="TEXT" NAME="where" SIZE=64><BR>
	</TD>
</TR>
<TR><TD COLSPAN=2>
	<H2>Who are you?</H2>
	Name: <INPUT TYPE="TEXT" NAME="org" SIZE=40><BR>
	Email: <INPUT TYPE="TEXT" NAME="email" SIZE=40><BR>
	Will you attend? &nbsp;
	<SELECT NAME="reply">
	<OPTION VALUE="Y" SELECTED>Yes</OPTION>
	<OPTION VALUE="M">Maybe</OPTION>
	<OPTION VALUE="N">No</OPTION>
	</SELECT><BR>
	Number of guests (including you): &nbsp;
	<INPUT TYPE="TEXT" NAME="heads" VALUE="1" SIZE=4><BR>
	Comments:<BR>
	<TEXTAREA NAME="comments" COLS=40 ROWS=5></TEXTAREA><BR>
	<INPUT TYPE="submit" VALUE="Create event">
	<BR ID="clear">
</TD></TR>
</TABLE>
</FORM>
<?php
}
?>
</BODY>
</HTML>
