<HTML>
<HEAD>
<LINK REL="StyleSheet" TYPE="text/css" HREF="style.css">
<?php

include "util.php";

toilet_init($toilet_init_path);
$db = toilet_open($toilet_db_path);
$events = toilet_gtable($db, "events");
$guests = toilet_gtable($db, "guests");

?>
<TITLE>Invite-o-toilet</TITLE>
</HEAD>
<BODY>
<TABLE CELLPADDING=5 CELLSPACING=5>
<TR>
	<TD WIDTH=1 HEIGHT=1><IMG SRC="toilet.png"></TD>
	<TD WIDTH="100%" VALIGN=TOP>
	<H1>Invite-o-toilet event listing</H1>
	<H3>Listing all events</H3>
	<A HREF="new.php">New event</A>
	</TD>
</TR>
<TR><TD COLSPAN=2>
<TABLE CELLPADDING=0 CELLSPACING=0>
<TR>
	<TH CLASS="fancy">Event</TH>
	<TH CLASS="fancy">Organizer</TH>
	<TH CLASS="fancy">Time</TH>
	<TH CLASS="fancy">Guests</TH>
</TR>
<?php
	$rows = gtable_rows($events);
	foreach($rows as $event)
	{
		$row = rowid_get_row($events, $event, array("hash", "name", "organizer", "time"));
		$hash = $row["hash"];
		$name = $row["name"];
		$org = $row["organizer"];
		$time = $row["time"];
		echo "<TR><TD CLASS=\"normal\"><A HREF=\"manage.php?event=$hash\">$name</A></TD>\n";
		$row = rowid_get_row($guests, $org, array("hash", "name", "email"));
		$hash = $row["hash"];
		$name = $row["name"];
		$email = $row["email"];
		echo "<TD CLASS=\"normal\"><A HREF=\"mailto:$email\">$name</A></TD>\n";
		$time = date("l\, F jS Y g:i A", $time);
		echo "<TD CLASS=\"normal\">$time</TD>\n";
		$count = gtable_count_query($guests, "event", $event);
		echo "<TD CLASS=\"normal\">$count</TD></TR>\n";
	}
?>
</TABLE>
</TD></TR>
</TABLE>
<?php

unset($guests);
unset($events);
toilet_close($db);

?>
</BODY>
</HTML>
