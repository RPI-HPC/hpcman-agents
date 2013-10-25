#!/usr/bin/perl

use DBI;
use DBD::DB2::Constants;
use DBD::DB2;

sub trim($);

$ENV{DB2DIR} = '/opt/IBM/db2/V8.1';
$ENV{DB2INSTANCE} = 'bgdb2cli';

my $LLRPIPGDB = $ENV{LLRPIPGDB};
my $LLRPIPGHOST = $ENV{LLRPIPGHOST};
my $LLRPIPGUSER = $ENV{LLRPIPGUSER};

my $dbh = DBI->connect("dbi:DB2:bgdb0", 'bglsysdb', 'db24bgls') 
  or die('no DB2');

my $sql_stmt="SELECT DISTINCT
	jobid,
	jobname,
	starttime,
	entrydate,
	(DAYS(entrydate)-DAYS(STARTTIME))*86400 
	+ (MIDNIGHT_SECONDS(entrydate)-MIDNIGHT_SECONDS(STARTTIME)) AS time, 
	nodesused,
	username,
	bgljob_history.blockid,
	memorymodulesize 
	FROM bgljob_history, tbglnodehwattr a, tbglnode b, tbglprocessorcard c,
	tbglnodecard d, tbglmidplane e, tbglbpblockmap f
	WHERE bgljob_history.username <> 'tomcat'
	AND bgljob_history.username <> 'bgladmin'
	AND bgljob_history.username <> 'root'
	AND a.serialnumber = b.serialnumber
	AND b.cardserialnumber = c.serialnumber
	AND c.nodecardserialnumber = d.serialnumber
	AND d.midplaneserialnumber = e.serialnumber
	AND e.posinmachine = f.bpid
	AND c.isiocard='F'
	AND f.blockid = bgljob_history.blockid
	AND date(starttime) > current date - 4 day
";

my $sth = $dbh->prepare($sql_stmt);
#$sth->bind_param(1,$job_step);
$sth->execute();
my( $jobid, $jobname, $starttime, $entrydate, $time, $nodes, $username, $blockid, $mem );
$sth->bind_columns( \$jobid, \$jobname, \$starttime, \$entrydate,
	\$time, \$nodes, \$username, \$blockid, \$mem );

my $hostname = trim(`hostname -f`);

while($sth->fetch()) {
  if(length $time > 0) {
    my $block = trim($blockid);

  my $hpcjobname = trim($jobname) .'.'. trim($jobid);

  # convert memory value
  $mem = ($mem == 6) ? 512 : 1024;

  # for HPCMan schema
    printf "INSERT INTO cputime (username,snuuid,jobstart,jobend,units,cputime,jobname,memory,machine) VALUES ('%s', 1, '%s', '%s', %d, %d, '%s', %d, '$hostname:$block');\n", trim($username), $starttime, $entrydate, $nodes, ($time*$nodes), $hpcjobname, ($mem * $nodes);

  }
}

$sth->finish();
$dbh->disconnect();

sub trim($) {
	my $string = shift;
	$string =~ s/^\s+//;
	$string =~ s/\s+$//;
	return $string;
}
