#!/usr/bin/php -q
<?php
/*
 * llJobHistoryAgent.php - todora - 20080410
 * Take the output of llsummary with the long format flag set (-l) and parse
 *  it into either comma separated values representing individual jobs (no
 *  argumrnts) or SQL INSERT statements suitable for the current version of
 *  HPCMan database schema (--sql).  Incomplete or failed jobs are not printed.
 * Multi-step jobs are theoretically supported but have not been tested.
 *
*/

$_snuuid=1;

$owner = '';
$stop = '';
$start = '';
$step = '';
$class = '';
$hosts = 0;

$sql = ($_SERVER['argc'] > 1) ? true : false;

if(!$sql) echo "JobID;UserID;Start Time;Stop Time;Class;Nodes Used;Runtime (sec)\n";

$lines = file("php://STDIN");

foreach($lines as $line) {
  if(strpos($line, "==================") !== FALSE) {
    $t = array();
    $t = explode(' ', $line);
      $owner = '';
      $stop = '';
      $start = '';
      $step = '';
      $class = '';
      $hosts = 0;
    $job = $t[2];
  }

  if(strpos($line, "Owner:") !== FALSE) {
    $t = array();
    $t = explode(' ', $line);
    $owner = chop($t[15]);
  }

  if(strpos($line, "------------------") !== FALSE) {
    $t = array();
    $t = explode(' ', $line);
    $step = $t[2];
  }

  # host count for serial
  if(strpos($line, "Step Type: Serial") !== FALSE) {
    $hosts=1;
  }

  # base case for parallel
  if(strpos($line, "Step Type: General Parallel") !== FALSE) {
    $hosts=1;
  }

  # host count for parallel
  if(strpos($line, "Alloc. Host Count:") !== FALSE) {
    $t = array();
    $t = explode(' ', $line);
    $hosts = chop($t[5]);
  }

  # Get job's class
  if(strpos($line, "Class:") !== FALSE) {
    $class = rtrim(preg_replace('/              Class: /', '', $line));
  }

  if(strpos($line, "Start Time:") !== FALSE) {
    $start = rtrim(preg_replace('/         Start Time: /', '', $line));
  }

  if(strpos($line, "Completion Date:") !== FALSE) {
    $stop = rtrim(preg_replace('/    Completion Date: /', '', $line));
  }

  if($job != '' && $step != '' && $owner != '' && $stop != ''
	&& $start != '' && $class != '' && $hosts > 0) {
    $runtime = strtotime($stop) - strtotime($start);
    if($runtime <= 0) {
      echo "* * * BAD DATA FOR $job  * * *
step = $step
owner = $owner
stop = $stop
start = $start
class = $class
hosts = $hosts
runtime = {strtotime($stop)} - {strtotime($start)}
";
      $owner = '';
      $stop = '';
      $start = '';
      $step = '';
      $class = '';
      $hosts = 0;
    } else {
      $cputime = $hosts * $runtime;
      if(!$sql) {
        echo "$step;$owner;$start;$stop;$class;$hosts;$runtime\n";
      } else  { 
        echo "INSERT INTO cputime (
	username,
        snuuid,
        jobstart,
        jobend,
	units,
	cputime,
	memory,
        jobname,
        machine
	) VALUES (
	'$owner',
	$_snuuid,
	'$start',
	'$stop',
	$hosts,
	$cputime,
	0,
	'$step',
	'$class'
	);
        ";
      }
      $owner = '';
      $stop = '';
      $start = '';
      $step = '';
      $class = '';
      $hosts = 0;
    }
  }
}
?>
