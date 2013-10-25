#!/usr/bin/php -q
<?php
/*
 *  slurmJobHistoryAgent.php - todora - 20080530
 *  Take job logs from SLURM's jobcomp/filetxt plugin and parse them into
 *   HPCMan SQL INSERT statements.
 */

$_snuuid=1;

// SLURM unfortunately does not include the year with its job accounting data 
//  time stamps, so we need to figure it out and add it.
// NOTE that this will cause a problem if we are reading jobs from other than
//  the current year!
$year = date('Y');

$owner = '';
$stop = '';
$start = '';
$step = '';
$class = '';
$hosts = 0;
$jobstate = '';

if(sizeof($_SERVER['argv']) > 1) {
  // check for first argument for SQL output
  $sql = ($_SERVER['argv'][1] === "--sql") ? true : false;

  // check for second argument for start date search in SLURM data
  $start_bound = (isset($_SERVER['argv'][2])) ? 
	strtotime($_SERVER['argv'][2]) : false;
} else {
  $sql = false;
  $start_bound = false;
}

if(!$sql) echo "JobID;UserID;Start Time;Stop Time;Class;Nodes Used;Runtime (sec)\n";

$lines = file("php://STDIN");

foreach($lines as $line) {
  sscanf($line, 'JobId=%d UserId=%s GroupId=%s', $step, $owner, $group);

  $temp = substr($line, strpos($line, 'Name='));

  $jobname = substr($temp, 5);

  $jobname = substr($jobname, 0, (strpos($jobname, "JobState="))-1);

  sscanf(strstr($line, "JobState="), '%s Partition=%s TimeLimit=%s StartTime=%s EndTime=%s NodeList=%s NodeCnt=%d', $jobstate, $class, $timelimit, $start, 
	$stop, $nodelist, $hosts);

  $step = exec('hostname').".slurm.$step";
//  $owner = str_replace('UserId=', '', $owner);
  // group here
  // job name here
//  $jobstate = str_replace('JobState=', '', $jobstate);
//  $class = str_replace('Partition=', '', $class);
  // time limit here
//  $start = str_replace('StartTime=', '', $start);
  $arr_start = explode('-', $start); 
  $start = $arr_start[0]."/$year ".$arr_start[1];
//  $stop = str_replace('EndTime=', '', $stop);
  $arr_stop = explode('-', $stop);
  $stop = $arr_stop[0]."/$year ".$arr_stop[1];
  // node list here
//  $hosts = str_replace('NodeCnt=', '', $hosts);

  // strip (UIDN) off of owner name
  $owner = array_pop(array_reverse(explode('(', $owner)));

//  if($jobstate === "COMPLETED" || $jobstate === "CANCELLED") {
  if(!$start_bound || strtotime($start) >= $start_bound) {
    if($step != '' && $owner != '' && $stop != ''
        && $start != '' && $class != '' && $hosts > 0) {
      $runtime = strtotime($stop) - strtotime($start);
      if($runtime < 0) {
        echo "* * * BAD DATA FOR $step  * * *
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
      } else if($runtime > 0) {
	$cputime = $runtime * $hosts;
        if(!$sql) {
          echo "$step;$owner;$start;$stop;$class;$hosts;$runtime\n";
        } else  {
        echo "INSERT INTO cputime (username,snuuid,jobstart,jobend,units,cputime,memory,jobname,machine) VALUES ('$owner',$_snuuid,'$start','$stop',$hosts,$cputime,0,'$step','$class');
        ";
        }
        $owner = '';
        $stop = '';
        $start = '';
        $step = '';
        $class = '';
        $hosts = 0;
	$jobstate = '';
      }
    }
  }
}

?>
