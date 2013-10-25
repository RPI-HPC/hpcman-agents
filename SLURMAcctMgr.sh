#!/bin/bash 
#
# SLURMAcctMgr.sh <days> - populate SLURM with account and user data
#  for projects that have had user accounts modified in the last
#  <days> days, or all projects if <days> is omitted.
#
#
#

export PGHOST=
export PGUSER=slurm
export PGDATABASE=hpcman
export PGPASSWORD=

SNUUID=1

# set to 1 to set up project owners as account coordinators
DO_COORD=1

# Path to sacctmgr
export SLURMACCTMGR="/opt/slurm2/bin/sacctmgr"
# Path to sacctmgr with read only flag(s)
export SLURMACCTMGR_RO="/opt/slurm2/bin/sacctmgr --readonly"

if [ x"$1" = "x" ]
then
  days=9999
else
  days=$1
fi

# check some things
if ! [ -x "$SLURMACCTMGR" ]
then
  echo "No working sacctmgr found!"
  exit 1
fi

if [ x"$2" = "x-d" ]
then
  SLURMACCTMGR="echo $SLURMACCTMGR"
fi

projcount=`psql --no-align --tuples-only -q -c \
	"SELECT count(projid) FROM projects WHERE snuuid=$SNUUID"`
if [ $? -ne 0 ]
then
  echo "DB connection not viable or other error."
  exit 1
fi

# query existing accounts
existing_accts=`$SLURMACCTMGR_RO -n list accounts |awk '{print $1}'`
existing_users=`$SLURMACCTMGR_RO -n list users |awk '{print $1}'`

# process new accounts and projects
for proj in `psql --no-align --tuples-only -c \
        "SELECT DISTINCT lower(projname)
        FROM projects, user_accounts
        WHERE projects.snuuid=$SNUUID and user_accounts.snuuid=$SNUUID
        AND user_accounts.projid=projects.projid
        AND user_accounts.useraccountstate='A'
        AND ( user_accounts.modified > now() - interval'$days days'
        OR projects.modified > now() - interval'$days days' )
UNION
        SELECT DISTINCT lower(projname)
        FROM projects, user_accounts, project_parents
        WHERE projects.snuuid=$SNUUID and user_accounts.snuuid=$SNUUID
	AND user_accounts.projid=projects.projid
        AND project_parents.snuuid=$SNUUID
        AND project_parents.projid=projects.projid
        AND user_accounts.useraccountstate='A'
        AND ( user_accounts.modified > now() - interval'$days days'
        OR projects.modified > now() - interval'$days days'
        OR project_parents.modified > now() - interval'$days days' )
UNION
        SELECT DISTINCT lower(projname)
        FROM projects, user_accounts, project_owners
        WHERE projects.snuuid=$SNUUID and user_accounts.snuuid=$SNUUID
	AND user_accounts.projid=projects.projid
        AND project_owners.snuuid=$SNUUID
        AND project_owners.projid=projects.projid
        AND user_accounts.useraccountstate='A'
        AND ( user_accounts.modified > now() - interval'$days days'
        OR projects.modified > now() - interval'$days days'
        OR project_owners.modified > now() - interval'$days days' )"` ;
do
    echo $existing_accts | grep -q $proj > /dev/null
    if ! [ $? -eq 0 ]
    then
      $SLURMACCTMGR --immediate add account $proj
    fi
    for user in `psql --no-align --tuples-only -c \
        "SELECT LOWER(username) from user_accounts, projects \
        where user_accounts.snuuid=projects.snuuid \
        and projects.snuuid=$SNUUID \
        and user_accounts.projid=projects.projid \
        and lower(projects.projname)='$proj'"` ;
    do
      echo $existing_users | grep -q $user > /dev/null
      if ! [ $? -eq 0 ]
      then
        $SLURMACCTMGR --immediate add user $user DefaultAccount=$proj
      fi
    done

    # add project hierarchy
    NONROOT_OWNER=0 # set this if we find a non-root owner below
    currparent=`$SLURMACCTMGR_RO -n list assoc tree account=$proj user='' format=cluster,parentname,account | head -n 1 | awk '{print $2}'`
    for projparent in `psql --no-align --tuples-only -c \
            "SELECT DISTINCT lower(projname) FROM projects JOIN project_parents \
	    ON projects.projid=project_parents.projparentid \
	    WHERE project_parents.snuuid=$SNUUID \
	    AND project_parents.projid=( \
	    	SELECT projid FROM  projects \
		WHERE snuuid=$SNUUID \
		AND lower(projname)='$proj' \
	    )"` ;
    do
      NONROOT_OWNER=1
      if ! [ "x$currparent" = "x$projparent" ]
      then
        $SLURMACCTMGR --immediate modify account with names=$proj set parent=$projparent
      fi
    done

    # remove project hierarchy
    if [ $NONROOT_OWNER -eq 0 ]
    then
      if ! [ "x$currparent" = "xroot" ]
      then
        $SLURMACCTMGR --immediate modify account with names=$proj set parent=root
      fi
    fi

    # set fair-share from projects->projCPUQuota
    fairshare=`psql --no-align --tuples-only -c \
    	"SELECT projCPUQuota FROM projects \
	WHERE snuuid=$SNUUID \
	AND lower(projname)='$proj'"`
    if ! [ $fairshare -eq 1 ]
    then
      currentfs=`$SLURMACCTMGR_RO -n list associations where account=$proj | head -n 1 | awk '{print $3}'`
      if ! [ "2$currentfs" = "2$fairshare" ]
      then
        $SLURMACCTMGR --immediate modify account with names=$proj set fairshare=$fairshare
      fi
    fi

    # add project owner(s) as manager if DO_COORD==1
    if [ $DO_COORD -eq 1 ] 
    then
      poc=0
      pos=''
      # build array of coordinators from hpcman
      for projowner in `psql --no-align --tuples-only -c \
	"SELECT username FROM user_accounts, project_owners, projects \
	WHERE user_accounts.projid=projects.projid \
	AND user_accounts.puuid=project_owners.puuid \
	AND projects.projid=project_owners.projid \
	AND lower(projects.projname)='$proj' \
	AND user_accounts.snuuid=$SNUUID \
	AND projects.snuuid=$SNUUID \
	AND project_owners.snuuid=$SNUUID"` ;
      do
        pos[$poc]=$projowner
	poc=`expr $poc + 1`
      done

      # build array of coordinators from SLURM
      slurm_pos=''
      spoc=0
      for slurmprojowner in `$SLURMACCTMGR_RO -n list accounts withcoordinator where name=$proj | awk '{print $4}'|tr ',' '\n'`
      do
	slurm_pos[$spoc]=$slurmprojowner
	spoc=`expr $spoc + 1`
      done
      # removals
      if [ $spoc -gt 0 ]
      then
        for k in `seq 0 $((spoc-1))`
        do
	  remove_po=${slurm_pos[k]}
	  if [ $poc -gt 0 ]
	  then
            for j in `seq 0 $((poc-1))`
  	    do
              echo ${pos[j]} | grep -q -i ${slurm_pos[k]}
	      if [ $? -eq 0 ]
	      then
	        remove_po=''
	      fi
	    done
	  fi
	  if ! [ "x$remove_po" = "x" ]
	  then
	    $SLURMACCTMGR --immediate delete coordinator account=$proj names=$remove_po
	  fi
        done
      fi
      # additions
      if [ $poc -gt 0 ]
      then
        for j in `seq 0 $((poc-1))`
        do
  	  add_po=${pos[j]}
	  if [ $spoc -gt 0 ]
	  then
  	    for k in `seq 0 $((spoc-1))`
            do
  	      echo ${slurm_pos[k]} | grep -q -i ${pos[j]}
	      if [ $? -eq 0 ]
              then
	        add_po=''
	      fi
	    done
	  fi
	  if ! [ "x$add_po" = "x" ]
	  then
	    $SLURMACCTMGR --immediate add coordinator names=$add_po account=$proj
	  fi
        done
      fi
    fi
done

# process deletions
#for proj in `psql --no-align --tuples-only -c \
#        "SELECT DISTINCT lower(projname) from projects, user_accounts
#        where projects.snuuid=1 and user_accounts.snuuid=$SNUUID
#        AND user_accounts.projid=projects.projid
#        AND user_accounts.useraccountstate <> 'A'
#        AND user_accounts.modified > now() - interval'$days days'"` ;
#do
#
#done
