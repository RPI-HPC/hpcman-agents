#!/bin/bash
#
#  This is a temporary script to generate an aliases entry for easily emailing
#  all addresses specified by the emailaddress attribute of principals with
#  active accounts for a given site, or emailing all active principals.
#

# If psql is not in $PATH, add it now!
#export PATH=/path/to/psql:$PATH

# sendmail needs to be available, too
SENDMAIL=/usr/sbin/sendmail

DBHOST=localhost
DBUSER=todora
DBPASS=
DB=hpcman
export PGPASSWORD=$DBPASS

SNUUID=1

# Are we emailing all with active accounts or all principals, period?
EMAILALL=true

# list addresses allowed to send mail here
ALLOWEDSENDERS="todora@ccnidev.ccni.rpi.edu"

if echo "$ALLOWEDSENDERS" | grep -q "$SENDER"
then
  if [ "$EMAILALL" == "false" ]
  then
    recips=`psql -h $DBHOST -U $DBUSER $DB --no-align --tuples-only -R , \
	-c "SELECT DISTINCT emailaddress FROM principals, \
	user_accounts WHERE user_accounts.snuuid=$SNUUID \
	AND user_accounts.puuid=principals.puuid \
	AND user_accounts.useraccountstate='A' \
	AND char_length('principals.emailaddress') > 6"`
  else
    recips=`psql -h $DBHOST -U $DBUSER $DB --no-align --tuples-only -R , \
        -c "SELECT DISTINCT emailaddress FROM principals \
        WHERE principals.principalstate='A' \
        AND char_length('principals.emailaddress') > 6"`
  fi
  cat - |`$SENDMAIL "$recips"`
else
  # exit with EX_NOUSER, can be any valid status from sysexits.h
  exit 67
fi
