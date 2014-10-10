SASROOT=/opt/sas/sas92/SASFoundation/9.2
export SASROOT
PROJETROOT=/project/aflac/ladmint
export PROJETROOT
SASPGM=$PROJETROOT/_40_program
export SASPGM
SASLOG=$PROJETROOT/_90_log
export SASLOG
SASOUT=$PROJETROOT/_70_output
export SASOUT
SASAUTOEXEC=$PROJETROOT/_10_autoexec
export SASAUTOEXEC
PGMSAS=call_create_report
export PGMSAS
#Date du jour
DATE_DU_JOUR=`date +%Y%m%d_%H%M%S`
export DATE_DU_JOUR
#echo "DATE_DU_JOUR="$DATE_DU_JOUR"
DATE_FONC=`date +%Y%m%d`
nohup $SASROOT/sas $SASPGM/$PGMSAS.sas -autoexec $SASAUTOEXEC/autoexec.sas -log $SASLOG/${PGMSAS}_$DATE_DU_JOUR.log -noterminal -syntaxcheck   -sysparm "$1,$DATE_FONC"
