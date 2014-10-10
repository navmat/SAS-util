/*-------------------------------------------------------------
+ < This macro program is called from shell script to invoke
    the sas macro for generating the .csv files for reqested report.
   >
+--------------------------------------------------------------*/
/*Hist---------------------------------------------------------
+ ykxjlau 27.02.2013: Created.
+--------------------------------------------------------------*/
%macro call_create_report();
  %let SYSPARM=&SYSPARM ;
  %put SYSPARM=&SYSPARM;

  data _null_ ;
	  call symput('id_report_control',trim(left(scan("&SYSPARM",1,',')))) ;
	  call symput('rundate',trim(left(scan("&SYSPARM",2,',')))) ;
  run ;
  
    /* rundate is the date variable of the extraction*/
	%let rundate = %SYSFUNC(INPUTN(&rundate,yymmdd8.));
	%put rundate=%sysfunc(putn(&rundate,yymmdd10.));
	
	/*datestamp is the wariable for the ZIP file*/
	%let datestamp = %sysfunc(DATE(),yymmdd6.);
	%put datestamp=&datestamp.;
	%let zip_counter = 0;
	/*timestamp is the wariable for the ZIP file*/
	%let timestamp = %sysfunc(substr(%sysfunc(DATETIME(),datetime19.),11,2))0000;
	%put timestamp=&timestamp.;
  
  
	  %put id_report_control = &id_report_control. ;
	  %put datestamp = &datestamp. ;
	  %create_report(&id_report_control.,&datestamp.,&timestamp.,&rundate.);
%mend call_create_report;
options mprint;
options MVARSIZE=MAX;
options noquotelenmax;
%call_create_report();
