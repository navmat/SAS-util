/*To create a SAS report based on the control table*/

options symbolgen mprint mlogic mlogicnest mprintnest;
options mvarsize=MAX;

%macro create_report(id_report_control,datestamp,timestamp,rundate);
	%let varsize=%sysfunc(getoption(mvarsize,keyword));
	%put varsize=&varsize;

	proc sql;
		update aflac.report_control 
			set 
				status='RUNNING',
				start_time=DATETIME()
			where id_report_control = &id_report_control.;
	quit;

	%let field_list = 0 dummy;
	%let source = dual;
	%let filter = 1=2;
	%let group_by =;
	%let report_name=report;
	%let filenames=;
	%let preprocess=;
	%let send_empty_files=;
	
	data _null_;
		set aflac.report_control(DBMAX_TEXT=32000  where=(id_report_control = &id_report_control.));
		call symput('field_list',trim(left(field_list)));
		call symput('source',trim(left(source)));
		call symput('filter',trim(left(filter)));
		call symput('group_by',trim(left(group_by)));		
		call symput('notification_email',trim(left(notification_email)));
		call symput('report_name',trim(left(report_name)));
		if missing(zip_name) then call symput('zip_name','NASDA');
		else call symput('zip_name',trim(left(zip_name)));
		call symput('csv_name',trim(left(csv_name)));
		call symput('autotransfer',trim(left(autotransfer)));
		call symput('preprocess',trim(left(preprocess)));
		call symput('send_empty_files',trim(left(send_empty_files)));
	run;

	%global idBatchAuto;
	%batchauto_start(AE_&report_name.);
	%create_date_variables(AE_&report_name.,&rundate.);
	
	%put datestamp = &datestamp.;
	%put field_list = &field_list.;
	%put source = &source.;
	%put filter = "&filter.";
	%let group_by_expression =;
	%put group_by = "&group_by.";	
	%if %bquote(&group_by) ne %then %do; 
		%let group_by_expression = GROUP BY %bquote(&group_by.);
	%end;	
	%put group_by_expression = "&group_by_expression.";
	%put zip_name = "&zip_name.";
	%put csv_name = "&csv_name.";
	%put autotransfer = "&autotransfer.";
	
	%put report_name=&report_name.;
	%put filenames=&filenames.;
	%put notification_email = &notification_email.;
	%put preprocess = "&preprocess.";
	%put send_empty_files = &send_empty_files.;

	
	%if "&preprocess." ne " " %then %do;
		proc sql;
			connect to oracle as conn_ora(user="&ora_user" password="&ora_pwd" path="&ora_path") ;
			execute (begin &preprocess.; end;) by conn_ora;
			disconnect from conn_ora;
		quit;
	%end;	
	
	proc sql;
		
		connect to oracle as conn_ora(user="&ora_user" password="&ora_pwd" path="&ora_path") ;
		execute (alter session set  "_fix_control"='7170213:OFF') by conn_ora;
	   /*execute (alter session set "_parallel_statement_queuing"=true) by conn_ora; 
	   execute (alter session set parallel_degree_policy=manual) by conn_ora;
	   execute (alter session force parallel query parallel 16) by conn_ora;
	   execute (alter session force parallel dml parallel 16) by conn_ora;
	   execute (alter session force parallel ddl parallel 16) by conn_ora;
	   execute (alter session set optimizer_index_cost_adj=9999) by conn_ora;
	   execute (alter session set "_optimizer_ignore_hints"=true) by conn_ora;
	   execute (alter session set optimizer_dynamic_sampling=0) by conn_ora;		*/
			create table export as  
				select * from connection to conn_ora
				(
					select
						&field_list.
					from 
						&source.
					WHERE
						&filter. 
					&group_by_expression.
				);
			disconnect from conn_ora;
	quit;

	%export_csv(&report_name.,&datestamp.,&timestamp.,,autotransfer=&autotransfer.,zip_name=&zip_name.,csv_name=&csv_name.);
	
	%if (&notification_email. ne   and  (&exported_lines.>0 or "&send_empty_files" ne "N") ) %then %do;
		%sendmail(&notification_email.);
	%end;	
	proc sql;
		update aflac.report_control set 
			status='OK',
			filenames = "&filenames.",
			start_time=DATETIME()

		where id_report_control = &id_report_control.;
	quit;
	%batchauto_end(&idBatchAuto.,&status.,&exported_lines.,last_basl_out_id = &idbatchauto_inbound.);
%mend create_report;
%macro sendmail(notification_email);
	filename mymail email 
		to=(&notification_email.)
		subject="Extraction mail: &report_name. sent from: &env_name. environment"
		from='AMOS-CDM@allianz.com'
		Attach=("&drecom_data_folder./LDM_&source_name._FRA_&ZIP_NAME._&datestamp._&timestamp_generated..ZIP")
		CONTENT_TYPE="text/html";
		
	ODS LISTING CLOSE;
	ODS HTML BODY=mymail style = mystyle 	;
		ods escapechar ='^';
		ods html text='Dear Recepient,';
		ods html text='<br>';
		ods html text="Please find the attached &report_name. extraction from &env_name. system.";
		ods html text='<br>';
		ods html text= 'Best regards,';
		ods html text= 'FINI DIALOG Team';
		ods html text="<br>";
	ODS HTML CLOSE;
	ODS LISTING;
%mend sendmail;
%macro create_date_variables(jobname,rundate);

	%global idbatchauto_inbound idbatchauto_last_processed_inb 
		date_1D date_1D_s date_1M date_1M_year date_1M_month date_1Y_year
		date_1Y_year_start date_1Y_year_start_s
		date_1Y_year_end date_1Y_year_end_s
		date_1M_month_start date_1M_month_start_s
		date_1M_month_end date_1M_month_end_s	
	;

	/* The last processed inbound idbatchauto, before the current job */ 
	%let idbatchauto_inbound=0;

	/* The last processed inbound idbatchauto, before the previous job with the same name */ 
	%let idbatchauto_last_processed_inb=0;
	
	proc sql;
		connect to oracle as conn_ora(user="&ora_user" password="&ora_pwd" path="&ora_path") ;

		select idbatchauto into:idbatchauto_inbound from connection to conn_ora (
			select max(idbatchauto) idbatchauto from batchauto
			where 
				batchmode='INBOUND' and 
				exec_status='O' and 
				idbatchauto < &idbatchauto.
		);

		select coalesce(idbatchauto,0) into:idbatchauto_last_processed_inb from connection to conn_ora (
			select max(last_basl_out_id) idbatchauto from batchauto
			where 
				batchmode=%nrbquote('&jobname') and 
				exec_status='O' and 
				idbatchauto < &idbatchauto
		);		


		disconnect from conn_ora;
	quit;	

	
	%put idbatchauto_inbound = &idbatchauto_inbound.;
	%put idbatchauto_last_processed_inb = &idbatchauto_last_processed_inb.;
	
	/*date -1 day*/
	%let date_1D = %sysfunc(intnx(day,&rundate,-1));
	%put date_1D=%sysfunc(putn(&date_1D,yymmdd10.));
	
	/*date -1 day, in string format yyyy-mm-dd*/
	%let date_1D_s = %sysfunc(putn(&date_1D,yymmdd10.));
	%put date_1D_s=	&date_1D_s.;
	
	/*date -1 month*/
	%let date_1M = %sysfunc(intnx(month,&rundate,-1));
	%put date_1M=%sysfunc(putn(&date_1M,yymmdd10.));
	
	/*date -1 month, year part*/
	%let date_1M_year = %sysfunc(year(&date_1M));
	%put date_1M_year=&date_1M_year.;

	/*date -1 year, year part*/
	%let date_1Y_year = %sysfunc(year(%sysfunc(intnx(year,&rundate,-1))));
	%put date_1Y_year=&date_1Y_year.;

	
	/*date -1 month, month part*/
	%let date_1M_month = %sysfunc(month(&date_1M));
	%put date_1M_month=&date_1M_month.;
	
	/*date -1 year, first day of year; _S in string format*/
	%let date_1Y_year_start = %sysfunc(intnx(year,&rundate,-1,BEGINNING));
	%put date_1Y_year_start= %sysfunc(putn(&date_1Y_year_start,yymmdd10.));
	%let date_1Y_year_start_s= %sysfunc(putn(&date_1Y_year_start,yymmdd10.));
	%put date_1Y_year_start_s=&date_1Y_year_start_s.;
	
	/*date -1 year, last day of year; _S in string format*/
	%let date_1Y_year_end = %sysfunc(intnx(year,&rundate,-1,END));
	%put date_1Y_year_end= %sysfunc(putn(&date_1Y_year_end,yymmdd10.));
	%let date_1Y_year_end_s= %sysfunc(putn(&date_1Y_year_end,yymmdd10.));
	%put date_1Y_year_end_s=&date_1Y_year_end_s.;
	
	/*date -1 month, first day of month; _S in string format*/
	%let date_1M_month_start = %sysfunc(intnx(month,&rundate,-1,BEGINNING));
	%put date_1M_month_start= %sysfunc(putn(&date_1M_month_start,yymmdd10.));
	%let date_1M_month_start_s= %sysfunc(putn(&date_1M_month_start,yymmdd10.));
	%put date_1M_month_start_s=&date_1M_month_start_s.;
	
	/*date -1 month, last day of month; _S in string format*/
	%let date_1M_month_end = %sysfunc(intnx(month,&rundate,-1,END));
	%put date_1M_month_end= %sysfunc(putn(&date_1M_month_end,yymmdd10.));
	%let date_1M_month_end_s= %sysfunc(putn(&date_1M_month_end,yymmdd10.));
	%put date_1M_month_end_s=&date_1M_month_end_s.;	
	
	
	
%mend create_date_variables;
