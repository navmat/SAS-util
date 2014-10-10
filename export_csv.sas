
options symbolgen mprint mlogic mlogicnest mprintnest;
%macro export_csv(export_name,datestamp,timestamp,bukrs,autotransfer=N,zip_name=NASDA,csv_name=);
	%global drecom_data_folder source_name timestamp_generated;
	%put timestamp=&timestamp.;
	%put export_name=&export_name.;
	%put datestamp=&datestamp.;
	%put bukrs=&bukrs.;
	%let limit=5000000;
	%let drecom_data_folder=/home/bgdft/APOOL/datei;	
	%let drecom_control_folder=/home/bgdft/APOOL;	
	%let source_name=PRD;
	%if "&env_name."="INT" %then %let source_name=UAT;
	
	/* determine how many csv (parts) needs to be created */
	%global exported_lines;
	%let exported_lines=0;	
	proc sql noprint;
		select count(1) into:exported_lines from export;
	quit;
	%put exported_lines=&exported_lines.;
	
	%let parts=1;	
	proc sql noprint;
		select max(1,(int((count(1)-1)/&limit.)+1)) into:parts from export;
	quit;
	%put parts=&parts.;
	%let parts=%trim(%left(&parts.));
	
	%do j=1 %to &parts.;
		/* Determine a uniqe number for the ZIP file timestamp */
		proc sql noprint;
			connect to oracle as conn_ora(user="&ora_user" password="&ora_pwd" path="&ora_path") ;
			select * into: zip_counter from connection to conn_ora
					( select seq_export_zip.nextval from dual);
			disconnect from conn_ora;			
		quit;
		%put zip_counter = &zip_counter.;
		%let zip_counter =  %sysfunc(mod(&zip_counter.,3600));
		%put zip_counter mod 3600 = &zip_counter.;
		%let timestamp_generated   = %substr(&timestamp.,1,2)%sysfunc(putn(%eval(%sysfunc(int(%eval(&zip_counter.)/60))),z2.))%sysfunc(putn(%sysfunc(mod(&zip_counter.,60)),z2.));
		%put timestamp_generated=&timestamp_generated.;
		%if &csv_name. ne  %then %do;
			%let outfile=&_70_report_csv./LDM_&source_name._FRA_&csv_name._&datestamp._&timestamp_generated..csv;
		%end;
		%else %do;
			%let outfile=&_70_report_csv./LDM_&source_name._FRA_&export_name._&datestamp._&bukrs._&j._of_&parts..csv;
		%end;
		%put outfile=&outfile.;
		
		/* ecport the file to the csv*/
		proc export  data=export(firstobs= %eval((&j-1)*&limit.+1) obs=%eval(&j*&limit.))
		   outfile="&outfile."
		   dbms=csv
		   replace;
		   delimiter=";";	
		run;
		
		/* Create the ZIP file*/
		X "zip -j &drecom_data_folder./LDM_&source_name._FRA_&ZIP_NAME._&datestamp._&timestamp_generated..ZIP &outfile." ;
		X "rm &outfile.";  
		
		
		/* Create a TOUCH file if it is needed, based on the rules in table REPORT_TOUCHFILE_CONTROL */
		%let day=%sysfunc(putn(%sysfunc(today()),downame.));
		%put day=&day.;
		%let time=%sysfunc(substr(%sysfunc(putn(%sysfunc(datetime()),datetime19.)),11,5));
		%put time=&time.;
		%let need_touch_file_flg=N;

		Proc sql noprint;
			select 
				'Y' into:need_touch_file_flg 
			from oraladm.report_touchfile_control
			where DAY_OF_WEEK = "&day." and 
				dhms(today(),input(substr(start_time,1,2),best.),input(substr(start_time,4,2),best.),0) <= datetime() and
				datetime() <=  dhms(today(),input(substr(end_time,1,2),best.),input(substr(end_time,4,2),best.),0) ;
		quit;
		
		%if &autotransfer.=Y %then %do;
			data _NULL_;
			call symput('need_touch_file_flg','Y');
			run;
		%end;
		
		%put need_touch_file_flg = &need_touch_file_flg.;
		%if "&need_touch_file_flg."="Y" %then %do;
			%put Create touch file.;
			X "touch &drecom_control_folder./LDM_&source_name._FRA_&ZIP_NAME._&datestamp._&timestamp_generated..ZIP";
		%end; 
		
		
		proc sql;
			connect to oracle(user="&ora_user" password="&ora_pwd" path="&ora_path"); 
			execute ( 
				insert into fb_e2e
					(idbatchauto,filename)
				values
					(&idBatchAuto.,%nrbquote('LDM_&source_name._FRA_&ZIP_NAME._&datestamp._&timestamp_generated.'))
			) by oracle; 
		disconnect from oracle;  
   quit;
		
	%end;
%mend export_csv;
