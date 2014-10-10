%macro send_email;
ODS PATH work.templat(update) sasuser.templat(read) sashelp.tmplmst(read);
	proc template;
		define style mystyle;
			style usertext from usertext / 
				backgroundcolor=white just=left;
			class body /
				just=l
				backgroundcolor=white
				fontsize=2
				fontfamily='Arial';
			class table /
				just=l
				backgroundcolor=white
				bordercolor=black
				borderstyle=solid
				cellpadding=2pt
				cellspacing=2pt
				fontsize=1;
			class data /
				backgroundcolor=white
				fontsize=1;
			class header /
				just=l
				fontweight=bold
				backgroundcolor=lightgrey
				fontsize=1;
		end;
	run;


filename mymail email 
		to=('user1@company.com' 'user2@company.com' 'user3@company.com')
		cc=('bigboss@company.com' )
		subject="Keep calm and carry on"
		from='support@company.com'
		CONTENT_TYPE="text/html";

ODS LISTING CLOSE;
	ODS HTML BODY=mymail style = mystyle 	;
		ods escapechar ='^';
		ods html text='Dear all,';
		ods html text='<br>';
		ods html text="please find below the daily report:";
		ods html text='<br>';


		PROC report DATA=report_out ;
			title ' ';
			format date datetime19.;

		RUN;

		ods html text="<br>";
		ods html text="<br>";
		ods html text= 'Best regards,';
		ods html text= 'SAS Developer Team';

		
	ODS HTML CLOSE;
	ODS LISTING;

%mend send_email;


