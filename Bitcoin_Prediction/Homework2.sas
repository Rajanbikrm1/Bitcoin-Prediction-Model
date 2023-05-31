libname main 'C:\Users\rajan\OneDrive\Desktop\DS'; run;
proc datasets kill nolist; run;

proc import out = bitcoin_crypto_active
	datafile = "C:\Users\rajan\OneDrive\Desktop\DS\Bitcoin\Bitcoin_Crypto_Active_wallets.csv"
	dbms = csv replace;
run;


data crypto_wallet;
set bitcoin_crypto_active;
rename n_unique_addresses = wallets_char;
sasdate = round(excel_date)-21916;
if sasdate<20818 then delete;
record+1;
run;

proc sort data= crypto_wallet; by decending record; run;

data wallets_compare;
set crypto_wallet;
format wallets 9.;
wallets=input(wallets_char,comma9.);
wallets_next=lag(wallets);
wallets_daydiff=wallets-wallets_next;
run;

data days_wallet;
set wallets_compare;
do i=0 to 2;
	new_date=sasdate+i;
	new_date1=put(new_date,date9.);
	change_val=i*(wallets_daydiff/3);
	new_wallet=sum(wallets,change_val);
	format new_wallet 9.;
	keep wallets wallets_daydiff date_orig sasdate record new_date new_wallet i;
output;
end;
run;

