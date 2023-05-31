libname main 'C:\Users\rajan\OneDrive\Desktop\DS\Bitcoin'; run;
proc datasets kill nolist; run;

proc import out = bitcoin_crypto_active
	datafile = "C:\Users\rajan\OneDrive\Desktop\DS\Bitcoin\Bitcoin_Crypto_Active_wallets.csv"
	dbms = csv replace;
run;
proc import out = bitcoin_crypto_price
	datafile = "C:\Users\rajan\OneDrive\Desktop\DS\Bitcoin\bitcoin_price_volume_main_2017_2021_fixed.xlsx"
	dbms = xlsx replace;
run;
proc import out = crypto_trends
	datafile = "C:\Users\rajan\OneDrive\Desktop\DS\Bitcoin\Google_Trends_BuyBitcoin_5yr.xlsx"
	dbms = xlsx replace;
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
wallets_daydiff=wallets_next-wallets;
run;

proc sort data= wallets_compare; by record; run;
data days_wallet;
set wallets_compare;
do i=0 to 2;
	new_date=sasdate+i;
	new_date1=put(new_date,date9.);
	change_val=i*(wallets_daydiff/3);
	new_wallet=sum(wallets,change_val);
	format new_wallet 9.;
output;
end;
keep new_date new_date1 new_wallet;
run;

data wallets2;
length year 4 week_str week_str $2 unique_week $6;
set days_wallet;
if new_date<20820 then delete;
year = year(new_date);
new_date = round(new_date);
week = week(new_date,'w');
week_str = week;
if week<10 then week_Str=cat(0,week);
unique_week = cats(year,week_Str);
run;

data bitcoin_price;
set bitcoin_crypto_price;
new_date = Date + 0;
if new_date<20820 then delete;
run;

proc sort data = bitcoin_price; by new_date; run;
proc sort data = wallets2; by new_date; run;

data combo_price_merged;
merge bitcoin_price(in = a) wallets2 (in= b);
by new_date;
year = year(date);
if a and b then count+1;
if a and b then output;
run;

data price_wallet;
length week_str $2 unique_week $6;
set combo_price_merged;
week = week(new_date);
week_u = week(new_date,'u');
week_v = week(new_date,'v');
week_w = week(new_date,'w');
week_str = week;
if week<10 then week_str= cats(0,week_w);
unique_week = cats(year,week_str);
run;

proc means data = price_wallet noprint;
class unique_week; var coin_avg new_wallet coin_day; output out = price_wallet_7days;
run;

data price_walet_7days_final;
length year 4;
set price_wallet_7days;
where _type_>0 and _stat_ = 'MEAN';
year2 = substr(unique_week,1,4);
year = year2+0;
drop _type_ _stat_ _freq_ year2;
run;



data google_trends;
length year 4 week_str week_str $2 unique_week $6;
set crypto_trends;
sasdate=round(excel_date_google)-21916;
if sasdate<20820 then delete;
year=year(sasdate);
week=week(sasdate,'w');
week_str=week;
google_date=a;drop a;
if week<10 then week_str=cats(0,week);
unique_week=cats(year,week_str);
run;

proc sort data=combo2;by unique_week;run;
proc sort data=google_trends;by unique_week;run;


data bitcoin_wallets_final_merge;
merge price_walet_7days_final (in=a) google_trends (in=b); by unique_week;
year = year(sasdate);
if a and b then count+1;
if a and b then output;
run;


/* Merge these all
set main.dollar_strength_dxy_index;

set main.gold_price_update;

set main.intel_stock;

set main.total_wallets;

set main.weekly_snp;
*/

data dollar_strength;
length year 4 week_str week_str $2 unique_week $6;
set main.dollar_strength_dxy_index;
if sasdate<20820 then delete;
year=year(sasdate);
week=week(sasdate,'w');
week_str=week;
if week<10 then week_str=cats(0,week);
unique_week=cats(year,week_str);
keep unique_week avg_dxy_value;
run;

data gold_price;
length year 4 week_str week_str $2 unique_week $6;
set main.gold_price_update;
if sasdate<20820 then delete;
year=year(sasdate);
week=week(sasdate,'w');
week_str=week;
if week<10 then week_str=cats(0,week);
unique_week=cats(year,week_str);
keep unique_week price;
run;

data intel_stock;
length year 4 week_str week_str $2 unique_week $6;
set main.intel_stock;
if sasdate<20820 then delete;
year=year(sasdate);
week=week(sasdate,'w');
week_str=week;
if week<10 then week_str=cats(0,week);
unique_week=cats(year,week_str);
keep unique_week intel_avg intel_day;
run;

data total_wallets;
length year 4 week_str week_str $2 unique_week $6;
set main.total_wallets;
if sas_date<20820 then delete;
year=year(sas_date);
week=week(sas_date,'w');
week_str=week;
if week<10 then week_str=cats(0,week);
unique_week=cats(year,week_str);
keep unique_week wallets_total;
run;

data weekly_snp;
set main.weekly_snp;
unique_week = year_week;
drop year_week;
run;

proc sort data=bitcoin_wallets_final_merge;by unique_week;run;
proc sort data=dollar_strength;by unique_week;run;
proc sort data=gold_price;by unique_week;run;
proc sort data=intel_stock;by unique_week;run;
proc sort data=total_wallets;by unique_week;run;
proc sort data=weekly_snp;by unique_week;run;

data All_bitcoin_wallets_merge;
merge bitcoin_wallets_final_merge (in=a) dollar_strength (in=b) gold_price (in=c) intel_stock (in=d) total_wallets (in=e) weekly_snp (in=e) ; by unique_week;
run;

data All_bitcoin_wallets_merge;
set All_bitcoin_wallets_merge;
drop week_str week google_date count Price sas_date year_week;
run;


proc reg data = All_bitcoin_wallets_merge;
model coin_avg = avg_dxy_value Snp_open wallets_total Buy_Bitcoin;
run;
