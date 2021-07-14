/*
Jared Wright
jaredwright217@gmail.com
This code replicates occupation destruction results, and combines and cleans 
all the messy do files I used the first time. The goal is to have this do file 
replicate all the results found in 
V:\FHSS-JoePriceResearch\papers\current\occupation_destruction\output\regressions_v1
*/

global directory "V:\FHSS-JoePriceResearch\papers\current\occupation_destruction\data\replication"
global years 1900 1910 1920 1930 1940

clear all
set more off

*** DEFINE DATASET LABELS
* value labels for occ bucket variable (occ_bucket)
label define occ_bucket_lbl 0 `"Other"'
label define occ_bucket_lbl 1 `"Expressman"', add
label define occ_bucket_lbl 2 `"Teamster"', add
label define occ_bucket_lbl 3 `"Truck driver"', add
label define occ_bucket_lbl 4 `"Bus driver"', add
label define occ_bucket_lbl 5 `"Chauffer"', add
label define occ_bucket_lbl 6 `"Taxi driver"', add
label define occ_bucket_lbl 7 `"Blacksmith"', add
label define occ_bucket_lbl 8 `"Carpenter"', add

* value labels for sex variable (is_male)
label define is_male_lbl 0 `"Female"'
label define is_male_lbl 1 `"Male"', add

* value labels for race variable (race)
label define race_lbl 0 `"American Indian/Alaska Native (AIAN)"'
label define race_lbl 1 `"Black/African American/Negro"', add
label define race_lbl 2 `"Chinese"', add
label define race_lbl 3 `"Filipino"', add
label define race_lbl 4 `"Japanese"', add
label define race_lbl 5 `"Mexican (1930)"', add
label define race_lbl 6 `"Mulatto"', add
label define race_lbl 7 `"White"', add

* value labels for marital status variable (marst)
label define marst_lbl 0 `"Divorced"'
label define marst_lbl 1 `"Married, spouse absent"', add
label define marst_lbl 2 `"Married, spouse present"', add
label define marst_lbl 3 `"Never married/single"', add
label define marst_lbl 4 `"Separated"', add
label define marst_lbl 5 `"Widowed"', add

*** IMPORT CENSUS DATA
local files: dir "V:\FHSS-JoePriceResearch\papers\current\occupation_destruction\data\raw" files "*.csv"
foreach csv of local files {
	preserve
	di "importing `csv'"

	quietly {
	* import csv file 
	import delimited using "V:\FHSS-JoePriceResearch\papers\current\occupation_destruction\data\raw\/`csv'", clear varnames(1) rowrange(1:100) ////////////////////////////////////////////////////////////
	
	* keep variables of interest and drop rows with missing data
	capture keep sex race marst occstr birthyr histid
	drop if missing(histid)
	capture destring age, force replace
		
	* create occ_bucket variable
	capture tostring occstr, force replace
	gen occ_bucket = 0
	replace occ_bucket = 1 if strpos(occstr, "EXPRESS") > 0
	replace occ_bucket = 2 if strpos(occstr, "TEAM") > 0 | strpos(occstr, "WAGON") > 0 | strpos(occstr, "CART") > 0 | strpos(occstr, "DRAY") > 0
	replace occ_bucket = 3 if strpos(occstr, "TRUCK") > 0
	replace occ_bucket = 4 if strpos(occstr, "BUS") > 0
	replace occ_bucket = 5 if strpos(occstr, "CHAUFFER") > 0 
	replace occ_bucket = 6 if strpos(occstr, "TAXI") > 0 | strpos(occstr, "CAB") > 0
	replace occ_bucket = 7 if strpos(occstr, "BLACKSMITH") > 0
	replace occ_bucket = 8 if strpos(occstr, "CARPENTER") > 0
	label values occ_bucket occ_bucket_lbl
	drop occstr
	
	* encode sex, create is_male variable
	gen is_male = .
	replace is_male = 0 if strpos(sex, "Female") > 0
	replace is_male = 1 if strpos(sex, "Male") > 0
	label values is_male is_male_lbl
	drop sex
	
	* encode race
	gen race_bucket = .
	replace race_bucket = 0 if strpos(race, "American Indian/Alaska Native (AIAN)")
	replace race_bucket = 1 if strpos(race, "Black/African American/Negro")
	replace race_bucket = 2 if strpos(race, "Chinese")
	replace race_bucket = 3 if strpos(race, "Filipino")
	replace race_bucket = 4 if strpos(race, "Japanese")
	replace race_bucket = 5 if strpos(race, "Mexican (1930)")
	replace race_bucket = 6 if strpos(race, "Mulatto")
	replace race_bucket = 7 if strpos(race, "White")
	
	/* these are all the ancestry races 1900-1940. Change labels above as well if you add these.
	Aleut
	American Indian/Alaska Native (AIAN)
	Asian Indian (Hindu 1920_1940)
	Asian, not specified
	Asiatic Hawaiian (1920)
	Black/African American/Negro
	Caucasian Hawaiian (1920)
	Chinese
	Eskimo
	Filipino
	Guamanian/Chamorro
	Hawaiian mixed
	Hmong
	Japanese
	Korean
	Malaysian
	Mexican (1930)
	Mulatto
	Native Hawaiian
	Other Asian or Pacific Islander (1980)
	Other Polynesian (1990)
	Portuguese
	Puerto Rican
	Samoan
	Spanish write_in
	Tahitian
	Thai
	White
	Yaqui
	*/
	label values race_bucket race_lbl
	drop race 
	rename race_bucket race
	
	* encode marst
	gen marst_bucket = .
	replace marst_bucket = 0 if strpos(marst, "Divorced")
	replace marst_bucket = 1 if strpos(marst, "Married, spouse absent")
	replace marst_bucket = 2 if strpos(marst, "Married, spouse present")
	replace marst_bucket = 3 if strpos(marst, "Never married/single")
	replace marst_bucket = 4 if strpos(marst, "Separated")
	replace marst_bucket = 5 if strpos(marst, "Widowed")
	label values marst_bucket marst_lbl
	drop marst
	rename marst_bucket marst
	
	* create year variable
	local year = regexm("`csv'", "^occ_(19[0-9][0-9])")
	local year = regexs(1)
	gen year = `year'
	
	* save and append
	save "$directory\temp.dta", replace
	restore
	cd $directory
	append using temp, force
	
	}
}

* drop duplicates and save
sort histid
drop if histid == histid[_n-1]
cd $directory
capture save "all_ppl", replace ////////////////////////////////////////////////////////////

foreach year of global years {
	rename histid histid`year'
	cd "V:\FHSS-JoePriceResearch\data\census_refined\anc\/`year'"
	merge 1:1 histid`year' using histid`year'_city, nogen keep(1 3) update replace
	capture rename us19 city
	merge 1:1 histid`year' using histid`year'_county, nogen keep(1 3) update replace
	capture rename us19 county
	merge 1:1 histid`year' using histid`year'_state, nogen keep(1 3) update replace
	capture rename us19 state
	merge 1:1 histid`year' using histid`year'_labforce, nogen keep(1 3) update replace
	/*
	Yes, in the labor force
	No, not in the labor force
	N/A
	*/
	merge 1:1 histid`year' using histid`year'_occscore, nogen keep(1 3) update replace
	drop if missing(histid`year')
}
rename histid histid

*** temp code
/*
gen city = ""
replace city = "NEW YORK" if !mod(_n, 5)
replace city = "BALTIMORE" if !mod(_n + 1, 5)
replace city = "SAN FRANCISCO" if !mod(_n + 2, 5)
replace city = "BOULDER" if !mod(_n + 3, 5)
replace city = "HERMISTON" if !mod(_n + 4, 5)
replace city = "RICHLAND" if !mod(_n, 7)
replace city = "KENNEWICK" if !mod(_n, 11)
gen county = ""
replace county = "NEW YORK" if !mod(_n, 3)
replace county = "UMATILLA" if !mod(_n+1, 3)
replace county = "BENTON" if !mod(_n+2, 3)
gen state = "TEXAS"
replace state = "WASHINGTON" if !mod(_n, 2)
*/

gen age = year - birthyr


encode city, gen(city_code) label(city_lbl)
encode county, gen(county_code) label(county_lbl)
encode state, gen(state_code) label(state_lbl)

gen person = 1
bysort city_code county_code state_code year: egen population = count(person)
gen is_wrk_male = 0
replace is_wrk_male = 1 if is_male==1 & strpos(labforce, "Yes, in the labor force") & (age > 16) & (age < 60)
bysort city_code county_code state_code year: egen pop_wrkg_males = count(is_wrk_male)


cd $directory
capture save "all_ppl2", replace ////////////////////////////////////////////////////////////



/*

*** this code is temporary. I'm using it to figure out all the possible options for race and marst. I can't find a dictionary anwhere, so I'm making one
local files: dir "V:\FHSS-JoePriceResearch\papers\current\occupation_destruction\data\raw" files "*.csv"
foreach csv of local files {
	preserve
	
	* import csv file 
	import delimited using "V:\FHSS-JoePriceResearch\papers\current\occupation_destruction\data\raw\/`csv'", clear varnames(1) //rowrange(1:1000)
	
	* keep variables of interest and drop rows with missing data
	capture keep race
	drop if missing(race)
	
	* save and append
	save "$directory\temp.dta", replace
	restore
	cd $directory
	append using temp, force

}
levelsof race, local(races)
foreach race of local races {
	di "`race'"
}


levelsof marst, local(marsts)
foreach race of local marsts {
	di "`race'"
}

*/

