/*
jared wright
jaredwright217@gmail.com
5 June 2021
This code cleans teamster information after running the code to scrape family search
1. Parse scraped data to get teamsters' siblings' pids
2. Merge in siblings and calculate lifespan 
*/


* define global variables
global directory "V:\FHSS-JoePriceResearch\papers\current\occupation_destruction\data\lifespan"


*** PART 1: PARSE SCRAPED DATA TO GET SIBLING PIDS
* import scraped teamster data
clear all
set more off
cd $directory
import delimited "teamster_info.csv", clear varnames(1)

* drop unneeded variables
drop pr_name pr_birthdate pr_sex pr_birthplace pr_deathdate pr_deathplace pr_burialdate pr_burialplace marriages marriageplaces sources spouse_name spouse_fsids spouse_birthdates spouse_birthplaces spouse_deathdates spouse_deathplaces spouse_burialdates spouse_burialplaces dad_names dad_fsids dad_birthdates dad_birthplaces dad_deathdates dad_deathplaces dad_burialdates dad_burialplaces mom_names mom_fsids mom_birthdates mom_birthplaces mom_deathdates mom_deathplaces mom_burialdates mom_burialplaces sibling_names sibling_birthdates sibling_sexes kid_names kid_fsids kid_birthdates kid_sexes kid_birthplaces kid_deathdates kid_deathplaces kid_burialdates kid_burialplaces

* reshape data (create a row for each sibling)
split sibling_fsids, parse(";")
reshape long sibling_fsids, i(fsid uid) j(sibling_number)
drop if missing(sibling_fsids)

* generate unique sibling id so that scraping data has a unique identifier. We can then merge in scraped birth and death years using this unique sibling identifier
gen sibling_id = _n
order sibling_id sibling_fsids

* export sibling fsids to be scraped
export delimited "sibling_fsids.csv", replace
rename sibling_fsids pid
export delimited pid using "temp_sibling_fsids_to_scrape.csv", replace


*** PART 2: AFTER SCRAPING FOR SIBLINGS, MERGE TEAMSTERS AND SIBLINGS AND CALCULATE LIFESPAN
* import scraped teamster data
clear all
cd $directory
import delimited "teamster_info.csv", clear varnames(1)

* clean data to find birth and death years
gen birthyear = ""
replace birthyear = regexs(0) if regexm(pr_birthdate, "[0-9]+$")
destring birthyear, force replace
drop if missing(birthyear) | birthyear < 1800 | birthyear > 1935
gen deathyear = ""
replace deathyear = regexs(0) if regexm(pr_deathdate, "[0-9]+$")
destring deathyear, force replace
drop if deathyear < 1900 | (deathyear > 2021 & !missing(deathyear))

* merge in sibling information
cd $directory
import delimited "sib_lifespan.csv", clear varnames(1)
rename ïpid pid
order pid birthyear deathyear

* save siblings that need to be rescraped
gen rescrape = 0
replace rescrape = 1 if (birthyear < "1750")
replace rescrape = 1 if strpos(birthyear, "Dece")
preserve
keep if rescrape == 1
keep pid
gen index = _n
order index
cd $directory
//export delimited "sibs_to_rescrape.csv", replace
restore
drop if rescrape==1
drop rescrape
destring birthyear, replace
sort pid
drop if pid==pid[_n-1]
//save "temp_orig", replace

* save pids that need to be rescraped.
preserve
keep pid
gen index = _n
order index
//export delimited "rescrape_2.csv", replace
restore



*** PART 2: MERGE AND CLEAN DATASETS
* merge siblings with sibling birth and death years
cd $directory
import delimited "sibling_fsids.csv", clear varnames(1)
rename fsid t_pid
rename birthyear t_birthyear
rename deathyear t_deathyear
rename lifespan t_lifespan
rename sibling_fsids pid
//merge m:1 pid using "temp_orig", nogen keep(1 3) * this line uses the first scrape, it doesn't have gender info, so I rescraped.


/******************************************************
* This is a mess. I didn't think about how the data should be formatted and this is the result. Just ignore it.
* merge in half the siblings
preserve
//cd $directory
//import delimited "sibs_rescraped.csv", clear varnames(1)
//save "temp_rescraped", replace
cd $directory
import delimited "rescraped_2.csv", clear varnames(1)
//append using "temp_rescraped", generate(scrape_1) // equals 1 if the observation is from the first, smaller scrape of people whose birthyears were not scraped the first time.
gen birthyear = regexs(1) if regexm(pr_birthdate, "(1[6-9][0-9][0-9])$")
destring birthyear, replace
gen deathyear = regexs(1) if regexm(pr_deathdate, "(1[6-9][0-9][0-9])$")
destring deathyear, replace
rename fsid pid
order pid birthyear deathyear
sort pid
drop if pid==pid[_n-1]
save "temp_rescraped", replace
restore
merge m:1 pid using "temp_rescraped", nogen keep(1 3 4 5) update replace

* clean data
gen is_teamster = (pid==t_pid)
drop if missing(birthyear)
sort t_pid

* compute lifespan
gen lifespan = deathyear - birthyear if !missing(deathyear)
order lifespan

* merge in teamster information (gender)
local sibling_vars lifespan pid birthyear deathyear pr_name pr_birthdate pr_sex pr_birthplace pr_deathdate pr_deathplace pr_burialdate pr_burialplace spouse_fsid marriages marriageplaces kid_fsids dad_fsids mom_fsids sources
foreach v of local sibling_vars {
	local newv = "s_" + "`v'"
	rename `v' `newv'
}
preserve
cd $directory
import delimited "teamster_info.csv", clear
foreach v of var * {
	rename `v' t_`v'
}
save temp, replace
restore
merge m:1 t_pid using temp, nogen keep(1 3)

cd $directory
save teamsters_sibs, replace
//use teamsters_sibs, clear

* PART 3: ANALYSIS
keep if t_pr_sex==s_pr_sex
keep if abs(s_birthyear - t_birthyear) < 15
bysort is_teamster: summarize s_lifespan
sort t_pid
*/

* import teamsters file
cd $directory
import delimited "sibling_fsids.csv", clear varnames(1)
rename fsid t_pid
rename birthyear t_birthyear
rename deathyear t_deathyear
rename lifespan t_lifespan
rename sibling_fsids pid

* merge in more teamster information
preserve
import delimited "teamster_info.csv", clear
save temp2, replace
restore
merge m:1 pid using temp2
replace t_pid = pid if _merge==2 & !missing(pid) & missing(t_pid)

* import siblings file part 1 and 2
preserve
cd $directory
import delimited "rescraped_2.csv", clear varnames(1)
rename fsid pid
rename spouse_fs* spouse_fsids
duplicates drop
save temp, replace
import delimited "more_siblings.csv", clear varnames(1)
rename fsid pid
rename spouse_fs* spouse_fsids
append using temp, generate(from_more_sibs)
sort pid
drop if pid==pid[_n-1]
save temp, replace
restore
merge m:1 pid using temp, update gen(from_sibs_files)
sum t_lifespan
sum t_lifespan if !missing(pr_birthdate)

* the scraper quit halfway through :( so this section exports pids to be scraped, again (9th time's a charm)
/*
sort pid
gen index = _n
gen keeps = 0
replace keeps = 1 if pid=="KWVL-N7T"
preserve
keep if keeps
levelsof index, local(keep_index)
restore
display "`keep_index'"
keep if index > `keep_index'
gen is_teamster = (pid==t_pid)
keep if is_teamster==0 & missing(pr_birthdate)
keep index pid
order index pid
export delimited "$directory\yet_another_rescrape.csv", replace
*/

* merge in 3rd sibling scrape
preserve
cd $directory
import delimited "yet_another_rescraped.csv", clear varnames(1)
rename fsid pid
rename spouse_fs* spouse_fsids
duplicates drop
sort pid
drop if pid==pid[_n-1]
save temp, replace
restore
merge m:1 pid using temp, update gen(from_yet_another_rescraped)
sum t_lifespan
sum t_lifespan if !missing(pr_birthdate)

* the scraper quit again... I think with the last scrape it did exactly 1.03 million and then couldn't scrape anymore. May have something to do with maximum file size somewhere in the process. 
/*
capture drop index
capture drop keeps
sort pid
gen index = _n
gen keeps = 0
replace keeps = 1 if pid=="LZ61-5G9"
preserve
keep if keeps
levelsof index, local(keep_index)
restore
display "`keep_index'"
keep if index > `keep_index'
gen is_teamster = (pid==t_pid)
keep if is_teamster==0 & missing(pr_birthdate)
keep index pid
order index pid
export delimited "$directory\yet_another_rescrape_2.csv", replace
*/

* merge in 4th sibling scrape
preserve
cd $directory
import delimited "yet_another_rescraped_2.csv", clear varnames(1)
rename fsid pid
rename spouse_fs* spouse_fsids
duplicates drop
sort pid
drop if pid==pid[_n-1]
save temp, replace
restore
merge m:1 pid using temp, update gen(from_yet_another_rescraped_2)
sum t_lifespan
sum t_lifespan if !missing(pr_birthdate)

* merge in teamster sibling shared info from teamster dataset so we can get more controls for the regressions
preserve
cd $directory
import delimited "teamster_info.csv", clear varnames(1)
capture rename pid t_pid
save temp3, replace
restore
merge m:1 t_pid using temp3, update gen(from_teamster_info)

* save temp file
save temp_merged, replace

* back to work
cd $directory
use temp_merged, clear
gen is_teamster = (pid==t_pid)
gen is_male = 1 if (pr_sex=="Male")
replace is_male = 0 if (pr_sex=="Female")

* clean data to find birthyear, deathyear, and lifespan
gen birthyear = ""
replace birthyear = regexs(0) if regexm(pr_birthdate, "[0-9]+$")
destring birthyear, force replace
gen deathyear = ""
replace deathyear = regexs(0) if regexm(pr_deathdate, "[0-9]+$")
destring deathyear, force replace
order is_male birthyear deathyear
drop if missing(birthyear) | birthyear < 1800 | birthyear > 1935
drop if deathyear < 1900 | (deathyear > 2021 & !missing(deathyear))
gen lifespan = deathyear - birthyear if !missing(deathyear)
order lifespan

*** first regression: all teamsters vs. all siblings
* drop duplicates and only keep the duplicate marked as a teamster
sort pid is_teamster
gen duplicate = (pid==pid[_n+1])
drop if duplicate
drop duplicate

* summary statistics / balance table
summarize is_teamster if lifespan > 18
bysort is_teamster: summarize is_male if lifespan > 18
bysort is_teamster: summarize birthyear if lifespan > 18
bysort is_teamster: summarize deathyear if lifespan > 18
bysort is_teamster: summarize lifespan if lifespan > 18





*** make some controls!!

* birthstate
gen birthstate = ""
replace birthstate = regexs(1) if regexm(pr_birthplace, "([^:]+)$")
replace birthstate = regexs(1) if regexm(pr_birthplace, "([^:]+): United States$")
replace birthstate = regexs(1) if regexm(pr_birthplace, "([^:]+): Canada$")
replace birthstate = strtrim(birthstate)
gen my_count = 1
bysort birthstate: egen state_freq = sum(my_count)
replace birthstate = "" if state_freq < 30 //we don't want a dummy indicator for states with just a few observations, so we drop states that don't appear often in the dataset. This choice of 30 is pretty arbitrary, what would be a better way to make this decision??
drop state_freq

* deathstate
gen deathstate = ""
replace deathstate = regexs(1) if regexm(pr_deathplace, "([^:]+)$")
replace deathstate = regexs(1) if regexm(pr_deathplace, "([^:]+): United States$")
replace deathstate = regexs(1) if regexm(pr_deathplace, "([^:]+): Canada$")
replace deathstate = strtrim(deathstate)
bysort deathstate: egen state_freq = sum(my_count)
replace deathstate = "" if state_freq < 30 //we don't want a dummy indicator for states with just a few observations, so we drop states that don't appear often in the dataset. This choice of 30 is pretty arbitrary, what would be a better way to make this decision??
drop state_freq

* regressions
eststo: areg lifespan is_teamster is_male if (lifespan > 18) & (lifespan < 120), absorb(birthyear)
esttab using "${directory}\reg1.csv", replace star(* 0.05 ** 0.01) se b(4) label
eststo drop *

//drop people with lifespan less than 18
* exact matching on gender
* control for birthyear
* control for birthplace
* control for birth order
* control for family size
* control for marital status
* control for number of parents??
* number of kids
* death state
* death state same as birth state


cd $directory
capture erase "temp"
capture erase "temp2"
capture erase "temp3"
capture erase "temp_orig"
capture erase "temp_rescraped"
