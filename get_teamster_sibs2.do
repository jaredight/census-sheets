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



* After a few failed scrapes, I ended up not needing this section.
/*
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
*/


*** PART 3: MERGE DATASETS

* import file of teamsters and siblings
cd $directory
import delimited "sibling_fsids.csv", clear varnames(1)
rename fsid t_pid
rename birthyear t_birthyear
rename deathyear t_deathyear
rename lifespan t_lifespan
rename sibling_fsids pid

* merge in more teamster information. This section only merges in family information for teamsters
preserve
import delimited "teamster_info.csv", clear
save temp2, replace
restore
merge m:1 pid using temp2
replace t_pid = pid if _merge==2 & !missing(pid) & missing(t_pid)

* import siblings file part 1 and 2. This merges in family information for some siblings
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

* merge in 3rd sibling scrape. This code merges in family information for more siblings
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

* merge in 4th sibling scrape. This code merges in family information for all the siblings that still have missing family information.
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
/* 
* I commented this out because I don't want to accidently use teamster info in place of sibling info when generating controls. I thought I needed this merge for some information but I don't remember why now.
preserve
cd $directory
import delimited "teamster_info.csv", clear varnames(1)
capture rename pid t_pid
save temp3, replace
restore
merge m:1 t_pid using temp3, update gen(from_teamster_info)
*/

* save file of all teamsters, siblings, and family information
save teamsters_sibs_info, replace




*** PART 4: now that the data is all merged together, we need to clean the data 
cd $directory
use teamsters_sibs_info, clear
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



*** PART 5: MAKE SOME CONTROLS!!
*** Controls:
* only run the regressions on people with lifespan less than 18
* exact matching on gender
* control for birthyear
* control for birthplace
* control for birth order  - this would be difficult. implement later.
* control for family size (number of siblings)
* control for marital status
* control for number of parents??
* number of kids - make sure this is in the basic scrape and I'm not overwriting kid info with teamster's kid info on the last merge with teamster_info
* death state
* death state same as birth state


* birthstate
capture drop birthstate deathstate same_birdeath_state birthstate_code deathstate_code
gen birthstate = ""
replace birthstate = regexs(1) if regexm(pr_birthplace, "([^:]+)$")
replace birthstate = regexs(1) if regexm(pr_birthplace, "([^:]+): United States$")
replace birthstate = regexs(1) if regexm(pr_birthplace, "([^:]+): Canada$")
replace birthstate = strtrim(birthstate)
replace birthstate = subinstr(birthstate, ".", "", .)
replace birthstate = subinstr(birthstate, ",", "", .)
replace birthstate = subinstr(birthstate, ">", "", .)
replace birthstate = lower(birthstate)

replace birthstate = "california" if birthstate=="ca"
replace birthstate = "iowa" if birthstate=="ia"
replace birthstate = "new jersey" if birthstate=="nj"
replace birthstate = "kentucky" if birthstate=="ky"
replace birthstate = "illinois" if birthstate=="il"
replace birthstate = "illinois" if birthstate=="ill"
replace birthstate = "indiana" if birthstate=="in"
replace birthstate = "indiana" if birthstate=="ind"
replace birthstate = "kansas" if birthstate=="ks"
replace birthstate = "massachusetts" if birthstate=="ma"
replace birthstate = "michigan" if birthstate=="mi"
replace birthstate = "michigan" if birthstate=="mich"
replace birthstate = "minnesota" if birthstate=="mn"
replace birthstate = "missouri" if birthstate=="mo"
replace birthstate = "north carolina" if birthstate=="nc"
replace birthstate = "new hampshire" if birthstate=="nh"
replace birthstate = "new york" if birthstate=="ny"
replace birthstate = "ohio" if birthstate=="oh"
replace birthstate = "pennsylvania" if birthstate=="pa"
replace birthstate = "texas" if birthstate=="tx"
replace birthstate = "virginia" if birthstate=="va"
replace birthstate = "wisconsin" if birthstate=="wi"
replace birthstate = "west virginia" if birthstate=="wv"
replace birthstate = "united states" if birthstate=="usa"
replace birthstate = "united states" if birthstate=="united states of america"

capture gen my_count = 1
bysort birthstate: egen state_freq = sum(my_count)
replace birthstate = "" if state_freq < 30 //we don't want a dummy indicator for states with just a few observations, so we drop states that don't appear often in the dataset. This choice of 30 is pretty arbitrary, what would be a better way to make this decision??
drop state_freq


* deathstate
gen deathstate = ""
replace deathstate = regexs(1) if regexm(pr_deathplace, "([^:]+)$")
replace deathstate = regexs(1) if regexm(pr_deathplace, "([^:]+): United States$")
replace deathstate = regexs(1) if regexm(pr_deathplace, "([^:]+): Canada$")
replace deathstate = strtrim(deathstate)
replace deathstate = subinstr(deathstate, ".", "", .)
replace deathstate = subinstr(deathstate, ",", "", .)
replace deathstate = subinstr(deathstate, ">", "", .)
replace deathstate = lower(deathstate)

replace deathstate = "california" if deathstate=="ca"
replace deathstate = "iowa" if deathstate=="ia"
replace deathstate = "new jersey" if deathstate=="nj"
replace deathstate = "kentucky" if deathstate=="ky"
replace deathstate = "illinois" if deathstate=="il"
replace deathstate = "illinois" if deathstate=="ill"
replace deathstate = "indiana" if deathstate=="in"
replace deathstate = "indiana" if deathstate=="ind"
replace deathstate = "kansas" if deathstate=="ks"
replace deathstate = "massachusetts" if deathstate=="ma"
replace deathstate = "michigan" if deathstate=="mi"
replace deathstate = "michigan" if deathstate=="mich"
replace deathstate = "minnesota" if deathstate=="mn"
replace deathstate = "missouri" if deathstate=="mo"
replace deathstate = "north carolina" if deathstate=="nc"
replace deathstate = "new hampshire" if deathstate=="nh"
replace deathstate = "new york" if deathstate=="ny"
replace deathstate = "ohio" if deathstate=="oh"
replace deathstate = "pennsylvania" if deathstate=="pa"
replace deathstate = "texas" if deathstate=="tx"
replace deathstate = "virginia" if deathstate=="va"
replace deathstate = "wisconsin" if deathstate=="wi"
replace deathstate = "west virginia" if deathstate=="wv"
replace deathstate = "united states" if deathstate=="usa"
replace deathstate = "united states" if deathstate=="united states of america"

bysort deathstate: egen state_freq = sum(my_count)
replace deathstate = "" if state_freq < 30 //we don't want a dummy indicator for states with just a few observations, so we drop states that don't appear often in the dataset. This choice of 30 is pretty arbitrary, what would be a better way to make this decision??
drop state_freq

* number of siblings
gen n_sibs = strlen(subinstr(sibling_fsids, ";", "", .)) / 8
tab n_sibs

* number of moms
gen n_moms = strlen(subinstr(mom_fsids, ";", "", .)) / 8
tab n_moms

* number of dads
gen n_dads = strlen(subinstr(dad_fsids, ";", "", .)) / 8
tab n_dads

* number of kids
gen n_kids = strlen(subinstr(kid_fsids, ";", "", .)) / 8
tab n_kids

* number of spouses
gen n_spouses = strlen(subinstr(spouse_fsids, ";", "", .)) / 8
tab n_spouses

* number of sources
gen n_sources = strlen(subinstr(sources, ";", "", .)) / 8
tab n_sources


*** derivative controls

* indicator for whether individual was born and died in the same state
gen same_birdeath_state = (birthstate==deathstate) if !missing(birthstate) & !missing(deathstate)
encode birthstate, gen(birthstate_code)
encode deathstate, gen(deathstate_code)

* non-linear controls for birthyear
gen byear2 = birthyear ^ 2
gen byear3 = birthyear ^ 3 
gen byear4 = birthyear ^ 4 
gen byear5 = birthyear ^ 5

* squared terms for siblings, parents, kids, spouses, number of sources
gen n_sibs2 = n_sibs ^ 2
gen n_moms2 = n_moms ^ 2
gen n_dads2 = n_dads ^ 2
gen n_kids2 = n_kids ^ 2
gen n_spouses2 = n_spouses ^ 2
gen n_sources2 = n_sources ^ 2


* REGRESSIONS!!!!!!!!!!
global controls birthyear byear2 byear3 byear4 byear5 n_sibs n_moms n_dads n_kids n_spouses n_sources n_sibs2 n_moms2 n_dads2 n_kids2 n_spouses2 n_sources2 same_birdeath_state

eststo drop *
eststo: reg lifespan is_teamster is_male if (lifespan > 18) & (lifespan < 120)

eststo: reg lifespan is_teamster is_male same_birdeath_state birthyear byear2 n_sibs n_moms n_dads n_kids n_spouses n_sources i.birthstate_code i.deathstate_code if (lifespan > 18) & (lifespan < 120)

eststo: reg lifespan is_teamster is_male same_birdeath_state $controls i.birthstate_code i.deathstate_code if (lifespan > 18) & (lifespan < 120)

eststo: reg lifespan is_teamster is_male same_birdeath_state i.birthstate_code i.deathstate_code birthyear byear2 byear3 byear4 byear5 i.n_moms i.n_dads i.n_sibs i.n_kids i.n_spouses i.n_sources if (lifespan > 18) & (lifespan < 120)

eststo: areg lifespan is_teamster is_male same_birdeath_state i.birthstate_code i.deathstate_code i.n_moms i.n_dads i.n_sibs i.n_kids i.n_spouses i.n_sources if (lifespan > 18) & (lifespan < 120), absorb(birthyear)

esttab using "${directory}\reg1.csv", replace star(* 0.05 ** 0.01) se b(4) keep(is_teamster is_male same_birdeath_state $controls 1.n_moms 1.n_dads) label
eststo drop *



cd $directory
capture erase "temp"
capture erase "temp2"
capture erase "temp3"
capture erase "temp_orig"
capture erase "temp_rescraped"
