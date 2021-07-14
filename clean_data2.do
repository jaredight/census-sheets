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


foreach year of global years {
	local year10 = `year' + 10
	di "`year' to `year10'"
	cd "V:\FHSS-JoePriceResearch\papers\current\occupation_destruction\data"
	use "occ_bucket_`year'", clear
	
	* generate unique integer index for each family (fam_code)
	gen is_head = strpos(relate, "Head/householder")
	gen fam_code = 0
	replace fam_code = _n if is_head
	replace fam_code = fam_code[_n-1] if fam_code==0
	
	* merge with location data (city, county, state) 
	sort histid
	drop if histid==histid[_n-1]
	rename histid histid`year'
	cd "V:\FHSS-JoePriceResearch\data\census_refined\anc\/`year'"
	merge 1:1 histid`year' using histid`year'_city, nogen keep(1 3) update replace
	capture rename us19 city`year'
	capture rename city city`year'
	merge 1:1 histid`year' using histid`year'_county, nogen keep(1 3) update replace
	capture rename us19 county`year'
	capture rename county county`year'
	merge 1:1 histid`year' using histid`year'_state, nogen keep(1 3) update replace
	capture rename us19 state`year'
	capture rename state state`year'
	
	* merge with labor force participation data
	merge 1:1 histid`year' using histid`year'_labforce, nogen keep(1 3) update replace
	
	* merge with crosswalk to find corresponding city in next census year 
	cd "V:\FHSS-JoePriceResearch\papers\current\occupation_destruction\data\anc_cities_crosswalk"
	capture noisily merge m:1 city`year' county`year' state`year' using "`year'_`year10'_crosswalk"
	
	* merge with population data
	cd "V:\FHSS-JoePriceResearch\papers\current\occupation_destruction\data\anc_cities_pop"
	merge 1:1 city`year' county`year' state`year' using "`year'_cities_pop", nogen

}



