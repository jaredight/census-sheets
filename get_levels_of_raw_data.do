

global directory "V:\FHSS-JoePriceResearch\papers\current\occupation_destruction\data\replication"
global years 1900 1910 1920 1930 1940

clear all
set more off

local files: dir "V:\FHSS-JoePriceResearch\papers\current\occupation_destruction\data\raw" files "*.csv"
foreach csv of local files {
	preserve
	
	* import csv file 
	import delimited using "V:\FHSS-JoePriceResearch\papers\current\occupation_destruction\data\raw\/`csv'", clear varnames(1) //rowrange(1:1000)
	
	* keep variables of interest and drop rows with missing data
	capture keep race marst
	drop if missing(race)
	
	* save and append
	save "$directory\temp2.dta", replace
	restore
	cd $directory
	append using temp2, force

}
levelsof race, local(races)
foreach race of local races {
	di "`race'"
}
levelsof marst, local(marsts)
foreach status of local marsts {
	di "`status'"
}