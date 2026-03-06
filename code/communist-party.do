
import delimited "data/cadre_machines_panel.csv", encoding(UTF-8) clear 

xtset kshcode year

gen n_total= n_cadre_total +n_first_secretary +n_secretary +n_dept_head + n_dept_head_deputy +n_political_worker+ n_member+ n_president+ n_other
gen treated_year=0


replace treated_year=1 if treated_1985 ==1&year==1985
replace treated_year=1 if treated_1986 ==1&year==1986
replace treated_year=1 if treated_1987 ==1&year==1987
replace treated_year=1 if treated_1988 ==1&year==1988
replace treated_year=1 if treated_1989 ==1&year==1989

xtset kshcode year

 sysdir set PERSONAL /Users/mucoa/Library/CloudStorage/Dropbox/Whistleblowing/ado/personal
preserve
simple_event n_total  treated_year  if city != "Budapest", leads(5) lags(5) fe(kshcode year) cluster(kshcode) normalizelead(1) 
restore


preserve
simple_event n_total  treated_year  if city != "Budapest", leads(5) lags(5) fe(kshcode year) cluster(kshcode) normalizelead(1) 
restore
-

* create event time dummies (omit t=-1)
* interact each with repression


xtset kshcode year
forvalues x= 1(1)5{
	qui:gen f_`x'_treated_year=f`x'.treated_year
	qui:replace f_`x'_treated_year = 0 if f_`x'_treated_year==.
	label var f_`x'_treated_year "Lead `x'"
	
	qui:  gen l_`x'_treated_year=l`x'.treated_year
	qui: replace l_`x'_treated_year = 0 if l_`x'_treated_year==.
	label var l_`x'_treated_year "Lag `x'"

}
	

replace  f_1_treated_year=0

	
forvalues k = 2/5 {
    gen pre`k'_X_rep = f_`k'_treated_year * any_exec_1956_59
}
forvalues k = 1/5 {
    gen post`k'_X_rep = l_`k'_treated_year * any_exec_1956_59
}
-
simple_event n_total treated, leads(5) lags(5) ///
    fe(kshcode year) cluster(kshcode) normalizelead(1) ///
    interact(any_exec_1956_59) 
	
	
	
	
* clean slate
cap drop f_*_treated l_*_treated t0_X f_*_X l_*_X lags_leads ///
         coefficients standard_errors upper_bound lower_bound

* version 1: no interaction
simple_event n_total treated, leads(5) lags(5) ///
    fe(kshcode year) cluster(kshcode) normalizelead(1)

* note the t=0 coefficient, then:
cap drop f_*_treated l_*_treated t0_X f_*_X l_*_X lags_leads ///
         coefficients standard_errors upper_bound lower_bound
		 
preserve

* version 2: with interaction
simple_event n_total treated, leads(5) lags(5) ///
    fe(kshcode year) cluster(kshcode) normalizelead(1) ///
    interact(any_exec_1956_59)
	
restore
