program define simple_event
    * ----------------------------------------------------------------
    * simple_event: event study with optional interaction term
    *
    * Without interact(): standard event study, plots main coefficients.
    * With interact(str): single regression with lead/lag × interact terms.
    *   Plots two series:
    *     blue dashed  = effect when interact==0  (main coefficients)
    *     red solid    = effect when interact==1  (main + interaction, via lincom)
    *
    * Syntax:
    *   simple_event outcome treatment [if] [in],
    *       leads(#) lags(#) fe(varlist) cluster(varlist) normalizelead(#)
    *       [interact(str) label0(str) label1(str)]
    * ----------------------------------------------------------------
    syntax varlist(min=2) [if] [in],   ///
        leads(integer)                  ///
        lags(integer)                   ///
        fe(varlist)                     ///
        cluster(varlist)                ///
        normalizelead(integer)          ///
        [interactvar(string)               ///
         label0(string)                 ///
         label1(string)]

    local outcome        = word("`varlist'", 1)
    local treatment_var  = word("`varlist'", 2)
    local max_leads      = `leads'
    local max_lags       = `lags'
    local normalize_lead = `normalizelead'
    local neg_leads      = -`max_leads'

    if `"`label0'"' == "" local label0 "`interactvar'=0"
    if `"`label1'"' == "" local label1 "`interactvar'=1"

    * ── Clean up leftover variables from any previous run ────────
    forvalues x = 1/`max_leads' {
        cap drop f_`x'_`treatment_var' f_`x'_X
    }
    forvalues x = 1/`max_lags' {
        cap drop l_`x'_`treatment_var' l_`x'_X
    }
    cap drop t0_X

    * ── Build lead/lag dummies (on real dataset, before preserve) ─
    forvalues x = 1/`max_leads' {
        qui gen f_`x'_`treatment_var' = f`x'.`treatment_var'
        qui replace f_`x'_`treatment_var' = 0 if f_`x'_`treatment_var' == .
    }
    forvalues x = 1/`max_lags' {
        qui gen l_`x'_`treatment_var' = l`x'.`treatment_var'
        qui replace l_`x'_`treatment_var' = 0 if l_`x'_`treatment_var' == .
    }
    * Normalize omitted period
    qui replace f_`normalize_lead'_`treatment_var' = 0

    * ── No interaction ────────────────────────────────────────────
    if "`interactvar'" == "" {

        local leadlist
        forvalues x = 1/`max_leads' {
            local leadlist `leadlist' f_`x'_`treatment_var'
        }
        local laglist
        forvalues x = 1/`max_lags' {
            local laglist `laglist' l_`x'_`treatment_var'
        }

        reghdfe `outcome' `leadlist' `treatment_var' `laglist' `if' `in', ///
            absorb(`fe') cluster(`cluster')

        * Collect into tempfile via postfile
        tempfile resfile
        tempname res
        postfile `res' t coef se using `resfile', replace

        forvalues x = 1/`max_leads' {
            post `res' (-`x') (_b[f_`x'_`treatment_var']) (_se[f_`x'_`treatment_var'])
        }
        post `res' (0) (_b[`treatment_var']) (_se[`treatment_var'])
        forvalues x = 1/`max_lags' {
            post `res' (`x') (_b[l_`x'_`treatment_var']) (_se[l_`x'_`treatment_var'])
        }
        postclose `res'

        * Clean up dummies
        forvalues x = 1/`max_leads' {
            cap drop f_`x'_`treatment_var'
        }
        forvalues x = 1/`max_lags' {
            cap drop l_`x'_`treatment_var'
        }

        preserve
            use `resfile', clear
            sort t
            gen ub = coef + 1.96*se
            gen lb = coef - 1.96*se

            twoway                                                        ///
                (rarea lb ub t, sort fcolor(navy%20) lcolor(none))       ///
             || (connected coef t,                                        ///
                    lpattern(solid) msymbol(diamond) msize(medsmall)      ///
                    mcolor(navy) lcolor(navy) lwidth(medthick))           ///
             ,  legend(off)                                               ///
                xtitle("Years relative to first machine", size(small))   ///
                ytitle("Coefficient", size(small))                        ///
                xlabel(`neg_leads'(1)`max_lags', labsize(small))         ///
                yline(0,    lpattern(solid) lcolor(gs10) lwidth(thin))   ///
                xline(-0.5, lpattern(dot)   lcolor(gs10) lwidth(thin))   ///
                graphregion(color(white)) plotregion(lcolor(none))
        restore
        exit
    }

    * ── Interaction ───────────────────────────────────────────────

    * Build interaction terms
    forvalues x = 1/`max_leads' {
        qui gen f_`x'_X = f_`x'_`treatment_var' * `interactvar'
    }
    qui gen t0_X = `treatment_var' * `interactvar'
    forvalues x = 1/`max_lags' {
        qui gen l_`x'_X = l_`x'_`treatment_var' * `interactvar'
    }

    * Build regressor lists
    local mainlist
    local xlist
    forvalues x = 1/`max_leads' {
        local mainlist `mainlist' f_`x'_`treatment_var'
        local xlist    `xlist'    f_`x'_X
    }
    local mainlist `mainlist' `treatment_var'
    local xlist    `xlist'    t0_X
    forvalues x = 1/`max_lags' {
        local mainlist `mainlist' l_`x'_`treatment_var'
        local xlist    `xlist'    l_`x'_X
    }

    * Single regression with main + interaction terms
    reghdfe `outcome' `mainlist' `interactvar' `xlist' `if' `in', ///
        absorb(`fe') cluster(`cluster')

    * Collect results via postfile
    tempfile resfile
    tempname res
    postfile `res' t b0 se0 b1 se1 using `resfile', replace

    forvalues x = 1/`max_leads' {
        local b0  = _b[f_`x'_`treatment_var']
        local se0 = _se[f_`x'_`treatment_var']
        qui lincom f_`x'_`treatment_var' + f_`x'_X
        post `res' (-`x') (`b0') (`se0') (`r(estimate)') (`r(se)')
    }
    local b0  = _b[`treatment_var']
    local se0 = _se[`treatment_var']
    qui lincom `treatment_var' + t0_X
    post `res' (0) (`b0') (`se0') (`r(estimate)') (`r(se)')
    forvalues x = 1/`max_lags' {
        local b0  = _b[l_`x'_`treatment_var']
        local se0 = _se[l_`x'_`treatment_var']
        qui lincom l_`x'_`treatment_var' + l_`x'_X
        post `res' (`x') (`b0') (`se0') (`r(estimate)') (`r(se)')
    }
    postclose `res'

    * Clean up dummies
    forvalues x = 1/`max_leads' {
        cap drop f_`x'_`treatment_var' f_`x'_X
    }
    forvalues x = 1/`max_lags' {
        cap drop l_`x'_`treatment_var' l_`x'_X
    }
    cap drop t0_X

    * Plot
    preserve
        use `resfile', clear
        sort t
        gen ub0 = b0 + 1.96*se0
        gen lb0 = b0 - 1.96*se0
        gen ub1 = b1 + 1.96*se1
        gen lb1 = b1 - 1.96*se1

        twoway                                                              ///
            (rarea lb0 ub0 t, sort fcolor(navy%20)     lcolor(none))      ///
         || (rarea lb1 ub1 t, sort fcolor(cranberry%20) lcolor(none))     ///
         || (connected b0 t,                                               ///
                lpattern(dash) msymbol(circle) msize(medsmall)             ///
                mcolor(navy) lcolor(navy) lwidth(medthick))                ///
         || (connected b1 t,                                               ///
                lpattern(solid) msymbol(diamond) msize(medsmall)           ///
                mcolor(cranberry) lcolor(cranberry) lwidth(medthick))      ///
         ,  legend(order(3 "`label0'" 4 "`label1'")                        ///
                pos(6) cols(2) ring(1) size(small))                        ///
            xtitle("Years relative to first machine", size(small))         ///
            ytitle("Coefficient", size(small))                             ///
            xlabel(`neg_leads'(1)`max_lags', labsize(small))               ///
            yline(0,    lpattern(solid) lcolor(gs10) lwidth(thin))         ///
            xline(-0.5, lpattern(dot)   lcolor(gs10) lwidth(thin))         ///
            graphregion(color(white)) plotregion(lcolor(none))
    restore

end
