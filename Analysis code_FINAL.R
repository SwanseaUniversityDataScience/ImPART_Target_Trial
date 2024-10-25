#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#

# Target Trial emulating a trial of comparing initiation of prophylactic antibiotics within 3 months of recurrent UTI (rUTI) diagnosis vs not initiating prophylactic antibiotics.

# Author: Leigh Sanyaolu

# This R script file contains the code used to undertake the analysis of the IMPART study to assess the impact of prophylactic antibiotic use of antimicrobial resistance.

# This code was developed from code used at the CAUSALab Target Trial course 2023 and we would like to acknowledge the course and organizers for      this. We would also like to acknowledge the publication:  Maringe, C et al Reflection on modern methods: trial emulation in the presence of immortal   -time bias. Assessing the benefit of major surgery for elderly lung cancer patients using observational data, International Journal of Epidemiology,   Volume 49, Issue 5, October 2020, Pages 1719–1729, https://doi.org/10.1093/ije/dyaa057 which also informed the developed code #

#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#-#


# VARIABLES

# Below is a list of the main variables used in this analysis and a description of what each means.

# ALF: Patient identifier
# Age.rUTIs.diagnosed (continuous): Age of patient when meeting rUTI threshold and entering cohort.
# Time (continuous): Follow-up time from cohort entry/ index date.
# Ethnicity (categorical): Ethnicity.
# Deprivation.WIMD (categorical): Deprivation quintile based on the Welsh Index of Multiple Deprivation.
# Frailty.eFI (categorical): Level of frailty based on the electronic frailty index from "Fit" to "Severe frailty".
# Diabetes (binary): Evidence of diabetes prior to index date.
# Immune.supp (binary): Evidence of immunosuppression prior to index date. See paper for details of this was derived.
# Urinary.tract.stone (binary): Evidence of a urinary tract stone prior to index date.
# Urinary.tract.abnorm (binary): Evidence of a structural urinary tract abnormality prior to index date.
# GP.UTIs.baseline (integer): The number of UTI diagnosed in General Practice in the 12 months prior to cohort entry.
# Hosp.UTIs.baseline (integer): The number of UTI diagnosed in Hospital in the 12 months prior to cohort entry.
# GP.prescribe.rate (continuous): The rate of antibiotic prescribing at the practice the patient is registered with study entry.
# Urine.AMR.baseline (binary): Evidence of resistance on urine culture in any urine sample in the 12 months before the index date.
# Urine.growth.baseline (categorical): Urine culture growth based on the most recent urine sample prior to the index date.
# pAbx.use (binary): Indicator of when prophylactic antibiotics are initiated.
# Hosp.AMR (binary): Outcome indicator when the outcome of interest has occurred. In this code this is "Hosp.AMR" to represent when a patient is admitted to hospital with a resistant infection.
# Death (binary): Indicator of when a patient has died during follow-up.
# Lost.to.FU (binary): Indicator of when a patient has been lost to follow-up.
# at.risk.of.exposure (binary): Indicator of when a patient is at risk of the exposure. This is to generate the treatment weights.


df <- Hosp.AMR.data # Hosp.AMR.data is the final dataset prepared for analysis


# This next section is performing the 1) Cloning, 2) Censoring and 3) Weighting.


##### 1) CLONING #####

# convert df to data.table
setDT(df)

# Create non-antibiotic (control) clone and assign arm 0
arm0 <- copy(df)[, arm := 0]

# Create antibiotic (intervention) arm and assign arm 1
arm1 <- copy(df)[, arm := 1]


##### 2) CENSORING #####

### Censor clones in arm 0 when non-adherent ie they initiate antibiotics during the grace period

# Create a function to define censoring protocol for the non-pAbx strategy
arm0.censor.grace <- function(pAbx.use) {
    n <- length(pAbx.use)
    my.censor <- rep(0, n)
    first.cens <- NA

    # Censor if received pAbx before or on week 12
    if (n > 1 && pAbx.use[2] == 1) {
        first.cens <- 2
    } else if (n > 2 && pAbx.use[2] == 0 && pAbx.use[3] == 1) {
        first.cens <- 3
    } else if (n > 3 && pAbx.use[2] == 0 && pAbx.use[3] == 0 && pAbx.use[4] == 1) {
        first.cens <- 4
    } else if (n > 4 && pAbx.use[2] == 0 && pAbx.use[3] == 0 && pAbx.use[4] == 0 && pAbx.use[4] == 1) {
        first.cens <- 5
    } else if (n > 5 && pAbx.use[2] == 0 && pAbx.use[3] == 0 && pAbx.use[4] == 0 && pAbx.use[4] == 0 && pAbx.use[5] == 1) {
        first.cens <- 6
    } else if (n > 6 && pAbx.use[2] == 0 && pAbx.use[3] == 0 && pAbx.use[4] == 0 && pAbx.use[4] == 0 && pAbx.use[5] == 0 && pAbx.use[6] == 1) {
        first.cens <- 7
    } else if (n > 7 && pAbx.use[2] == 0 && pAbx.use[3] == 0 && pAbx.use[4] == 0 && pAbx.use[4] == 0 && pAbx.use[5] == 0 && pAbx.use[6] == 0 && pAbx.use[7] == 1) {
        first.cens <- 8
    } else if (n > 8 && pAbx.use[2] == 0 && pAbx.use[3] == 0 && pAbx.use[4] == 0 && pAbx.use[4] == 0 && pAbx.use[5] == 0 && pAbx.use[6] == 0 && pAbx.use[7] == 0 && pAbx.use[8] == 1) {
        first.cens <- 9
    } else if (n > 9 && pAbx.use[2] == 0 && pAbx.use[3] == 0 && pAbx.use[4] == 0 && pAbx.use[4] == 0 && pAbx.use[5] == 0 && pAbx.use[6] == 0 && pAbx.use[7] == 0 && pAbx.use[8] == 0 && pAbx.use[9] == 1) {
        first.cens <- 10
    } else if (n > 10 && pAbx.use[2] == 0 && pAbx.use[3] == 0 && pAbx.use[4] == 0 && pAbx.use[4] == 0 && pAbx.use[5] == 0 && pAbx.use[6] == 0 && pAbx.use[7] == 0 && pAbx.use[8] == 0 && pAbx.use[9] == 0 && pAbx.use[10] == 1) {
        first.cens <- 11
    } else if (n > 11 && pAbx.use[2] == 0 && pAbx.use[3] == 0 && pAbx.use[4] == 0 && pAbx.use[4] == 0 && pAbx.use[5] == 0 && pAbx.use[6] == 0 && pAbx.use[7] == 0 && pAbx.use[8] == 0 && pAbx.use[9] == 0 && pAbx.use[10] == 0 && pAbx.use[11] == 1) {
        first.cens <- 12
    } else if (n > 12 && pAbx.use[2] == 0 && pAbx.use[3] == 0 && pAbx.use[4] == 0 && pAbx.use[4] == 0 && pAbx.use[5] == 0 && pAbx.use[6] == 0 && pAbx.use[7] == 0 && pAbx.use[8] == 0 && pAbx.use[9] == 0 && pAbx.use[10] == 0 && pAbx.use[11] == 0 && pAbx.use[12] == 1) {
        first.cens <- 13
    }
    if (!(is.na(first.cens))) {
        my.censor[first.cens:n] <- rep(1, (n - first.cens + 1))
    }

    return(my.censor)
}

# Censor clones (apply censoring algorithm to each individual, retain uncensored person-weeks)
arm0.c.grace <- copy(arm0)[
    , my.censor := arm0.censor.grace(pAbx.use),
    by = ALF
][, good := cumsum(my.censor), by = ALF][good <= 1][, good := NULL]

### Censor clones in arm 1 when non-adherent i.e. not initiating antibiotic within the grace period

# Create a function to define censoring protocol prophylactic antibiotic (pAbx) strategy
arm1.censor.grace <- function(pAbx.use) {
    n <- length(pAbx.use)
    my.censor <- rep(0, n)
    first.cens <- NA

    # Censor if did not start pAbx by week 12
    if (n > 12 && pAbx.use[2] == 0 && pAbx.use[3] == 0 && pAbx.use[4] == 0 && pAbx.use[4] == 0 && pAbx.use[5] == 0 && pAbx.use[6] == 0 && pAbx.use[7] == 0 && pAbx.use[8] == 0 && pAbx.use[9] == 0 && pAbx.use[10] == 0 && pAbx.use[11] == 0 && pAbx.use[12] == 0 && pAbx.use[13] == 0) {
        first.cens <- 13
    }
    if (!(is.na(first.cens))) {
        my.censor[first.cens:n] <- rep(1, (n - first.cens + 1))
    }

    return(my.censor)
}

# Censor clones (apply censoring algorithm to each individual, retain uncensored person-weeks)
arm1.c.grace <- copy(arm1)[
    , my.censor := arm1.censor.grace(pAbx.use),
    by = ALF
][, good := cumsum(my.censor), by = ALF][good <= 1][, good := NULL]

### Combine data from both arms ###
both.arms <- rbind(arm0.c.grace, arm1.c.grace)[
    , Hosp.AMR := ifelse(my.censor == 1, NA, Hosp.AMR)
]


#### 3) WEIGHTING ####

# First step - Fit a pooled logistic model to estimate the denominator of the stabilized inverse probability of treatment weights
psc.denom <- glm(
    pAbx.use ~ Age.rUTIs.diagnosed + Age.rUTIs.diagnosed.sqrd +
        Frailty.eFI +
        GP.prescribe.rate + GP.prescribe.rate.sqrd + Immune.supp +
        Diabetes + Urinary.tract.stone +
        Urinary.tract.abnorm + Deprivation.WIMD + Hosp.UTIs.baseline + Hosp.UTIs.baseline.sqrd +
        GP.UTIs.baseline + GP.UTIs.baseline.sqrd + Urine.AMR.baseline + Urine.growth.baseline +
        time + timesqr,
    family = binomial(link = "logit"),
    data = df[which(df$at.risk.of.exposure == 1), ]
)

# Obtain predicted probabilities of being treated
df$psc.denom <- predict(psc.denom, df, type = "response")

# Obtain probabilities of receiving treatment actually received
df$psc.denom.t <- ifelse(df$pAbx.use == 1, df$psc.denom, 1 - df$psc.denom)

# set weights to 1 at times where there is not a positive probability of receiving pAbx
df$psc.denom.t <- ifelse((df$at.risk.of.exposure == 1), df$psc.denom.t, 1)

# Second step -  Fit model for the numerator of the stabilized inverse probability of treatment weights
psc.num <- glm(pAbx.use ~ time + timesqr,
    family = binomial(link = "logit"),
    data = df[which(df$at.risk.of.exposure == 1), ]
)

# Obtain predicted probabilities of being treated
df$psc.num <- predict(psc.num, df, type = "response")

# Obtain probabilities of receiving treatment actually received
df$psc.num.t <- ifelse(df$pAbx.use == 1, df$psc.num, 1 - df$psc.num)

# set weights to 1 at times where there is not a positive probability of receiving pAbx
df$psc.num.t <- ifelse((df$at.risk.of.exposure == 1), df$psc.num.t, 1)

### Estimate each individual's time-varying stabilized inverse probability of treatment weight ###
# Take the cumulative product at each time point
df <- df %>%
    group_by(ALF) %>%
    mutate(
        sw_a = cumprod(psc.num.t) / cumprod(psc.denom.t)
    ) %>%
    ungroup()

# add weights to the cloned dataset
df_sw_a <- df[, c("ALF", "time", "sw_a")]
both.arms.sw <- merge(df_sw_a, both.arms, by = c("ALF", "time"))

# Truncate the weights at the 99.95th percentile #
threshold_99 <- quantile(both.arms.sw$sw_a, 0.9995)
both.arms.sw$sw_ac_99 <- both.arms.sw$sw_a
both.arms.sw$sw_ac_99[both.arms.sw$sw_ac_99 > threshold_99] <- threshold_99


### OUTCOME MODEL ###

# This next section is using a weighted Kaplan Meier (KM) estimator to estimate the survival (and risk) of the outcome

# KM survival curves
KM_abx_arm <- survfit(Surv(time, time + 1, Hosp.AMR) ~ 1,
    data = both.arms.sw[both.arms.sw$arm == 1, ], weights = sw_ac_99
)
S1 <- min(KM_abx_arm$surv) # 1 year survival in the pAbx group

KM_no_abx_arm <- survfit(Surv(time, time + 1, Hosp.AMR) ~ 1,
    data = both.arms.sw[both.arms.sw$arm == 0, ], weights = sw_ac_99
)
S0 <- min(KM_no_abx_arm$surv) # 1 year survival in the non-pAbx group

R1 <- 1 - S1 # risk in prophylactic antibiotic arm

R0 <- 1 - S0 # risk in non-prophylactic antibiotic arm

RD <- R1 - R0 # risk difference between arms

RR <- R1 / R0 # risk ratio

NNT <- 1 / RD # numbers needed to treat


# Bootstrapping is then used to compute the 95% confidence intervals.
