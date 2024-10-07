---------------------------------------------------------------------------
---------------------------------------------------------------------------
--This script builds upon the cohorts developed for Recurrent urinary
--tract infections and prophylactic antibiotic use in women: a cross-
--sectional study in primary care, for the years 2015 to 2020.
--further information and code is available in github respository
--https://github.com/SwanseaUniversityDataScience/ImPART/tree/main/Additional%20Documents
--initial tables:
--		SAILW1169V.V15VB_COHORT1
--		SAILW1169V.V15VB_COHORT3_ALL_RPT_ABX
---------------------------------------------------------------------------
---------------------------------------------------------------------------

--cohort comprised of women with rUTIs between 2015 and 2020.
--general inclusion criteria matches impart cohort 1 with additional exclusion 
--criteria cannot have used pAbx within the 12 months prior to rUTI.
--and must be Welsh resident at baseline (rUTI diagnosis date)
--to exclude hospital acquired infections, the episode must start within 2 days
--of an admission and must not occur within 3 days of a prior discharge

-----------------------------------------------------------------------------

--code to look at patients with pAbx between 2015 and 2020
--exclude those with pabx in 12 months prior to rUTI
--add if left cohort during study period and whether lost to follow up or died
--where GP registration ended the day before date of death then classified as died

CALL fnc.drop_if_exists('sailw1169v.VB_7day_TARGET_TRIAL_PRE');

CREATE TABLE sailw1169v.VB_7day_TARGET_TRIAL_PRE
AS(
SELECT alf_pe,
	diag_date,
	wob,
	AGEDIAG,
	GP_END_DATE,
	WIMD_2019_QUINTILE,
	ETHNIC,
	BMI_VAL,
	SMOk,
	SMOKING_STATUS_DESCRIPTION,
	ALCOHOL_FLG,
	ALCOHOL,
	EFI,
	DIABETES,
	CANCER,
	RENAL_DISEASE,
	HYPERTENSION,
	CVD,
	CEREBROV,
	HEARTFAIL,
	MS,
	MND,
	DEMENTIA,
	PARKINSONS,
	SMH,
	ASTHMA,
	COPD,
	IMMUN_SUPP,
	RENAL_STONE,
	ABN_RENAL,
	PABX_AFTER_RUTI AS PABX_STR
FROM sailw1169v.V15VB_COHORT1)
WITH NO DATA;

ALTER TABLE sailw1169v.VB_7day_TARGET_TRIAL_PRE
ADD COLUMN days_to_pabx integer
ADD COLUMN dod date
ADD COLUMN cohort_end_dt date
ADD COLUMN cohort_end_reason varchar(10)
ADD COLUMN gp_int_days integer;

INSERT INTO sailw1169v.VB_7day_TARGET_TRIAL_PRE
WITH cte AS -- add dod and study end date and reason to cohort 1
(SELECT coh.*,
		date(ad.DEATH_DT) AS dod,
		CASE WHEN ((coh.diag_date + 12 MONTHS < coh.wob + 120 YEARS)
					AND (coh.diag_date + 12 MONTHS < coh.gp_end_date)
				AND (coh.wob+120 < ad.death_dt
						OR ad.death_dt IS null))
					THEN coh.diag_date + 12 MONTHS
				WHEN ((coh.gp_end_date < ad.death_dt - 1 day
					OR YEAR(coh.gp_end_date) BETWEEN '2015' and '2020' AND ad.death_dt IS NULL)
				AND coh.gp_end_date < coh.WOB + 120 years)
					THEN date(gp_end_date)
			WHEN (ad.death_dt <= coh.gp_end_date + 1 day
				AND ad.death_dt <= coh.WOB + 120 years)
					THEN date(ad.death_dt)
			WHEN (coh.wob + 120 years < coh.gp_end_date
				AND (coh.wob+120 < ad.death_dt
						OR ad.death_dt IS null))
					THEN date(coh.wob + 120 YEARS)
		ELSE '2020-12-31'
			END AS cohort_end_dt,
	CASE WHEN ((coh.diag_date + 12 MONTHS < coh.wob + 120 YEARS)
					AND (coh.diag_date + 12 MONTHS < coh.gp_end_date)
				AND (coh.wob+120 < ad.death_dt
						OR ad.death_dt IS null)
				AND coh.diag_date + 12 MONTHS <= '2020-12-31')
					THEN 'DG +12mths'
				WHEN ((coh.gp_end_date < ad.death_dt - 1 day
					OR YEAR(coh.gp_end_date) BETWEEN '2015' and '2020' AND ad.death_dt IS NULL)
				AND coh.gp_end_date < coh.WOB + 120 YEARS
				AND coh.GP_END_DATE <= '2020-12-31')
					THEN 'LTF'
				WHEN (ad.death_dt <= coh.gp_end_date + 1 day
					AND ad.death_dt <= coh.WOB + 120 YEARS
					AND ad.death_dt <= '2020-12-31')
						THEN 'DIED'
				WHEN (coh.wob + 120 years < coh.gp_end_date
					AND (coh.wob+120 < ad.death_dt
						OR ad.death_dt IS null)
					AND coh.wob + 120 years <= '2020-12-31')
						THEN 'LTF'
		ELSE 'STUDY END'
			END AS cohort_end_reason
FROM sailw1169v.V15VB_COHORT1 AS coh
LEFT JOIN sail1169v.ADDE_DEATHS_20210628 AS ad
	ON coh.ALF_PE = ad.ALF_PE
WHERE coh.WIMD_2019_QUINTILE IS NOT NULL
AND coh.pabx = 0),
CTE2 AS --identify those with pabx in the 12 months prior to rUTI
(SELECT DISTINCT(coh.ALF_PE)
FROM SAILW1169V.V15VB_ALTERNATING_PABX_COH3_PRE AS abx
INNER JOIN sailw1169v.V15VB_COHORT1 AS coh
ON ABX.ALF_PE = COH.alf_pe
WHERE (coh.DIAG_DATE > abx.ALT_END_DT_COMB 
		AND	MONTHS_BETWEEN(coh.diag_date,abx.ALT_END_DT_COMB) < 12)
		OR coh.diag_date BETWEEN coh.PABX_AFTER_RUTI AND abx.ALT_END_DT_COMB),
cte3 AS --calculate gp interaction days in the 12 months after baseline
(SELECT a.ALF_PE,					
	a.DIAG_DATE AS diag_dt_int,				
	count(DISTINCT EVENT_DT) AS gp_int_count				
FROM					
	sail1169v.WLGP_GP_EVENT_CLEANSED_20220301 AS b				
INNER JOIN sailw1169v.V15VB_COHORT1 AS a					
		ON a.alf_pe = b.alf_pe			
WHERE					
	b.EVENT_DT BETWEEN a.DIAG_DATE + 1 day AND a.DIAG_DATE + 1 year				
	AND ((substr(EVENT_CD,				
	1,				
	1) <> lower(substr(EVENT_CD, 1, 1)) )				
	OR LEFT(event_cd,1) IN ('0', '1', '2', '3', '4', '5', '6', '7', '8', '9'))				
GROUP BY a.ALF_PE, a.DIAG_DATE)
SELECT cte.alf_pe,
	cte.diag_date,
	wob,
	AGEDIAG,
	GP_END_DATE,
	WIMD_2019_QUINTILE,
	CASE WHEN ETHNIC IS NOT NULL 
			THEN ethnic
			ELSE eth.ETHN_EC_ONS_DATE_LATEST_CODE
		END
	AS ethnic,
	BMI_VAL,
	SMOk,
	SMOKING_STATUS_DESCRIPTION,
	ALCOHOL_FLG,
	ALCOHOL,
	EFI,
	DIABETES,
	CANCER,
	RENAL_DISEASE,
	HYPERTENSION,
	CVD,
	CEREBROV,
	HEARTFAIL,
	MS,
	MND,
	DEMENTIA,
	PARKINSONS,
	SMH,
	ASTHMA,
	COPD,
	IMMUN_SUPP,
	RENAL_STONE,
	ABN_RENAL,
	PABX_AFTER_RUTI,
	days_between(pabx_after_ruti, DIAG_DATE),
	dod,
	cohort_end_dt,
	cohort_end_reason,
	CASE WHEN cte3.gp_int_count IS NULL
			THEN 0
		ELSE cte3.gp_int_count
	end
FROM CTE 
LEFT JOIN sailw1169v.ETHN_1169 AS eth
ON cte.alf_pe = eth.alf_pe
LEFT JOIN cte3
ON cte.alf_pe = cte3.alf_pe
WHERE CTE.ALF_PE NOT IN  --remove those with prior pabx in 12 months from cohort1
(SELECT CTE2.ALF_PE FROM CTE2);

---------------------------------------------------------------------------------
--add pabx first prescription and pabx end dates

CALL fnc.drop_if_exists('sailw1169v.VB_7day_tt_pabx_in_12_mnth');

CREATE TABLE sailw1169v.VB_7day_tt_pabx_in_12_mnth
(alf_pe varchar(15),
PABX_str date,
pabx_first_script date,
pabx_end date);

INSERT INTO sailw1169v.VB_7day_tt_pabx_in_12_mnth
WITH cte as
(SELECT coh.alf_pe,
		coh.pabx_str,
		min(aa.alt_str_dt) AS alt_str_dt
	FROM sailw1169v.VB_7day_TARGET_TRIAL_PRE AS coh
	INNER JOIN SAILW1169V.VB_ALTERNATING_PABX_COH3 AS aa
		ON coh.alf_pe = aa.alf_pe
		AND coh.pabx_str = aa.pabx_str
	GROUP BY coh.alf_pe, coh.pabx_str)
SELECT DISTINCT cte.alf_pe,
				cte.pabx_str,
				cte.alt_str_dt,
				max(ed.ALT_END_DT_COMB) AS pabx_end
	FROM cte
	INNER JOIN SAILW1169V.VB_ALTERNATING_PABX_COH3_PRE AS ed
		ON cte.alf_pe = ed.alf_pe
		AND cte.pabx_str = ed.pabx_str
	GROUP BY cte.alf_pe, cte.pabx_str, cte.alt_str_dt;

ALTER TABLE sailw1169v.VB_7day_TARGET_TRIAL_PRE
ADD COLUMN first_abx_dt date
ADD COLUMN pabx_end date;

MERGE into sailw1169v.VB_7day_TARGET_TRIAL_PRE AS coh
USING sailw1169v.VB_7day_tt_pabx_in_12_mnth AS pabx
	ON coh.alf_pe = pabx.alf_pe
		WHEN MATCHED THEN
			UPDATE 
				SET (coh.first_abx_dt, coh.pabx_end) = (pabx.pabx_first_script, pabx.pabx_end)
;

--------------------------------------------------------------------------
--------------------------------------------------------------------------

--add prescription dose

CALL fnc.drop_if_exists('SESSION.vb_tt_dose_consistency');	
	
DECLARE GLOBAL TEMPORARY TABLE SESSION.vb_tt_dose_consistency
(alf_pe varchar(15),
pabx_str date,
dose varchar(10),
dose_consistency varchar(20))
ON COMMIT PRESERVE ROWS;

COMMIT;


--Insert where there is more than one dose due to multi Abx types
INSERT INTO SESSION.vb_tt_dose_consistency
WITH cte as	
(SELECT DISTINCT amr.alf_pe, 
					abx.ANTIBX_CATEGORY,
					abx.DOSE,
					abx.DOSE_CONSISTENCY 
FROM sailw1169v.VB_7day_TARGET_TRIAL_PRE AS amr
	left JOIN SAILW1169V.V15VB_COHORT3_ALL_RPT_ABX AS abx
		ON amr.alf_pe = abx.alf_pe
		AND amr.PABX_STR = abx.EVENT_DT
	WHERE abx.EVENT_IN_GRP = 3),
cte2 AS (SELECT alf_pe, COUNT(alf_pe) AS alf_count
		FROM cte
		GROUP BY alf_pe
		HAVING COUNT(alf_pe)>1)
SELECT DISTINCT coh.alf_pe,
				coh.PABX_STR,
				'Multi',
				'Multi'
	FROM sailw1169v.VB_7day_TARGET_TRIAL_AMR_WRRS AS coh
	RIGHT JOIN cte
		ON coh.alf_pe = cte.alf_pe
	INNER JOIN cte2
		ON coh.alf_pe = cte2.alf_pe
			ORDER BY coh.alf_pe;
	
COMMIT;

--Insert where there is a single dose
INSERT INTO SESSION.vb_tt_dose_consistency
WITH cte as	
(SELECT DISTINCT amr.alf_pe, 
					abx.ANTIBX_CATEGORY,
					abx.DOSE, 
					abx.DOSE_CONSISTENCY 
FROM sailw1169v.VB_7day_TARGET_TRIAL_PRE AS amr
	left JOIN SAILW1169V.V15VB_COHORT3_ALL_RPT_ABX AS abx
		ON amr.alf_pe = abx.alf_pe
		AND amr.PABX_STR = abx.EVENT_DT
	WHERE abx.EVENT_IN_GRP = 3),
cte2 AS (SELECT alf_pe, COUNT(alf_pe) AS alf_count
		FROM cte
		GROUP BY alf_pe
		HAVING COUNT(alf_pe) = 1)
SELECT DISTINCT coh.alf_pe,
				coh.PABX_STR,
				cte.dose,
				cte.dose_consistency
	FROM sailw1169v.VB_7day_TARGET_TRIAL_AMR_WRRS AS coh
	RIGHT JOIN cte
		ON coh.alf_pe = cte.alf_pe
	INNER JOIN cte2
		ON coh.alf_pe = cte2.alf_pe
			ORDER BY coh.alf_pe;
	
COMMIT;

ALTER table sailw1169v.VB_7day_TARGET_TRIAL_PRE
ADD COLUMN dose varchar(15)
ADD COLUMN dose_consistency varchar(20);

UPDATE sailw1169v.VB_7day_TARGET_TRIAL_PRE AS amr
SET amr.dose = (SELECT abx.dose
				FROM SESSION.vb_tt_dose_consistency AS abx
				WHERE abx.alf_pe = amr.alf_pe);
			
UPDATE sailw1169v.VB_7day_TARGET_TRIAL_PRE AS amr
SET amr.dose_consistency = (SELECT abx.dose_consistency
							FROM SESSION.vb_tt_dose_consistency AS abx
							WHERE abx.alf_pe = amr.alf_pe);

-------------------------------------------------------------------------------------------
--add next pabx start after first sequence has ended

ALTER TABLE sailw1169v.VB_7day_TARGET_TRIAL_PRE
ADD COLUMN NEXT_PABX_STR date;

merge INTO sailw1169v.VB_7day_TARGET_TRIAL_PRE AS ttp
USING
(SELECT tt.alf_pe,
		tt.pabx_str,
		tt.pabx_end,
		min(abx.PABX_STR) AS next_pabx_str
FROM sailw1169v.VB_7day_TARGET_TRIAL_PRE AS tt
INNER JOIN SAILW1169V.V15VB_ALTERNATING_PABX_COH3 AS abx
ON tt.alf_pe = abx.alf_pe
WHERE abx.PABX_STR > tt.PABX_END
GROUP BY tt.alf_pe, tt.PABX_STR, tt.PABX_END
ORDER BY tt.alf_pe) AS pabx
ON ttp.alf_pe = pabx.ALF_PE
WHEN MATCHED THEN UPDATE 
SET NEXT_PABX_STR = pabx.next_pabx_str;

UPDATE sailw1169v.VB_7day_TARGET_TRIAL_PRE
SET cohort_end_reason = CASE WHEN (next_pabx_str < DOD 
									OR (next_pabx_str IS NOT NULL 
										AND dod IS NULL))
							AND next_pabx_str < GP_END_DATE
							AND next_pabx_str < '2020-12-31'
							AND next_pabx_str < diag_date + 12 months
						THEN 'NEW PABX'
						ELSE COHORT_END_REASON
					END;

---------------------------------------------------------------------
--add pabx start at any time after rUTI (not just 12 months)
				
CALL fnc.drop_if_exists('sailw1169v.tt_first_pabx_after_ruti');				
				
CREATE TABLE sailw1169v.tt_first_pabx_after_ruti
(alf_pe varchar(15),
PABX_str_all date,
pabx_all_first_script date,
pabx_end_all date);

INSERT INTO sailw1169v.tt_first_pabx_after_ruti
WITH cte as
(SELECT cohp.alf_pe, 
		min(pabx.PABX_STR) AS PABX_str_all
	FROM sailw1169v.VB_7day_TARGET_TRIAL_PRE AS cohp
	left JOIN SAILW1169V.VB_ALTERNATING_PABX_COH3 AS pabx
		ON pabx.ALF_PE = cohp.ALF_PE
		AND DAYS_BETWEEN(pabx.PABX_STR,cohp.DIAG_Date) >= 0
	GROUP BY cohp.alf_pe
	ORDER BY cohp.alf_pe),
cte2 as
(SELECT cte.alf_pe,
		cte.pabx_str_all,
		min(aa.alt_str_dt) AS alt_str_dt
	FROM cte
	INNER JOIN SAILW1169V.VB_ALTERNATING_PABX_COH3 AS aa
		ON cte.alf_pe = aa.alf_pe
		AND cte.pabx_str_all = aa.pabx_str
	GROUP BY cte.alf_pe, cte.pabx_str_all)
SELECT DISTINCT cte2.alf_pe,
				cte2.pabx_str_all,
				cte2.alt_str_dt,
				max(ed.ALT_END_DT_COMB) AS pabx_end_all
	FROM cte2
	INNER JOIN SAILW1169V.VB_ALTERNATING_PABX_COH3_PRE AS ed
		ON cte2.alf_pe = ed.alf_pe
		AND cte2.pabx_str_all = ed.pabx_str
	GROUP BY cte2.alf_pe, cte2.pabx_str_all, cte2.alt_str_dt;

ALTER TABLE sailw1169v.VB_7day_TARGET_TRIAL_PRE
ADD COLUMN PABX_str_all date
ADD COLUMN pabx_all_first_script date
ADD COLUMN pabx_end_all date;

MERGE into sailw1169v.VB_7day_TARGET_TRIAL_PRE AS coh
USING sailw1169v.tt_first_pabx_after_ruti AS pabx
	ON coh.alf_pe = pabx.alf_pe
		WHEN MATCHED THEN
			UPDATE 
				SET (coh.PABX_str_all, coh.pabx_all_first_script, coh.pabx_end_all) = (pabx.PABX_str_all, pabx.pabx_all_first_script, pabx.pabx_end_all)
;

-------------------------------------------------------------------------------------------
-- add GP prescribing rate

ALTER TABLE sailw1169v.VB_7day_TARGET_TRIAL_PRE
ADD COLUMN GP_PRESC_RT double;

MERGE INTO sailw1169v.VB_7day_TARGET_TRIAL_PRE AS tt
USING 
(SELECT mqo.*, CAST(abx_presc_rt AS decimal(5,3)) AS abx_presc_rt FROM (SELECT coh.*, reg.PRAC_CD_PE FROM sailw1169v.V15VB_COHORT1 AS coh
	LEFT JOIN sail1169v.WLGP_CLEAN_GP_REG_BY_PRAC_INCLNONSAIL_MEDIAN_20220301 AS reg
	ON coh.alf_pe = reg.alf_pe
	AND coh.DIAG_DATE BETWEEN reg.START_DATE AND reg.END_DATE) AS mqo
LEFT JOIN sailw1169v.vb_gp_prescribing_rt AS rt
ON mqo.prac_cd_pe = rt.PRAC_CD_PE
ORDER BY alf_pe, diag_date) AS cte2
ON tt.alf_pe = cte2.alf_pe
WHEN MATCHED THEN 
UPDATE 
SET tt.gp_presc_rt = CAST(cte2.abx_presc_rt AS decimal(5,3));

-------------------------------------------------------------------------------------------

--add AMR in the 12 months prior to rUTI

ALTER table sailw1169v.VB_7day_TARGET_TRIAL_PRE
ADD COLUMN AMR_BASELINE integer;

UPDATE sailw1169v.VB_7day_TARGET_TRIAL_PRE
SET AMR_BASELINE = 1
WHERE alf_pe IN 
(SELECT tt.ALF_PE
FROM sailw1169v.VB_7day_TARGET_TRIAL_PRE AS tt
INNER JOIN SAILW1169V.COHORT_WRRS_RESULTS_AGREED AS wrrs
ON tt.ALF_PE = wrrs.ALF_PE
WHERE (TRIMETHOPRIM = 'Resistant'
OR NITROFURANTOIN = 'Resistant'
OR CEPHALEXIN = 'Resistant'
OR COAMOXICLAV = 'Resistant'
OR AMOXICILLIN = 'Resistant'
OR PIVMECILLINAM = 'Resistant'
OR FOSFOMYCIN = 'Resistant'
OR CIPROFLOXACIN = 'Resistant'
OR AMOXICILLIN_CLAVULANATE = 'Resistant')
AND wrrs.SPCM_COLLECTED_DT BETWEEN tt.DIAG_DATE - 365 days AND tt.DIAG_DATE);

UPDATE sailw1169v.VB_7day_TARGET_TRIAL_PRE
SET AMR_BASELINE = CASE WHEN AMR_BASELINE IS NULL THEN 0
					ELSE AMR_BASELINE
					END;

-------------------------------------------------------------------------------------------

CALL fnc.drop_if_exists('session.VB_TT_MIN_DT');

DECLARE GLOBAL TEMPORARY TABLE session.VB_TT_MIN_DT
AS
(SELECT ALF_PE,
gp_end_date,
dod,
next_pabx_str,
dod AS study_end_dt,
dod AS followup_12mths
FROM sailw1169v.VB_7day_TARGET_TRIAL_PRE)
DEFINITION ONLY
ON COMMIT PRESERVE rows;

COMMIT;

INSERT INTO session.VB_TT_MIN_DT
SELECT alf_pe,
		CASE WHEN gp_end_date IS NULL
				THEN '2099-01-31'
			ELSE gp_end_date
		END,
		CASE WHEN dod IS NULL
				THEN '2099-01-31'
			ELSE dod
		end,
		CASE WHEN next_pabx_str IS NULL
				THEN '2099-01-31'
			ELSE next_pabx_str
		end,
		'2020-12-31',
		diag_date + 12 months
FROM sailw1169v.VB_7day_TARGET_TRIAL_PRE;

COMMIT;
				
CALL fnc.drop_if_exists('session.VB_TAB_MIN_PRE');

DECLARE GLOBAL TEMPORARY TABLE session.VB_TAB_MIN_PRE
(ALF_PE varchar(15),
ruti_diag_dt date,
CAL_Dt date,
first_abx_dt date,
PABX_str date,
PABX_END date,
GP_END_DT date,
study_end date,
dod date,
next_pabx_str date,
followup_12mths date,
cohort_end_reason varchar(10),
min_end_dt date,
PABX_str_all date,
pabx_all_first_script date,
pabx_end_all date)
ON COMMIT preserve rows;

COMMIT;

INSERT INTO session.VB_TAB_MIN_PRE
SELECT coh.ALF_PE,
	coh.DIAG_DATE,
	coh.DIAG_DATE,
	coh.first_abx_dt,
	coh.PABX_str,
	coh.pabx_end,
	coh.GP_END_DATE,
	'2020-12-31',
	coh.dod,
	coh.NEXT_PABX_STR,
	mdt.followup_12mths,
	coh.cohort_end_reason,
	CASE WHEN coh.DOD < mdt.followup_12mths 
			AND cohort_end_reason = 'DIED' 
		THEN coh.DOD --to account for where gp_end occurs day before death and leaving reason is assigned as died
			ELSE least(mdt.gp_end_date, mdt.dod, mdt.study_end_dt, mdt.next_pabx_str, mdt.followup_12mths)
		END,
	coh.PABX_str_all,
	coh.pabx_all_first_script,
	coh.pabx_end_all
FROM sailw1169v.VB_7day_TARGET_TRIAL_PRE AS coh
INNER JOIN session.VB_TT_MIN_DT AS mdt
ON coh.alf_pe = mdt.alf_pe
WHERE diag_date IS NOT NULL;

COMMIT;

CALL fnc.drop_if_exists('sailw1169v.VB_7day_TARGET_TRIAL_PRE2');

CREATE TABLE sailw1169v.VB_7day_TARGET_TRIAL_PRE2
(ALF_PE varchar(15),
ruti_diag_dt date,
CAL_Dt date,
first_abx_dt date,
PABX_STR date,
PABX_END date,
gp_end_dt date,
STUDY_END date,
dod date,
next_pabx_str date,
cohort_end_reason varchar(10),
min_end_dt date,
PABX_str_all date,
pabx_all_first_script date,
pabx_end_all date
);

ALTER TABLE sailw1169v.VB_7day_TARGET_TRIAL_PRE2 activate NOT logged INITIALLY;

INSERT INTO sailw1169v.VB_7day_TARGET_TRIAL_PRE2
WITH cte (alf_pe, ruti_diag_dt, cal_dt, first_abx_dt, pabx_str, pabx_end, gp_end_dt, study_end, 
			dod, next_pabx_str, cohort_end_reason, min_end_dt, PABX_str_all, pabx_all_first_script, pabx_end_all) AS
(SELECT DISTINCT 
		alf_pe,
		ruti_diag_dt,
		cal_dt,
		first_abx_dt,
		pabx_str,
		pabx_end,
		gp_end_dt,
		study_end,
		dod,
		next_pabx_str,
		cohort_end_reason,
		min_end_dt,
		PABX_str_all,
		pabx_all_first_script,
		pabx_end_all
FROM session.VB_TAB_MIN_PRE
UNION ALL
SELECT 	cte.alf_pe AS alf_pe,
		ruti_diag_dt,
		add_days(cte.cal_dt,7) AS cal_dt,
		cte.first_abx_dt,
		cte.PABX_str,
		cte.pabx_end,
		cte.gp_end_dt,
		cte.study_end,
		cte.dod,
		cte.next_pabx_str,
		cte.cohort_end_reason,
		cte.min_end_dt,
		cte.PABX_str_all,
		cte.pabx_all_first_script,
		cte.pabx_end_all
	FROM cte
	WHERE cal_dt + 6 days < min_end_dt)
SELECT DISTINCT 
		alf_pe,
		ruti_diag_dt,
		cal_dt,
		first_abx_dt,
		pabx_str,
		pabx_end,
		gp_end_dt,
		study_end,
		dod,
		next_pabx_str,
		cohort_end_reason,
		min_end_dt,
		PABX_str_all,
		pabx_all_first_script,
		pabx_end_all
	FROM cte
ORDER BY alf_pe, cal_dt;

ALTER TABLE sailw1169v.VB_7day_TARGET_TRIAL_PRE2
ADD COLUMN cal_time integer
ADD COLUMN LTF integer
ADD COLUMN death integer
ADD COLUMN study_ended integer
ADD COLUMN diag_12months integer
ADD COLUMN pabx integer
ADD COLUMN new_pabx integer
ADD COLUMN all_pabx integer;

UPDATE sailw1169v.VB_7day_TARGET_TRIAL_PRE2
SET cal_time = -1 + row_number() OVER(PARTITION BY alf_pe ORDER BY cal_dt);

UPDATE sailw1169v.VB_7day_TARGET_TRIAL_PRE2
SET pabx = CASE WHEN pabx_str IS null
					THEN 0
				WHEN cal_dt BETWEEN pabx_str AND pabx_end
					THEN 1
				WHEN pabx_str BETWEEN CAL_DT AND CAL_DT + 6 DAYS 
					THEN 1
				ELSE 0
			END;		
		
UPDATE sailw1169v.VB_7day_TARGET_TRIAL_PRE2
SET all_pabx = CASE WHEN pabx_str_all IS null
					THEN 0
				WHEN cal_dt BETWEEN pabx_str_all AND pabx_end_all
					THEN 1
				WHEN pabx_str_all BETWEEN CAL_DT AND CAL_DT + 6 DAYS 
					THEN 1
				ELSE 0
			END;		
		
UPDATE sailw1169v.VB_7day_TARGET_TRIAL_PRE2
SET death = CASE WHEN  cohort_end_reason <> 'DIED'
					OR cohort_end_reason IS NULL
						THEN 0
				WHEN cohort_end_reason = 'DIED'
					AND dod between cal_dt AND cal_dt + 6 days
					THEN 1
				ELSE 0
			END;
		
UPDATE sailw1169v.VB_7day_TARGET_TRIAL_PRE2
SET ltf = CASE WHEN  cohort_end_reason <> 'LTF'
					OR cohort_end_reason IS NULL
						THEN 0
				WHEN cohort_end_reason = 'LTF'
					AND gp_end_dt BETWEEN cal_dt AND cal_dt + 6 days
					THEN 1
				ELSE 0
			END;
		
UPDATE sailw1169v.VB_7day_TARGET_TRIAL_PRE2
SET study_ended = CASE WHEN  cohort_end_reason <> 'STUDY END'
					OR cohort_end_reason IS NULL
						THEN 0
				WHEN cohort_end_reason = 'STUDY END'
					AND STUDY_END between cal_dt AND cal_dt + 6 days
					THEN 1
				ELSE 0
			END;
		
UPDATE sailw1169v.VB_7day_TARGET_TRIAL_PRE2
SET new_pabx = CASE WHEN  cohort_end_reason <> 'NEW PABX'
					OR cohort_end_reason IS NULL
						THEN 0
				WHEN cohort_end_reason = 'NEW PABX'
					AND next_pabx_str BETWEEN cal_dt AND cal_dt + 6 days
					THEN 1
				ELSE 0
			END;
		
UPDATE sailw1169v.VB_7day_TARGET_TRIAL_PRE2
SET diag_12months = CASE WHEN  cohort_end_reason <> 'DG +12mths'
					OR cohort_end_reason IS NULL
						THEN 0
				WHEN cohort_end_reason = 'DG +12mths'
					AND ruti_diag_dt + 12 months BETWEEN cal_dt AND cal_dt + 6 days
					THEN 1
				ELSE 0
			END;
		
SELECT * FROM sailw1169v.VB_7day_TARGET_TRIAL_PRE2
ORDER BY alf_pe, cal_dt;

-----------------------------------------------------------------------------
--gp UTIs in prev 12 months
		
ALTER TABLE sailw1169v.VB_7day_TARGET_TRIAL_PRE
ADD COLUMN gp_uti_pre integer
ADD COLUMN pedw_uti_pre integer;
		
MERGE into sailw1169v.VB_7day_TARGET_TRIAL_PRE AS ttp
using
(SELECT tt.alf_pe, count(jny.alf_pe) AS uti_count FROM 
(SELECT distinct gp.alf_pe, gp.event_str_dt, gp.event_end_dt, gp.data_source
FROM SAILW1169V.VB_WLGP_UTI_ALL_DATE AS gp
left JOIN (SELECT distinct alf_pe, event_str_dt, event_end_dt, data_source FROM SAILW1169V.VB_PEDW_UTI_ALL_DATE) AS pd
ON gp.alf_pe = pd.alf_pe
WHERE (pd.alf_pe IS NULL 
OR gp.EVENT_STR_DT NOT BETWEEN pd.event_str_dt + 1 DAY AND pd.event_end_dt)) AS jny
INNER JOIN sailw1169v.VB_7day_TARGET_TRIAL_PRE AS tt
ON tt.alf_pe = jny.alf_pe
WHERE (jny.event_str_dt BETWEEN tt.diag_date - 1 YEAR AND tt.diag_date)
AND data_source = 'WLGP'
GROUP BY tt.alf_pe) AS gp
		ON ttp.alf_pe = gp.alf_pe
			WHEN MATCHED THEN
				UPDATE
				SET ttp.gp_uti_pre = gp.uti_count
;
---------------------------------------------------------------------------------
--pedw UTIs in prev 12 months
--looks at UTIs with an end date in the prior 12 months
		
MERGE into sailw1169v.VB_7day_TARGET_TRIAL_PRE AS ttp
using
(SELECT tt.alf_pe, count(jny.alf_pe) AS uti_count FROM 
	(SELECT distinct alf_pe, event_str_dt, event_end_dt, data_source FROM SAILW1169V.VB_PEDW_UTI_ALL_DATE) AS jny
INNER JOIN sailw1169v.VB_7day_TARGET_TRIAL_PRE AS tt
ON tt.alf_pe = jny.alf_pe
WHERE (jny.event_end_dt BETWEEN tt.diag_date - 1 YEAR AND tt.diag_date
		OR jny.event_str_dt BETWEEN tt.diag_date - 1 YEAR AND tt.diag_date)
AND data_source = 'PEDW'
GROUP BY tt.alf_pe) AS gp
		ON ttp.alf_pe = gp.alf_pe
			WHEN MATCHED THEN
				UPDATE
				SET ttp.pedw_uti_pre = gp.uti_count
;
		
------------------------------------------------------------------------
		
--identify outcomes FOR target trial

-------------------------------------------------------------------------
--AMR hospitalisation
--identified AS ANY PEDW diag code recorded FOR antimicrobial resistance 
--FROM pAbx START

---------------------------------------------------
--hospitalisation with an AMR infection
--episode with AMR diagnosis code must start within 2 days of admission 
--and must not occur within 3 days of a prior hospital discharge

CALL FNC.DROP_IF_EXISTS('SESSION.VB_PEDW_ABX_RES');

DECLARE GLOBAL TEMPORARY TABLE SESSION.VB_PEDW_ABX_RES
	AS (SELECT * FROM SAIL1169V.PEDW_DIAG_20210704) DEFINITION ONLY
ON COMMIT PRESERVE ROWS;

Commit;

INSERT INTO SESSION.VB_PEDW_ABX_RES
	SELECT * FROM SAIL1169V.PEDW_DIAG_20210704 AS diag
		WHERE diag.DIAG_CD_1234 IN ('U820',
									'U821',
									'U822',
									'U828',
									'U829',
									'U830',
									'U830',
									'U831',
									'U832',
									'U837',
									'U838',
									'U839',
									'U847',
									'U848',
									'U849',
									'U800',
									'U801',
									'U808',
									'U810',
									'U818',
									'U890',
									'U898',
									'U899',
									'U880');
	
Commit;

CALL FNC.DROP_IF_EXISTS('SESSION.VB_PEDW_EPS_ABX_RES');

DECLARE GLOBAL TEMPORARY TABLE SESSION.VB_PEDW_EPS_ABX_RES
	AS (SELECT	eps.PROV_UNIT_CD,
				eps.SPELL_NUM_PE,
				eps.EPI_NUM,
				diag.DIAG_CD_1234,
				eps.EPI_STR_DT,
				eps.EPI_END_DT,
				eps.EPI_DUR
			FROM SAIL1169V.PEDW_EPISODE_20210704 AS EPS,
				SAIL1169V.PEDW_DIAG_20210704 AS diag) DEFINITION ONLY
ON COMMIT PRESERVE ROWS;

Commit;

INSERT INTO SESSION.VB_PEDW_EPS_ABX_RES
		(PROV_UNIT_CD,
		SPELL_NUM_PE,
		EPI_NUM,
		DIAG_CD_1234,
		EPI_STR_DT,
		EPI_END_DT,
		EPI_DUR)
		SELECT	eps.PROV_UNIT_CD,
				eps.SPELL_NUM_PE,
				eps.EPI_NUM,
				diag.DIAG_CD_1234,
				eps.EPI_STR_DT,
				eps.EPI_END_DT,
				eps.EPI_DUR
			FROM SESSION.VB_PEDW_ABX_RES AS diag
				LEFT JOIN SAIL1169V.PEDW_EPISODE_20210704 AS eps
					ON diag.PROV_UNIT_CD = eps.PROV_UNIT_CD
					AND diag.SPELL_NUM_PE = eps.SPELL_NUM_PE
					AND diag.EPI_NUM = eps.EPI_NUM
		WHERE eps.EPI_STR_DT BETWEEN '2015-01-01' AND '2020-12-31';
	
Commit;

CALL FNC.DROP_IF_EXISTS('sailw1169v.VB_7day_PEDW_ABX_RES');

CREATE TABLE sailw1169v.VB_7day_PEDW_ABX_RES AS (SELECT
		sp.ALF_PE,
		sp.ALF_STS_CD,
		sp.PROV_UNIT_CD,
		sp.SPELL_NUM_PE,
		sp.GNDR_CD AS PEDW_GNDR_CD,
		sp.ADMIS_DT,
		sp.DISCH_DT,
		sp.SPELL_DUR,
		eps.EPI_NUM,
		eps.EPI_STR_DT,
		eps.EPI_END_DT,
		eps.EPI_DUR,
		diag.DIAG_CD_1234
			FROM SAIL1169V.PEDW_SPELL_20210704 AS sp,
				SAIL1169V.PEDW_EPISODE_20210704 AS eps,
				SAIL1169V.PEDW_DIAG_20210704 AS diag) WITH NO DATA;	
			
alter table sailw1169v.VB_7day_PEDW_ABX_RES activate not logged INITIALLY;
			
INSERT INTO sailw1169v.VB_7day_PEDW_ABX_RES
	SELECT 	sp.ALF_PE,
			sp.ALF_STS_CD,
			sp.PROV_UNIT_CD,
			sp.SPELL_NUM_PE,
			sp.GNDR_CD,
			sp.ADMIS_DT,
			sp.DISCH_DT,
			sp.SPELL_DUR,
			eps.EPI_NUM,
			eps.EPI_STR_DT,
			eps.EPI_END_DT,
			eps.EPI_DUR,
			eps.DIAG_CD_1234
		FROM SAIL1169V.PEDW_SPELL_20210704 AS sp
			RIGHT JOIN SESSION.VB_PEDW_EPS_ABX_RES AS eps
				ON sp.PROV_UNIT_CD = eps.PROV_UNIT_CD
				AND sp.SPELL_NUM_PE = eps.SPELL_NUM_PE
			INNER JOIN sailw1169v.VB_7day_TARGET_TRIAL_PRE AS coh
				ON sp.ALF_PE = coh.ALF_PE
			WHERE (eps.epi_str_dt BETWEEN coh.diag_date + 1 day AND '2020-12-31')
			AND (eps.epi_str_dt BETWEEN admis_dt AND admis_dt + 2 DAYS);
		
---------------------------------------------------------------------------------------
--delete where there is a prior hospital discharge within 3 days of diagnosis (episode start date)

DELETE FROM sailw1169v.VB_7day_PEDW_ABX_RES AS coh
	WHERE EXISTS 
	(SELECT mqo.alf_pe, mqo.epi_str_dt FROM 
		(SELECT amr.alf_pe, sp.disch_dt, amr.EPI_STR_DT, sp.DISCH_DT AS prior_disch, amr.DISCH_DT
			FROM sailw1169v.VB_7day_PEDW_ABX_RES AS amr
			INNER JOIN sail1169v.PEDW_SPELL_20210704 AS sp
				ON amr.ALF_PE = sp.ALF_PE
				and (amr.EPI_STR_DT BETWEEN sp.DISCH_DT AND sp.DISCH_DT + 3 DAYS)
				AND amr.SPELL_NUM_PE <> sp.SPELL_NUM_PE --different spell_num_pe required so that episode start on same day as discharge are not excluded
	) AS mqo
	WHERE coh.alf_pe = mqo.alf_pe
	AND coh.EPI_STR_DT = mqo.epi_str_dt);
	
---------------------------------------------------------------------------------------		
		
--delete all but first episode start (event start date) for ABX_RES

DELETE FROM 
	(SELECT ROWNUMBER()	OVER(PARTITION BY ALF_PE ORDER BY EPI_STR_DT) AS rn
			FROM sailw1169v.VB_7day_PEDW_ABX_RES) AS mqo
			WHERE rn > 1;
			
COMMIT;		
	
-----------------------------------------------------------------------------
--create table for target trial hospital AMR outcome

CALL fnc.drop_if_exists('sailw1169v.VB_7day_TARGET_TRIAL_HOSP_AMR');

CREATE TABLE sailw1169v.VB_7day_TARGET_TRIAL_HOSP_AMR
AS (SELECT * FROM sailw1169v.VB_7day_TARGET_TRIAL_PRE2)
WITH NO DATA;

INSERT INTO sailw1169v.VB_7day_TARGET_TRIAL_HOSP_AMR
SELECT * FROM sailw1169v.VB_7day_TARGET_TRIAL_PRE2;

--update target trial table with amr date

ALTER TABLE sailw1169v.VB_7day_TARGET_TRIAL_HOSP_AMR
ADD COLUMN AMR_dt date
ADD COLUMN AMR integer;

MERGE INTO  sailw1169v.VB_7day_TARGET_TRIAL_HOSP_AMR AS tt
	USING (SELECT DISTINCT alf_pe, epi_str_dt FROM sailw1169v.VB_7day_PEDW_ABX_RES) AS amr
		ON tt.alf_pe = amr.alf_pe
			WHEN MATCHED THEN
				UPDATE
				SET tt.amr_dt = amr.epi_str_dt
			;
		
UPDATE sailw1169v.VB_7day_TARGET_TRIAL_HOSP_AMR
SET amr = CASE WHEN amr_dt IS null
						THEN 0
				WHEN (amr_dt > dod
					OR amr_dt > GP_END_DT
					OR amr_dt > study_end
					OR amr_dt > ruti_diag_dt + 12 months)
						THEN 0
				WHEN amr_dt IS NOT null
					AND amr_dt between cal_dt AND cal_dt + 6 DAYS
					THEN 1
				ELSE 0
			END;
		
--update table to ensure only the first outcome is flagged if occuring in the same cal_time		
		
UPDATE sailw1169v.VB_7day_TARGET_TRIAL_HOSP_AMR
SET death = CASE WHEN amr = 1
			THEN 0
			ELSE DEATH
		END;
	
UPDATE sailw1169v.VB_7day_TARGET_TRIAL_HOSP_AMR
SET ltf = CASE WHEN amr = 1
			THEN 0
			ELSE LTF
		END;
	
UPDATE sailw1169v.VB_7day_TARGET_TRIAL_HOSP_AMR
SET study_ended = CASE WHEN amr = 1
			THEN 0
			ELSE STUDY_ENDED 
		END;
	
UPDATE sailw1169v.VB_7day_TARGET_TRIAL_HOSP_AMR
SET DIAG_12MONTHS = CASE WHEN amr = 1
			THEN 0
			ELSE DIAG_12MONTHS
		END;
	
--adjust the end reason flag to the earliest of outcome and new_pabx when they 
--occur in the same cal_time.
--if occuring on the same day then prioritise outcome
	
UPDATE sailw1169v.VB_7day_TARGET_TRIAL_HOSP_AMR
SET NEW_PABX = CASE WHEN AMR = 1
                AND NEW_PABX = 1
                AND amr_dt < NEXT_PABX_STR
			THEN 0
			ELSE NEW_PABX 
		END;
	
UPDATE sailw1169v.VB_7day_TARGET_TRIAL_HOSP_AMR
SET AMR = CASE WHEN AMR = 1
                AND NEW_PABX = 1
			THEN 0
			ELSE AMR
		END;

---------------------------------------------------------------------
---------------------------------------------------------------------
--DELETE ANY ROWS WHERE CAL_TIME OVER THE AMR OUTCOME

DELETE FROM sailw1169v.VB_7day_TARGET_TRIAL_HOSP_AMR AS amr
    WHERE amr_dt NOT BETWEEN cal_dt AND CAL_dt + 6 days
	AND cal_dt >= amr_dt;
 
       
DELETE FROM sailw1169v.VB_7day_TARGET_TRIAL_HOSP_AMR AS amr
    WHERE next_pabx_str NOT BETWEEN cal_dt AND CAL_dt + 6 days
	AND cal_dt >= next_pabx_str;

----------------------------------------------------------------------------
--calculate inclusion days

CALL fnc.drop_if_exists('SESSION.VB_Inc_duration');
		
DECLARE GLOBAL TEMPORARY TABLE SESSION.VB_Inc_duration
(alf_pe varchar(15),
inc_dur integer)
ON COMMIT PRESERVE rows;

COMMIT;

INSERT INTO SESSION.VB_Inc_duration
SELECT DISTINCT tt.alf_pe, days_between(ex.exit_dt, tt.ruti_diag_dt) AS inc_dur
FROM sailw1169v.VB_7day_target_trial_pre2 AS tt
INNER join
(SELECT DISTINCT alf_pe,
		CASE WHEN sum(ltf) = 1
				THEN gp_end_dt
			WHEN sum(death) = 1
				THEN dod
			WHEN sum(amr) = 1
				THEN amr_dt
			WHEN sum(new_pabx) = 1
				THEN next_pabx_str
			WHEN sum(diag_12months) = 1
				THEN ruti_diag_dt + 12 months
			ELSE study_end
		END AS exit_dt
	FROM sailw1169v.VB_7day_TARGET_TRIAL_HOSP_AMR
GROUP BY alf_pe, gp_end_dt, gp_end_dt, amr_dt, dod, study_end, next_pabx_str, ruti_diag_dt
ORDER BY alf_pe) AS ex
ON tt.alf_pe = ex.alf_pe;

COMMIT;

------------------------------------------------------------------------------------
--add WRRS organism growth in the 12 months up to and including rUTI date indicator

-------------------------------------------------------------------------------------
--identify where multiple WRRS results on the same day closest to rUTI diag date
--assign hierarchy as follows:
	--confirmed UTI
	--possible UTI
	--heavy mixed growth
	--mixed growth
	--no micro evidence
	--exclude NULL culture

CALL fnc.drop_if_exists('SESSION.Multi_uti_outcomes');

DECLARE GLOBAL TEMPORARY TABLE SESSION.Multi_uti_outcomes
AS (SELECT alf_pe,
			spcm_collected_dt,
			uti_outcome,
			alf_pe AS UTI_CD
		FROM SAILW1169V.COHORT_WRRS_RESULTS_AGREED)
DEFINITION ONLY
ON COMMIT PRESERVE ROWS;

COMMIT;

INSERT INTO SESSION.Multi_uti_outcomes
WITH cte as
(SELECT coh.alf_pe, coh.RUTI_DIAG_DT, max(SPCM_COLLECTED_DT) AS closest_spec_dt FROM SAILW1169V.COHORT_WRRS_RESULTS_AGREED AS wrrs
INNER JOIN sailw1169v.VB_7day_TARGET_TRIAL_HOSP_AMR AS coh
ON wrrs.alf_pe = coh.ALF_PE
WHERE wrrs.SPCM_COLLECTED_DT BETWEEN coh.RUTI_DIAG_DT - 1 YEAR AND coh.RUTI_DIAG_DT
GROUP BY coh.alf_pe, coh.RUTI_DIAG_DT),
cte2 as
(SELECT wrrs1.* FROM SAILW1169V.COHORT_WRRS_RESULTS_AGREED AS wrrs1
INNER JOIN cte
ON wrrs1.alf_pe = cte.alf_pe
AND wrrs1.SPCM_COLLECTED_DT = cte.closest_spec_dt
ORDER BY wrrs1.alf_pe, wrrs1.spcm_collected_dt),
cte3 as
(SELECT alf_pe||spcm_collected_dt AS ALF_ID, count(alf_pe||spcm_collected_dt) AS spec_count
FROM cte2
GROUP BY alf_pe||spcm_collected_dt
HAVING count(alf_pe||spcm_collected_dt) > 1)
SELECT DISTINCT wr.alf_pe, 
				wr.SPCM_COLLECTED_DT,
				wr.UTI_OUTCOME,
				CASE WHEN uti_outcome = 'Confirmed UTI'
					THEN 1
						WHEN uti_outcome = 'Possible UTI'
					THEN 2
						WHEN uti_outcome = 'Heavy mixed growth'
					THEN 3
						WHEN uti_outcome = 'Mixed growth'
					THEN 4
						WHEN uti_outcome = 'No microbiological evidence of UTI'
					THEN 5
						WHEN uti_outcome = 'Exclude NULL culture'
					THEN 6
				END AS UTI_CD 
FROM SAILW1169V.COHORT_WRRS_RESULTS_AGREED AS wr
INNER JOIN cte3
ON cte3.alf_id = wr.alf_pe||spcm_collected_dt
ORDER BY wr.ALF_PE, wr.SPCM_COLLECTED_DT, uti_cd;

COMMIT;

DELETE FROM 
	(SELECT ROWNUMBER()	OVER(PARTITION BY ALF_PE, spcm_collected_dt ORDER BY UTI_CD) AS rn
			FROM SESSION.Multi_uti_outcomes) AS mqo
			WHERE rn > 1;

COMMIT;

--create closest WRRS result table, using above code where there were >1 result on same day

CALL fnc.drop_if_exists('sailw1169v.VB_7day_target_trial_closest_wrrs');

CREATE TABLE sailw1169v.VB_7day_target_trial_closest_wrrs
AS (SELECT alf_pe,
			spcm_collected_dt,
			uti_outcome,
			alf_pe AS UTI_IND
		FROM SAILW1169V.COHORT_WRRS_RESULTS_AGREED)
	WITH NO DATA;

INSERT INTO sailw1169v.VB_7day_target_trial_closest_wrrs
WITH cte as
(SELECT coh.alf_pe, coh.RUTI_DIAG_DT, max(SPCM_COLLECTED_DT) AS closest_spec_dt FROM SAILW1169V.COHORT_WRRS_RESULTS_AGREED AS wrrs
INNER JOIN sailw1169v.VB_7day_TARGET_TRIAL_HOSP_AMR AS coh
ON wrrs.alf_pe = coh.ALF_PE
WHERE wrrs.SPCM_COLLECTED_DT BETWEEN coh.RUTI_DIAG_DT - 1 YEAR AND coh.RUTI_DIAG_DT
GROUP BY coh.alf_pe, coh.RUTI_DIAG_DT),
cte2 as
(SELECT wrrs1.* FROM SAILW1169V.COHORT_WRRS_RESULTS_AGREED AS wrrs1
INNER JOIN cte
ON wrrs1.alf_pe = cte.alf_pe
AND wrrs1.SPCM_COLLECTED_DT = cte.closest_spec_dt
ORDER BY wrrs1.alf_pe, wrrs1.spcm_collected_dt),
cte3 as
(SELECT alf_pe||spcm_collected_dt AS ALF_ID, count(alf_pe||spcm_collected_dt) AS spec_count
FROM cte2
GROUP BY alf_pe||spcm_collected_dt
HAVING count(alf_pe||spcm_collected_dt) = 1)
SELECT DISTINCT wr.alf_pe, 
				wr.SPCM_COLLECTED_DT,
				wr.UTI_OUTCOME,
				CASE WHEN wr.UTI_OUTCOME IN ('Confirmed UTI',
											'Possible UTI')
						THEN 1
					WHEN wr.UTI_OUTCOME IN ('Heavy mixed growth',
											'Mixed growth')
						THEN 2
					WHEN wr.UTI_OUTCOME IN ('No microbiological evidence of UTI',
											'Exclude NULL culture')
						THEN 0
				end
FROM SAILW1169V.COHORT_WRRS_RESULTS_AGREED AS wr
INNER JOIN cte3
ON cte3.alf_id = wr.alf_pe||spcm_collected_dt
UNION
SELECT alf_pe,
		spcm_collected_dt,
		uti_outcome,
		CASE WHEN UTI_OUTCOME IN ('Confirmed UTI',
									'Possible UTI')
				THEN 1
			WHEN UTI_OUTCOME IN ('Heavy mixed growth',
									'Mixed growth')
				THEN 2
			WHEN UTI_OUTCOME IN ('No microbiological evidence of UTI',
									'Exclude NULL culture')
				THEN 0
		end
FROM SESSION.Multi_uti_outcomes;

--Update AMR table with confirmed or possible UTI = 1
					--	heavy mixed growth or mixed growth = 2
					--	no micro evidence or exclude NULL culture = 3
					--	no WRRS in prior 12 months = NULL
					
ALTER TABLE sailw1169v.VB_7day_TARGET_TRIAL_HOSP_AMR
ADD COLUMN MOST_RECENT_URINE integer;
				
MERGE INTO sailw1169v.VB_7day_TARGET_TRIAL_HOSP_AMR AS coh
	USING sailw1169v.VB_7day_target_trial_closest_wrrs AS wr
		ON coh.ALF_PE = wr.ALF_PE
			WHEN MATCHED THEN
				UPDATE
				SET coh.MOST_RECENT_URINE = wr.UTI_IND
			;
		
----------------------------------------------------------------------------------
--Add pAbx type field

ALTER TABLE sailw1169v.VB_7day_TARGET_TRIAL_HOSP_AMR
ADD COLUMN PABX_TYPE varchar(20);

MERGE INTO sailw1169v.VB_7day_TARGET_TRIAL_HOSP_AMR AS coh
USING
(SELECT DISTINCT mindt.alf_pe, mindt.PABX_AFTER_RUTI1, pabx.ALT_ABX_TYPE
from
(SELECT mqo.ALF_PE, min(mqo.PABX_AFTER_RUTI1) AS PABX_AFTER_RUTI1 FROM
(SELECT cohp.*, pabxp.PABX_STR AS PABX_AFTER_RUTI1 FROM sailw1169v.VB_7day_TARGET_TRIAL_HOSP_AMR AS cohp
LEFT JOIN SAILW1169V.VB_ALTERNATING_PABX_COH3 AS pabxp
ON cohp.ALF_PE = pabxp.ALF_PE
AND MONTHS_BETWEEN(pabxp.PABX_STR,cohp.ruti_diag_dt) >= 0
AND MONTHS_BETWEEN(pabxp.PABX_STR,cohp.ruti_diag_dt) < 12
ORDER BY ALF_PE) AS mqo
WHERE mqo.PABX_AFTER_RUTI1 IS NOT NULL
GROUP BY mqo.ALF_PE) AS mindt
INNER JOIN SAILW1169V.VB_ALTERNATING_PABX_COH3 AS pabx
ON mindt.alf_pe = pabx.alf_pe
AND mindt.PABX_AFTER_RUTI1 = pabx.PABX_STR)
AS pabx
		ON coh.ALF_PE = pabx.ALF_PE
			WHEN MATCHED THEN
				UPDATE
				SET coh.PABX_TYPE = pabx.ALT_ABX_TYPE;
			
----------------------------------------------------------------------------------
--update pabx_str and pabx_end to null/inclusion end date where this occurs outside the study period or criteria

UPDATE sailw1169v.VB_7day_TARGET_TRIAL_HOSP_AMR
SET pabx_str = CASE WHEN pabx_str > MIN_END_DT
					THEN NULL
					ELSE pabx_str
				end;
			
UPDATE sailw1169v.VB_7day_TARGET_TRIAL_HOSP_AMR
SET pabx_end = CASE WHEN pabx_str IS NULL
					THEN NULL
				ELSE CASE WHEN pabx_end > MIN_END_DT
					THEN MIN_END_DT
					ELSE pabx_end
				END
			end;
		
----------------------------------------------------------------------------------
--add column pabx_duration

ALTER TABLE sailw1169v.VB_7day_TARGET_TRIAL_HOSP_AMR
ADD COLUMN pabx_duration integer;

UPDATE sailw1169v.VB_7day_TARGET_TRIAL_HOSP_AMR
SET pabx_duration = CASE WHEN pabx_str IS NULL 
							then NULL
						ELSE days_between(PABX_END, first_abx_dt)
					END;
				
----------------------------------------------------------------------------------
--update pabx_str_all and pabx_end_all to null/inclusion end date where this occurs outside the study period or criteria

UPDATE sailw1169v.VB_7day_TARGET_TRIAL_HOSP_AMR
SET pabx_str_all = CASE WHEN pabx_str_all > MIN_END_DT
					THEN NULL
					ELSE pabx_str_all
				end;
			
UPDATE sailw1169v.VB_7day_TARGET_TRIAL_HOSP_AMR
SET pabx_end_all = CASE WHEN pabx_str_all IS NULL
					THEN NULL
				ELSE CASE WHEN pabx_end_all > MIN_END_DT
					THEN MIN_END_DT
					ELSE pabx_end_all
				END
			end;
		
----------------------------------------------------------------------------------
--add column pabx_duration_all

ALTER TABLE sailw1169v.VB_7day_TARGET_TRIAL_HOSP_AMR
ADD COLUMN pabx_duration_all integer;

UPDATE sailw1169v.VB_7day_TARGET_TRIAL_HOSP_AMR
SET pabx_duration_all = CASE WHEN pabx_str_all IS NULL 
							then NULL
						ELSE days_between(PABX_END_all, PABX_ALL_FIRST_SCRIPT)
					END;
		
----------------------------------------------------------------------------------
--add column to identify whether death occurs between amr_outcome and study end date or gp_end
--do not flag if lost to follow up due to gp_end prior to study end in case they have moved out 
--of the country and we would not have follow up data

ALTER table sailw1169v.VB_7day_TARGET_TRIAL_HOSP_AMR
ADD COLUMN death_after_outcome integer;

UPDATE sailw1169v.VB_7day_TARGET_TRIAL_HOSP_AMR AS tt
SET death_after_outcome = 1
	WHERE tt.alf_pe IN 
	(SELECT DISTINCT coh.alf_pe FROM sailw1169v.VB_7day_TARGET_TRIAL_HOSP_AMR AS coh
INNER join
(SELECT * FROM (SELECT alf_pe, min(COALESCE(gp_end_dt, STUDY_END)) AS min_end
FROM sailw1169v.VB_7day_TARGET_TRIAL_HOSP_AMR
GROUP BY alf_pe) AS mqo) AS jj
ON coh.alf_pe = jj.alf_pe
WHERE coh.dod BETWEEN coh.AMR_DT AND jj.min_end);

UPDATE sailw1169v.VB_7day_TARGET_TRIAL_HOSP_AMR AS tt
SET death_after_outcome = CASE WHEN death_after_outcome = 1
								THEN death_after_outcome
							ELSE 0 
						END;
					
---------------------------------------------------------------------------------
--add column for next UTI

ALTER TABLE sailw1169v.VB_7day_TARGET_TRIAL_HOSP_AMR
ADD COLUMN next_uti_dt date
ADD COLUMN next_uti integer;

ALTER TABLE sailw1169v.VB_7day_TARGET_TRIAL_HOSP_AMR activate NOT logged INITIALLY;			
					
UPDATE sailw1169v.VB_7day_TARGET_TRIAL_HOSP_AMR AS t1
SET t1.next_uti_dt = 
(SELECT min(uti.EVENT_STR_DT)
	FROM SAILW1169V.VB_ALL_UTI_COMB AS uti
	INNER JOIN sailw1169v.VB_7day_TARGET_TRIAL_HOSP_AMR AS amr
		ON uti.ALF_PE = amr.ALF_PE
		AND uti.EVENT_STR_DT > amr.RUTI_DIAG_DT
WHERE t1.alf_pe = uti.alf_pe
GROUP BY uti.alf_pe);

COMMIT;
					
UPDATE sailw1169v.VB_7day_TARGET_TRIAL_HOSP_AMR AS amr
SET next_uti = case when amr.next_uti_dt
					BETWEEN cal_dt AND cal_dt + 6 DAYS THEN 1
						ELSE 0
				end;
			
------------------------------------------------------------------------------------
--additional exclusion criteria, delete all patients from the cohort where first prescription was prior to baseline

DELETE from sailw1169v.VB_7day_TARGET_TRIAL_HOSP_AMR AS amr
WHERE amr.alf_pe IN (SELECT DISTINCT coh.alf_pe FROM sailw1169v.VB_7day_TARGET_TRIAL_HOSP_AMR  AS coh --working tab
INNER JOIN SAILW1169V.VB_ALTERNATING_PABX_COH3 AS pabx
ON coh.ALF_pe = pabx.ALF_PE
AND coh.PABX_STR = pabx.PABX_STR
WHERE pabx.ALT_STR_DT < ruti_diag_dt
AND pabx.PABX_STR >= ruti_diag_dt
ORDER BY coh.alf_pe);
					
---------------------------------------------------------------------------------
----------------------------------------------------------------------------------
----------------------------------------------------------------------------------
--create outcome 1 hospital amr event table
--sets subsequent pAbx date information to NULL if occurring after study end date

CALL fnc.drop_if_exists('sailw1169v.VB_7day_target_trial_hosp_adm_amr');

CREATE TABLE sailw1169v.VB_7day_target_trial_hosp_adm_amr
(ALF varchar(15),
RUTI_DIAGDATE date,
cal_time integer,
fup_obs integer,
birth_week date,
ruti_age integer,
ethnic integer,
WIMD integer,
BMI integer,
smoking integer,
alcohol integer,
frailty integer,
diab integer,
cancer integer,
ckd integer,
htn integer,
ihd integer,
cvd integer,
ccf integer,
ms integer,
mnd integer,
dementia integer,
pd integer,
severe_ment integer,
asthma integer,
copd integer,
immun_supp integer,
renal_stone integer,
urin_tract_abnorm integer,
amr_baseline integer,
MOST_RECENT_URINE integer,
utis_gp_baseline integer,
utis_hosp_baseline integer,
gp_prescrib_rate double,
pabx_start integer,
pabx_type varchar(20),
dose varchar(15),
dose_consistency varchar(20),
time_to_pabx integer,
pabx_duration integer,
all_pabx integer,
all_pabx_duration integer,
gp_int_days integer,
hosp_adm_amr integer,
hosp_amd_ame_dt date,
death integer,
death_dt date,
fu_loss integer,
fu_loss_dt date,
study_end integer,
diag_12months integer,
diag_12months_dt date,
new_pabx_str integer,
new_pabx_str_dt date,
death_after_outcome integer,
next_uti integer)
NOT logged initially;


INSERT INTO sailw1169v.VB_7day_target_trial_hosp_adm_amr
SELECT tt.alf_pe,
		tt.RUTI_DIAG_Dt,
		tt.cal_time,
		dur.inc_dur,
		pre.wob,
		pre.agediag,
		pre.ethnic,
		pre.WIMD_2019_quintile,
		pre.BMI_VAL,
		pre.smok,
		pre.alcohol_flg,
		pre.efi,
		pre.diabetes,
		pre.cancer,
		pre.renal_disease,
		pre.hypertension,
		pre.cvd,
		pre.cerebrov,
		pre.heartfail,
		pre.ms,
		pre.mnd,
		pre.dementia,
		pre.parkinsons,
		pre.smh,
		pre.asthma,
		pre.copd,
		pre.immun_supp,
		pre.renal_stone,
		pre.abn_renal,
		pre.amr_baseline,
		tt.MOST_RECENT_URINE,
		pre.gp_uti_pre,
		pre.pedw_uti_pre,
		pre.gp_presc_rt,
		CASE WHEN (tt.pabx_str > tt.MIN_END_DT
					OR tt.pabx_str IS NULL
					OR tt.pabx_str > tt.AMR_DT)
			THEN 0 
				ELSE tt.PABX
			end,
		CASE WHEN (tt.pabx_str > tt.MIN_END_DT
					OR tt.pabx_str IS NULL
					OR tt.pabx_str > tt.AMR_DT)
			THEN NULL
				ELSE tt.pabx_type
			END,
		CASE WHEN (tt.pabx_str > tt.MIN_END_DT
					OR tt.pabx_str IS NULL
					OR tt.pabx_str > tt.AMR_DT)
			THEN NULL
				ELSE pre.dose
			end,
		CASE WHEN (tt.pabx_str > tt.MIN_END_DT
					OR tt.pabx_str IS NULL
					OR tt.pabx_str > tt.AMR_DT)
			THEN NULL
				ELSE pre.dose_consistency
			end,
		CASE WHEN (tt.pabx_str > tt.MIN_END_DT
					OR tt.pabx_str IS NULL
					OR tt.pabx_str > tt.AMR_DT)
			THEN NULL 
				ELSE pre.days_to_pabx
			end,
		CASE WHEN (tt.pabx_str > tt.MIN_END_DT
					OR tt.pabx_str IS NULL
					OR tt.pabx_str > tt.AMR_DT)
			THEN NULL 
				ELSE tt.pabx_duration
			end,
		CASE WHEN (tt.pabx_str_all > tt.MIN_END_DT
					OR tt.pabx_str_all IS NULL
					OR tt.pabx_str_all > tt.AMR_DT)
			THEN 0
				ELSE tt.all_pabx
			end,
		CASE WHEN (tt.pabx_str_all > tt.MIN_END_DT
					OR tt.pabx_str_all IS NULL
					OR tt.pabx_str_all > tt.AMR_DT)
			THEN NULL 
				ELSE tt.pabx_duration_all
			end,
		pre.gp_int_days,
		tt.amr,
		tt.amr_dt,
		tt.death,
		tt.dod,
		tt.ltf,
		tt.GP_END_DT,
		tt.study_ended,
		tt.diag_12months,
		tt.RUTI_DIAG_DT + 12 MONTHS,
		tt.new_pabx,
		tt.NEXT_PABX_STR,
		tt.death_after_outcome,
		tt.next_uti
FROM sailw1169v.VB_7day_TARGET_TRIAL_HOSP_AMR AS tt
	INNER JOIN sailw1169v.VB_7day_target_trial_pre AS pre
	ON tt.alf_pe = pre.alf_pe
	INNER JOIN SESSION.VB_Inc_duration AS dur
	ON tt.alf_pe = dur.alf_pe
ORDER BY alf_pe, cal_dt;

GRANT ALL ON sailw1169v.VB_7day_target_trial_hosp_adm_amr TO USER sanyaoll;

---------------------------------------------------------------------
---------------------------------------------------------------------
