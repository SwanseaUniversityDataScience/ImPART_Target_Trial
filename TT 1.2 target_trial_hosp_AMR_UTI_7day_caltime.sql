---------------------------------------------------------------------------
---------------------------------------------------------------------------
--****MUST RUN SCRIPT TT 1.1 target_trial_hosp_AMR_7day_caltime PRIOR TO THIS SCRIPT***
--initial table sailw1169v.VB_7day_TARGET_TRIAL_PRE2
---------------------------------------------------------------------------
---------------------------------------------------------------------------

--Hospitalisation with an AMR UTI
--identified AS ANY PEDW diag code recorded FOR antimicrobial resistance FROM pAbx START
--where a UTI diagnosis is recorded in the same episode
--to exclude hospital acquired infections, the episode must start within 2 days of an 
--admission and must not occur within 3 days of a prior discharge

--------------------------------------------------------------------------
--AMR hospitalisation
--identified AS ANY PEDW diag code recorded FOR antimicrobial resistance FROM pAbx START

---------------------------------------------------
--hospitalisation with an AMR infection

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

-------------------------------------------------------------------------
-------------------------------------------------------------------------

--hospitalisation with an AMR UTI

CALL FNC.DROP_IF_EXISTS('SESSION.VB_PEDW_AMR_UTI');

DECLARE GLOBAL TEMPORARY TABLE SESSION.VB_PEDW_AMR_UTI
	AS (SELECT * FROM SAIL1169V.PEDW_DIAG_20210704) DEFINITION ONLY
ON COMMIT PRESERVE ROWS;

Commit;

INSERT INTO SESSION.VB_PEDW_AMR_UTI
	SELECT * FROM SAIL1169V.PEDW_DIAG_20210704 AS dg
	WHERE (dg.DIAG_CD_1234 LIKE '%N10%'
	OR dg.DIAG_CD_1234 LIKE '%N12%'
	OR dg.DIAG_CD_1234 LIKE '%N300%'
	OR dg.DIAG_CD_1234 LIKE '%N308%'
	OR dg.DIAG_CD_1234 LIKE '%N309%'
	OR dg.DIAG_CD_1234 LIKE '%N390%');
	
Commit;

CALL FNC.DROP_IF_EXISTS('SESSION.VB_PEDW_EPS_AMR_UTI');

DECLARE GLOBAL TEMPORARY TABLE SESSION.VB_PEDW_EPS_AMR_UTI
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

INSERT INTO SESSION.VB_PEDW_EPS_AMR_UTI
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
			FROM SESSION.VB_PEDW_AMR_UTI AS diag
				LEFT JOIN SAIL1169V.PEDW_EPISODE_20210704 AS eps
					ON diag.PROV_UNIT_CD = eps.PROV_UNIT_CD
					AND diag.SPELL_NUM_PE = eps.SPELL_NUM_PE
					AND diag.EPI_NUM = eps.EPI_NUM
		WHERE eps.EPI_STR_DT BETWEEN '2015-01-01' AND '2020-12-31';
	
Commit;

CALL FNC.DROP_IF_EXISTS('SAILW1169V.VB_7day_AMR_UTI');

CREATE TABLE SAILW1169V.VB_7day_AMR_UTI AS (SELECT
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
			
alter table SAILW1169V.VB_7day_AMR_UTI activate not logged INITIALLY;
			
INSERT INTO SAILW1169V.VB_7day_AMR_UTI
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
			RIGHT JOIN SESSION.VB_PEDW_EPS_AMR_UTI AS eps
				ON sp.PROV_UNIT_CD = eps.PROV_UNIT_CD
				AND sp.SPELL_NUM_PE = eps.SPELL_NUM_PE
			INNER JOIN sailw1169v.VB_7day_PEDW_ABX_RES AS amr
				ON sp.ALF_PE = amr.alf_pe
				AND sp.PROV_UNIT_CD = amr.prov_unit_cd
				AND sp.SPELL_NUM_PE = amr.spell_num_pe
			INNER JOIN sailw1169v.VB_7day_TARGET_TRIAL_PRE AS coh
				ON sp.ALF_PE = coh.ALF_PE
			WHERE eps.epi_str_dt BETWEEN coh.diag_date + 1 day AND '2020-12-31'
			AND (eps.epi_str_dt BETWEEN sp.admis_dt AND sp.admis_dt + 2 DAYS);
		
---------------------------------------------------------------------------------------
--delete where there is a prior hospital discharge within 3 days of diagnosis (episode start date)

DELETE FROM SAILW1169V.VB_7day_AMR_UTI AS coh
	WHERE EXISTS 
	(SELECT mqo.alf_pe, mqo.epi_str_dt FROM 
		(SELECT amr.alf_pe, sp.disch_dt, amr.EPI_STR_DT, sp.DISCH_DT AS prior_disch, amr.DISCH_DT
			FROM SAILW1169V.VB_7day_AMR_UTI AS amr
			INNER JOIN sail1169v.PEDW_SPELL_20210704 AS sp
				ON amr.ALF_PE = sp.ALF_PE
				and (amr.EPI_STR_DT BETWEEN sp.DISCH_DT AND sp.DISCH_DT + 3 DAYS)
				AND amr.SPELL_NUM_PE <> sp.SPELL_NUM_PE --different spell_num_pe required so that episode start on same day as discharge are not excluded
	) AS mqo
	WHERE coh.alf_pe = mqo.alf_pe
	AND coh.EPI_STR_DT = mqo.epi_str_dt);

---------------------------------------------------------------------------------------				
--delete all but first episode start (event start date) for AMR UTI

DELETE FROM 
	(SELECT ROWNUMBER()	OVER(PARTITION BY ALF_PE ORDER BY EPI_STR_DT) AS rn
			FROM SAILW1169V.VB_7day_AMR_UTI) AS mqo
			WHERE rn > 1;
			
COMMIT;
	
-----------------------------------------------------------------------------
--create table for target trial hospital AMR with UTI outcome

CALL fnc.drop_if_exists('sailw1169v.VB_7day_TARGET_TRIAL_HOSP_AMR_UTI');

CREATE TABLE sailw1169v.VB_7day_TARGET_TRIAL_HOSP_AMR_UTI
AS (SELECT * FROM sailw1169v.VB_7day_TARGET_TRIAL_PRE2)
WITH NO DATA;

INSERT INTO sailw1169v.VB_7day_TARGET_TRIAL_HOSP_AMR_UTI
SELECT * FROM sailw1169v.VB_7day_TARGET_TRIAL_PRE2;

--update target trial table with amr date

ALTER TABLE sailw1169v.VB_7day_TARGET_TRIAL_HOSP_AMR_UTI
ADD COLUMN HOSP_ADM_URINE_AMR_DT date
ADD COLUMN HOSP_ADM_URINE_AMR integer;

MERGE INTO  sailw1169v.VB_7day_TARGET_TRIAL_HOSP_AMR_UTI AS tt
	USING (SELECT DISTINCT alf_pe, epi_str_dt FROM SAILW1169V.VB_7day_AMR_UTI) AS amr
		ON tt.alf_pe = amr.alf_pe
			WHEN MATCHED THEN
				UPDATE
				SET tt.HOSP_ADM_URINE_AMR_DT = amr.epi_str_dt
			;
		
UPDATE sailw1169v.VB_7day_TARGET_TRIAL_HOSP_AMR_UTI
SET HOSP_ADM_URINE_AMR = CASE WHEN HOSP_ADM_URINE_AMR_DT IS null
						THEN 0
				WHEN (HOSP_ADM_URINE_AMR_DT > dod
					OR HOSP_ADM_URINE_AMR_DT > GP_END_DT
					OR HOSP_ADM_URINE_AMR_DT > study_end
					OR HOSP_ADM_URINE_AMR_DT > ruti_diag_dt + 12 months)
						THEN 0
				WHEN HOSP_ADM_URINE_AMR_DT IS NOT null
					AND HOSP_ADM_URINE_AMR_DT between cal_dt AND cal_dt + 6 DAYS
					THEN 1
				ELSE 0
			END;
		
--update table to ensure only the first outcome is flagged if occuring in the same cal_time		
		
UPDATE sailw1169v.VB_7day_TARGET_TRIAL_HOSP_AMR_UTI
SET death = CASE WHEN HOSP_ADM_URINE_AMR = 1
			THEN 0
			ELSE DEATH
		END;
	
UPDATE sailw1169v.VB_7day_TARGET_TRIAL_HOSP_AMR_UTI
SET ltf = CASE WHEN HOSP_ADM_URINE_AMR = 1
			THEN 0
			ELSE LTF
		END;
	
UPDATE sailw1169v.VB_7day_TARGET_TRIAL_HOSP_AMR_UTI
SET study_ended = CASE WHEN HOSP_ADM_URINE_AMR = 1
			THEN 0
			ELSE STUDY_ENDED 
		END;
	
	
UPDATE sailw1169v.VB_7day_TARGET_TRIAL_HOSP_AMR_UTI
SET DIAG_12MONTHS = CASE WHEN HOSP_ADM_URINE_AMR = 1
			THEN 0
			ELSE DIAG_12MONTHS
		END;
	
--adjust the end reason flag to the earliest of outcome and new_pabx when they occur in the same cal_time
--if occuring on the same day then prioritise outcome
	
UPDATE sailw1169v.VB_7day_TARGET_TRIAL_HOSP_AMR_UTI
SET NEW_PABX = CASE WHEN HOSP_ADM_URINE_AMR = 1
                AND NEW_PABX = 1
                AND HOSP_ADM_URINE_AMR_DT < NEXT_PABX_STR
			THEN 0
			ELSE NEW_PABX 
		END;
	
UPDATE sailw1169v.VB_7day_TARGET_TRIAL_HOSP_AMR_UTI
SET HOSP_ADM_URINE_AMR = CASE WHEN HOSP_ADM_URINE_AMR = 1
                AND NEW_PABX = 1
			THEN 0
			ELSE HOSP_ADM_URINE_AMR
		END;

---------------------------------------------------------------------
---------------------------------------------------------------------
--DELETE ANY ROWS WHERE CAL_TIME OVER THE AMR OUTCOME

DELETE FROM sailw1169v.VB_7day_TARGET_TRIAL_HOSP_AMR_UTI AS amr
    WHERE HOSP_ADM_URINE_AMR_DT NOT BETWEEN cal_dt AND CAL_dt + 6 days
	AND cal_dt >= HOSP_ADM_URINE_AMR_DT;
 
       
DELETE FROM sailw1169v.VB_7day_TARGET_TRIAL_HOSP_AMR_UTI AS amr
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
			WHEN sum(HOSP_ADM_URINE_AMR) = 1
				THEN HOSP_ADM_URINE_AMR_DT
			WHEN sum(new_pabx) = 1
				THEN next_pabx_str
			WHEN sum(diag_12months) = 1
				THEN ruti_diag_dt + 12 months
			ELSE study_end
		END AS exit_dt
	FROM sailw1169v.VB_7day_TARGET_TRIAL_HOSP_AMR_UTI
GROUP BY alf_pe, gp_end_dt, gp_end_dt, HOSP_ADM_URINE_AMR_DT, dod, study_end, next_pabx_str, ruti_diag_dt
ORDER BY alf_pe) AS ex
ON tt.alf_pe = ex.alf_pe;

COMMIT;

----------------------------------------------------------------------------------
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
INNER JOIN sailw1169v.VB_7day_TARGET_TRIAL_HOSP_AMR_UTI AS coh
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
INNER JOIN sailw1169v.VB_7day_TARGET_TRIAL_HOSP_AMR_UTI AS coh
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
					
ALTER TABLE sailw1169v.VB_7day_TARGET_TRIAL_HOSP_AMR_UTI
ADD COLUMN MOST_RECENT_URINE integer;
				
MERGE INTO sailw1169v.VB_7day_TARGET_TRIAL_HOSP_AMR_UTI AS coh
	USING sailw1169v.VB_7day_target_trial_closest_wrrs AS wr
		ON coh.ALF_PE = wr.ALF_PE
			WHEN MATCHED THEN
				UPDATE
				SET coh.MOST_RECENT_URINE = wr.UTI_IND
			;
		
----------------------------------------------------------------------------------
--Add pAbx type field

ALTER TABLE sailw1169v.VB_7day_TARGET_TRIAL_HOSP_AMR_UTI
ADD COLUMN PABX_TYPE varchar(20);

MERGE INTO sailw1169v.VB_7day_TARGET_TRIAL_HOSP_AMR_UTI AS coh
USING
(SELECT DISTINCT mindt.alf_pe, mindt.PABX_AFTER_RUTI1, pabx.ALT_ABX_TYPE
from
(SELECT mqo.ALF_PE, min(mqo.PABX_AFTER_RUTI1) AS PABX_AFTER_RUTI1 FROM
(SELECT cohp.*, pabxp.PABX_STR AS PABX_AFTER_RUTI1 FROM sailw1169v.VB_7day_TARGET_TRIAL_HOSP_AMR_UTI AS cohp
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

UPDATE sailw1169v.VB_7day_TARGET_TRIAL_HOSP_AMR_UTI
SET pabx_str = CASE WHEN pabx_str > MIN_END_DT
					THEN NULL
					ELSE pabx_str
				end;
			
UPDATE sailw1169v.VB_7day_TARGET_TRIAL_HOSP_AMR_UTI
SET pabx_end = CASE WHEN pabx_str IS NULL
					THEN NULL
				ELSE CASE WHEN pabx_end > MIN_END_DT
					THEN MIN_END_DT
					ELSE pabx_end
				END
			end;
		
----------------------------------------------------------------------------------
--add column pabx_duration

ALTER TABLE sailw1169v.VB_7day_TARGET_TRIAL_HOSP_AMR_UTI
ADD COLUMN pabx_duration integer;

UPDATE sailw1169v.VB_7day_TARGET_TRIAL_HOSP_AMR_UTI
SET pabx_duration = CASE WHEN pabx_str IS NULL 
							then NULL
						ELSE days_between(PABX_END, first_abx_dt)
					END;
				
----------------------------------------------------------------------------------
--update pabx_str_all and pabx_end_all to null/inclusion end date where this occurs outside the study period or criteria

UPDATE sailw1169v.VB_7day_TARGET_TRIAL_HOSP_AMR_UTI
SET pabx_str_all = CASE WHEN pabx_str_all > MIN_END_DT
					THEN NULL
					ELSE pabx_str_all
				end;
			
UPDATE sailw1169v.VB_7day_TARGET_TRIAL_HOSP_AMR_UTI
SET pabx_end_all = CASE WHEN pabx_str_all IS NULL
					THEN NULL
				ELSE CASE WHEN pabx_end_all > MIN_END_DT
					THEN MIN_END_DT
					ELSE pabx_end_all
				END
			end;
		
----------------------------------------------------------------------------------
--add column pabx_duration_all

ALTER TABLE sailw1169v.VB_7day_TARGET_TRIAL_HOSP_AMR_UTI
ADD COLUMN pabx_duration_all integer;

UPDATE sailw1169v.VB_7day_TARGET_TRIAL_HOSP_AMR_UTI
SET pabx_duration_all = CASE WHEN pabx_str_all IS NULL 
							then NULL
						ELSE days_between(PABX_END_all, PABX_ALL_FIRST_SCRIPT)
					END;
		
----------------------------------------------------------------------------------
--add column to identify whether death occurs between amr_outcome and study end date or gp_end
--do not flag if lost to follow up due to gp_end prior to study end in case they have moved out of the country and we would not have follow up data
ALTER table sailw1169v.VB_7day_TARGET_TRIAL_HOSP_AMR_UTI
ADD COLUMN death_after_outcome integer;

UPDATE sailw1169v.VB_7day_TARGET_TRIAL_HOSP_AMR_UTI AS tt
SET death_after_outcome = 1
	WHERE tt.alf_pe IN 
	(SELECT DISTINCT coh.alf_pe FROM sailw1169v.VB_7day_TARGET_TRIAL_HOSP_AMR_UTI AS coh
INNER join
(SELECT * FROM (SELECT alf_pe, min(COALESCE(gp_end_dt, STUDY_END)) AS min_end
FROM sailw1169v.VB_7day_TARGET_TRIAL_HOSP_AMR_UTI
GROUP BY alf_pe) AS mqo) AS jj
ON coh.alf_pe = jj.alf_pe
WHERE coh.dod BETWEEN coh.HOSP_ADM_URINE_AMR_DT AND jj.min_end);

UPDATE sailw1169v.VB_7day_TARGET_TRIAL_HOSP_AMR_UTI AS tt
SET death_after_outcome = CASE WHEN death_after_outcome = 1
								THEN death_after_outcome
							ELSE 0 
						END;
					
---------------------------------------------------------------------------------
--add column for next UTI

ALTER TABLE sailw1169v.VB_7day_TARGET_TRIAL_HOSP_AMR_UTI
ADD COLUMN next_uti_dt date
ADD COLUMN next_uti integer;

ALTER TABLE sailw1169v.VB_7day_TARGET_TRIAL_HOSP_AMR_UTI activate NOT logged INITIALLY;			
					
UPDATE sailw1169v.VB_7day_TARGET_TRIAL_HOSP_AMR_UTI AS t1
SET t1.next_uti_dt = 
(SELECT min(uti.EVENT_STR_DT)
	FROM SAILW1169V.VB_ALL_UTI_COMB AS uti
	INNER JOIN sailw1169v.VB_7day_TARGET_TRIAL_HOSP_AMR_UTI AS amr
		ON uti.ALF_PE = amr.ALF_PE
		AND uti.EVENT_STR_DT > amr.RUTI_DIAG_DT
WHERE t1.alf_pe = uti.alf_pe
GROUP BY uti.alf_pe);

COMMIT;
					
UPDATE sailw1169v.VB_7day_TARGET_TRIAL_HOSP_AMR_UTI AS amr
SET next_uti = case when amr.next_uti_dt
					BETWEEN cal_dt AND cal_dt + 6 DAYS THEN 1
						ELSE 0
				end;
			
------------------------------------------------------------------------------------
--additional exclusion criteria, delete all patients from the cohort where first prescription was prior to baseline

DELETE from sailw1169v.VB_7day_TARGET_TRIAL_HOSP_AMR_UTI AS amr
WHERE amr.alf_pe IN (SELECT DISTINCT coh.alf_pe FROM sailw1169v.VB_7day_TARGET_TRIAL_HOSP_AMR_UTI  AS coh --working tab
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

CALL fnc.drop_if_exists('sailw1169v.VB_7day_target_trial_hosp_adm_urine_amr');

CREATE TABLE sailw1169v.VB_7day_target_trial_hosp_adm_urine_amr
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
HOSP_ADM_URINE_AMR integer,
hosp_adm_urine_amr_dt date,
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


INSERT INTO sailw1169v.VB_7day_target_trial_hosp_adm_urine_amr
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
					OR tt.pabx_str > tt.HOSP_ADM_URINE_AMR_DT)
			THEN 0 
				ELSE tt.PABX
			end,
		CASE WHEN (tt.pabx_str > tt.MIN_END_DT
					OR tt.pabx_str IS NULL
					OR tt.pabx_str > tt.HOSP_ADM_URINE_AMR_DT)
			THEN NULL
				ELSE tt.pabx_type
			end,
		CASE WHEN (tt.pabx_str > tt.MIN_END_DT
					OR tt.pabx_str IS NULL
					OR tt.pabx_str > tt.HOSP_ADM_URINE_AMR_DT)
			THEN NULL
				ELSE pre.dose
			end,
		CASE WHEN (tt.pabx_str > tt.MIN_END_DT
					OR tt.pabx_str IS NULL
					OR tt.pabx_str > tt.HOSP_ADM_URINE_AMR_DT)
			THEN NULL
				ELSE pre.dose_consistency
			end,
		CASE WHEN (tt.pabx_str > tt.MIN_END_DT
					OR tt.pabx_str IS NULL
					OR tt.pabx_str > tt.HOSP_ADM_URINE_AMR_DT)
			THEN NULL 
				ELSE pre.days_to_pabx
			end,
		CASE WHEN (tt.pabx_str > tt.MIN_END_DT
					OR tt.pabx_str IS NULL
					OR tt.pabx_str > tt.HOSP_ADM_URINE_AMR_DT)
			THEN NULL 
				ELSE tt.pabx_duration
			end,
		CASE WHEN (tt.pabx_str_all > tt.MIN_END_DT
					OR tt.pabx_str_all IS NULL
					OR tt.pabx_str_all > tt.HOSP_ADM_URINE_AMR_DT)
			THEN 0
				ELSE tt.all_pabx
			end,
		CASE WHEN (tt.pabx_str_all > tt.MIN_END_DT
					OR tt.pabx_str_all IS NULL
					OR tt.pabx_str_all > tt.HOSP_ADM_URINE_AMR_DT)
			THEN NULL 
				ELSE tt.pabx_duration_all
			end,
		pre.gp_int_days,
		tt.HOSP_ADM_URINE_AMR,
		tt.hosp_adm_urine_amr_dt,
		tt.death,
		tt.dod,
		tt.ltf,
		tt.gp_end_dt,
		tt.study_ended,
		tt.diag_12months,
		tt.ruti_diag_dt + 12 MONTHS,
		tt.new_pabx,
		tt.next_pabx_str,
		tt.death_after_outcome,
		tt.next_uti
FROM sailw1169v.VB_7day_TARGET_TRIAL_HOSP_AMR_UTI AS tt
	INNER JOIN sailw1169v.VB_7day_target_trial_pre AS pre
	ON tt.alf_pe = pre.alf_pe
	INNER JOIN SESSION.VB_Inc_duration AS dur
	ON tt.alf_pe = dur.alf_pe
ORDER BY alf_pe, cal_dt;

GRANT ALL ON sailw1169v.VB_7day_target_trial_HOSP_ADM_URINE_AMR TO USER sanyaoll;
