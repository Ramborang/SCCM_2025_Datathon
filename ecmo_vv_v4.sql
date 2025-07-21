--v4
-- FINAL IMPROVED ECMO COHORT DATASET QUERY
-- OPTIMIZED BASED ON ACTUAL DATA ANALYSIS AND FEEDBACK
-- Target: 171 unique patients, 175 total patient-visit combinations
-- All joins use (person_id, visit_occurrence_id) for proper data integrity

WITH research_cohort AS (
  -- Step 1: Identify patients with acute-on-chronic respiratory failure + VV ECMO
  -- This identifies 194 patients at person-level, filtered to 171 with valid visit IDs
  SELECT DISTINCT r.person_id
  FROM `sccm-discovery.rediscover_2025_v1.CONDITION_OCCURRENCE` r
  INNER JOIN `sccm-discovery.rediscover_2025_v1.PROCEDURE_OCCURRENCE` e
    ON r.person_id = e.person_id  -- Person-level join for initial cohort identification
  WHERE r.condition_concept_id IN (
    37016114, -- Acute on chronic hypoxemic respiratory failure
    312940,   -- Acute-on-chronic respiratory failure  
    36716978  -- Acute on chronic hypercapnic respiratory failure
  )
  AND e.procedure_concept_id IN (
    46257510, -- VV ECMO daily management (most common procedure)
    46257543, -- VV ECMO initiation (start procedure)  
    1531630   -- VV ECMO cannulation (insertion procedure)
  )
),

-- Step 2: Get ECMO visits with proper (person_id, visit_occurrence_id) tracking
-- CRITICAL: 23 patients excluded here due to NULL visit_occurrence_id (194→171)
-- JUSTIFICATION: Need valid visit IDs for proper data linkage per rediscover guidance
ecmo_visits AS (
  SELECT DISTINCT 
    p.person_id, 
    p.visit_occurrence_id,
    MIN(p.procedure_date) as visit_ecmo_start_date,
    MAX(p.procedure_date) as visit_ecmo_end_date,
    -- Convert to DATETIME for hour calculations (fixes previous DATE_DIFF error)
    MIN(DATETIME(p.procedure_date)) as visit_ecmo_start_datetime,
    MAX(DATETIME(p.procedure_date)) as visit_ecmo_end_datetime
  FROM `sccm-discovery.rediscover_2025_v1.PROCEDURE_OCCURRENCE` p
  INNER JOIN research_cohort rc ON p.person_id = rc.person_id
  WHERE p.procedure_concept_id IN (46257510, 46257543, 1531630)
    AND p.visit_occurrence_id IS NOT NULL  -- CRITICAL: Exclude NULL visit IDs
  GROUP BY p.person_id, p.visit_occurrence_id
),

-- Step 3: Get visit demographics with correct age calculation
-- IMPROVEMENT: Age at admission (not 2024!) for clinical relevance
visit_demographics AS (
  SELECT 
    ev.person_id,
    ev.visit_occurrence_id,
    
    -- Patient demographics
    p.gender_concept_id,               -- 8507=Male, 8532=Female
    p.race_concept_id,                 -- Race concept ID
    p.ethnicity_concept_id,            -- Ethnicity concept ID
    p.src_name as patient_site,        -- Treatment site (SITE-1 through SITE-8)
    
    -- Visit timing
    v.visit_start_date,                -- Hospital admission date
    -- CORRECTED: Age at this specific admission, not age in 2024
    DATE_DIFF(v.visit_start_date, DATE(p.year_of_birth, 1, 1), YEAR) as age_at_admission,
    
    -- ECMO timing within this visit
    ev.visit_ecmo_start_date,          -- First ECMO procedure this visit
    ev.visit_ecmo_end_date,            -- Last ECMO procedure this visit  
    ev.visit_ecmo_start_datetime,      -- For hour calculations
    ev.visit_ecmo_end_datetime,        -- For hour calculations
    -- ECMO duration: 0 days = same-day procedures, up to 123 days for long runs
    DATE_DIFF(ev.visit_ecmo_end_date, ev.visit_ecmo_start_date, DAY) as ecmo_duration_days
    
  FROM ecmo_visits ev
  INNER JOIN `sccm-discovery.rediscover_2025_v1.PERSON` p 
    ON ev.person_id = p.person_id
  INNER JOIN `sccm-discovery.rediscover_2025_v1.VISIT_OCCURRENCE` v 
    ON ev.person_id = v.person_id AND ev.visit_occurrence_id = v.visit_occurrence_id
),

-- Step 4: REMOVED mechanical ventilation tracking 
-- JUSTIFICATION: Analysis showed 0% of patients had recorded mechanical ventilation procedures
-- The concept ID 4230167 doesn't exist in this dataset for our cohort

-- Step 5: Lab measurements using IMPROVED concept IDs based on data analysis
-- OPTIMIZATION: Using concept IDs that actually have data in our cohort
visit_measurements AS (
  SELECT 
    ev.person_id,
    ev.visit_occurrence_id,
    m.measurement_concept_id, 
    m.value_as_number,
    m.measurement_date
  FROM ecmo_visits ev
  INNER JOIN `sccm-discovery.rediscover_2025_v1.MEASUREMENT` m 
    ON ev.person_id = m.person_id AND ev.visit_occurrence_id = m.visit_occurrence_id
  WHERE m.measurement_concept_id IN (
    -- Core lab panel (high data completeness)
    3038553,  -- BMI [Ratio] 
    3010813,  -- Leukocytes [#/volume] in Blood (WBC)
    3008037,  -- Lactate [Moles/volume] in Venous blood
    3016723,  -- Creatinine [Mass/volume] in Serum or Plasma
    3024128,  -- Bilirubin.total [Mass/volume] in Serum or Plasma (94.9% completeness)
    
    -- SOFA Components (using concept IDs with actual data)
    3027801,  -- PaO2 [Partial pressure] in Arterial blood (94.3% completeness)
    3026238,  -- FiO2 primary concept (76.6% completeness)
    4353936,  -- FiO2 fallback concept (18.3% completeness) - ADDED for fallback strategy
    3007461,  -- Platelets [#/volume] in Blood (IMPROVED: 141 patients vs 56 with 3024929)
    3004249,  -- Systolic Blood Pressure 
    3012888,  -- Diastolic Blood Pressure
    
    -- REMOVED: 3032652 (Glasgow Coma Scale) - 0% data completeness in our cohort
    
    -- Enhanced lab panel
    3001122,  -- Ferritin [Mass/volume] in Serum or Plasma
    3051714,  -- Fibrin D-dimer FEU [Mass/volume] (primary fibrin marker)
    3017732,  -- Neutrophils [#/volume] in Blood
    3019198,  -- Lymphocytes [#/volume] in Blood
    3020460   -- C reactive protein [Mass/volume] in Serum or Plasma
  )
  AND m.value_as_number IS NOT NULL
  AND m.value_as_number > 0  -- Remove erroneous zero/negative values
),

-- Step 6: Aggregate lab values with improved platelet concept ID
aggregated_visit_labs AS (
  SELECT
    person_id,
    visit_occurrence_id,
    
    -- Core lab variables
    ROUND(AVG(CASE WHEN measurement_concept_id = 3038553 THEN value_as_number END), 1) AS bmi_avg,
    ROUND(AVG(CASE WHEN measurement_concept_id = 3010813 THEN value_as_number END), 1) AS wbc_avg,
    ROUND(AVG(CASE WHEN measurement_concept_id = 3008037 THEN value_as_number END), 2) AS lactate_avg,
    ROUND(AVG(CASE WHEN measurement_concept_id = 3016723 THEN value_as_number END), 2) AS creatinine_avg,
    ROUND(AVG(CASE WHEN measurement_concept_id = 3024128 THEN value_as_number END), 2) AS bilirubin_avg,
    
    -- Enhanced lab panel
    ROUND(AVG(CASE WHEN measurement_concept_id = 3001122 THEN value_as_number END), 1) AS ferritin_avg,
    ROUND(AVG(CASE WHEN measurement_concept_id = 3051714 THEN value_as_number END), 2) AS fibrin_avg,
    ROUND(AVG(CASE WHEN measurement_concept_id = 3017732 THEN value_as_number END), 1) AS neutrophil_count_avg,
    ROUND(AVG(CASE WHEN measurement_concept_id = 3019198 THEN value_as_number END), 1) AS lymphocyte_count_avg,
    ROUND(AVG(CASE WHEN measurement_concept_id = 3020460 THEN value_as_number END), 1) AS crp_avg,
    
    -- CALCULATED: Neutrophil to Lymphocyte Ratio (inflammatory/prognostic marker)
    ROUND(
      AVG(CASE WHEN measurement_concept_id = 3017732 THEN value_as_number END) / 
      NULLIF(AVG(CASE WHEN measurement_concept_id = 3019198 THEN value_as_number END), 0), 
      2
    ) AS neutrophil_lymphocyte_ratio,
    
    -- SOFA Components (using improved concept IDs with FiO2 fallback strategy)
    ROUND(AVG(CASE WHEN measurement_concept_id = 3027801 THEN value_as_number END), 1) AS pao2_avg,
    
    -- IMPROVED FiO2 with fallback strategy: Use 3026238 first, fallback to 4353936
    -- This increases data completeness from 76.6% to 94.9% (adds 32 more patients!)
    ROUND(COALESCE(
      AVG(CASE WHEN measurement_concept_id = 3026238 THEN value_as_number END),
      AVG(CASE WHEN measurement_concept_id = 4353936 THEN value_as_number END)
    ), 1) AS fio2_avg,
    ROUND(AVG(CASE WHEN measurement_concept_id = 3007461 THEN value_as_number END), 0) AS platelets_avg,  -- IMPROVED concept ID
    ROUND(AVG(CASE WHEN measurement_concept_id = 3004249 THEN value_as_number END), 1) AS systolic_bp_avg,
    ROUND(AVG(CASE WHEN measurement_concept_id = 3012888 THEN value_as_number END), 1) AS diastolic_bp_avg
    
  FROM visit_measurements
  GROUP BY person_id, visit_occurrence_id
),

-- Step 7: Drug exposures for comprehensive medication tracking
visit_drugs AS (
  SELECT 
    ev.person_id,
    ev.visit_occurrence_id,
    
    -- Vasopressors (critical for SOFA cardiovascular component)
    MAX(CASE WHEN d.drug_concept_id IN (1321341, 740244, 742155, 1321363, 19040856, 740243) THEN 1 ELSE 0 END) AS received_norepinephrine,
    MAX(CASE WHEN d.drug_concept_id IN (35202042, 1507835, 1507838, 35202043, 1759255, 44132646, 35201749) THEN 1 ELSE 0 END) AS received_vasopressin,
    MAX(CASE WHEN d.drug_concept_id IN (1343916, 19076899, 1344245, 46275916) THEN 1 ELSE 0 END) AS received_epinephrine,
    MAX(CASE WHEN d.drug_concept_id IN (963889, 963897) THEN 1 ELSE 0 END) AS received_angiotensin_ii,
    
    -- Any vasopressor (for SOFA cardiovascular scoring)
    MAX(CASE WHEN d.drug_concept_id IN (
      1321341, 740244, 742155, 1321363, 19040856, 740243,        -- Norepinephrine
      35202042, 1507835, 1507838, 35202043, 1759255, 44132646, 35201749,  -- Vasopressin
      1343916, 19076899, 1344245, 46275916,                      -- Epinephrine
      963889, 963897                                             -- Angiotensin II
    ) THEN 1 ELSE 0 END) AS received_any_vasopressor,
    
    -- Paralytics (indicates severe respiratory failure)
    MAX(CASE WHEN d.drug_concept_id IN (45776038, 45776042, 836208, 40079815, 779334, 780241, 45776040) THEN 1 ELSE 0 END) AS received_succinylcholine,
    MAX(CASE WHEN d.drug_concept_id IN (42707627, 42708029, 19003953) THEN 1 ELSE 0 END) AS received_rocuronium,
    MAX(CASE WHEN d.drug_concept_id IN (40165389, 40165390, 19012598, 37498072) THEN 1 ELSE 0 END) AS received_vecuronium,
    MAX(CASE WHEN d.drug_concept_id IN (19015726, 19016315, 19016316, 40029077, 19044710, 35605393) THEN 1 ELSE 0 END) AS received_cisatracurium,
    
    -- Steroids (anti-inflammatory therapy)
    MAX(CASE WHEN d.drug_concept_id IN (975505, 35604741, 19006967, 975168, 975125) THEN 1 ELSE 0 END) AS received_hydrocortisone,
    MAX(CASE WHEN d.drug_concept_id IN (35606533, 1506430, 19080181, 1506270, 42901997, 35606542, 35606538, 19034806, 1506426, 1506315) THEN 1 ELSE 0 END) AS received_methylprednisolone,
    MAX(CASE WHEN d.drug_concept_id IN (40241504, 19076145, 40028260, 1719012, 1518292, 1518608, 1518259, 1518293, 1518258, 1518254) THEN 1 ELSE 0 END) AS received_dexamethasone,
    
    -- Other critical care medications
    MAX(CASE WHEN d.drug_concept_id = 43011850 THEN 1 ELSE 0 END) AS received_heparin,
    MAX(CASE WHEN d.drug_concept_id = 1154029 THEN 1 ELSE 0 END) AS received_fentanyl,
    MAX(CASE WHEN d.drug_concept_id = 753626 THEN 1 ELSE 0 END) AS received_propofol,
    MAX(CASE WHEN d.drug_concept_id = 40160973 THEN 1 ELSE 0 END) AS received_enoxaparin
    
  FROM ecmo_visits ev
  LEFT JOIN `sccm-discovery.rediscover_2025_v1.DRUG_EXPOSURE` d 
    ON ev.person_id = d.person_id AND ev.visit_occurrence_id = d.visit_occurrence_id
  WHERE d.drug_concept_id IN (
    1321341, 740244, 742155, 1321363, 19040856, 740243,
    35202042, 1507835, 1507838, 35202043, 1759255, 44132646, 35201749,
    1343916, 19076899, 1344245, 46275916,
    963889, 963897,
    45776038, 45776042, 836208, 40079815, 779334, 780241, 45776040,
    42707627, 42708029, 19003953,
    40165389, 40165390, 19012598, 37498072,
    19015726, 19016315, 19016316, 40029077, 19044710, 35605393,
    975505, 35604741, 19006967, 975168, 975125,
    35606533, 1506430, 19080181, 1506270, 42901997, 35606542, 35606538, 19034806, 1506426, 1506315,
    40241504, 19076145, 40028260, 1719012, 1518292, 1518608, 1518259, 1518293, 1518258, 1518254,
    43011850, 1154029, 753626, 40160973
  )
  GROUP BY ev.person_id, ev.visit_occurrence_id
),

-- Step 8: CRRT procedure tracking (dialysis)
visit_procedures AS (
  SELECT 
    ev.person_id,
    ev.visit_occurrence_id,
    MAX(CASE WHEN p.procedure_concept_id = 37018292 THEN 1 ELSE 0 END) AS received_crrt
  FROM ecmo_visits ev
  LEFT JOIN `sccm-discovery.rediscover_2025_v1.PROCEDURE_OCCURRENCE` p 
    ON ev.person_id = p.person_id AND ev.visit_occurrence_id = p.visit_occurrence_id
  WHERE p.procedure_concept_id = 37018292  -- Continuous renal replacement therapy
  GROUP BY ev.person_id, ev.visit_occurrence_id
),

-- Step 9: MODIFIED SOFA SCORE CALCULATION 
-- IMPROVEMENT: Removed GCS component due to 0% data availability
-- IMPROVEMENT: Using better platelet concept ID (3007461 vs 3024929)
sofa_calculations AS (
  SELECT
    asl.person_id,
    asl.visit_occurrence_id,
    
    -- Lab values
    asl.bmi_avg,
    asl.wbc_avg,
    asl.lactate_avg,
    asl.creatinine_avg,
    asl.bilirubin_avg,
    asl.ferritin_avg,
    asl.fibrin_avg,
    asl.neutrophil_count_avg,
    asl.lymphocyte_count_avg,
    asl.crp_avg,
    asl.neutrophil_lymphocyte_ratio,
    asl.pao2_avg,
    asl.fio2_avg,
    asl.platelets_avg,
    asl.systolic_bp_avg,
    asl.diastolic_bp_avg,
    
    -- Get vasopressor status for cardiovascular SOFA
    vd.received_any_vasopressor,
    
    -- Calculate Mean Arterial Pressure: MAP = (2 × Diastolic + Systolic) / 3
    ROUND((2 * asl.diastolic_bp_avg + asl.systolic_bp_avg) / 3, 1) AS map_avg,
    
    -- Calculate PaO2/FiO2 Ratio (key ARDS severity marker)
    CASE 
      WHEN asl.pao2_avg IS NOT NULL AND asl.fio2_avg IS NOT NULL AND asl.fio2_avg > 0 
      THEN ROUND(asl.pao2_avg / (asl.fio2_avg / 100), 1)
      ELSE NULL 
    END AS pao2_fio2_ratio,
    
    -- SOFA RESPIRATORY COMPONENT (MedCalc standard)
    -- PaO2/FiO2 ratio: ≥400=0, 300-399=1, 200-299=2, 100-199=3, <100=4
    CASE 
      WHEN asl.pao2_avg IS NOT NULL AND asl.fio2_avg IS NOT NULL AND asl.fio2_avg > 0 THEN
        CASE 
          WHEN (asl.pao2_avg / (asl.fio2_avg / 100)) >= 400 THEN 0
          WHEN (asl.pao2_avg / (asl.fio2_avg / 100)) >= 300 THEN 1
          WHEN (asl.pao2_avg / (asl.fio2_avg / 100)) >= 200 THEN 2
          WHEN (asl.pao2_avg / (asl.fio2_avg / 100)) >= 100 THEN 3
          ELSE 4
        END
      ELSE NULL
    END AS sofa_respiratory_score,
    
    -- SOFA COAGULATION COMPONENT (MedCalc standard)
    -- Platelets (×10³/μL): ≥150=0, 100-149=1, 50-99=2, 20-49=3, <20=4
    CASE 
      WHEN asl.platelets_avg IS NOT NULL THEN
        CASE 
          WHEN asl.platelets_avg >= 150 THEN 0
          WHEN asl.platelets_avg >= 100 THEN 1
          WHEN asl.platelets_avg >= 50 THEN 2
          WHEN asl.platelets_avg >= 20 THEN 3
          ELSE 4
        END
      ELSE NULL
    END AS sofa_coagulation_score,
    
    -- SOFA LIVER COMPONENT (MedCalc standard)
    -- Bilirubin (mg/dL): <1.2=0, 1.2-1.9=1, 2.0-5.9=2, 6.0-11.9=3, ≥12.0=4
    CASE 
      WHEN asl.bilirubin_avg IS NOT NULL THEN
        CASE 
          WHEN asl.bilirubin_avg < 1.2 THEN 0
          WHEN asl.bilirubin_avg < 2.0 THEN 1
          WHEN asl.bilirubin_avg < 6.0 THEN 2
          WHEN asl.bilirubin_avg < 12.0 THEN 3
          ELSE 4
        END
      ELSE NULL
    END AS sofa_liver_score,
    
    -- SOFA NEUROLOGICAL COMPONENT REMOVED
    -- JUSTIFICATION: GCS has 0% data completeness in our cohort
    -- All patients will have NULL for neurological score
    NULL AS sofa_neurological_score,
    
    -- SOFA CARDIOVASCULAR COMPONENT (MedCalc standard)
    -- Based on MAP and vasopressor requirement
    -- MAP≥70 + no vasopressors=0, MAP<70 or any vasopressor=1-3
    CASE 
      WHEN asl.systolic_bp_avg IS NOT NULL AND asl.diastolic_bp_avg IS NOT NULL THEN
        CASE 
          WHEN ((2 * asl.diastolic_bp_avg + asl.systolic_bp_avg) / 3) >= 70 AND vd.received_any_vasopressor = 0 THEN 0
          WHEN ((2 * asl.diastolic_bp_avg + asl.systolic_bp_avg) / 3) >= 70 AND vd.received_any_vasopressor = 1 THEN 2
          WHEN ((2 * asl.diastolic_bp_avg + asl.systolic_bp_avg) / 3) < 70 AND vd.received_any_vasopressor = 0 THEN 1
          WHEN ((2 * asl.diastolic_bp_avg + asl.systolic_bp_avg) / 3) < 70 AND vd.received_any_vasopressor = 1 THEN 3
          ELSE 2
        END
      WHEN vd.received_any_vasopressor = 1 THEN 2  -- Vasopressors without MAP data
      ELSE NULL
    END AS sofa_cardiovascular_score
    
  FROM aggregated_visit_labs asl
  LEFT JOIN visit_drugs vd ON asl.person_id = vd.person_id AND asl.visit_occurrence_id = vd.visit_occurrence_id
),

-- Step 10: Calculate MODIFIED total SOFA scores
-- IMPROVEMENT: Only sum available components (excluding neurological due to 0% data)
final_sofa_calculations AS (
  SELECT
    sc.*,
    
    -- MODIFIED TOTAL SOFA SCORE (4 components instead of 5)
    -- Excludes neurological component due to no GCS data
    (COALESCE(sc.sofa_respiratory_score, 0) + 
     COALESCE(sc.sofa_coagulation_score, 0) + 
     COALESCE(sc.sofa_liver_score, 0) + 
     COALESCE(sc.sofa_cardiovascular_score, 0)) AS sofa_total_score,
     
    -- Count available SOFA components (max 4 instead of 5)
    (CASE WHEN sc.sofa_respiratory_score IS NOT NULL THEN 1 ELSE 0 END +
     CASE WHEN sc.sofa_coagulation_score IS NOT NULL THEN 1 ELSE 0 END +
     CASE WHEN sc.sofa_liver_score IS NOT NULL THEN 1 ELSE 0 END +
     CASE WHEN sc.sofa_cardiovascular_score IS NOT NULL THEN 1 ELSE 0 END) AS sofa_components_available
     
  FROM sofa_calculations sc
),

-- Step 11: REMOVED ventilation timing calculation
-- JUSTIFICATION: Mechanical ventilation procedures have 0% data completeness
-- Original hours_ventilation_to_ecmo will be NULL for all patients

-- Step 12: Mortality outcomes
mortality_data AS (
  SELECT 
    vd.person_id,
    vd.visit_occurrence_id,
    CASE WHEN d.death_date IS NOT NULL THEN 1 ELSE 0 END AS died,
    d.death_date,
    
    -- 30-day mortality relative to ECMO end
    CASE 
      WHEN d.death_date IS NULL THEN NULL
      WHEN d.death_date BETWEEN vd.visit_ecmo_start_date AND DATE_ADD(vd.visit_ecmo_end_date, INTERVAL 30 DAY) THEN 1 
      ELSE 0 
    END AS died_within_30_days_of_ecmo
    
  FROM visit_demographics vd
  LEFT JOIN `sccm-discovery.rediscover_2025_v1.DEATH` d ON vd.person_id = d.person_id
)

-- STEP 13: FINAL IMPROVED OUTPUT
-- OPTIMIZED based on data analysis feedback
SELECT 
  -- PATIENT AND VISIT IDENTIFIERS
  vd.person_id,                       -- Unique patient identifier
  vd.visit_occurrence_id,             -- Unique visit identifier (with person_id makes unique combination)
  
  -- DEMOGRAPHICS AND SITE INFORMATION (USER-FRIENDLY LABELS)
  vd.patient_site,                    -- Original site code (SITE-1, SITE-2, SITE-7)
  CASE 
    WHEN vd.patient_site = 'SITE-1' THEN 'Site 1 (9 patients)'
    WHEN vd.patient_site = 'SITE-2' THEN 'Site 2 (31 patients)' 
    WHEN vd.patient_site = 'SITE-7' THEN 'Site 7 (131 patients)'
    ELSE vd.patient_site
  END AS site_description,            -- USER-FRIENDLY: Site with patient counts
  
  -- Gender with descriptive labels for easy analysis
  vd.gender_concept_id,               -- Original concept ID (8507=Male, 8532=Female)
  CASE 
    WHEN vd.gender_concept_id = 8507 THEN 'Male'
    WHEN vd.gender_concept_id = 8532 THEN 'Female'
    ELSE 'Unknown'
  END AS gender,                      -- USER-FRIENDLY: Actual gender labels
  
  -- Race with descriptive labels for easy analysis
  vd.race_concept_id,                 -- Original concept ID
  CASE 
    WHEN vd.race_concept_id = 8527 THEN 'White'
    WHEN vd.race_concept_id = 8516 THEN 'Black or African American'
    WHEN vd.race_concept_id = 38003599 THEN 'African American'
    WHEN vd.race_concept_id = 8515 THEN 'Asian'
    WHEN vd.race_concept_id = 8657 THEN 'American Indian or Alaska Native'
    WHEN vd.race_concept_id = 0 THEN 'Not Specified'
    ELSE 'Other/Unknown'
  END AS race,                        -- USER-FRIENDLY: Actual race labels
  
  -- Ethnicity with descriptive labels for easy analysis
  vd.ethnicity_concept_id,            -- Original concept ID
  CASE 
    WHEN vd.ethnicity_concept_id = 38003564 THEN 'Not Hispanic or Latino'
    WHEN vd.ethnicity_concept_id = 38003563 THEN 'Hispanic or Latino'
    WHEN vd.ethnicity_concept_id = 0 THEN 'Not Specified'
    ELSE 'Other/Unknown'
  END AS ethnicity,                   -- USER-FRIENDLY: Actual ethnicity labels
  vd.age_at_admission,                -- CORRECTED: Age at this admission (not 2024!)
  
  -- Age categories for easier analysis
  CASE 
    WHEN vd.age_at_admission < 30 THEN 'Under 30'
    WHEN vd.age_at_admission < 50 THEN '30-49'
    WHEN vd.age_at_admission < 70 THEN '50-69'
    ELSE '70 and over'
  END AS age_category,                -- USER-FRIENDLY: Age groups for analysis
  
  -- VISIT AND ECMO TIMING
  vd.visit_start_date,                -- Hospital admission date
  vd.visit_ecmo_start_date,           -- First ECMO procedure this visit
  vd.visit_ecmo_end_date,             -- Last ECMO procedure this visit
  vd.ecmo_duration_days,              -- Days on ECMO (0-123 days range is clinically normal)
  
  -- ECMO duration categories for easier analysis
  CASE 
    WHEN vd.ecmo_duration_days = 0 THEN 'Same day (0 days)'
    WHEN vd.ecmo_duration_days <= 7 THEN 'Short (1-7 days)'
    WHEN vd.ecmo_duration_days <= 30 THEN 'Medium (8-30 days)'
    ELSE 'Long (>30 days)'
  END AS ecmo_duration_category,      -- USER-FRIENDLY: Duration groups for analysis
  
  -- REMOVED: hours_ventilation_to_ecmo (mechanical ventilation has 0% data)
  -- JUSTIFICATION: Concept ID 4230167 has no data in our cohort
  
  -- CORE LABORATORY VALUES (high data completeness)
  fsc.bmi_avg,                        -- Body Mass Index
  fsc.wbc_avg,                        -- White Blood Cell count
  fsc.lactate_avg,                    -- Lactate (tissue perfusion marker)
  fsc.creatinine_avg,                 -- Creatinine (kidney function)
  
  -- ENHANCED LABORATORY PANEL
  fsc.ferritin_avg,                   -- Iron storage/inflammation marker
  fsc.fibrin_avg,                     -- Coagulation marker (D-dimer)
  fsc.crp_avg,                        -- C-reactive protein (inflammation)
  fsc.neutrophil_count_avg,           -- Primary immune cells
  fsc.lymphocyte_count_avg,           -- Secondary immune cells
  fsc.neutrophil_lymphocyte_ratio,    -- CALCULATED inflammatory ratio (>10 = severe inflammation)
  
  -- SOFA SCORE COMPONENTS (using improved concept IDs)
  fsc.pao2_avg,                       -- Arterial oxygen pressure (94.3% data completeness)
  fsc.fio2_avg,                       -- Inspired oxygen fraction (76.6% data completeness)
  fsc.pao2_fio2_ratio,                -- CALCULATED P/F ratio (key ARDS severity marker)
  fsc.platelets_avg,                  -- IMPROVED: Using concept 3007461 (more data than 3024929)
  -- REMOVED: gcs_avg (0% data completeness)
  fsc.bilirubin_avg,                  -- Total bilirubin (94.9% data completeness)
  fsc.systolic_bp_avg,                -- Systolic blood pressure
  fsc.diastolic_bp_avg,               -- Diastolic blood pressure
  fsc.map_avg,                        -- CALCULATED Mean Arterial Pressure
  
  -- MODIFIED SOFA SCORES (4 components instead of 5)
  fsc.sofa_respiratory_score,         -- 0-4: Based on PaO2/FiO2 ratio
  fsc.sofa_coagulation_score,         -- 0-4: Based on platelet count
  fsc.sofa_liver_score,               -- 0-4: Based on bilirubin level
  fsc.sofa_neurological_score,        -- Always NULL (GCS has 0% data)
  fsc.sofa_cardiovascular_score,      -- 0-4: Based on MAP + vasopressor use
  fsc.sofa_total_score,               -- MODIFIED: Sum of 4 components (0-16 instead of 0-20)
  fsc.sofa_components_available,      -- Data quality: How many components calculated (max 4)
  
  -- SOFA severity categories for easier clinical interpretation
  CASE 
    WHEN fsc.sofa_total_score IS NULL THEN 'Cannot Calculate'
    WHEN fsc.sofa_total_score <= 6 THEN 'Low (0-6)'
    WHEN fsc.sofa_total_score <= 9 THEN 'Moderate (7-9)'
    WHEN fsc.sofa_total_score <= 12 THEN 'High (10-12)'
    ELSE 'Very High (13-16)'
  END AS sofa_severity_category,      -- USER-FRIENDLY: SOFA severity groups
  
  -- VASOPRESSOR THERAPY (binary indicators: 1=received, 0=not received)
  vdr.received_norepinephrine,        -- First-line vasopressor
  vdr.received_vasopressin,           -- Second-line vasopressor
  vdr.received_epinephrine,           -- High-dose vasopressor
  vdr.received_angiotensin_ii,        -- Rescue vasopressor
  vdr.received_any_vasopressor,       -- Any vasopressor (cardiovascular failure indicator)
  
  -- PARALYTIC AGENTS (binary indicators)
  vdr.received_succinylcholine,       -- Rapid sequence intubation
  vdr.received_rocuronium,            -- Intermediate-acting paralytic
  vdr.received_vecuronium,            -- Intermediate-acting paralytic
  vdr.received_cisatracurium,         -- Long-acting paralytic
  
  -- STEROID THERAPY (binary indicators)
  vdr.received_hydrocortisone,        -- Stress-dose steroid
  vdr.received_methylprednisolone,    -- Anti-inflammatory steroid
  vdr.received_dexamethasone,         -- Long-acting steroid
  
  -- OTHER CRITICAL CARE MEDICATIONS
  vdr.received_heparin,               -- Anticoagulation
  vdr.received_fentanyl,              -- Sedation/analgesia
  vdr.received_propofol,              -- Sedation
  vdr.received_enoxaparin,            -- Anticoagulation
  
  -- PROCEDURES
  COALESCE(vp.received_crrt, 0) AS received_crrt,  -- Continuous renal replacement therapy (dialysis)
  
  -- REMOVED: received_mechanical_ventilation (always 0 in our data)
  
  -- PRIMARY AND SECONDARY OUTCOMES
  md.died,                            -- Mortality (1=died, 0=alive)
  md.death_date,                      -- Date of death if applicable
  md.died_within_30_days_of_ecmo      -- 30-day mortality relative to ECMO end

FROM visit_demographics vd

-- Join all components using proper (person_id, visit_occurrence_id) pairs
LEFT JOIN final_sofa_calculations fsc 
  ON vd.person_id = fsc.person_id AND vd.visit_occurrence_id = fsc.visit_occurrence_id

LEFT JOIN visit_drugs vdr 
  ON vd.person_id = vdr.person_id AND vd.visit_occurrence_id = vdr.visit_occurrence_id

LEFT JOIN visit_procedures vp 
  ON vd.person_id = vp.person_id AND vd.visit_occurrence_id = vp.visit_occurrence_id

LEFT JOIN mortality_data md 
  ON vd.person_id = md.person_id AND vd.visit_occurrence_id = md.visit_occurrence_id

-- Final ordering for consistent output
-- CONFIRMED: All 175 rows represent unique (person_id, visit_occurrence_id) combinations
ORDER BY vd.person_id, vd.visit_occurrence_id;