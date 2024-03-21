

# library(CDMConnector)
# con <- DBI::dbConnect(duckdb::duckdb(), eunomia_dir())
# cdm <- cdm_from_con(con, "main", "main")
# 
# cohortSet <- readCohortSet(system.file("cohorts2", package = "CDMConnector"))
# exportFolder <- here::here("work", "export")
# runAnalysis <- 1:9
# minCellCount <- 5
# cohortTable <- NULL

executeDiagnostics <- function(cdm,
                               cohortSet,
                               cohortTableName,
                               exportFolder,
                               runAnalysis = 1:9,
                               minCellCount = 5,
                               cohortTable = NULL) {
  
  # Params ----
  fs::dir_create(exportFolder)
  checkmate::assertDirectoryExists(exportFolder, access = "w")
  
  allAnalyses <- c(
    "InclusionStatistics",
    "IncludedSourceConcepts",
    "OrphanConcepts",
    "TimeSeries",
    "VisitContext",
    "BreakdownIndexEvents",
    "IncidenceRate",
    "CohortRelationship",
    "TemporalCohortCharacterization")
  
  selectedAnalyses <- allAnalyses[runAnalysis]
  
  # Counts and Attrition ----
  prefix <-  paste0("tmp", stringr::str_remove_all(uuid::UUIDgenerate(), '-'))
  cohortTableName <- paste0(prefix, "cohort")
  cdm <- CDMConnector::generateCohortSet(
    cdm,
    cohortSet = cohortSet,
    name = cohortTableName
  )
  
  cohortCount(cdm[[cohortTableName]]) %>% 
    dplyr::left_join(settings(cdm[[cohortTableName]]), by = "cohort_definition_id") %>% 
    dplyr::select("cohort_name", "number_records", "number_subjects") %>% 
    readr::write_csv(file.path(exportFolder, "cohort_counts.csv"))
  
  attrition(cdm[[cohortTableName]]) %>% 
    dplyr::left_join(settings(cdm[[cohortTableName]]), by = "cohort_definition_id") %>% 
    dplyr::select(-"cohort_definition_id") %>% 
    dplyr::select("cohort_name", dplyr::everything()) %>% 
    readr::write_csv(file.path(exportFolder, "cohort_attrition.csv"))
  
  
  # Incidence Rate ----
  # Stratified by Age 10y, Gender, Calendar Year
  # For now stratified by kid-Adult-Older Adult 
  # it is the step it takes longest
  
  if ("IncidenceRate" %in% selectedAnalyses) {
    tictoc::tic(msg = "Incidence by year, age, sex")
    
    cdm <- IncidencePrevalence::generateDenominatorCohortSet(
      cdm = cdm, 
      name = "denominator", 
      # ageGroup = list(c(0,17), c(18,64), c(65,199)),
      ageGroup = purrr::map(0:10, ~c(.*10, (.+1)*10-1)),
      sex = c("Male", "Female", "Both"),
      daysPriorObservation = 180
    )
    
    inc <- IncidencePrevalence::estimateIncidence(
      cdm = cdm,
      denominatorTable = "denominator",
      outcomeTable = cohortTableName,
      interval = "years",
      repeatedEvents = FALSE,
      outcomeWashout = Inf,
      completeDatabaseIntervals = FALSE,
      minCellCount = 0 )
    
    tictoc::toc(log = TRUE)
    tictoc::tic(msg = "Prevalence by year, age, sex")
    
    prev <- IncidencePrevalence::estimatePeriodPrevalence(
      cdm = cdm,
      denominatorTable = "denominator",
      outcomeTable = cohortTableName,
      interval = "years",
      completeDatabaseIntervals = TRUE,
      fullContribution = FALSE,
      minCellCount = 5,
      temporary = TRUE,
      returnParticipants = FALSE
    )
    
    tictoc::toc(log = TRUE)
    
    readr::write_csv(inc, file.path(exportFolder, "incidence.csv"))
    readr::write_csv(prev, file.path(exportFolder, "prevalence.csv"))
  }
  
  
  # Time Distributions ----
  # observation time (days) after index , observation time (days) prior to index, time (days) between cohort start and end
  # Need to add better characterisation of demographics (a sort of table 1)
  
  tictoc::tic(msg = "Demographics")
  
  qry <- cdm[[cohortTableName]]%>%
    PatientProfiles::addDemographics() %>% 
    dplyr::mutate(time_in_cohort = !!CDMConnector::datediff("cohort_start_date", "cohort_end_date")) %>% 
    dplyr::mutate(time_in_cohort = dplyr::case_when(.data$time_in_cohort == 0L ~ 1L, TRUE ~ .data$time_in_cohort)) %>% # should we default to a minimum of 1 day?
    dplyr::group_by(.data$cohort_definition_id) %>% 
    dplyr::compute()
    
  probs <- c(.1, .25, .5, .75, .9)
  time1 <- dplyr::bind_rows(
    CDMConnector::summariseQuantile(qry, x = time_in_cohort, probs = probs) %>% dplyr::mutate(period = "time_in_cohort") %>% dplyr::collect(),
    CDMConnector::summariseQuantile(qry, x = future_observation, probs = probs) %>% dplyr::mutate(period = "future_observation") %>% dplyr::collect(),
    CDMConnector::summariseQuantile(qry, x = prior_observation, probs = probs) %>% dplyr::mutate(period = "prior_observation") %>% dplyr::collect(),
  )
    
  time2 <- dplyr::bind_rows(
    dplyr::summarise(qry, 
      avg = mean(prior_observation, na.rm = TRUE),
      min = min(prior_observation, na.rm = TRUE),
      max = max(prior_observation, na.rm = TRUE)) %>% 
    dplyr::collect() %>% 
    dplyr::mutate(period = "prior_observation"),
    
    dplyr::summarise(qry, 
      avg = mean(future_observation, na.rm = TRUE),
      min =  min(future_observation, na.rm = TRUE),
      max =  max(future_observation, na.rm = TRUE)) %>% 
    dplyr::collect() %>% 
    dplyr::mutate(period = "future_observation"),
    
    dplyr::summarise(qry, 
      avg = mean(time_in_cohort, na.rm = TRUE),
      min = min(time_in_cohort, na.rm = TRUE),
      max = max(time_in_cohort, na.rm = TRUE)) %>% 
    dplyr::collect() %>% 
    dplyr::mutate(period = "time_in_cohort")
  )
  
  time <- dplyr::inner_join(time1, time2, by = c("cohort_definition_id", "period")) %>% 
    dplyr::select("cohort_definition_id", "period", "min", "p10_value", "p25_value", "p50_value", "p75_value", "p90_value", "max", "avg")
  
  readr::write_csv(time, file.path(exportFolder, "time_distributions.csv"))
  
  
  # Included Concepts ----
  
  # Upload concept sets to the database
  conceptSets <- cohortSet %>% 
    tidyr::unnest_wider(col = "cohort") %>% 
    dplyr::select("cohort_definition_id", "ConceptSets") %>% 
    tidyr::unnest_longer(col = "ConceptSets") %>% 
    tidyr::unnest_wider(col = "ConceptSets") %>% 
    tidyr::unnest_wider(col = "expression") %>% 
    tidyr::unnest_longer(col = "items") %>% 
    tidyr::unnest_wider(col = "items") %>% 
    tidyr::unnest_wider(col = "concept") %>% 
    dplyr::rename_all(tolower) %>% 
    dplyr::select("cohort_definition_id", 
                  concept_set_id = "id", 
                  cohort_name = "name",
                  "concept_id", 
                  is_excluded = "isexcluded", 
                  include_descendants = "includedescendants") %>% 
    dplyr::mutate(is_excluded = tidyr::replace_na(is_excluded, FALSE)) %>% 
    dplyr::mutate(include_descendants = tidyr::replace_na(include_descendants, FALSE)) 
    
  
  # upload concept data to the database 
  tempName <- paste0("tmp", as.integer(Sys.time()))
  
  DBI::dbWriteTable(con,
                    name = inSchema(attr(cdm, "write_schema"), tempName, dbms = CDMConnector::dbms(con)),
                    value = dplyr::select(conceptSets, -"cohort_name"),
                    overwrite = TRUE)
  
  if (any(conceptSets$include_descendants)) {
    CDMConnector::assert_tables(cdm, "concept_ancestor")
  }
  
  ## upload concept set table ----
  conceptSetsDb <- dplyr::tbl(CDMConnector::cdmCon(cdm), inSchema(attr(cdm, "write_schema"), tempName, dbms = CDMConnector::dbms(con)))
  
  concepts <- { if (any(conceptSets$include_descendants)) {
    conceptSetsDb %>% 
      dplyr::filter(.data$include_descendants) %>%
      dplyr::inner_join(cdm$concept_ancestor, by = c("concept_id" = "ancestor_concept_id")) %>%
      dplyr::select("cohort_definition_id",
                    "concept_set_id",
                    "concept_id" = "descendant_concept_id", 
                    "is_excluded") 
    } else conceptSetsDb } %>% 
    dplyr::filter(.data$is_excluded == FALSE) %>%
    dplyr::select("cohort_definition_id", "concept_set_id", "concept_id") %>% 
    # Note that concepts that are not in the vocab will be silently ignored
    dplyr::inner_join(
      dplyr::select(cdm$concept, "concept_id", "domain_id", "concept_name", "concept_code"),
      by = "concept_id") %>%
    dplyr::distinct() %>%
    CDMConnector::computeQuery(
      name = paste0(tempName, "_concepts"),
      temporary = FALSE,
      overwrite = TRUE,
      schema = attr(cdm, "write_schema"))
  
  DBI::dbRemoveTable(con, name = inSchema(CDMConnector::cdmWriteSchema(cdm), tempName, dbms = CDMConnector::dbms(con)))
  
  on.exit({
    if (DBI::dbIsValid(con)) {
      DBI::dbRemoveTable(con, name = CDMConnector::inSchema(CDMConnector::cdmWriteSchema(cdm), paste0(tempName, "_conceptsets"), dbms = dbms(con)))
      # DBI::dbRemoveTable(con, name = CDMConnector::inSchema(CDMConnector::cdmWriteSchema(cdm), paste0(tempName, "_concepts"), dbms = dbms(con)))
    }
  }, add = TRUE)
  
  # count occurrences of the concepts used in the cohort definitions
  
  domains <- dplyr::distinct(concepts, .data$domain_id) %>% dplyr::pull("domain_id") %>% tolower()
  
  # cdm$concept %>% dplyr::distinct(.data$domain_id) %>% dplyr::pull("domain_id") # look at all domains
  
  # Tables to get counts for
  tbls <- c(condition = "condition_occurrence", 
            procedure = "procedure_occurrence", 
            observation = "observation", 
            measurement = "measurement", 
            drug = "drug_exposure")[domains] %>%
    unname()
  
  includedConceptCounts <- purrr::map(tbls, function(tbl_name) {
    domain <- stringr::str_remove_all(tbl_name, "_occurrence|_exposure")
    
    cdm[[tbl_name]] %>% 
      dplyr::rename(concept_id = !!paste0(domain, "_concept_id")) %>% 
      dplyr::semi_join(concepts, by = "concept_id") %>% 
      dplyr::group_by(.data$concept_id) %>% 
      dplyr::summarize(concept_subjects = dplyr::n_distinct(.data$person_id),
                       concept_count = dplyr::n()) %>% 
      dplyr::mutate(source_concept_id = 0L)
  }) %>% 
  { if (length(.) > 0) {
    purrr::reduce(., dplyr::union_all) %>% 
      dplyr::left_join(dplyr::select(cdm$concept, "concept_id", "concept_name", "vocabulary_id", "concept_code"), by = "concept_id") %>% 
      dplyr::collect()
  } else {
    # case where we have no concepts in domains we are counting
    dplyr::tibble(
      concept_id = integer(),
      concept_subjects = double(),
      councept_count = double(),
      source_concept_id = integer(),
      concept_name = character(),
      vocabulary_id = character(),
      concept_code = character()
    )
  }}

  readr::write_csv(includedConceptCounts, file.path(exportFolder, "included_concept_counts.csv"))
  
  # Index event breakdown ----
  # counts of concept ids occurring on the cohort index date 
  indexConceptCounts <- purrr::map(tbl, function(tbl_name) {
    domain <- stringr::str_remove_all(tbl_name, "_occurrence|_exposure")
    
    start_date <- switch(tbl_name,
                         "condition_occurrence" = "condition_occurrence_start_date", 
                         "procedure_occurrence" = "procedure_occurrence_date", 
                         "observation" = "observation_date", 
                         "measurement" = "measurement_date", 
                         "drug_exposure" = "drug_exposure_start_date")
    
    join_by <- setNames(c("subject_id","cohort_start_date"), c("person_id", start_date))
    
    cdm[[tbl_name]] %>% 
      dplyr::semi_join(cdm[[cohortTableName]], by = join_by) %>% 
      dplyr::rename(concept_id = !!paste0(domain, "_concept_id")) %>% 
      dplyr::inner_join(concepts, by = "concept_id") %>% 
      dplyr::group_by(.data$concept_id) %>% 
      dplyr::summarize(concept_subjects = dplyr::n_distinct(.data$person_id), concept_count = dplyr::n()) %>% 
      dplyr::mutate(source_concept_id = 0L)
  }) %>% { 
    if (length(.) > 0) {
      purrr::reduce(dplyr::union_all) %>% 
      dplyr::left_join(dplyr::select(cdm$concept, "concept_id", "concept_name", "vocabulary_id", "concept_code"), by = "concept_id") %>% 
      dplyr::collect()
    } else {
      dplyr::tibble(
        concept_id = integer(),
        concept_subjects = double(),
        councept_count = double(),
        source_concept_id = integer(),
        concept_name = character(),
        vocabulary_id = character(),
        concept_code = character())
    }
  }
  
  readr::write_csv(file.path(exportFolder, "index_event_concept_counts.csv"), indexConceptCounts)
  

}






  