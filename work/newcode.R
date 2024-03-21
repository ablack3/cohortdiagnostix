# Copyright 2023 Observational Health Data Sciences and Informatics
#
# This file is part of CohortDiagnostics
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

#' executeDiagnosticsCdm
#'
#' @param cdm CDM reference object created with `CDMConnector::cdmFromCon()`
#' @param cohortDefinitionSet Cohort Set dataframe created with `CDMConnector::readCohortSet()`
#' @param cohortTable Cohort table name as a length 1 character vector.
#' @param conceptCountsTable Concept counts table name with precomputed concept counts created with `createConceptCountsTable`.
#' @param exportFolder The folder where the output will be exported to. If this folder does not exist it will be created.
#' @param minCellCount The minimum cell count for fields contains person counts or fractions.
#' @param runAnalysis A numeric vector corresponding to the diagnostic analyses should be executed. See Details
#'
#' @details runAnalysis argument
#' 
#' Pass a numeric vector matching the following analyses. Negative numbers will be interpreted as exclusions.
#' Use a numeric vector to select the analyses you want to run. Negative numbers will exclude analyses.
#' The selection works the same way as vector subsetting in R.
#' 
#' # Analyses to choose from:
#' 1. InclusionStatistics - A table showing the number of subject that match specific inclusion rules in the cohort definition. 
#' 2. IncludedSourceConcepts - A table showing the (source) concepts observed in the database that are included in a concept set of a cohort. 
#' 3. OrphanConcepts - A table showing the (source) concepts observed in the database that are not included in a concept set of a cohort, but maybe should be.
#' 4. TimeSeries - Boxplot and a table showing the distribution of time (in days) before and after the cohort index date (cohort start date), and the time between cohort start and end date.
#' 5. VisitContext - A table showing the relationship between the cohort start date and visits recorded in the database.
#' 6. BreakdownIndexEvents - A table showing the concepts belonging to the concept sets in the entry event definition that are observed on the index date. 
#' 7. IncidenceRate - A graph showing the incidence rate, optionally stratified by age (in 10-year bins), gender, and calendar year.
#' 8. CohortRelationship - Stacked bar graph showing the overlap between two cohorts, and a table listing several overlap statistics.
#' 9. TemporalCohortCharacterization - A table showing temporal cohort characteristics (covariates) captured at specific time intervals before or after cohort start date. 
#' 
#' @md
#' 
#' 
#' @examples
#' \dontrun{
#' library(CohortDiagnostics)
#' library(CDMConnector)
#' 
#' cohortTable <- "mycohort"
#' 
#' con <- DBI::dbConnect(duckdb::duckdb(), dbdir = CDMConnector::eunomia_dir())
#' 
#' cdm <- cdmFromCon(con, cdmSchema = "main", writeSchema = "main", cdmName = "eunomia")
#' 
#' cohortDefinitionSet <- readCohortSet(system.file("cohorts1", package = "CDMConnector"))
#' 
#' cdm <- generateCohortSet(cdm, cohortDefinitionSet, name = cohortTable)
#' 
#' executeDiagnosticsCdm(cdm = cdm,
#'  cohortDefinitionSet = cohortDefinitionSet,
#'  cohortTable = cohortTable,
#'  exportFolder = "output",
#'  minCellCount = 5)
#' }
#' 
#' @export
executeDiagnosticsCdm <- function(cdm,
                                  cohortTable,
                                  cohortDefinitionSet,
                                  exportFolder,
                                  runAnalysis = 1:9,
                                  minCellCount = 5,
                                  conceptCountsTable = NULL) {
  
  # Check inputs and set params ----
  if (!inherits(cdm, "cdm_reference")) {
    cli::cli_abort("- cdm must be a CDMConnector CDM reference object")
  }
  
  checkmate::assertIntegerish(runAnalysis, lower = -9, upper = 9, min.len = 1, any.missing = FALSE)
  
  # Data.frame of cohorts must include columns cohortId, cohortName, json, sql.
  
  if (!is.null(conceptCountsTable) && is.character(conceptCountsTable)) {
    if (conceptCountsTable %in% CDMConnector::list_tables(attr(cdm, "dbcon"), schema = attr(cdm, "write_schema"))) {
      useExternalConceptCountsTable = TRUE
    } else {
      cli::cli_abort("precomputed conceptCountsTable `{conceptCountsTable}` was not found in schema `{attr(cdm, 'write_schema')`}`")
    }
  } else {
    useExternalConceptCountsTable = FALSE
  }
  
  if (any(c("cohort_table", "GeneratedCohortSet") %in% class(cdm[[cohortTable]]))) {
    
    cohortDefinitionSet$sql <- character(nrow(cohortDefinitionSet))
    
    cohortDefinitionSet <- cohortDefinitionSet %>% 
      dplyr::mutate(
        cohortName = cohort_name, 
        sql = "",
        json = as.character(json),
        cohortId = as.numeric(cohort_definition_id),
        isSubset = FALSE)
    
    # fill in the sql column with circe SQL
    for (i in seq_len(nrow(cohortDefinitionSet))) {
      cohortJson <- cohortDefinitionSet$json[[i]]
      cohortExpression <- CirceR::cohortExpressionFromJson(expressionJson = cohortJson)
      cohortSql <- CirceR::buildCohortQuery(expression = cohortExpression,
                                            options = CirceR::createGenerateOptions(generateStats = TRUE))
      cohortDefinitionSet$sql[i] <- SqlRender::render(cohortSql, warnOnMissingParameters = FALSE)
    }
  }
  
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
  
  # Set parameters ----
  con <- attr(cdm, "dbcon") # TODO support dataframe cdms
  cdmVersion <- floor(as.numeric(CDMConnector::version(cdm)))
  cohortTable <- glue::glue("cd_cohort_{stringr::str_remove_all(uuid::UUIDgenerate(), '-')}")
  conceptCountsTable <- conceptCountsTable
  cohortDatabaseSchema <- attr(cdm, "write_schema")
  targetDialect <- CDMConnector::dbms(con)
  cohortTableNames <- list(
    cohortTable = cohortTable, 
    cohortInclusionTable = paste0(cohortTable, "_inclusion"), 
    cohortInclusionResultTable = paste0(cohortTable, "_inclusion_result"),
    cohortInclusionStatsTable = paste0(cohortTable, "_inclusion_stats"), 
    cohortSummaryStatsTable = paste0(cohortTable, "_summary_stats"), 
    cohortCensorStatsTable = paste0(cohortTable, "_censor_stats"))
  
  
  if ("write_prefix" %in% names(attr(cdm, "cdm_schema"))) {
    checkmate::assertNames(attr(cdm, "cdm_schema"), subset.of = c("catalog", "schema"), must.include = "schema")
    writePrefix <- attr(cdm, "cdm_schema")["write_prefix"]
    cdmSchema <- paste0(
      tidyr::replace_na(attr(cdm, "cdm_schema")["catalog"], ""), 
      attr(cdm, "cdm_schema")["schema"], 
      collapse = ".")
  } else {
    writePrefix <- ""
    cdmSchema <- paste0(attr(cdm, "cdm_schema"), collapse = ".")
  }
  
  cdmName <- attr(cdm, "cdm_name")
  
  exportFolder <- normalizePath(exportFolder, mustWork = FALSE)
  incrementalFolder <- normalizePath(incrementalFolder, mustWork = FALSE)
  executionTimePath <- file.path(exportFolder, "taskExecutionTimes.csv")
  fs::dir_create(exportFolder)
  fs::dir_create(incrementalFolder)
  
  # Start logging ----
  ParallelLogger::addDefaultFileLogger(file.path(exportFolder, "log.txt"))
  ParallelLogger::addDefaultErrorReportLogger(file.path(exportFolder, "errorReportR.txt"))
  on.exit(ParallelLogger::unregisterLogger("DEFAULT_FILE_LOGGER", silent = TRUE))
  on.exit(
    ParallelLogger::unregisterLogger("DEFAULT_ERRORREPORT_LOGGER", silent = TRUE),
    add = TRUE
  )
  
  start <- Sys.time()
  cli::cli_inform("Run Cohort Diagnostics started at ", start)
  
  # Database metadata ---------------------------------------------
  cli::cli_inform("Saving database metadata")
  
  database <- dplyr::tibble(
    databaseName = databaseName,
    vocabularyVersion = vocabularyVersion
  )
  
  readr::write_csv(database, file.path(exportFolder, "database.csv"))
  
  # Temporal Characterization ----
  # if (temporalCohortCharacterization %in% ) {
  #   if (is(temporalCovariateSettings, "covariateSettings")) {
  #     temporalCovariateSettings <- list(temporalCovariateSettings)
  #   }
  #   
  #   # All temporal covariate settings objects must be covariateSettings
  #   checkmate::assert_true(all(lapply(temporalCovariateSettings, class) == c("covariateSettings")), add = errorMessage)
  #   
  #   requiredCharacterisationSettings <- c(
  #     "DemographicsGender", "DemographicsAgeGroup", "DemographicsRace",
  #     "DemographicsEthnicity", "DemographicsIndexYear", "DemographicsIndexMonth",
  #     "ConditionEraGroupOverlap", "DrugEraGroupOverlap", "CharlsonIndex",
  #     "Chads2", "Chads2Vasc"
  #   )
  #   presentSettings <- temporalCovariateSettings[[1]][requiredCharacterisationSettings]
  #   if (!all(unlist(presentSettings))) {
  #     warning(
  #       "For cohort charcterization to display standardized results the following covariates must be present in your temporalCovariateSettings: \n\n",
  #       paste(requiredCharacterisationSettings, collapse = ", ")
  #     )
  #   }
  #   
  #   requiredTimeDistributionSettings <- c(
  #     "DemographicsPriorObservationTime",
  #     "DemographicsPostObservationTime",
  #     "DemographicsTimeInCohort"
  #   )
  #   
  #   presentSettings <- temporalCovariateSettings[[1]][requiredTimeDistributionSettings]
  #   if (!all(unlist(presentSettings))) {
  #     warning(
  #       "For time distributions diagnostics to display standardized results the following covariates must be present in your temporalCovariateSettings: \n\n",
  #       paste(requiredTimeDistributionSettings, collapse = ", ")
  #     )
  #   }
  #   
  #   # forcefully set ConditionEraGroupStart and drugEraGroupStart to NULL
  #   # because of known bug in FeatureExtraction. https://github.com/OHDSI/FeatureExtraction/issues/144
  #   temporalCovariateSettings[[1]]$ConditionEraGroupStart <- NULL
  #   temporalCovariateSettings[[1]]$DrugEraGroupStart <- NULL
  #   
  #   checkmate::assert_integerish(
  #     x = temporalCovariateSettings[[1]]$temporalStartDays,
  #     any.missing = FALSE,
  #     min.len = 1,
  #     add = errorMessage
  #   )
  #   checkmate::assert_integerish(
  #     x = temporalCovariateSettings[[1]]$temporalEndDays,
  #     any.missing = FALSE,
  #     min.len = 1,
  #     add = errorMessage
  #   )
  #   checkmate::reportAssertions(collection = errorMessage)
  #   
  #   # Adding required temporal windows required in results viewer
  #   requiredTemporalPairs <-
  #     list(
  #       c(-365, 0),
  #       c(-30, 0),
  #       c(-365, -31),
  #       c(-30, -1),
  #       c(0, 0),
  #       c(1, 30),
  #       c(31, 365),
  #       c(-9999, 9999)
  #     )
  #   for (p1 in requiredTemporalPairs) {
  #     found <- FALSE
  #     for (i in 1:length(temporalCovariateSettings[[1]]$temporalStartDays)) {
  #       p2 <- c(
  #         temporalCovariateSettings[[1]]$temporalStartDays[i],
  #         temporalCovariateSettings[[1]]$temporalEndDays[i]
  #       )
  #       
  #       if (p2[1] == p1[1] & p2[2] == p1[2]) {
  #         found <- TRUE
  #         break
  #       }
  #     }
  #     
  #     if (!found) {
  #       temporalCovariateSettings[[1]]$temporalStartDays <-
  #         c(temporalCovariateSettings[[1]]$temporalStartDays, p1[1])
  #       temporalCovariateSettings[[1]]$temporalEndDays <-
  #         c(temporalCovariateSettings[[1]]$temporalEndDays, p1[2])
  #     }
  #   }
  # }
  
  
  
  # CDM source information----
  if (nrow(cohortDefinitionSet) == 0) {
    cli::cli_abort("{.arg cohortDefinitionSet} is empty")
  }
  
  readr::write_csv(cohortDefinitionSet, file.path(exportFolder, "cohort.csv"))
  
  cdmDataSource <- dplyr::collect(cdm$cdm_source) %>% 
    dplyr::select(
      "source_description",
      "cdm_source_name",
      "source_release_date",
      "cdm_release_date",
      "cdm_version",
      "vocabulary_version"
    )
  
  if (nrow(cdmDataSource) == 0) {
    cli::cli_alert_warning("CDM Source table does not have any records. Metadata on CDM source will be limited.")
  }
  
  if (nrow(cdmDataSource) > 1) {
    cli::cli_alert_warning("CDM Source table has more than one record while only one is expected. This may represent an ETL convention issue. Using the max value for each column.")
    cdmDataSource <- dplyr::mutate_all(cdmDataSource, max)
  }
  
  vocabularyVersion <- cdm$vocabulary %>% 
    dplyr::filter(vocabulary_id == "None") %>% 
    dplyr::collect() %>% 
    dplyr::pull(vocabulary_version) %>%
    unique() %>% 
    paste0(collapse = ";") # in case of multiple rows. Should not happen but could.
  
  cohortDefinitionSet$checksum <- computeChecksum(cohortDefinitionSet$sql)
  
  if (incremental) {
    ParallelLogger::logDebug("Working in incremental mode.")
    recordKeepingFile <- file.path(incrementalFolder, "CreatedDiagnostics.csv")
    if (file.exists(path = recordKeepingFile)) {
      cli::cli_inform("Found existing record keeping file in incremental folder - CreatedDiagnostics.csv")
    }
  }
  
  # Observation period----
  cli::cli_inform(" - Collecting date range from Observational period table.")
  observationPeriodDateRange <- cdm$observation_period %>% 
    mutate(person_days = !!CDMConnector::datediff("observation_period_start_date", "observation_period_end_date")) %>% 
    dplyr::summarise(
      observation_period_end_date = min(.data$observation_period_start_date),
      observation_period_start_date = max(.data$observation_period_end_date),
      persons = dplyr::n_distinct(.data$person_id),
      records = dplyr::n(),
      person_days = sum(person_days)
    ) %>% 
    dplyr::collect()
  
  # Counting cohorts -----------------------------------------------------------------------
  cohortCounts <- omopgenerics::cohortCount(cdm[[cohortTableName]])
  
  if (nrow(cohortCounts) > 0) {
    instantiatedCohorts <- cohortCounts %>%
      dplyr::filter(.data$cohortEntries > 0) %>%
      dplyr::pull(.data$cohortId)
    cli::cli_inform(
      sprintf(
        "Found %s of %s (%1.2f%%) submitted cohorts instantiated. ",
        length(instantiatedCohorts),
        nrow(cohortDefinitionSet),
        100 * (length(instantiatedCohorts) / nrow(cohortDefinitionSet))
      ),
      "Beginning cohort diagnostics for instantiated cohorts. "
    )
  } else {
    stop("All cohorts were either not instantiated or all have 0 records.")
  }
  
  # Inclusion statistics --------------------------------------------------
  readr::write_csv(
    CDMConnector::cohort_attrition(cdm[[cohortTableName]]),
    file.path(exportFolder, "cohort_attrition.csv")
  )
  
  # Concept set diagnostics -----------------------------------------------
  cli::cli_inform("Creating concept ID table for tracking concepts used in diagnostics")
  
  # unnest all the things
  df <- cohort_set %>% 
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
                  "concept_id", 
                  is_exclude = "isexcluded", 
                  include_descendants = "includedescendants")
  
  # upload concept data to the database 
  tempName <- paste0("tmp", as.integer(Sys.time()))
  
  DBI::dbWriteTable(con,
                    name = inSchema(attr(cdm, "write_schema"), tempName, dbms = CDMConnector::dbms(con)),
                    value = df,
                    overwrite = TRUE)
  
  if (any(df$include_descendants)) {
    CDMConnector::assert_tables(cdm, "concept_ancestor")
  }
  
  # realize full list of concepts in the database----
  concepts <- dplyr::tbl(attr(cdm, "db_con"), 
                         inSchema(attr(cdm, "write_schema"), tempName, dbms = CDMConnector::dbms(con))) %>%
    dplyr::rename_all(tolower) %>%
    { if (any(df$include_descendants)) {
      dplyr::filter(., .data$include_descendants) %>%
        dplyr::inner_join(cdm$concept_ancestor, by = c("concept_id" = "ancestor_concept_id")) %>%
        dplyr::select(
          "cohort_definition_id", "cohort_name",
          "concept_id" = "descendant_concept_id", "is_excluded",
          dplyr::any_of(c("limit", "prior_observation", "future_observation", "end"))
        ) %>%
        dplyr::union_all(
          dplyr::tbl(con, inSchema(cdmWriteSchema(cdm), tempName, dbms = dbms(con))) %>%
            dplyr::select(dplyr::any_of(c(
              "cohort_definition_id", "cohort_name", "concept_id", "is_excluded",
              "limit", "prior_observation", "future_observation", "end"
            )))
        )
    } else . } %>% # only handle descendants if we need to
    dplyr::filter(.data$is_excluded == FALSE) %>%
    # Note that concepts that are not in the vocab will be silently ignored
    dplyr::inner_join(dplyr::select(cdm$concept, "concept_id", "domain_id"), by = "concept_id") %>%
    dplyr::select(
      "cohort_definition_id", "cohort_name", "concept_id", "domain_id",
      dplyr::any_of(c("limit", "prior_observation", "future_observation", "end"))
    ) %>%
    dplyr::distinct() %>%
    CDMConnector::computeQuery(
      name = paste0(tempName, "_concepts"),
      temporary = FALSE,
      overwrite = TRUE,
      schema = attr(cdm, "write_schema"))
  
  DBI::dbRemoveTable(con, name = inSchema(cdmWriteSchema(cdm), tempName, dbms = dbms(con)))
  
  on.exit({
    if (DBI::dbIsValid(con))
      DBI::dbRemoveTable(con, name = inSchema(cdmWriteSchema(cdm), paste0(tempName, "_concepts"), dbms = dbms(con)))
  },
  add = TRUE
  )
  
  if (runIncludedSourceConcepts || runOrphanConcepts || runBreakdownIndexEvents) {
    
    cli::cli_inform("Starting concept set diagnostics")
    startConceptSetDiagnostics <- Sys.time()
    subset <- dplyr::tibble()
    
    # don't necessisarily need to run this. also consider if achilles tables are available.
    # if ((runIncludedSourceConcepts && nrow(subsetIncluded) > 0) ||
    #     (runOrphanConcepts && nrow(subsetOrphans) > 0)) {
    #   if (!useExternalConceptCountsTable) {
    cli::cli_inform("Count all concepts in database")
    
    tbls <- c("condition_occurrence", "procedure_occurrence", "observation", "measurement")
    
    all_concept_counts <- purrr::map(tbl, function(tbl_name) {
      domain <- stringr::str_remove_all(tbl_name, "_occurrence")
      
      q1 <- cdm[[tbl_name]] %>% 
        dplyr::rename(concept_id = !!paste0(domain, "_concept_id")) %>% 
        group_by(.data$concept_id) %>% 
        summarize(concept_subjects = dplyr::n_distinct(.data$person_id),
                  concept_count = dplyr::n()) %>% 
        dplyr::mutate(source_concept_id = 0L)
      
      q2 <- cdm[[tbl_name]] %>% 
        dplyr::rename(concept_id = !!paste0(domain, "_source_concept_id")) %>% 
        group_by(.data$concept_id) %>% 
        summarize(concept_subjects = dplyr::n_distinct(.data$person_id),
                  concept_count = dplyr::n()) %>% 
        dplyr::mutate(source_concept_id = 1L)
      
      dplyr::union_all(q1, q2)
    }) %>% 
      purrr::reduce(dplyr::union_all) %>% 
      CDMConnector::computeQuery(
        name = paste0(tempName, "_all_concept_counts"),
        temporary = FALSE,
        overwrite = TRUE,
        schema = attr(cdm, "write_schema")
      )
    
    if (runIncludedSourceConcepts) {
      # Included concepts ------------------------------------------------------------------
      cli::cli_inform("Fetching included source concepts")
      
      
      if (nrow(subsetIncluded) > 0) {
        start <- Sys.time()
        sql <- loadRenderTranslateSql(
          "CohortSourceCodes.sql",
          packageName = utils::packageName(),
          dbms = getDbms(con),
          tempEmulationSchema = tempEmulationSchema,
          cdm_database_schema = cdmDatabaseSchema,
          instantiated_concept_sets = "#inst_concept_sets",
          include_source_concept_table = "#inc_src_concepts",
          by_month = FALSE
        )
        
        executeSql(con = con, sql = sql)
        counts <-
          renderTranslateQuerySql(
            con = con,
            sql = "SELECT * FROM @include_source_concept_table;",
            include_source_concept_table = "#inc_src_concepts",
            tempEmulationSchema = tempEmulationSchema,
            snakeCaseToCamelCase = TRUE
          ) %>%
          dplyr::tibble()
        
        counts <- counts %>%
          dplyr::distinct() %>%
          dplyr::rename("uniqueConceptSetId" = "conceptSetId") %>%
          dplyr::inner_join(
            conceptSets %>% dplyr::select(
              "uniqueConceptSetId",
              "cohortId",
              "conceptSetId"
            ) %>% dplyr::distinct(),
            by = "uniqueConceptSetId",
            relationship = "many-to-many"
          ) %>%
          dplyr::select(-"uniqueConceptSetId") %>%
          dplyr::mutate(databaseId = !!databaseId) %>%
          dplyr::relocate(
            "databaseId",
            "cohortId",
            "conceptSetId",
            "conceptId"
          ) %>%
          dplyr::distinct()
        
        counts <- counts %>%
          dplyr::group_by(
            .data$databaseId,
            .data$cohortId,
            .data$conceptSetId,
            .data$conceptId,
            .data$sourceConceptId
          ) %>%
          dplyr::summarise(
            conceptCount = max(.data$conceptCount),
            conceptSubjects = max(.data$conceptSubjects)
          ) %>%
          dplyr::ungroup()
        
        counts <- makeDataExportable(
          x = counts,
          tableName = "included_source_concept",
          minCellCount = minCellCount,
          databaseId = databaseId
        )
        
        writeToCsv(
          counts,
          file.path(exportFolder, "included_source_concept.csv"),
          incremental = incremental,
          cohortId = subsetIncluded$cohortId
        )
        
        if (!is.null(conceptIdTable)) {
          sql <- "INSERT INTO @concept_id_table (concept_id)
                  SELECT DISTINCT concept_id
                  FROM @include_source_concept_table;

                  INSERT INTO @concept_id_table (concept_id)
                  SELECT DISTINCT source_concept_id
                  FROM @include_source_concept_table;"
          renderTranslateExecuteSql(
            con = con,
            sql = sql,
            tempEmulationSchema = tempEmulationSchema,
            concept_id_table = conceptIdTable,
            include_source_concept_table = "#inc_src_concepts"
          )
        }
        sql <-
          "TRUNCATE TABLE @include_source_concept_table;\nDROP TABLE @include_source_concept_table;"
        renderTranslateExecuteSql(
          con = con,
          sql = sql,
          tempEmulationSchema = tempEmulationSchema,
          include_source_concept_table = "#inc_src_concepts"
        )
        
        delta <- Sys.time() - start
        cli::cli_inform(paste(
          "Finding source codes took",
          signif(delta, 3),
          attr(delta, "units")
        ))
        
        
        # Time series ----------------------------------------------------------------------
        if (runTimeSeries) {
          timeExecution(
            exportFolder,
            "executeTimeSeriesDiagnostics",
            cohortIds,
            parent = "executeDiagnostics",
            expr = {
              executeTimeSeriesDiagnostics(
                con = con,
                tempEmulationSchema = tempEmulationSchema,
                cdmSchema = cdmSchema,
                cohortDatabaseSchema = cohortDatabaseSchema,
                cohortTable = cohortTable,
                cohortDefinitionSet = cohortDefinitionSet,
                cdmName = cdmName,
                exportFolder = exportFolder,
                minCellCount = minCellCount,
                instantiatedCohorts = instantiatedCohorts,
                incremental = incremental,
                recordKeepingFile = recordKeepingFile,
                observationPeriodDateRange = observationPeriodDateRange
              )
            }
          )
        }
        
        
        # Visit context ----------------------------------------------------------------------------
        if (runVisitContext) {
          timeExecution(
            exportFolder,
            "executeVisitContextDiagnostics",
            cohortIds,
            parent = "executeDiagnostics",
            expr = {
              executeVisitContextDiagnostics(
                con = con,
                tempEmulationSchema = tempEmulationSchema,
                cdmSchema = cdmSchema,
                cohortDatabaseSchema = cohortDatabaseSchema,
                cohortTable = cohortTable,
                cdmVersion = cdmVersion,
                cdmName = cdmName,
                exportFolder = exportFolder,
                minCellCount = minCellCount,
                cohorts = cohortDefinitionSet,
                instantiatedCohorts = instantiatedCohorts,
                recordKeepingFile = recordKeepingFile,
                incremental = incremental
              )
            }
          )
        }
        
        # Incidence rates --------------------------------------------------------------------------------------
        if (runIncidenceRate) {
          timeExecution(
            exportFolder,
            "computeIncidenceRates",
            cohortIds,
            parent = "executeDiagnostics",
            expr = {
              computeIncidenceRates(
                con = con,
                tempEmulationSchema = tempEmulationSchema,
                cdmSchema = cdmSchema,
                cohortDatabaseSchema = cohortDatabaseSchema,
                cohortTable = cohortTable,
                cdmName = cdmName,
                exportFolder = exportFolder,
                minCellCount = minCellCount,
                cohorts = cohortDefinitionSet,
                washoutPeriod = irWashoutPeriod,
                instantiatedCohorts = instantiatedCohorts,
                recordKeepingFile = recordKeepingFile,
                incremental = incremental
              )
            }
          )
        }
        
        # Cohort relationship ---------------------------------------------------------------------------------
        if (runCohortRelationship) {
          timeExecution(
            exportFolder,
            "executeCohortRelationshipDiagnostics",
            cohortIds,
            parent = "executeDiagnostics",
            expr = {
              executeCohortRelationshipDiagnostics(
                con = con,
                cdmName = cdmName,
                exportFolder = exportFolder,
                cohortDatabaseSchema = cohortDatabaseSchema,
                cdmSchema = cdmSchema,
                tempEmulationSchema = tempEmulationSchema,
                cohortTable = cohortTable,
                cohortDefinitionSet = cohortDefinitionSet,
                temporalCovariateSettings = temporalCovariateSettings[[1]],
                minCellCount = minCellCount,
                recordKeepingFile = recordKeepingFile,
                incremental = incremental
              )
            }
          )
        }
        
        # Temporal Cohort characterization ---------------------------------------------------------------
        if (runTemporalCohortCharacterization) {
          timeExecution(
            exportFolder,
            "executeCohortCharacterization",
            cohortIds,
            parent = "executeDiagnostics",
            expr = {
              executeCohortCharacterization(
                con = con,
                cdmName = cdmName,
                exportFolder = exportFolder,
                cdmSchema = cdmSchema,
                cohortDatabaseSchema = cohortDatabaseSchema,
                cohortTable = cohortTable,
                covariateSettings = temporalCovariateSettings,
                tempEmulationSchema = tempEmulationSchema,
                cdmVersion = cdmVersion,
                cohorts = cohortDefinitionSet,
                cohortCounts = cohortCounts,
                minCellCount = minCellCount,
                instantiatedCohorts = instantiatedCohorts,
                incremental = incremental,
                recordKeepingFile = recordKeepingFile,
                task = "runTemporalCohortCharacterization",
                jobName = "Temporal Cohort characterization",
                covariateValueFileName = file.path(exportFolder, "temporal_covariate_value.csv"),
                covariateValueContFileName = file.path(exportFolder, "temporal_covariate_value_dist.csv"),
                covariateRefFileName = file.path(exportFolder, "temporal_covariate_ref.csv"),
                analysisRefFileName = file.path(exportFolder, "temporal_analysis_ref.csv"),
                timeRefFileName = file.path(exportFolder, "temporal_time_ref.csv"),
                minCharacterizationMean = minCharacterizationMean
              )
            }
          )
        }
        
        # Store information from the vocabulary on the concepts used -------------------------
        timeExecution(
          exportFolder,
          "exportConceptInformation",
          parent = "executeDiagnostics",
          expr = {
            exportConceptInformation(
              con = con,
              cdmSchema = cdmSchema,
              tempEmulationSchema = tempEmulationSchema,
              conceptIdTable = "#concept_ids",
              incremental = incremental,
              exportFolder = exportFolder
            )
          }
        )
        # Delete unique concept ID table ---------------------------------
        cli::cli_inform("Deleting concept ID table")
        timeExecution(
          exportFolder,
          "DeleteConceptIdTable",
          parent = "executeDiagnostics",
          expr = {
            sql <- "TRUNCATE TABLE @table;\nDROP TABLE @table;"
            renderTranslateExecuteSql(
              con = con,
              sql = sql,
              tempEmulationSchema = tempEmulationSchema,
              table = "#concept_ids"
            )
          }
        )
        
        # Writing metadata file
        cli::cli_inform("Retrieving metadata information and writing metadata")
        
        packageName <- utils::packageName()
        packageVersion <- if (!methods::getPackageName() == ".GlobalEnv") {
          as.character(utils::packageVersion(packageName))
        } else {
          ""
        }
        delta <- Sys.time() - start
        
        timeExecution(
          exportFolder = exportFolder,
          taskName = "executeDiagnostics",
          parent = NULL,
          cohortIds = NULL,
          start = start,
          execTime = delta
        )
        
        variableField <- c(
          "timeZone",
          # 1
          "runTime",
          # 2
          "runTimeUnits",
          # 3
          "packageDependencySnapShotJson",
          # 4
          "argumentsAtDiagnosticsInitiationJson",
          # 5
          "rversion",
          # 6
          "currentPackage",
          # 7
          "currentPackageVersion",
          # 8
          "sourceDescription",
          # 9
          "cdmSourceName",
          # 10
          "sourceReleaseDate",
          # 11
          "cdmVersion",
          # 12
          "cdmReleaseDate",
          # 13
          "vocabularyVersion",
          # 14
          "datasourceName",
          # 15
          "datasourceDescription",
          # 16
          "vocabularyVersionCdm",
          # 17
          "observationPeriodMinDate",
          # 18
          "observationPeriodMaxDate",
          # 19
          "personsInDatasource",
          # 20
          "recordsInDatasource",
          # 21
          "personDaysInDatasource" # 22
        )
        valueField <- c(
          as.character(Sys.timezone()),
          # 1
          as.character(as.numeric(
            x = delta, units = attr(delta, "units")
          )),
          # 2
          as.character(attr(delta, "units")),
          # 3
          "{}",
          # 4
          callingArgsJson,
          # 5
          as.character(R.Version()$version.string),
          # 6
          as.character(nullToEmpty(packageName)),
          # 7
          as.character(nullToEmpty(packageVersion)),
          # 8
          as.character(nullToEmpty(
            cdmSourceInformation$sourceDescription
          )),
          # 9
          as.character(nullToEmpty(cdmSourceInformation$cdmSourceName)),
          # 10
          as.character(nullToEmpty(
            cdmSourceInformation$sourceReleaseDate
          )),
          # 11
          as.character(nullToEmpty(cdmSourceInformation$cdmVersion)),
          # 12
          as.character(nullToEmpty(cdmSourceInformation$cdmReleaseDate)),
          # 13
          as.character(nullToEmpty(
            cdmSourceInformation$vocabularyVersion
          )),
          # 14
          as.character(cdmName),
          # 15
          as.character(databaseDescription),
          # 16
          as.character(nullToEmpty(cdmSourceInformation$vocabularyVersion)),
          # 17
          as.character(observationPeriodDateRange$observationPeriodMinDate),
          # 18
          as.character(observationPeriodDateRange$observationPeriodMaxDate),
          # 19
          as.character(observationPeriodDateRange$persons),
          # 20
          as.character(observationPeriodDateRange$records),
          # 21
          as.character(observationPeriodDateRange$personDays) # 22
        )
        metadata <- dplyr::tibble(
          cdmName = as.character(!!cdmName),
          startTime = paste0("TM_", as.character(start)),
          variableField = variableField,
          valueField = valueField
        )
        metadata <- makeDataExportable(
          x = metadata,
          tableName = "metadata",
          minCellCount = minCellCount,
          cdmName = cdmName
        )
        writeToCsv(
          data = metadata,
          fileName = file.path(exportFolder, "metadata.csv"),
          incremental = TRUE,
          start_time = as.character(start)
        )
        
        # Add all to zip file -------------------------------------------------------------------------------
        timeExecution(
          exportFolder,
          "writeResultsZip",
          NULL,
          parent = "executeDiagnostics",
          expr = {
            writeResultsZip(exportFolder, cdmName)
          }
        )
        
        cli::cli_inform(
          "Computing all diagnostics took ",
          signif(delta, 3),
          " ",
          attr(delta, "units")
        )
        
        
      }
      