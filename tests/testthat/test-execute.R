
test_that("execute diagnostics works", {
  skip()
  con <- DBI::dbConnect(duckdb::duckdb(CDMConnector::eunomia_dir("synthea-covid19-10k")))
  cdm <- CDMConnector::cdm_from_con(con, "main", "main")
  
  cohort_set <- read_cohort_set(system.file("cohorts2", package = "CDMConnector"))
  
  executeDiagnosticsCdm(cdm, cohort_set, exportFolder = here::here("export"))
  
})






