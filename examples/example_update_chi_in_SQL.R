##### Sample code for updating indicator data in SQL ######
##### Abby Schachter 
##### 9/30/2019


library(odbc) # Read to and write from SQL

# connect to SQL servers
db_extract51 <- dbConnect(odbc(), "PHExtractStore51")
db_extract50 <- dbConnect(odbc(), "PHExtractStore50")



## Output from analyses should include tables for Results, Metadata, Titles, and Table of Contents (ToC) 
## in the Tableau-ready output format



## UPDATE RESULTS TABLE WITH NEW INDICATORS (edit table names and indicators as needed)
dbGetQuery(db_extract51,
           
           "DELETE APDE_WIP.hys_results 
           WHERE indicator_key IN 
           ('tanytob_nv1', 't_smk30d', 'ecig_vape', 'marij30day', 't_smkany')")


dbWriteTable(db_extract51,
             name = DBI::Id(schema = "APDE_WIP", table = "hys_results"),
             value = hys_chi_tobacco_mj,
             overwrite = F, append = T)


#### UPDATE METADATA ####

### overwrite metadata table
### use overwrite = T if rewriting the entire table, otherwise use append = T if updating specific rows

dbWriteTable(db_extract51,
             DBI::Id(schema = "APDE_WIP", table = "hys_metadata"),
             hys_meta_join, overwrite = T, append = F)


#### UPDATE TABLE OF CONTENTS ####

### Drop rows in current TOC
dbGetQuery(db_extract51,
           "DELETE FROM APDE_WIP.indicators_toc
           WHERE indicator_key IN 
           ('tanytob_nv1', 't_smk30d', 'ecig_vape', 'marij30day', 't_smkany')")

### Add new rows
dbWriteTable(db_extract51,
             name = DBI::Id(schema = "APDE_WIP", table = "indicators_toc"),
             value = hys_toc,
             overwrite = F, append = T)


#### UPDATE TITLES ####

### Drop rows in current titles
dbGetQuery(db_extract51,
           "DELETE FROM APDE_WIP.indicators_titles
           WHERE indicator_key IN 
           ('tanytob_nv1', 't_smk30d', 'ecig_vape', 'marij30day', 't_smkany')")

### Add new rows
dbWriteTable(db_extract51,
             name = DBI::Id(schema = "APDE_WIP", table = "indicators_titles"),
             value = hys_titles,
             overwrite = F, append = T)


#### COPY DATA FROM SQL 51 (Development) to SQL 50 (Production) ####
# Only do this once data have passed QA!
# Alternativlely, use "S:\WORK\CHI Visualizations\Table of contents\Update Tableau SQL prod server.sql" as template


# create function to delete existing table in SQL50 and write new table from SQL51
sql_update <- function(to_schema, from_schema, table_name, con) {
  sql_delete <- glue::glue_sql("DELETE FROM KCITSQLPRPDBM50_APDE.PHExtractStore.{`to_schema`}.{`table_name`}", 
                               .con = con)
  
  dbGetQuery(con, sql_delete)
  
  sql_load <- glue::glue_sql("INSERT INTO KCITSQLPRPDBM50_APDE.PHExtractStore.{`to_schema`}.{`table_name`} WITH (TABLOCK) 
                                SELECT * FROM 
                                {`from_schema`}.{`table_name`}", 
                             .con = con)
  
  dbGetQuery(con, sql_load)
}

# create list of tables to be updated. indicators titles and toc should stay the same, results and metadata may change based on data source
tables <- list("hys_results", "hys_metadata", "indicators_titles", "indicators_toc") 

# run function on list of tables
lapply(tables, sql_update, from_schema = "APDE_WIP", to_schema = "APDE", con = db_extract51)
