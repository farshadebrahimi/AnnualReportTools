
# MARS Annual Report Stats
# By: Farshad Ebrahimi, Last modified: 08/11/2022

library(shiny)
library(reactable)
library(odbc)
library(DBI)
library(lubridate)
library(tidyverse)
library(dplyr)
library(reactablefmtr)
library(shinydashboard)

## Database Connection to pg9
#connection 
con <- dbConnect(odbc::odbc(), dsn = "mars_testing", uid = Sys.getenv("shiny_uid_pg9_admin"), pwd = Sys.getenv("shiny_pwd_pg9_admin"))
#con <- odbc::dbConnect(odbc::odbc(), "mars_testing")

#FY choices for input
years_vector <- as.character(12:99)

# Define UI 
ui <-dashboardPage(skin = 'blue',
                dashboardHeader(title = "MARS Annual Report Statistics", titleWidth = 300),
                dashboardSidebar(
                  selectizeInput(
                    'fy', label = 'Fiscal Year', choices = years_vector, selected = "22",
                    options = list(maxOptions = 30)
                  ), width = 300
                ),
                dashboardBody(
                  fluidPage(
                    strong("Table 3-1: Summary of Post-Construction CWL Monitoring of Public SMPs"),
                    reactableOutput("Table 3-1"),
                    strong("Table 3-2: Post-Construction CWL Monitoring of Public SMPs Listed by Type "),
                    reactableOutput("Table 3-2"),
                    strong("Table 3-3: Post-Construction SRTs performed on Public Systems"),
                    reactableOutput("Table 3-3"),
                    strong("Table 3-4: Public Systems with Post-Construction SRTs Performed"),
                    reactableOutput("Table 3-4"),
                    strong("Table 3-5: Public Systems with CETs Administered"),
                    reactableOutput("Table 3-5"),
                    strong("Table 3-6: Public Systems with Infiltration Testing Administered"),
                    reactableOutput("Table 3-6"),
                    strong("Table 3-7: Construction-Phase SRTs Performed on Public Systems"),
                    reactableOutput("Table 3-7"),
                    strong("Table 3-8: Public Systems with Construction-Phase SRTs Performed"),
                    reactableOutput("Table 3-8"),
                    strong("Table 3-9: Groundwater Monitoring for Public GSI"),
                    reactableOutput("Table 3-9"),
                    strong("Table 3-10: Summary of Post-Construction CWL Monitoring of Private SMPs"),
                    reactableOutput("Table 3-10"),
                    strong("Table 3-11: Post-Construction CWL-Monitoring of Private SMPs Listed by Type "),
                    reactableOutput("Table 3-11"),
                    strong("Table 3-12: Post-Construction SRTs performed on Private Systems"),
                    reactableOutput("Table 3-12"),
                    strong("Table 3-13: Private SMPs with Post-Construction SRTs Performed"),
                    reactableOutput("Table 3-13"),
                    strong("Table 3-14: Private Systems with CETs Administered"),
                    reactableOutput("Table 3-14")
                  )
                  
                )
  )
  
# backend
server <- function(input, output) {
  
  
    #reactive FY start and END
    FYSTART_reactive <- reactive({
    fystart_string <-"20%s-07-01 00:00:00"
    FYSTART <- paste(sprintf(fystart_string, as.character(as.numeric(input$fy)-1)),collapse="")
    return(FYSTART)
    })
    
    FYEND_reactive <- reactive({
    fyend_string <-"20%s-06-30 11:59:59"
    FYEND <- paste(sprintf(fyend_string, input$fy),collapse="")
    #reset the db connection
    con <- dbConnect(odbc::odbc(), dsn = "mars_testing", uid = Sys.getenv("shiny_uid_pg9_admin"), pwd = Sys.getenv("shiny_pwd_pg9_admin"))
    return(FYEND)
    })
    
    
    #Reactive table poplutions here-all outputs must be reactive dataframes
    
    table_31 <- reactive({
      
      ## queries to gets stats
      #Section 3.1: Post-Construction GSI Monitoring + Testing
      #Section 3.1.1: CWL Monitoring
      #Sensors deployed this fiscal year
      #In the text, and in table 3.1
      
      sql_string_1 <- "select count(*) from fieldwork.deployment_full_cwl
                                            where (collection_dtime_est > '%s' OR collection_dtime_est is null)
                                            and deployment_dtime_est < '%s'
                                            and public = TRUE"
      table_3_1_public_sensors_deployed_postcon <- dbGetQuery(con, paste(sprintf(sql_string_1, FYSTART_reactive(), FYEND_reactive()),collapse="")) 
      
      ##Public systems monitored this fiscal year
      #In the text, and in table 3.1
      sql_string_2 <- "select count(distinct smp_to_system(d.smp_id)) from fieldwork.deployment_full_cwl d
                                            where deployment_dtime_est >= '%s'
                                            and (collection_dtime_est <= '%s'
                                            	or collection_dtime_est is null) 
                                            and d.public = true"
      table_3_1_public_systems_monitored <- dbGetQuery(con, paste(sprintf(sql_string_2, FYSTART_reactive(), FYEND_reactive()),collapse="")) 
      
      
      #Newly monitored systems this fiscal year
      #In the text, and in table 3.1
      sql_string_3 <- "select count(distinct smp_to_system(newdeployments.smp_id)) FROM 
                                          	(select d.smp_id FROM fieldwork.deployment_full_cwl d 
                                               group BY d.smp_id, d.public
                                               having min(d.deployment_dtime_est) > '%s'
                                               and min(d.deployment_dtime_est) <= '%s'
                                               and d.public = true) newdeployments"
      table_3_1_public_new_systems_monitored<- dbGetQuery(con, paste(sprintf(sql_string_3, FYSTART_reactive(), FYEND_reactive()),collapse=""))
      
      
      #Sensors deployed to date
      sql_string_4 <- "select count(*) from fieldwork.deployment_full_cwl
                                                where deployment_dtime_est <= '%s'
                                                and public = TRUE"
      
      table_3_1_public_sensors_deployed_todate <- dbGetQuery(con, paste(sprintf(sql_string_4,FYEND_reactive()),collapse=""))
      
      #Public systems monitored to date
      #In the text, and in table 3.1
      sql_string_5 <- "select count(distinct smp_to_system(d.smp_id)) from fieldwork.deployment_full_cwl d
                                                where deployment_dtime_est <= '%s'
                                                and d.public = true"
      table_3_1_public_systems_monitored_todate <- dbGetQuery(con, paste(sprintf(sql_string_5,FYEND_reactive()),collapse=""))
      
      
      `This Fiscal Year`<- data.frame(c(pull(table_3_1_public_sensors_deployed_postcon),pull(table_3_1_public_systems_monitored),pull(table_3_1_public_new_systems_monitored)))
      `To Date`<-data.frame(c(pull(table_3_1_public_sensors_deployed_todate),pull(table_3_1_public_systems_monitored_todate),NA))
       table_3_1 <- bind_cols(`This Fiscal Year`,`To Date`)
       colnames(table_3_1)<- c("This Fiscal Year","To Date")
       rownames(table_3_1)<-c("Sensors Deployment","Systems","Systems Newly Monitored")
      
    
      
      return(table_3_1)
      
      
    })
    
    table_32 <- reactive({
    
      #Monitored public smps by type, to date
      #Table 3-2 first column
      sql_string_6 <- "select sfc.asset_type, count(distinct(d.smp_id)), d.public from
                                                fieldwork.deployment_full_cwl d
                                                left join public.smpid_facilityid_componentid sfc on d.smp_id = sfc.smp_id
                                                where sfc.component_id is null
                                                and d.smp_id is not null
                                                and d.deployment_dtime_est < '%s'
                                                and d.public = true
                                                group by sfc.asset_type, d.public"
      
      table_3_2_public_smp_bytype_todate <- dbGetQuery(con, paste(sprintf(sql_string_6,FYEND_reactive()),collapse=""))
      
      #CAPIT statuses indicating constructed systems are Jillian Simmons's best recommendation
      #Table 3-2 second column
      sql_string_7 <- "select count(*), smp_smptype from greenit_smpbestdata g where g.smp_notbuiltretired is null 
                                            and (g.capit_status = 'Closed' 
                                            or g.capit_status = 'Construction-Substantially Complete' 
                                            or g.capit_status = 'Construction-Contract Closed') 
                                            group by smp_smptype"
      table_3_2_public_constructed_systems_total <- dbGetQuery(con,sql_string_7)

      table_3_2_public_smp_bytype_todate <- table_3_2_public_smp_bytype_todate %>%
        select(`SMP Type` = asset_type,`Monitored SMPs`=count) 
      table_3_2_public_smp_bytype_todate[table_3_2_public_smp_bytype_todate[,"SMP Type"] == "Trench",1] <- "Infiltration/Storage Trench"
      
      
      table_3_2_public_constructed_systems_total <- table_3_2_public_constructed_systems_total %>%
        select(`SMP Type` = smp_smptype,`Total Constructed Public SMPs`=count) 
      table_3_2_public_constructed_systems_total[table_3_2_public_constructed_systems_total[,"SMP Type"] == "Pervious Paving",1] <- "Permeable Pavement"
      table_3_2 <- table_3_2_public_constructed_systems_total %>% 
        left_join(table_3_2_public_smp_bytype_todate, by="SMP Type")
      table_3_2<-table_3_2[,c(1,3,2)]
      table_3_2[is.na(table_3_2)] <-  0
      
      return(table_3_2)
      
    })
    
    
    table_33 <- reactive({
      
      #Section 3.1.2: SRT Testing
      #Table 3-3
      #Post-construction SRTs performed on Public Systems
      #Also appears in the 3.1.2 opening paragraph
      #First column: This fiscal year
      
      sql_string_8 <- "select count(*), type from fieldwork.srt_full 
                                      where test_date >= '%s'
                                      and test_date <= '%s'
                                      and phase = 'Post-Construction'
                                      and public = TRUE
                                      group by type"
      table_3_3_public_system_postcon_srt <-dbGetQuery(con, paste(sprintf(sql_string_8, FYSTART_reactive(), FYEND_reactive()),collapse=""))
      
      
      #Post-construction SRTs performed on Public Systems TO DATE
      #Second column: The above, to date
      
      sql_string_9 <-"select count(*), type from fieldwork.srt_full 
                                                        where test_date <= '%s'
                                                        and phase = 'Post-Construction'
                                                        and public = TRUE
                                                        group by type"
      
      table_3_3_public_system_postcon_srt_todate <- dbGetQuery(con, paste(sprintf(sql_string_9,FYEND_reactive()),collapse=""))
      
      
      
      table_3_3 <- table_3_3_public_system_postcon_srt_todate %>%
        left_join(table_3_3_public_system_postcon_srt, by="type") %>%
        select(`SRT Type`=type,`This Fiscal Year`=count.y, `To Date`=count.x)
      table_3_3[is.na(table_3_3)] <-  0
      
      return(table_3_3)
      
    })
    
    table_34 <- reactive({
    
      #Table 3.4: Public Systems with Post-Construction SRTs Performed
      #Note: This is a count of systems, not SMPs. The table title and caption for the table in the FY21 annual report implies that it is counting SMPs.
      #The numbers in the old table are systems, not SMPs, and were generated by this query
      #I suggest we rename the table to be counting systems, and remove the caption
      #We could just as easily count SMPs, by replacing the count(distinct(srt.system_id)) with count(distinct(sfc.smp_id))
      
      #Column 1: Current fiscal year
      sql_string_10 <-"select sfc.asset_type, count(distinct(srt.system_id))
                                                  from fieldwork.srt_full srt
                                                  left join smpid_facilityid_componentid sfc on srt.system_id = sfc.system_id
                                                  where sfc.component_id is null
                                                  and test_date >= '%s'
                                                  and test_date <= '%s'
                                                  and phase = 'Post-Construction'
                                                  and public = TRUE
                                                  group by sfc.asset_type"
      
      table_3_4_public_systems_postcon_srt <-dbGetQuery(con, paste(sprintf(sql_string_10, FYSTART_reactive(), FYEND_reactive()),collapse=""))
      
      #Public Systems with Post-Construction SRTs Performed TO DATE
      #Column 2: 
      sql_string_11 <-"select sfc.asset_type, count(distinct(srt.system_id))
                                                  from fieldwork.srt_full srt
                                                  left join smpid_facilityid_componentid sfc on srt.system_id = sfc.system_id
                                                  where sfc.component_id is null
                                                  and test_date <= '%s'
                                                  and phase = 'Post-Construction'
                                                  and public = TRUE
                                                  group by sfc.asset_type"
      table_3_4_public_systems_postcon_srt_todate <-dbGetQuery(con, paste(sprintf(sql_string_11, FYEND_reactive()),collapse=""))
      
      
      table_3_4 <- table_3_4_public_systems_postcon_srt_todate %>%
        left_join(table_3_4_public_systems_postcon_srt, by="asset_type")
      table_3_4 <- table_3_4[,c(1,3,2)]
      colnames(table_3_4) <- c("SMP Type","This Fiscal Year","To Date")
      table_3_4[is.na(table_3_4)] <-  0
      
      return(table_3_4)
      
    
    })
    
    table_35 <- reactive({
      
      #Section 3.1.3: CET Testing
      #Table 3.5 and the 3.1.3 paragraph
      #Column 1: Public systems with CET this fiscal year
      
      sql_string_12 <-"select count(distinct system_id) 
                                      from fieldwork.capture_efficiency_full 
                                      where phase = 'Post-Construction'
                                      and test_date >= '%s'
                                      and test_date <= '%s'
                                      and public = TRUE"
      table_3_5_public_systems_cet <- dbGetQuery(con, paste(sprintf(sql_string_12, FYSTART_reactive(), FYEND_reactive()),collapse=""))
      
      #Public systems with CET this fiscal year TO DATE
      #Column 2: 
      
      sql_string_13 <-"select count(distinct system_id) 
                                      from fieldwork.capture_efficiency_full 
                                      where phase = 'Post-Construction'
                                      and test_date <= '%s'
                                      and public = TRUE"
      table_3_5_public_systems_cet_todate <- dbGetQuery(con, paste(sprintf(sql_string_13,FYEND_reactive()),collapse=""))
      
      table_3_5_public_systems_cet["To Date"] <-table_3_5_public_systems_cet_todate
      table_3_5 <-table_3_5_public_systems_cet
      colnames(table_3_5) <- c("This Fiscal Year","To Date")
      rownames(table_3_5) <- "Number of Systems with CETs Administered"
      
  
     return(table_3_5) 
    })
    
    table_36 <- reactive({
      #Section 3.1.4: Porous Pavement and PPSIRT
      #Table 3.6 and in the 3.1.4 paragraph
      #Column 1: Systems with PP/PPSIRT in this fiscal year
      
      
      sql_string_14 <-"select count(distinct smp_to_system(smp_id))
                                      from fieldwork.porous_pavement_full
                                      where test_date >= '%s'
                                      and test_date <= '%s'
                                      and public = TRUE"
      table_3_6_systems_pp_ppsirt <- dbGetQuery(con, paste(sprintf(sql_string_14, FYSTART_reactive(), FYEND_reactive()),collapse=""))
      
      #Systems with PP/PPSIRT in this fiscal year TO DATE
      #column 2
      sql_string_15 <-"select count(distinct smp_to_system(smp_id))
                                          from fieldwork.porous_pavement_full
                                          where test_date <= '%s'
                                          and public = TRUE"
      table_3_6_systems_pp_ppsirt_todate <- dbGetQuery(con, paste(sprintf(sql_string_15, FYEND_reactive()),collapse=""))
      
      table_3_6_systems_pp_ppsirt["To Date"] <- table_3_6_systems_pp_ppsirt_todate
      table_3_6 <- table_3_6_systems_pp_ppsirt
      colnames(table_3_6)<- c("This Fiscal Year","To Date")
      rownames(table_3_6)<- "Number of Systems with Infiltration Testing Administered"
      
      return(table_3_6) 
      
    })
    
    
    table_37 <- reactive({
      
   
      #Section 3.2: Public GSI Monitoring during Construction
      #3.2.1 SRTs in Construction
      
      #Table 3.7, and also the opening paragraph
      #Mid-construction SRTs performed on Public Systems
      #First column: This fiscal year
      
      sql_string_16 <- "select count(*), type
                            from fieldwork.srt_full 
                            where test_date >= '%s'
                            and test_date <= '%s'
                            and phase = 'Construction'
                            and public = TRUE
                            group by type"
      
      
      
      table_3_7_public_midcon_srt <- dbGetQuery(con, paste(sprintf(sql_string_16, FYSTART_reactive(), FYEND_reactive()),collapse=""))
      
      #Second column: The above, to date
      sql_string_17 <- "select count(*), type
                          from fieldwork.srt_full 
                          where test_date <= '%s'
                          and phase = 'Construction'
                          and public = TRUE
                          group by type"
      
      table_3_7_public_midcon_srt_todate <- dbGetQuery(con, paste(sprintf(sql_string_17, FYEND_reactive()),collapse=""))
      
      table_3_7 <- table_3_7_public_midcon_srt_todate %>%
        left_join(table_3_7_public_midcon_srt, by="type") %>%
        select(`SRT Type`=type,`This Fiscal Year`=count.y, `To Date`=count.x)
      table_3_7[is.na(table_3_7)] <-  0
      
         
    return(table_3_7) 
      
    })
    
    table_38 <- reactive({
      #Table 3.8, and the opening paragraph
      #Note: This is a count of systems, not SMPs. The table title and caption for the table in the FY21 annual report implies that it is counting SMPs.
      #The numbers in the old table are systems, not SMPs, and were generated by this query
      #I suggest we rename the table to be counting systems, and remove the caption
      #We could just as easily count SMPs, by replacing the count(distinct(srt.system_id)) with count(distinct(sfc.smp_id))
      #Column 1: Current fiscal year
      
      
      sql_string_18 <-"select sfc.asset_type, count(distinct(srt.system_id))
                              from fieldwork.srt_full srt
                              left join smpid_facilityid_componentid sfc on srt.system_id = sfc.system_id
                              where sfc.component_id is null
                              and test_date >= '%s'
                              and test_date <= '%s'
                              and phase = 'Construction'
                              and public = TRUE
                              group by sfc.asset_type"
      table_3_8_public_systems_duringcon_srt <- dbGetQuery(con, paste(sprintf(sql_string_18, FYSTART_reactive(), FYEND_reactive()),collapse=""))
      
      #SRTs in Construction (count of systems) TO DATE
      sql_string_19 <-"select sfc.asset_type, count(distinct(srt.system_id))
                              from fieldwork.srt_full srt
                              left join smpid_facilityid_componentid sfc on srt.system_id = sfc.system_id
                              where sfc.component_id is null
                              and test_date <= '%s'
                              and phase = 'Construction'
                              and public = TRUE
                              group by sfc.asset_type"
      table_3_8_public_systems_duringcon_srt_todate <- dbGetQuery(con, paste(sprintf(sql_string_19,FYEND_reactive()),collapse=""))
      
      table_3_8 <- table_3_8_public_systems_duringcon_srt_todate %>%
        left_join(table_3_8_public_systems_duringcon_srt, by="asset_type") 
      table_3_8[is.na(table_3_8)] <-  0
      table_3_8 <- table_3_8[,c(1,3,2)]
      colnames(table_3_8) <- c("System Type","This Fiscal Year", "To Date")
      
      
    return(table_3_8) 
 
    })
    
    
    table_39 <- reactive({
      
      #Section 3.3 Groundwater Level Monitoring for Public GSI
      #Table 3.9 and the opening paragraph
      #First column, first row - GW monitoring prior to construction of GSI this fiscal year
      
      sql_string_20 <-"select count(distinct(site_name)) from fieldwork.deployment_full where smp_id is null 
                                                and deployment_dtime_est <= '%s'
                                                and (collection_dtime_est >= '%s' OR collection_dtime_est is null)
                                                and (ow_suffix LIKE 'GW_' or ow_suffix LIKE 'CW_')"
      table_3_9_public_gw_monitoring_precon <- dbGetQuery(con, paste(sprintf(sql_string_20, FYEND_reactive(), FYSTART_reactive()),collapse=""))
      
      #GW monitoring prior to construction of GSI TO THIS DATE 
      #Second column, first row - the same, to date
      sql_string_21 <-"select count(distinct(site_name)) from fieldwork.deployment_full where smp_id is null 
                                                and (ow_suffix LIKE 'GW_' or ow_suffix LIKE 'CW_')"
      table_3_9_public_gw_monitoring_precon_todate <- dbGetQuery(con, sql_string_21)
      
      #Post-construction GW monitoring at GSI this fiscal year
      #First column, second row 
      sql_string_22 <-"select count(distinct(site_name)) from fieldwork.deployment_full where smp_id is not null 
                                                and deployment_dtime_est <= '%s'
                                                and (collection_dtime_est >= '%s' OR collection_dtime_est is null)
                                                and (ow_suffix LIKE 'GW_' or ow_suffix LIKE 'CW_')"
      table_3_9_public_gw_monitoring_postcon <- dbGetQuery(con, paste(sprintf(sql_string_22, FYEND_reactive(), FYSTART_reactive()),collapse=""))
      
      
      #Post-construction GW monitoring at GSI TO THIS DATE
      #Second column, second row - the same, to date
      sql_string_23 <-"select count(distinct(smp_id)) from fieldwork.deployment_full where smp_id is not null 
                                                                        and (ow_suffix LIKE 'GW_' or ow_suffix LIKE 'CW_')"
      table_3_9_public_gw_monitoring_postcon_todate <- dbGetQuery(con, sql_string_23)
      
      table_3_9_public_gw_monitoring_precon["To Date"] <- table_3_9_public_gw_monitoring_precon_todate
      table_3_9_public_gw_monitoring_postcon["To Date"] <- table_3_9_public_gw_monitoring_postcon_todate
      table_3_9 <- bind_rows(table_3_9_public_gw_monitoring_precon,table_3_9_public_gw_monitoring_postcon)
      rownames(table_3_9) <- c("Prior to Construction of GSI (Systems)","Post-Construction (Active GSI)")
      colnames(table_3_9) <- c("This Fiscal Year","To Date")
      
      return(table_3_9)
    })
    
    table_310 <- reactive({
      
      #Section 3.4: Post-Construction Private GSI Monitoring and Testing
      #Section 3.4.1: Private CWL Monitoring
      #Table 3.10, and also the opening paragraph
      #First column, first row - Sensor deployments this fiscal year
      
      sql_string_24 <- "select count(*) from fieldwork.deployment_full_cwl
                                  where (collection_dtime_est > '%s' OR collection_dtime_est is null)
                                  and deployment_dtime_est < '%s'
                                  and public = FALSE"
      
      table_3_10_private_sensors_deployed <- dbGetQuery(con, paste(sprintf(sql_string_24, FYSTART_reactive(), FYEND_reactive()),collapse="")) 
      
      #Second column, first row - Sensor deployments to date (private)
      sql_string_25 <- "select count(*) from fieldwork.deployment_full_cwl
                                  where deployment_dtime_est < '%s'
                                  and public = FALSE"
      
      table_3_10_private_sensors_deployed_todate <- dbGetQuery(con, paste(sprintf(sql_string_25,FYEND_reactive()),collapse="")) 
      
      #Systems monitored this fiscal year (private)
      #First column, second row - Systems monitored this fiscal year
      
      sql_string_26 <- "select count(distinct smp_to_system(d.smp_id)) from fieldwork.deployment_full_cwl d
                                  where deployment_dtime_est <= '%s'
                                  and (collection_dtime_est >= '%s'
                                      or collection_dtime_est is null) 
                                  and d.public = false"
      table_3_10_private_systems_monitored <- dbGetQuery(con, paste(sprintf(sql_string_26, FYSTART_reactive(), FYEND_reactive()),collapse="")) 
      
      #Systems monitored to date (private)
      sql_string_27 <- "select count(distinct smp_to_system(d.smp_id)) from fieldwork.deployment_full_cwl d
                                  where deployment_dtime_est <= '%s'
                                  and d.public = false"
      table_3_10_private_systems_monitored_todate <- dbGetQuery(con, paste(sprintf(sql_string_27, FYEND_reactive()),collapse="")) 
      
      
      #Newly monitored systems this fiscal year (private)
      sql_string_28 <- "select count(distinct smp_to_system(newdeployments.smp_id)) FROM 
                                          	(select d.smp_id FROM fieldwork.deployment_full_cwl d 
                                               group BY d.smp_id, d.public
                                               having min(d.deployment_dtime_est) > '%s'
                                               and min(d.deployment_dtime_est) <= '%s'
                                               and d.public = false) newdeployments"
      table_3_10_private_new_systems_monitored<- dbGetQuery(con, paste(sprintf(sql_string_28, FYSTART_reactive(), FYEND_reactive()),collapse=""))
      
      `This Fiscal Year`<- data.frame(c(pull(table_3_10_private_sensors_deployed),pull(table_3_10_private_systems_monitored ),pull( table_3_10_private_new_systems_monitored)))
      `To Date`<-data.frame(c(pull(table_3_10_private_sensors_deployed_todate),pull(table_3_10_private_systems_monitored_todate),NA))
      table_3_10 <- bind_cols(`This Fiscal Year`,`To Date`)
      colnames(table_3_10)<- c("This Fiscal Year","To Date")
      rownames(table_3_10)<-c("Sensors Deployments","Systems","Systems Newly Monitored")
      
      return(table_3_10)
      
    })
    
    
    table_311 <- reactive({
      
      #Table 3.11: Post-Construction CWL-Monitoring of Private SMPs Listed by Type 
      #First column: Monitored private SMPs to date
      
      sql_string_29 <- "select cr.\"SMPType\", count(distinct(d.smp_id)), d.public from
                  fieldwork.deployment_full_cwl d
                  left join planreview_view_smpsummary_crosstab_asbuiltall cr on d.smp_id = cr.\"SMPID\"::text
                  where d.smp_id is not null
                  and d.deployment_dtime_est < '%s'
                  and d.public = false
                  group by cr.\"SMPType\", d.public;"
      
      table_3_11_private_monitored_smps_postcon<- dbGetQuery(con, paste(sprintf(sql_string_29,FYEND_reactive()),collapse=""))
      
      
      #Second column: Total constructed private SMPs
      
      
      sql_string_30 <- "select count(*), cr.\"SMPType\"
                  from planreview_projectsmpconcat pl
                  left join planreview_view_smp_designation de on de.\"ProjectID\" = pl.project_id
                  left join planreview_view_smpsummary_crosstab_asbuiltall cr on de.\"SMPID\" = cr.\"SMPID\"
                  left join smpid_facilityid_componentid sfc on de.\"SMPID\"::text = sfc.smp_id
                  where cr.\"DCIA\" is not null
                  and sfc.smp_id is not null
                  and sfc.component_id is null
                  group by cr.\"SMPType\";"
  
      
      table_3_11_private_total_constructed_smps<- dbGetQuery(con, sql_string_30)
      
      table_3_11_private_total_constructed_smps <- table_3_11_private_total_constructed_smps %>%
        select(`SMP Type` = SMPType,`Total Constructed Private SMPs`=count)
      table_3_11_private_monitored_smps_postcon <- table_3_11_private_monitored_smps_postcon %>%
        select(`SMP Type` = SMPType,`Monitored SMPs`=count)
      table_3_11 <- table_3_11_private_total_constructed_smps %>% 
        left_join(table_3_11_private_monitored_smps_postcon, by="SMP Type")
      table_3_11<-table_3_11[,c(1,3,2)]
      table_3_11[is.na(table_3_11)] <-  0

      return(table_3_11)
    })
      
    table_312 <- reactive({
      
      #3.4.2: Private SRTs
      #Table 3.12: Post-construction private SRTs
      #First column: This fiscal year
      sql_string_31 <-"select count(*), type
                          from fieldwork.srt_full 
                          where test_date >= '%s'
                          and test_date <= '%s'
                          and phase = 'Post-Construction'
                          and public = false
                          group by type"
      
      
      table_3_12_private_postcon_srt <- dbGetQuery(con, paste(sprintf(sql_string_31, FYSTART_reactive(), FYEND_reactive()),collapse=""))
      
      
      #3.4.2: Private SRTs
      #Table 3.12: Post-construction private SRTs
      #2nd column: To date
      sql_string_32 <-"select count(*), type
                        from fieldwork.srt_full 
                        where test_date <= '%s'
                        and phase = 'Post-Construction'
                        and public = false
                        group by type"
      
      
      table_3_12_private_postcon_srt_todate <- dbGetQuery(con, paste(sprintf(sql_string_32,FYEND_reactive()),collapse=""))
      
      table_3_12 <- table_3_12_private_postcon_srt_todate %>%
        left_join(table_3_12_private_postcon_srt, by="type") %>%
        select(`SRT Type`=type,`This Fiscal Year`=count.y, `To Date`=count.x)
      table_3_12[is.na(table_3_12)] <-  0
      
      return(table_3_12)
      
    })
    
    table_313 <- reactive({
      
      #Table 3.13: Private SMPs with Post-Construction SRTs
      #First column: This fiscal year
      
      #Table 3.13: Private SMPs with Post-Construction SRTs
      #First column: This fiscal year
      
          sql_string <- "select cr.\"SMPType\", count(distinct newtests.system_id) FROM 
    	(select system_id from fieldwork.srt_full srt
        group by system_id, public
        having min(test_date) >= '%s'
        and min(test_date) <= '%s'
        and public = false) newtests
    left join planreview_view_smpsummary_crosstab_asbuiltall cr on newtests.system_id = cr.\"SMPID\"::text
    group by cr.\"SMPType\""
          
          
          table_3_13_private_postcon_smp_withSRT <- dbGetQuery(con, paste(sprintf(sql_string, FYSTART_reactive(), FYEND_reactive()),collapse=""))
          
          #Table 3.13: Private SMPs with Post-Construction SRTs
          #2nd column: To Date
          sql_string <-"select cr.\"SMPType\", count(distinct newtests.system_id) FROM 
    	(select system_id from fieldwork.srt_full srt
        group by system_id, public
        having min(test_date) <= '%s'
        and public = false) newtests
    left join planreview_view_smpsummary_crosstab_asbuiltall cr on newtests.system_id = cr.\"SMPID\"::text
    group by cr.\"SMPType\""
          
      table_3_13_private_postcon_smp_withSRT_todate <- dbGetQuery(con, paste(sprintf(sql_string,FYEND_reactive()),collapse=""))
      
      table_3_13 <- table_3_13_private_postcon_smp_withSRT_todate %>%
        left_join(table_3_13_private_postcon_smp_withSRT, by="SMPType")
      
      table_3_13[is.na(table_3_13)] <-  0
      table_3_13<-table_3_13[,c(1,3,2)]
      colnames(table_3_13)<- c("SMP Type","This Fiscal Year","To Date")
      

     return(table_3_13) 
    })
      
    table_314 <- reactive({
      
      #Section 3.4.3: Private CET Testing
      #Table 3.14 and the 3.4.3 paragraph
      #Column 1: Private systems with CET this fiscal year
      sql_string_35 <-"select count(distinct system_id) 
                    from fieldwork.capture_efficiency_full 
                    where phase = 'Post-Construction'
                    and test_date >= '%s'
                    and test_date <= '%s'
                    and public = FALSE"
      
      table_3_14_private_systems_cet <- dbGetQuery(con, paste(sprintf(sql_string_35,FYSTART_reactive(), FYEND_reactive()),collapse=""))
      
      
      #Section 3.4.3: Private CET Testing
      #Table 3.14 and the 3.4.3 paragraph
      #Column 2: Private systems with CET to date
      sql_string_36 <-"select count(distinct system_id) 
                  from fieldwork.capture_efficiency_full 
                  where phase = 'Post-Construction'
                  and test_date <= '%s'
                  and public = FALSE"
      
      table_3_14_private_systems_cet_todate <- dbGetQuery(con, paste(sprintf(sql_string_36, FYEND_reactive()),collapse=""))
      
      table_3_14_private_systems_cet["To Date"] <-table_3_14_private_systems_cet_todate
      table_3_14 <-table_3_14_private_systems_cet
      colnames(table_3_14) <- c("This Fiscal Year","To Date")
      rownames(table_3_14) <- "Number of Systems with CETs Administered"
      
    
    
    
      return(table_3_14)
    })
      
    #ractable table outputs
    output$`Table 3-1` <- renderReactable(reactable(table_31()))
    output$`Table 3-2` <- renderReactable(reactable(table_32(),pagination = FALSE))
    output$`Table 3-3` <- renderReactable(reactable(table_33()))
    output$`Table 3-4` <- renderReactable(reactable(table_34()))
    output$`Table 3-5` <- renderReactable(reactable(table_35()))
    output$`Table 3-6` <- renderReactable(reactable(table_36()))
    output$`Table 3-7` <- renderReactable(reactable(table_37()))
    output$`Table 3-8` <- renderReactable(reactable(table_38()))
    output$`Table 3-9` <- renderReactable(reactable(table_39()))
    output$`Table 3-10` <- renderReactable(reactable(table_310()))
    output$`Table 3-11` <- renderReactable(reactable(table_311(),pagination = FALSE))
    output$`Table 3-12` <- renderReactable(reactable(table_312()))
    output$`Table 3-13` <- renderReactable(reactable(table_313()))
    output$`Table 3-14` <- renderReactable(reactable(table_314()))
    output$help_text <- renderText({
      paste("A Shiny App to Populate the Annual Report Stats" , 
            "First Version Published on 08/05/2022 by Farshad Ebrahimi",
            sep="\n")
    })
    
    
}

# Run the application 
shinyApp(ui = ui, server = server)



  
