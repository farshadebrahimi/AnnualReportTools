#Scripts to populate updated stats for the annual report
# By Farshad Ebrahimi, Last modified: 08/02/2022

## set up

  library(odbc)
  library(DBI)
  library(lubridate)
  library(tidyverse)
  library(dplyr)
  library(ggplot2)

## Database Connection to pg9

  #connection 
  con <- odbc::dbConnect(odbc::odbc(), "mars_testing")
  
  
## Creating variables for FY22 dates
  
  FYSTART <- '2021-07-01 00:00:00'
  FYEND <- '2022-06-30 11:59:59'
  
## queries to gets stats
  
  #Sensors deployed this fiscal year
  sql_string <- "select count(*) from fieldwork.deployment_full_cwl
                                            where (collection_dtime_est > '%s' OR collection_dtime_est is null)
                                            and deployment_dtime_est < '%s'
                                            and public = TRUE"
  sensors_deployed <- dbGetQuery(con, paste(sprintf(sql_string, FYSTART, FYEND),collapse="")) 
  
  ##Public systems monitored this fiscal year
  sql_string <- "select count(distinct smp_to_system(d.smp_id)) from fieldwork.deployment_full_cwl d
                                            where deployment_dtime_est >= '%s'
                                            and (collection_dtime_est <= '%s'
                                            	or collection_dtime_est is null) 
                                            and d.public = true"
  public_systems_monitored <- dbGetQuery(con, paste(sprintf(sql_string, FYSTART, FYEND),collapse="")) 
  
  
  #Newly monitored systems this fiscal year
  sql_string <- "select count(distinct smp_to_system(newdeployments.smp_id)) FROM 
                                          	(select d.smp_id FROM fieldwork.deployment_full_cwl d 
                                               group BY d.smp_id, d.public
                                               having min(d.deployment_dtime_est) > '%s'
                                               and min(d.deployment_dtime_est) <= '%s'
                                               and d.public = true) newdeployments"
  new_systems_monitored<- dbGetQuery(con, paste(sprintf(sql_string, FYSTART, FYEND),collapse=""))
  
  
  #Sensors deployed to date
  sql_string <- "select count(*) from fieldwork.deployment_full_cwl
                                                where deployment_dtime_est <= '%s'
                                                and public = TRUE"
                                                  
  sensors_deployed_todate <- dbGetQuery(con, paste(sprintf(sql_string,FYEND),collapse=""))
  
  #Public systems monitored to date
  sql_string <- "select count(distinct smp_to_system(d.smp_id)) from fieldwork.deployment_full_cwl d
                                                where deployment_dtime_est <= '%s'
                                                and d.public = true"
  systems_deployed_todate <- dbGetQuery(con, paste(sprintf(sql_string,FYEND),collapse=""))
  
  #Monitored public smps by type, to date
  sql_string <- "select sfc.asset_type, count(distinct(d.smp_id)), d.public from
                                                fieldwork.deployment_full_cwl d
                                                left join public.smpid_facilityid_componentid sfc on d.smp_id = sfc.smp_id
                                                where sfc.component_id is null
                                                and d.smp_id is not null
                                                and d.deployment_dtime_est < '%s'
                                                and d.public = true
                                                group by sfc.asset_type, d.public"
  
  systems_deployed_todate <- dbGetQuery(con, paste(sprintf(sql_string,FYEND),collapse=""))
  
  #CAPIT statuses indicating constructed systems are Jillian Simmons's best recommendation
  sql_string <- "select count(*), smp_smptype from greenit_smpbestdata g where g.smp_notbuiltretired is null 
                                            and (g.capit_status = 'Closed' 
                                            or g.capit_status = 'Construction-Substantially Complete' 
                                            or g.capit_status = 'Construction-Contract Closed') 
                                            group by smp_smptype"
  capit_status <- dbGetQuery(con,sql_string)
  
  #Post-construction SRTs performed on Public Systems
  sql_string <- "select count(*), type from fieldwork.srt_full 
                                      where test_date >= '%s'
                                      and test_date <= '%s'
                                      and phase = 'Post-Construction'
                                      and public = TRUE
                                      group by type"
  postcon_srt_pubsys <-dbGetQuery(con, paste(sprintf(sql_string, FYSTART, FYEND),collapse=""))
                                                  
  
  #Post-construction SRTs performed on Public Systems TO DATE
  sql_string <-"select count(*), type from fieldwork.srt_full 
                                                        where test_date <= '%s'
                                                        and phase = 'Post-Construction'
                                                        and public = TRUE
                                                        group by type"
                      
  postcon_srt_pubsys_todate <- dbGetQuery(con, paste(sprintf(sql_string,FYEND),collapse=""))
  
  #Public Systems with Post-Construction SRTs Performed
  sql_string <-"select sfc.asset_type, count(distinct(srt.system_id))
                                                  from fieldwork.srt_full srt
                                                  left join smpid_facilityid_componentid sfc on srt.system_id = sfc.system_id
                                                  where sfc.component_id is null
                                                  and test_date >= '%s'
                                                  and test_date <= '%s'
                                                  and phase = 'Post-Construction'
                                                  and public = TRUE
                                                  group by sfc.asset_type"
  
  pubsys_postcon_srt <-dbGetQuery(con, paste(sprintf(sql_string, FYSTART, FYEND),collapse=""))
  
  #Public Systems with Post-Construction SRTs Performed TO DATE
  sql_string <-"select sfc.asset_type, count(distinct(srt.system_id))
                                                  from fieldwork.srt_full srt
                                                  left join smpid_facilityid_componentid sfc on srt.system_id = sfc.system_id
                                                  where sfc.component_id is null
                                                  and test_date <= '%s'
                                                  and phase = 'Post-Construction'
                                                  and public = TRUE
                                                  group by sfc.asset_type"
  pubsys_postcon_srt_todate <-dbGetQuery(con, paste(sprintf(sql_string, FYEND),collapse=""))
  
  
  #Public systems with CET this fiscal year
  sql_string <-"select count(distinct system_id) 
                                      from fieldwork.capture_efficiency_full 
                                      where phase = 'Post-Construction'
                                      and test_date >= '%s'
                                      and test_date <= '%s'
                                      and public = TRUE"
  pubsys_cet <- dbGetQuery(con, paste(sprintf(sql_string, FYSTART, FYEND),collapse=""))
  
  #Public systems with CET this fiscal year TO DATE
  sql_string <-"select count(distinct system_id) 
                                      from fieldwork.capture_efficiency_full 
                                      where phase = 'Post-Construction'
                                      and test_date <= '%s'
                                      and public = TRUE"
  pubsys_cet_todate <- dbGetQuery(con, paste(sprintf(sql_string,FYEND),collapse=""))
  
  #Systems with PP/PPSIRT in this fiscal year
  sql_string <-"select count(distinct smp_to_system(smp_id))
                                      from fieldwork.porous_pavement_full
                                      where test_date >= '%s'
                                      and test_date <= '%s'
                                      and public = TRUE"
  systems_pp_ppsirt <- dbGetQuery(con, paste(sprintf(sql_string, FYSTART, FYEND),collapse=""))
  
  #Systems with PP/PPSIRT in this fiscal year TO DATE
  sql_string <-"select count(distinct smp_to_system(smp_id))
                                          from fieldwork.porous_pavement_full
                                          where test_date <= '%s'
                                          and public = TRUE"
  systems_pp_ppsirt_todate <- dbGetQuery(con, paste(sprintf(sql_string, FYEND),collapse=""))
  
  
  ##Public GSI Monitoring during Construction
  #SRTs in Construction (count of systems)
  sql_string <-"select sfc.asset_type, count(distinct(srt.system_id))
                              from fieldwork.srt_full srt
                              left join smpid_facilityid_componentid sfc on srt.system_id = sfc.system_id
                              where sfc.component_id is null
                              and test_date >= '%s'
                              and test_date <= '%s'
                              and phase = 'Construction'
                              and public = TRUE
                              group by sfc.asset_type"
  systems_duringcon_srt <- dbGetQuery(con, paste(sprintf(sql_string, FYSTART, FYEND),collapse=""))
  
  #SRTs in Construction (count of systems) TO DATE
  sql_string <-"select sfc.asset_type, count(distinct(srt.system_id))
                              from fieldwork.srt_full srt
                              left join smpid_facilityid_componentid sfc on srt.system_id = sfc.system_id
                              where sfc.component_id is null
                              and test_date <= '%s'
                              and phase = 'Construction'
                              and public = TRUE
                              group by sfc.asset_type"
  systems_duringcon_srt <- dbGetQuery(con, paste(sprintf(sql_string,FYEND),collapse=""))
  
  #Systems with Mid-Construction SRTs this fiscal year
  sql_string <-"select sfc.asset_type, count(distinct(srt.system_id))
                                                from fieldwork.srt_full srt
                                                left join smpid_facilityid_componentid sfc on srt.system_id = sfc.system_id
                                                where sfc.component_id is null
                                                and test_date >= '%s'
                                                and test_date <= '%s'
                                                and public = TRUE
                                                and phase = 'Construction'
                                                group by sfc.asset_type"
  
  systems_midcon_srt <- dbGetQuery(con, paste(sprintf(sql_string, FYSTART, FYEND),collapse=""))
  
  #Systems with Mid-Construction SRTs TO DATE
  sql_string <-"select sfc.asset_type, count(distinct(srt.system_id))
                                                from fieldwork.srt_full srt
                                                left join smpid_facilityid_componentid sfc on srt.system_id = sfc.system_id
                                                where sfc.component_id is null
                                                and test_date <= '%s'
                                                and public = TRUE
                                                and phase = 'Construction'
                                                group by sfc.asset_type"
  
  systems_midcon_srt_todate <- dbGetQuery(con, paste(sprintf(sql_string, FYEND),collapse=""))
  
  #GW monitoring prior to construction of GSI this fiscal year
  sql_string <-"select count(distinct(site_name)) from fieldwork.deployment_full where smp_id is null 
                                                and deployment_dtime_est <= '%s'
                                                and (collection_dtime_est >= '%s' OR collection_dtime_est is null)
                                                and (ow_suffix LIKE 'GW_' or ow_suffix LIKE 'CW_')"
  gw_monitoring_precon <- dbGetQuery(con, paste(sprintf(sql_string, FYEND, FYSTART),collapse=""))
  
  #GW monitoring prior to construction of GSI TO THIS DATE 
  sql_string <-"select count(distinct(site_name)) from fieldwork.deployment_full where smp_id is null 
                                                and (ow_suffix LIKE 'GW_' or ow_suffix LIKE 'CW_')"
  gw_monitoring_precon_todate <- dbGetQuery(con, sql_string)
  
  #Post-construction GW monitoring at GSI this fiscal year
  sql_string <-"select count(distinct(site_name)) from fieldwork.deployment_full where smp_id is not null 
                                                and deployment_dtime_est <= '%s'
                                                and (collection_dtime_est >= '%s' OR collection_dtime_est is null)
                                                and (ow_suffix LIKE 'GW_' or ow_suffix LIKE 'CW_')"
  gw_monitoring_postcon <- dbGetQuery(con, paste(sprintf(sql_string, FYEND, FYSTART),collapse=""))
  
  
  #Post-construction GW monitoring at GSI TO THIS DATE
  sql_string <-"select count(distinct(smp_id)) from fieldwork.deployment_full where smp_id is not null 
                                                                        and (ow_suffix LIKE 'GW_' or ow_suffix LIKE 'CW_')"
  gw_monitoring_postcon_todate <- dbGetQuery(con, sql_string)
  
  #Section 3.4.1: Private CWL Monitoring
  #Table 3.10, and also the opening paragraph
  #First column, first row - Sensor deployments this fiscal year
  
  sql_string <- "select count(*) from fieldwork.deployment_full_cwl
                                  where (collection_dtime_est > '%s' OR collection_dtime_est is null)
                                  and deployment_dtime_est < '%s'
                                  and public = FALSE"
  
  sensors_deployed_private <- dbGetQuery(con, paste(sprintf(sql_string, FYSTART, FYEND),collapse="")) 
  
  #Second column, first row - Sensor deployments to date (private)
  sql_string <- "select count(*) from fieldwork.deployment_full_cwl
                                  where deployment_dtime_est < '%s'
                                  and public = FALSE"
                                    
  sensors_deployed_private_todate <- dbGetQuery(con, paste(sprintf(sql_string,FYEND),collapse="")) 
  
  #Systems monitored this fiscal year (private)
  sql_string <- "select count(distinct smp_to_system(d.smp_id)) from fieldwork.deployment_full_cwl d
                                  where deployment_dtime_est <= '%s'
                                  and (collection_dtime_est >= '%s'
                                      or collection_dtime_est is null) 
                                  and d.public = false"
  private_systems_monitored <- dbGetQuery(con, paste(sprintf(sql_string, FYSTART, FYEND),collapse="")) 
  
  #Systems monitored to date (private)
  sql_string <- "select count(distinct smp_to_system(d.smp_id)) from fieldwork.deployment_full_cwl d
                                  where deployment_dtime_est <= '%s'
                                  and d.public = false"
  private_systems_monitored_todate <- dbGetQuery(con, paste(sprintf(sql_string, FYEND),collapse="")) 
  
  #Newly monitored systems this fiscal year (private)
  sql_string <- "select count(distinct smp_to_system(newdeployments.smp_id)) FROM 
                                          	(select d.smp_id FROM fieldwork.deployment_full_cwl d 
                                               group BY d.smp_id, d.public
                                               having min(d.deployment_dtime_est) > '%s'
                                               and min(d.deployment_dtime_est) <= '%s'
                                               and d.public = false) newdeployments"
  new_systems_monitored_private<- dbGetQuery(con, paste(sprintf(sql_string, FYSTART, FYEND),collapse=""))
  
  #Table 3.11: Post-Construction CWL-Monitoring of Private SMPs Listed by Type 
  #First column: Monitored private SMPs FY21
  
  sql_string <- "select sfc.asset_type, count(distinct(d.smp_id)), d.public from
                                              fieldwork.deployment_full_cwl d
                                              left join public.smpid_facilityid_componentid sfc on d.smp_id = sfc.smp_id
                                              where sfc.component_id is null
                                              and d.smp_id is not null
                                              and d.deployment_dtime_est < '%s'
                                              and d.public = false
                                              group by sfc.asset_type, d.public"
                                                
  monitored_private_smps_postcon<- dbGetQuery(con, paste(sprintf(sql_string,FYEND),collapse=""))
  
  
  #Second column: Total constructed private SMPs
  #placeholder 
  
  
  
  
  
  
  
  
  
  
  
  
  #3.4.2: Private SRTs
  #Table 3.12: Post-construction private SRTs
  #First column: This fiscal year
  sql_string <-"select count(*), type
                          from fieldwork.srt_full 
                          where test_date >= '%s'
                          and test_date <= '%s'
                          and phase = 'Post-Construction'
                          and public = false
                          group by type"
                            
  
  private_postcon_srt <- dbGetQuery(con, paste(sprintf(sql_string, FYSTART, FYEND),collapse=""))
  
  
  #3.4.2: Private SRTs
  #Table 3.12: Post-construction private SRTs
  #First column: To date
  sql_string <-"select count(*), type
                        from fieldwork.srt_full 
                        where test_date <= '%s'
                        and phase = 'Post-Construction'
                        and public = false
                        group by type"
  
  
  private_postcon_srt_todate <- dbGetQuery(con, paste(sprintf(sql_string,FYEND),collapse=""))
  
  
  #Table 3.13: Private SMPs with Post-Construction SRTs
  #First column: This fiscal year
  
  sql_string <- "select sfc.asset_type, count(distinct(srt.system_id))
                      from fieldwork.srt_full srt
                      left join smpid_facilityid_componentid sfc on srt.system_id = sfc.system_id
                      where sfc.component_id is null
                      and test_date > '%s'
                      and test_date < '%s'
                      and public = FALSE
                      and phase = 'Post-Construction'
                      group by sfc.asset_type"
  
  
  private_postcon_smp <- dbGetQuery(con, paste(sprintf(sql_string, FYSTART, FYEND),collapse=""))
  
  #Table 3.13: Private SMPs with Post-Construction SRTs
  #2nd column: To Date
  sql_string <-"select sfc.asset_type, count(distinct(srt.system_id))
                      from fieldwork.srt_full srt
                      left join smpid_facilityid_componentid sfc on srt.system_id = sfc.system_id
                      where sfc.component_id is null
                      and test_date < '%s'
                      and public = FALSE
                      and phase = 'Post-Construction'
                      group by sfc.asset_type "
  
  private_postcon_smp_todate <- dbGetQuery(con, paste(sprintf(sql_string,FYEND),collapse=""))
  
  #Section 3.4.3: Private CET Testing
  #Table 3.14 and the 3.4.3 paragraph
  #Column 1: Private systems with CET this fiscal year
  sql_string <-"select count(distinct system_id) 
                    from fieldwork.capture_efficiency_full 
                    where phase = 'Post-Construction'
                    and test_date >= '%s'
                    and test_date <= '%s'
                    and public = FALSE"
  
  private_systems_cet <- dbGetQuery(con, paste(sprintf(sql_string,FYSTART, FYEND),collapse=""))
  
  
  #Section 3.4.3: Private CET Testing
  #Table 3.14 and the 3.4.3 paragraph
  #Column 2: Private systems with CET to date
  sql_string <-"select count(distinct system_id) 
                  from fieldwork.capture_efficiency_full 
                  where phase = 'Post-Construction'
                  and test_date <= '%s'
                  and public = FALSE"
  
  private_systems_cet_todate <- dbGetQuery(con, paste(sprintf(sql_string, FYEND),collapse=""))
   
  
  