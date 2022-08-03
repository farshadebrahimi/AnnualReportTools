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
  #Section 3.1: Post-Construction GSI Monitoring + Testing
  #Section 3.1.1: CWL Monitoring
  #Sensors deployed this fiscal year
  #In the text, and in table 3.1
  
  sql_string <- "select count(*) from fieldwork.deployment_full_cwl
                                            where (collection_dtime_est > '%s' OR collection_dtime_est is null)
                                            and deployment_dtime_est < '%s'
                                            and public = TRUE"
  sensors_deployed_postcon_public <- dbGetQuery(con, paste(sprintf(sql_string, FYSTART, FYEND),collapse="")) 
  
  ##Public systems monitored this fiscal year
  #In the text, and in table 3.1
  sql_string <- "select count(distinct smp_to_system(d.smp_id)) from fieldwork.deployment_full_cwl d
                                            where deployment_dtime_est >= '%s'
                                            and (collection_dtime_est <= '%s'
                                            	or collection_dtime_est is null) 
                                            and d.public = true"
  public_systems_monitored <- dbGetQuery(con, paste(sprintf(sql_string, FYSTART, FYEND),collapse="")) 
  
  
  #Newly monitored systems this fiscal year
  #In the text, and in table 3.1
  sql_string <- "select count(distinct smp_to_system(newdeployments.smp_id)) FROM 
                                          	(select d.smp_id FROM fieldwork.deployment_full_cwl d 
                                               group BY d.smp_id, d.public
                                               having min(d.deployment_dtime_est) > '%s'
                                               and min(d.deployment_dtime_est) <= '%s'
                                               and d.public = true) newdeployments"
  new_public_systems_monitored<- dbGetQuery(con, paste(sprintf(sql_string, FYSTART, FYEND),collapse=""))
  
  
  #Sensors deployed to date
  sql_string <- "select count(*) from fieldwork.deployment_full_cwl
                                                where deployment_dtime_est <= '%s'
                                                and public = TRUE"
                                                  
  sensors_deployed_todate <- dbGetQuery(con, paste(sprintf(sql_string,FYEND),collapse=""))
  
  #Public systems monitored to date
  #In the text, and in table 3.1
  sql_string <- "select count(distinct smp_to_system(d.smp_id)) from fieldwork.deployment_full_cwl d
                                                where deployment_dtime_est <= '%s'
                                                and d.public = true"
  public_systems_monitored_todate <- dbGetQuery(con, paste(sprintf(sql_string,FYEND),collapse=""))
  
  #Monitored public smps by type, to date
  #Table 3-2 first column
  sql_string <- "select sfc.asset_type, count(distinct(d.smp_id)), d.public from
                                                fieldwork.deployment_full_cwl d
                                                left join public.smpid_facilityid_componentid sfc on d.smp_id = sfc.smp_id
                                                where sfc.component_id is null
                                                and d.smp_id is not null
                                                and d.deployment_dtime_est < '%s'
                                                and d.public = true
                                                group by sfc.asset_type, d.public"
  
  public_smp_bytype_todate <- dbGetQuery(con, paste(sprintf(sql_string,FYEND),collapse=""))
  
  #CAPIT statuses indicating constructed systems are Jillian Simmons's best recommendation
  #Table 3-2 second column
  sql_string <- "select count(*), smp_smptype from greenit_smpbestdata g where g.smp_notbuiltretired is null 
                                            and (g.capit_status = 'Closed' 
                                            or g.capit_status = 'Construction-Substantially Complete' 
                                            or g.capit_status = 'Construction-Contract Closed') 
                                            group by smp_smptype"
  public_constructed_systems_total <- dbGetQuery(con,sql_string)
  
  #Section 3.1.2: SRT Testing
  #Table 3-3
  #Post-construction SRTs performed on Public Systems
  #Also appears in the 3.1.2 opening paragraph
  #First column: This fiscal year
  
  sql_string <- "select count(*), type from fieldwork.srt_full 
                                      where test_date >= '%s'
                                      and test_date <= '%s'
                                      and phase = 'Post-Construction'
                                      and public = TRUE
                                      group by type"
  public_system_postcon_srt <-dbGetQuery(con, paste(sprintf(sql_string, FYSTART, FYEND),collapse=""))
                                                  
  
  #Post-construction SRTs performed on Public Systems TO DATE
  #Second column: The above, to date
  
  sql_string <-"select count(*), type from fieldwork.srt_full 
                                                        where test_date <= '%s'
                                                        and phase = 'Post-Construction'
                                                        and public = TRUE
                                                        group by type"
                      
  public_system_postcon_srt_todate <- dbGetQuery(con, paste(sprintf(sql_string,FYEND),collapse=""))
  
  #Table 3.4: Public Systems with Post-Construction SRTs Performed
  #Note: This is a count of systems, not SMPs. The table title and caption for the table in the FY21 annual report implies that it is counting SMPs.
  #The numbers in the old table are systems, not SMPs, and were generated by this query
  #I suggest we rename the table to be counting systems, and remove the caption
  #We could just as easily count SMPs, by replacing the count(distinct(srt.system_id)) with count(distinct(sfc.smp_id))
  
  #Column 1: Current fiscal year
  sql_string <-"select sfc.asset_type, count(distinct(srt.system_id))
                                                  from fieldwork.srt_full srt
                                                  left join smpid_facilityid_componentid sfc on srt.system_id = sfc.system_id
                                                  where sfc.component_id is null
                                                  and test_date >= '%s'
                                                  and test_date <= '%s'
                                                  and phase = 'Post-Construction'
                                                  and public = TRUE
                                                  group by sfc.asset_type"
  
  public_systems_postcon_srt <-dbGetQuery(con, paste(sprintf(sql_string, FYSTART, FYEND),collapse=""))
  
  #Public Systems with Post-Construction SRTs Performed TO DATE
  #Column 2: 
  sql_string <-"select sfc.asset_type, count(distinct(srt.system_id))
                                                  from fieldwork.srt_full srt
                                                  left join smpid_facilityid_componentid sfc on srt.system_id = sfc.system_id
                                                  where sfc.component_id is null
                                                  and test_date <= '%s'
                                                  and phase = 'Post-Construction'
                                                  and public = TRUE
                                                  group by sfc.asset_type"
  public_systems_postcon_srt_todate <-dbGetQuery(con, paste(sprintf(sql_string, FYEND),collapse=""))
  
  
  #Section 3.1.3: CET Testing
  #Table 3.5 and the 3.1.3 paragraph
  #Column 1: Public systems with CET this fiscal year
  
  sql_string <-"select count(distinct system_id) 
                                      from fieldwork.capture_efficiency_full 
                                      where phase = 'Post-Construction'
                                      and test_date >= '%s'
                                      and test_date <= '%s'
                                      and public = TRUE"
  public_systems_cet <- dbGetQuery(con, paste(sprintf(sql_string, FYSTART, FYEND),collapse=""))
  
  #Public systems with CET this fiscal year TO DATE
  #Column 2: 
  
  sql_string <-"select count(distinct system_id) 
                                      from fieldwork.capture_efficiency_full 
                                      where phase = 'Post-Construction'
                                      and test_date <= '%s'
                                      and public = TRUE"
  public_systems_cet_todate <- dbGetQuery(con, paste(sprintf(sql_string,FYEND),collapse=""))
  
  #Section 3.1.4: Porous Pavement and PPSIRT
  #Table 3.6 and in the 3.1.4 paragraph
  #Column 1: Systems with PP/PPSIRT in this fiscal year
  
  
  sql_string <-"select count(distinct smp_to_system(smp_id))
                                      from fieldwork.porous_pavement_full
                                      where test_date >= '%s'
                                      and test_date <= '%s'
                                      and public = TRUE"
  systems_pp_ppsirt <- dbGetQuery(con, paste(sprintf(sql_string, FYSTART, FYEND),collapse=""))
  
  #Systems with PP/PPSIRT in this fiscal year TO DATE
  #column 2
  sql_string <-"select count(distinct smp_to_system(smp_id))
                                          from fieldwork.porous_pavement_full
                                          where test_date <= '%s'
                                          and public = TRUE"
  systems_pp_ppsirt_todate <- dbGetQuery(con, paste(sprintf(sql_string, FYEND),collapse=""))
  
  
  
  #Section 3.2: Public GSI Monitoring during Construction
  #3.2.1 SRTs in Construction
  
  #Table 3.7, and also the opening paragraph
  #Mid-construction SRTs performed on Public Systems
  #First column: This fiscal year
  
  sql_string <- "select count(*), type
                            from fieldwork.srt_full 
                            where test_date >= '%s'
                            and test_date <= '%s'
                            and phase = 'Construction'
                            and public = TRUE
                            group by type"
  
  
  
  public_midcon_srt <- dbGetQuery(con, paste(sprintf(sql_string, FYSTART, FYEND),collapse=""))
  
  #Second column: The above, to date
  sql_string <- "select count(*), type
                          from fieldwork.srt_full 
                          where test_date <= '%s'
                          and phase = 'Construction'
                          and public = TRUE
                          group by type"
  
  public_midcon_srt_todate <- dbGetQuery(con, paste(sprintf(sql_string, FYEND),collapse=""))
  
  #Table 3.8, and the opening paragraph
  #Note: This is a count of systems, not SMPs. The table title and caption for the table in the FY21 annual report implies that it is counting SMPs.
  #The numbers in the old table are systems, not SMPs, and were generated by this query
  #I suggest we rename the table to be counting systems, and remove the caption
  #We could just as easily count SMPs, by replacing the count(distinct(srt.system_id)) with count(distinct(sfc.smp_id))
  #Column 1: Current fiscal year
  
  
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
  systems_duringcon_srt_todate <- dbGetQuery(con, paste(sprintf(sql_string,FYEND),collapse=""))

  
  #Section 3.3 Groundwater Level Monitoring for Public GSI
  #Table 3.9 and the opening paragraph
  #First column, first row - GW monitoring prior to construction of GSI this fiscal year
  
  sql_string <-"select count(distinct(site_name)) from fieldwork.deployment_full where smp_id is null 
                                                and deployment_dtime_est <= '%s'
                                                and (collection_dtime_est >= '%s' OR collection_dtime_est is null)
                                                and (ow_suffix LIKE 'GW_' or ow_suffix LIKE 'CW_')"
  publicGSI_gw_monitoring_precon <- dbGetQuery(con, paste(sprintf(sql_string, FYEND, FYSTART),collapse=""))
  
  #GW monitoring prior to construction of GSI TO THIS DATE 
  #Second column, first row - the same, to date
  sql_string <-"select count(distinct(site_name)) from fieldwork.deployment_full where smp_id is null 
                                                and (ow_suffix LIKE 'GW_' or ow_suffix LIKE 'CW_')"
  publicGSI_gw_monitoring_precon_todate <- dbGetQuery(con, sql_string)
  
  #Post-construction GW monitoring at GSI this fiscal year
  #First column, second row 
  sql_string <-"select count(distinct(site_name)) from fieldwork.deployment_full where smp_id is not null 
                                                and deployment_dtime_est <= '%s'
                                                and (collection_dtime_est >= '%s' OR collection_dtime_est is null)
                                                and (ow_suffix LIKE 'GW_' or ow_suffix LIKE 'CW_')"
  publicGSI_gw_monitoring_postcon <- dbGetQuery(con, paste(sprintf(sql_string, FYEND, FYSTART),collapse=""))
  
  
  #Post-construction GW monitoring at GSI TO THIS DATE
  #Second column, second row - the same, to date
  sql_string <-"select count(distinct(smp_id)) from fieldwork.deployment_full where smp_id is not null 
                                                                        and (ow_suffix LIKE 'GW_' or ow_suffix LIKE 'CW_')"
  publicGSI_gw_monitoring_postcon_todate <- dbGetQuery(con, sql_string)
  
  #Section 3.4: Post-Construction Private GSI Monitoring and Testing
  #Section 3.4.1: Private CWL Monitoring
  #Table 3.10, and also the opening paragraph
  #First column, first row - Sensor deployments this fiscal year
  
  sql_string <- "select count(*) from fieldwork.deployment_full_cwl
                                  where (collection_dtime_est > '%s' OR collection_dtime_est is null)
                                  and deployment_dtime_est < '%s'
                                  and public = FALSE"
  
  private_sensors_deployed <- dbGetQuery(con, paste(sprintf(sql_string, FYSTART, FYEND),collapse="")) 
  
  #Second column, first row - Sensor deployments to date (private)
  sql_string <- "select count(*) from fieldwork.deployment_full_cwl
                                  where deployment_dtime_est < '%s'
                                  and public = FALSE"
                                    
  private_sensors_deployed_todate <- dbGetQuery(con, paste(sprintf(sql_string,FYEND),collapse="")) 
  
  #Systems monitored this fiscal year (private)
  #First column, second row - Systems monitored this fiscal year
  
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
  private_new_systems_monitored<- dbGetQuery(con, paste(sprintf(sql_string, FYSTART, FYEND),collapse=""))
  
  #Table 3.11: Post-Construction CWL-Monitoring of Private SMPs Listed by Type 
  #First column: Monitored private SMPs FY22
  
  sql_string <- "select sfc.asset_type, count(distinct(d.smp_id)), d.public from
                                              fieldwork.deployment_full_cwl d
                                              left join public.smpid_facilityid_componentid sfc on d.smp_id = sfc.smp_id
                                              where sfc.component_id is null
                                              and d.smp_id is not null
                                              and d.deployment_dtime_est < '%s'
                                              and d.public = false
                                              group by sfc.asset_type, d.public"
                                                
  private_monitored_smps_postcon<- dbGetQuery(con, paste(sprintf(sql_string,FYEND),collapse=""))
  
  
  #Second column: Total constructed private SMPs
 
  
  sql_string <- "select count(*), sfc.asset_type
                                from planreview_projectsmpconcat pl
                                left join planreview_view_smp_designation de on de.\"ProjectID\" = pl.project_id
                                 left join planreview_view_smpsummary_crosstab_asbuiltall cr on de.\"SMPID\" = cr.\"SMPID\"
                                 left join smpid_facilityid_componentid sfc on de.\"SMPID\"::text = sfc.smp_id
                                 where cr.\"DCIA\" is not null
                                 and sfc.smp_id is not null
                                 and sfc.component_id is null
                                    group by sfc.asset_type;"
  
  total_private_constructed_smps<- dbGetQuery(con, sql_string)

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
  #2nd column: To date
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
  
  
  private_postcon_smp_withSRT <- dbGetQuery(con, paste(sprintf(sql_string, FYSTART, FYEND),collapse=""))
  
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
  
  private_postcon_smp_withSRT_todate <- dbGetQuery(con, paste(sprintf(sql_string,FYEND),collapse=""))
  
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
   
  
  