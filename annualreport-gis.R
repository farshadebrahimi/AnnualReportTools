library(odbc)
library(tidyverse)
library(lubridate)
library(rgdal)

#set up -------
mars <- dbConnect(odbc(), "mars_testing")
fy <- 2022
fystart <- paste0(fy - 1, "-07-01 00:00:00") %>% ymd_hms
fyend <- paste0(fy, "-06-30 23:59:59") %>% ymd_hms

folder <- "//pwdoows/oows/Watershed Sciences/GSI Monitoring/03 Reports and Presentations/01 Regulatory/Annual Report Submissions/2022/shapefiles"

ow <- dbGetQuery(mars, "select ow_uid, smp_id from fieldwork.ow_all")
smp_loc <- dbGetQuery(mars, "select * from smp_loc")

crs <- CRS("+init=epsg:4269")

#Figure 3-1 -----

#All Public CWL
public_cwl_query <- paste("select distinct smp_id from fieldwork.deployment_full_cwl where
                          deployment_dtime_est <'", fyend, "'
                   and smp_id is not null
                   and public = TRUE")
public_cwl <- dbGetQuery(mars, public_cwl_query)

public_cwl_data <- public_cwl %>% left_join(smp_loc) %>% select(-smp_loc_uid) %>% dplyr::filter(!is.na(lon_wgs84))

public_cwl_coord <- public_cwl_data[,c(2,3)]

public_cwl_spdf <- SpatialPointsDataFrame(coords = public_cwl_coord, data = public_cwl_data,proj4string = crs)

public_cwl_folder <- paste0(folder, "/public_cwl")

dir.create(public_cwl_folder)

writeOGR(public_cwl_spdf, layer = 'public_cwl', dsn = public_cwl_folder, driver = "ESRI Shapefile")

#FY22 Public CWL
fy_public_cwl_query <- paste("select distinct smp_id from fieldwork.deployment_full_cwl where (collection_dtime_est >= '",
                          fystart, "' OR collection_dtime_est is null) and deployment_dtime_est <'", fyend, 
                   "'
                   and smp_id is not null
                   and public = TRUE")
fy_public_cwl <- dbGetQuery(mars, fy_public_cwl_query)

fy_public_cwl_data <- fy_public_cwl %>% left_join(smp_loc) %>% select(-smp_loc_uid) %>% dplyr::filter(!is.na(lon_wgs84))

fy_public_cwl_coord <- fy_public_cwl_data[,c(2,3)]

fy_public_cwl_spdf <- SpatialPointsDataFrame(coords = fy_public_cwl_coord, data = fy_public_cwl_data,proj4string = crs)

fy_public_cwl_folder <- paste0(folder, "/fy_public_cwl")

dir.create(fy_public_cwl_folder)

writeOGR(fy_public_cwl_spdf, layer = 'fy_public_cwl', dsn = fy_public_cwl_folder, driver = "ESRI Shapefile")

#FY22 Baro 
fy_baro_query <- paste("select distinct smp_id from fieldwork.deployment_full_cwl_with_baro where type = 'BARO'
                           and term = 'Long'
                           and deployment_dtime_est < '", fyend, "'
                           and (collection_dtime_est > '", fystart, "'or collection_dtime_est is null)")

fy_baro <- dbGetQuery(mars, fy_baro_query)

fy_baro_data <- fy_baro %>% left_join(smp_loc) %>% select(-smp_loc_uid) %>% dplyr::filter(!is.na(lon_wgs84))

fy_baro_coord <- fy_baro_data[,c(2,3)]

fy_baro_spdf <- SpatialPointsDataFrame(coords = fy_baro_coord, data = fy_baro_data,proj4string = crs)

fy_baro_folder <- paste0(folder, "/fy_baro")

dir.create(fy_baro_folder)

writeOGR(fy_baro_spdf, layer = 'fy_baro', dsn = fy_baro_folder, driver = "ESRI Shapefile")

#Figure 3-2 -----
#FY22 public post con SRTs
public_post_con_srt_query <- paste("select distinct system_id from fieldwork.srt_full where test_date >='", 
                            fystart, 
                            "' and test_date <= '", 
                            fyend, 
                            "' and phase = 'Post-Construction' and public = TRUE")
fy_public_post_con_srt <- dbGetQuery(mars, public_post_con_srt_query) %>% 
  mutate(smp_id = paste0(system_id, "-1")) %>% 
  dplyr::select("smp_id")

fy_public_post_con_srt_data <- fy_public_post_con_srt %>% left_join(smp_loc) %>% select(-smp_loc_uid) %>% dplyr::filter(!is.na(lon_wgs84))

fy_public_post_con_srt_coord <- fy_public_post_con_srt_data[,c(2,3)]

fy_public_post_con_srt_spdf <- SpatialPointsDataFrame(coords = fy_public_post_con_srt_coord, data = fy_public_post_con_srt_data,proj4string = crs)

fy_public_post_con_srt_folder <- paste0(folder, "/fy_public_post_con_srt")

dir.create(fy_public_post_con_srt_folder)

writeOGR(fy_public_post_con_srt_spdf, layer = 'fy_public_post_con_srt', dsn = fy_public_post_con_srt_folder, driver = "ESRI Shapefile")


#FY22 Public Construction SRTs
fy_public_con_srt_query <- paste("select distinct system_id from fieldwork.srt_full where test_date >='", 
                       fystart, 
                       "' and test_date <= '", 
                       fyend, 
                       "' and phase = 'Construction'
                       and public = TRUE")

fy_public_con_srt <- dbGetQuery(mars, fy_public_con_srt_query) %>% 
  mutate(smp_id = paste0(system_id, "-1")) %>% 
  dplyr::select("smp_id")

fy_public_con_srt_data <- fy_public_con_srt %>% left_join(smp_loc) %>% select(-smp_loc_uid) %>% dplyr::filter(!is.na(lon_wgs84))

fy_public_con_srt_coord <- fy_public_con_srt_data[,c(2,3)]

fy_public_con_srt_spdf <- SpatialPointsDataFrame(coords = fy_public_con_srt_coord, data = fy_public_con_srt_data,proj4string = crs)

fy_public_con_srt_folder <- paste0(folder, "/fy_public_con_srt")

dir.create(fy_public_con_srt_folder)

writeOGR(fy_public_con_srt_spdf, layer = 'fy_public_con_srt', dsn = fy_public_con_srt_folder, driver = "ESRI Shapefile")

#FY22 Private Post-Con SRTs
private_post_con_srt_query <- paste("select distinct system_id as smp_id from fieldwork.srt_full where test_date >='", 
                                   fystart, 
                                   "' and test_date <= '", 
                                   fyend, 
                                   "' and phase = 'Post-Construction' and public = FALSE")
fy_private_post_con_srt <- dbGetQuery(mars, private_post_con_srt_query)

fy_private_post_con_srt_data <- fy_private_post_con_srt %>% left_join(smp_loc) %>% select(-smp_loc_uid) %>% dplyr::filter(!is.na(lon_wgs84))

fy_private_post_con_srt_coord <- fy_private_post_con_srt_data[,c(2,3)]

fy_private_post_con_srt_spdf <- SpatialPointsDataFrame(coords = fy_private_post_con_srt_coord, data = fy_private_post_con_srt_data,proj4string = crs)

fy_private_post_con_srt_folder <- paste0(folder, "/fy_private_post_con_srt")

dir.create(fy_private_post_con_srt_folder)

writeOGR(fy_private_post_con_srt_spdf, layer = 'fy_private_post_con_srt', dsn = fy_private_post_con_srt_folder, driver = "ESRI Shapefile")

#Permeable Pavement Tests
public_post_con_pp_query <- paste("select distinct smp_id from fieldwork.porous_pavement_full where test_date >='", 
                           fystart, 
                           "' and test_date <= '", 
                           fyend, 
                           "' and phase = 'Post-Construction' and public = TRUE")
fy_public_post_con_pp <- dbGetQuery(mars, public_post_con_pp_query)

fy_public_post_con_pp_data <- fy_public_post_con_pp %>% left_join(smp_loc) %>% select(-smp_loc_uid) %>% dplyr::filter(!is.na(lon_wgs84))

fy_public_post_con_pp_coord <- fy_public_post_con_pp_data[,c(2,3)]

fy_public_post_con_pp_spdf <- SpatialPointsDataFrame(coords = fy_public_post_con_pp_coord, data = fy_public_post_con_pp_data,proj4string = crs)

fy_public_post_con_pp_folder <- paste0(folder, "/fy_public_post_con_pp")

dir.create(fy_public_post_con_pp_folder)

writeOGR(fy_public_post_con_pp_spdf, layer = 'fy_post_con_pp', dsn = fy_public_post_con_pp_folder, driver = "ESRI Shapefile")

#Figure 3-3  ------
#All Private CWL
private_cwl_query <- paste("select distinct smp_id from fieldwork.deployment_full_cwl where
                          deployment_dtime_est <'", fyend, "'
                   and smp_id is not null
                   and public = FALSE")
private_cwl <- dbGetQuery(mars, private_cwl_query)

private_cwl_data <- private_cwl %>% left_join(smp_loc) %>% select(-smp_loc_uid) %>% dplyr::filter(!is.na(lon_wgs84))

private_cwl_coord <- private_cwl_data[,c(2,3)]

private_cwl_spdf <- SpatialPointsDataFrame(coords = private_cwl_coord, data = private_cwl_data,proj4string = crs)

private_cwl_folder <- paste0(folder, "/private_cwl")

dir.create(private_cwl_folder)

writeOGR(private_cwl_spdf, layer = 'private_cwl', dsn = private_cwl_folder, driver = "ESRI Shapefile")

#FY22 Private CWL
fy_private_cwl_query <- paste("select distinct smp_id from fieldwork.deployment_full_cwl where (collection_dtime_est >= '",
                             fystart, "' OR collection_dtime_est is null) and deployment_dtime_est <'", fyend, 
                             "'
                   and smp_id is not null
                   and public = FALSE")
fy_private_cwl <- dbGetQuery(mars, fy_private_cwl_query)

fy_private_cwl_data <- fy_private_cwl %>% left_join(smp_loc) %>% select(-smp_loc_uid) %>% dplyr::filter(!is.na(lon_wgs84))

fy_private_cwl_coord <- fy_private_cwl_data[,c(2,3)]

fy_private_cwl_spdf <- SpatialPointsDataFrame(coords = fy_private_cwl_coord, data = fy_private_cwl_data,proj4string = crs)

fy_private_cwl_folder <- paste0(folder, "/fy_private_cwl")

dir.create(fy_private_cwl_folder)

writeOGR(fy_private_cwl_spdf, layer = 'fy_private_cwl', dsn = fy_private_cwl_folder, driver = "ESRI Shapefile")

# 
# #Bonus ------
# #created before and no longer needed, but keeping in here just in case
# 
# gw_query <- paste("select distinct(smp_id) from fieldwork.deployment_full where (collection_dtime_est >='", 
#                    fystart, 
#                    "' OR collection_dtime_est is null) and deployment_dtime_est <= '", 
#                    fyend, 
#                    "' and (ow_suffix like 'GW%' or ow_suffix like 'CW%') 
#                   and smp_id is not null")
# fy_gw <- dbGetQuery(mars, gw_query)
# 
# 
# 
# 
# srt_query <- paste("select distinct system_id from fieldwork.srt where test_date >='",
#                   fystart,
#                   "' and test_date <= '",
#                   fyend,
#                   "'")
# fy_srt <- dbGetQuery(mars, srt_query)
# 
# 
# 
# post_con_cet_query <- paste("select distinct system_id from fieldwork.capture_efficiency_full where test_date >='", 
#                             fystart, 
#                             "' and test_date <= '", 
#                             fyend, 
#                             "' and phase = 'Post-Construction'")
# 
# fy_post_con_cet <- dbGetQuery(mars, post_con_cet_query)
# 
# 
# data_gw <- fy_gw %>% left_join(smp_loc) %>% select(-smp_loc_uid)
# data_pp <- fy_pp %>% left_join(smp_loc)  %>% select(-smp_loc_uid)
# data_srt <- mutate(smp_loc, system_id = gsub("-\\d+$", "", smp_id)) %>% 
#               group_by(system_id) %>%
#               summarize(lon_wgs84 = mean(lon_wgs84), lat_wgs84 = mean(lat_wgs84)) %>%
#               right_join(fy_srt)
# 
# #cwl
# cwl_coord <- data_cwl[,c(2,3)]
# 
# cwl_spdf <- SpatialPointsDataFrame(coords = cwl_coord, data = data_cwl,proj4string = crs)
# 
# writeOGR(cwl_spdf, layer = 'fy21_total_monitored_smps', dsn = 'O:/Watershed Sciences/GSI Monitoring/03 Reports and Presentations/01 Regulatory/Annual Report Submissions/2021/supporting_files', driver = "ESRI Shapefile")
# 
# # #gw
# # gw_coord <- data_gw[,c(2,3)]
# # 
# # gw_spdf <- SpatialPointsDataFrame(coords = gw_coord, data = data_gw, proj4string = crs)
# # 
# # writeOGR(gw_spdf, layer = 'fy21_groundwater_monitoring', dsn = 'O:/Watershed Sciences/GSI Monitoring/03 Reports and Presentations/01 Regulatory/Annual Report Submissions/2021/supporting_files', driver = "ESRI Shapefile")
# 
# #srt
# srt_coord <- data_srt[,c(2,3)]
# 
# srt_spdf <- SpatialPointsDataFrame(coords = srt_coord, data = data_srt, proj4string = crs)
# 
# writeOGR(srt_spdf, layer = 'fy21_simulated_runoff_tests', dsn = 'O:/Watershed Sciences/GSI Monitoring/03 Reports and Presentations/01 Regulatory/Annual Report Submissions/2021/supporting_files', driver = "ESRI Shapefile")
# 
# #pp
# pp_coord <- data_pp[,c(2,3)]
# 
# pp_spdf <- SpatialPointsDataFrame(coords = pp_coord, data = data_pp, proj4string = crs)
# 
# writeOGR(pp_spdf, layer = 'fy21_porous_pavement_tests', dsn = 'O:/Watershed Sciences/GSI Monitoring/03 Reports and Presentations/01 Regulatory/Annual Report Submissions/2021/supporting_files', driver = "ESRI Shapefile")
