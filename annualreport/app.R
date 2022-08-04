#
# This is a Shiny web application. You can run the application by clicking
# the 'Run App' button above.
#
# Find out more about building applications with Shiny here:
#
#    http://shiny.rstudio.com/
#

library(shiny)
library(reactable)
library(odbc)
library(DBI)
library(lubridate)
library(tidyverse)
library(dplyr)

## Database Connection to pg9
#connection 
con <- odbc::dbConnect(odbc::odbc(), "mars_testing")

#FY choices for input
years_vector <- as.character(12:99)

# Define UI 
ui <- fluidPage(
  navbarPage(
    "MARS Annual Report Stats",   
    tabPanel("Stats", 
             
             sidebarLayout(
               sidebarPanel(
                 selectizeInput(
                   'fy', label = 'Fiscal Year', choices = years_vector, selected = "22",
                   options = list(maxOptions = 30)
                 ), width = 2
               ), 
               mainPanel(tabsetPanel(
                 tabPanel("Table 3-1",reactableOutput("Table 3-1")
                 ), 
                 tabPanel("Table 3-2",reactableOutput("Table 3-2")
                 ), 
                 tabPanel("Table 3-3",reactableOutput("Table 3-3")
                 ),
                 tabPanel("Table 3-4",reactableOutput("Table 3-4")
                 ),
                 tabPanel("Table 3-5",reactableOutput("Table 3-5")
                 ),
                 tabPanel("Table 3-6",reactableOutput("Table 3-6")
                 ),
                 tabPanel("Table 3-7",reactableOutput("Table 3-7")
                 ),
                 tabPanel("Table 3-8",reactableOutput("Table 3-8")
                 ),
                 tabPanel("Table 3-9",reactableOutput("Table 3-9")
                 ),
                 tabPanel("Table 3-10",reactableOutput("Table 3-10")
                 ),
                 tabPanel("Table 3-11",reactableOutput("Table 3-11")
                 ),
                 tabPanel("Table 3-12",reactableOutput("Table 3-12")
                 ),
                 tabPanel("Table 3-13",reactableOutput("Table 3-13")
                 ),
                 tabPanel("Table 3-14",reactableOutput("Table 3-14")
                 ) 
               ), width = 10
               )
             )
             
          
    ),
    tabPanel("Help",verbatimTextOutput("help_text")
    ),
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
      return(table_3_2)
      
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
    output$`Table 3-11` <- renderReactable(reactable(table_311()))
    output$`Table 3-12` <- renderReactable(reactable(table_312()))
    output$`Table 3-13` <- renderReactable(reactable(table_313()))
    output$`Table 3-14` <- renderReactable(reactable(table_314()))
    
    
}

# Run the application 
shinyApp(ui = ui, server = server)
