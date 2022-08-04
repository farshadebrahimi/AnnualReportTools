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
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    #ractable table outputs
    output$`Table 3-1` <- renderReactable(reactable(table_31()))
    output$`Table 3-2` <- renderReactable(reactable(table_32()))
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
