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

# Define UI 
ui <- fluidPage(
  navbarPage(
    "MARS Annual Report Stats",   
    tabPanel("Stats", 
             
             sidebarLayout(
               sidebarPanel(
                 selectizeInput(
                   'system_id', label = 'Fiscal Year', choices = "FY22", selected = "FY22",
                   options = list(maxOptions = 5)
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
  

    output$`Table 3-1` <- renderReactable(reactable(head(mtcars)))
    output$`Table 3-2` <- renderReactable(reactable(tail(mtcars)))
    output$`Table 3-3` <- renderReactable(reactable(tail(mtcars)))
    output$`Table 3-4` <- renderReactable(reactable(tail(mtcars)))
    output$`Table 3-5` <- renderReactable(reactable(tail(mtcars)))
    output$`Table 3-6` <- renderReactable(reactable(tail(mtcars)))
    output$`Table 3-7` <- renderReactable(reactable(tail(mtcars)))
    output$`Table 3-8` <- renderReactable(reactable(tail(mtcars)))
    output$`Table 3-9` <- renderReactable(reactable(tail(mtcars)))
    output$`Table 3-10` <- renderReactable(reactable(tail(mtcars)))
    output$`Table 3-11` <- renderReactable(reactable(tail(mtcars)))
    output$`Table 3-12` <- renderReactable(reactable(tail(mtcars)))
    output$`Table 3-13` <- renderReactable(reactable(tail(mtcars)))
    output$`Table 3-14` <- renderReactable(reactable(tail(mtcars)))
    
    
}

# Run the application 
shinyApp(ui = ui, server = server)
