library(shiny)
library(DBI)
library(pool)
library(RPostgreSQL)
library(DT)
library(shinysky)
library(shinydashboard)
library(dplyr)
library(tidyr)
library(lubridate)
library(ggplot2)
library(zoo)
library(scales)


mycss <- "
.shinysky-busy-indicator {
  z-index: 1000;
}
"

pass <- Sys.getenv('MJN_DB_PASS')

pool <- dbPool(
  drv = RPostgreSQL::PostgreSQL(max.con=100),
  dbname = "jds",
  host = "localhost",
  user = "jds",
  password = pass
)

getUniqueValues <- function(table, column){
  sql <- sprintf("SELECT DISTINCT %s FROM %s;", column, table)
  choices <-rbind("%",  dbGetQuery(pool, sql))
}

getUniqueValuesSolo <- function(table, column){
  sql <- sprintf("SELECT DISTINCT %s FROM %s;", column, table)
  choices <-dbGetQuery(pool, sql)
}

getUniqueValuesWithPrecedent <- function(table, column, conditional_column, i_condition){
  sql <- sprintf("SELECT DISTINCT %s FROM %s WHERE CLIENT like ?condition", column, table, conditional_column)
  query <- sqlInterpolate(pool, sql, condition = i_condition)
  choices <-rbind("%",  dbGetQuery(pool, query))
}

getMaxDate <- function(table){
  sql <- sprintf("select date from trans_data order by date desc limit 1;", table)
  dbGetQuery(pool, sql)
}

dataRetrival <- function(sql_table, start, end, i_brand, i_format, i_flavor, i_uom, i_sku, i_city, i_state, i_channel, i_client, i_pos) {
  
  if(length(i_channel) == 1) {
    #ff <- paste0('channel %like% ', paste0("'", i_channel, "'"))
    .dots = list(~channel %like% i_channel)
  } else {
    #ff <- paste0('channel %in% ', i_channel)
    .dots = list(~channel %in% i_channel)
  }
  
                       
  if(is.null(i_pos)) { i_pos = '%'}
  if(sql_table == "ag_pos_data") {
    
    #i_channel <- noquote(sprintf("(%s)",toString(i_channel)))

    src_pool(pool) %>% tbl(sql_table) %>% 
      filter(date >= start & date <= end &
               brand %like% i_brand &
               format %like% i_format &
               flavor %like% i_flavor &
               uom %like% i_uom &
               description %like% i_sku &
               city %like% i_city &
               state %like% i_state &
               client %like% i_client &
               #channel %in% i_channel &
               pos %like% i_pos) %>% 
      filter_(.dots = .dots) %>% collect(n = Inf)
  } else {
    
    src_pool(pool) %>% tbl(sql_table) %>% 
      filter(date >= start & date <= end &
               brand %like% i_brand &
               format %like% i_format &
               flavor %like% i_flavor &
               uom %like% i_uom &
               description %like% i_sku &
               city %like% i_city &
               state %like% i_state &
               #channel %in% i_channel &
               client %like% i_client)  %>% 
      filter_(.dots = .dots) %>% collect(n = Inf)
    
  }
  
  #data <- dbGetQuery(pool, query)
  #data <- tbl_df(data)
}

rollMeanRetrival <- function(data){
  if(is.null(data$SO)){
    ro <- 0
  } else {
    ro <- rollmean(data$SO, 3, align='right', fill = 0)
  }
}

plotData <- function(data){
  if(is.null(data$IN)){
    data$IN <- 0
  }
  if(is.null(data$SO)){
    data$SO <- 0
  }
  if(is.null(data$SI)){
    data$SI <- 0
  }
  
  ggplot2::ggplot(data, aes(date)) + geom_bar(aes(y = IN), stat = "identity", alpha = 0.4, fill = "black") + 
    geom_line(aes(y = SO, colour = "SO")) + 
    geom_line(aes(y = SI, colour = "SI")) +
    scale_x_date(name = "Dates", date_breaks = '1 month', date_labels = "%b %y") +
    scale_y_continuous(name = "", labels = comma, breaks = pretty_breaks(n=10))
}

tablesRetrival <- function(raw_data, data) {
  #start <- input$dti[1]
  #end <- input$dti[2]
  
  i_currency <- ""
  i_digits <- 1
  
  i_var <- switch(raw_data,
                  "Qty" = "qty",
                  "Tons" = "tons",
                  "Value" = "value")
  
  
  mp <- data %>% 
    select(date, i_var = one_of(i_var), type) %>% 
    group_by(date, type) %>% 
    summarise(i_var = sum(i_var, na.rm = T)) %>% 
    spread(type, i_var) %>% 
    arrange(date)
  
  mp$av3 <- rollMeanRetrival(mp)
  mp$DOIe <- ifelse(mp$av3 == 0,0, (mp$IN/mp$av3)*30)
  
  #mp$DOIr <- 
  
  rpi <- data %>% 
    filter(regular_doi == 'Y' & cedi == 'WHS' & type == 'IN') %>% 
    select(date, i_var = one_of(i_var), type) %>% 
    group_by(date, type) %>% 
    summarise(i_var = sum(i_var, na.rm = T))
  if(dim(rpi)[1] == 0) {
    rpi <- 0
  } else {
    rpi <- rpi %>% spread(type, i_var) %>% arrange(date)
  }
  
  rpo <- data %>% 
    filter(regular_doi == 'Y' & type == 'SO') %>% 
    select(date, i_var = one_of(i_var), type) %>% 
    group_by(date, type) %>% 
    summarise(i_var = sum(i_var, na.rm = T))
  if(dim(rpo)[1] == 0) {
    rpo <- 0
  } else {
    rpo <- rpo %>% spread(type, i_var) %>% arrange(date)
  }
  
  doir <- inner_join(rpi, rpo, by=c("date"))
  doir$av3 <- rollMeanRetrival(doir)
  doir$DOIr <- ifelse(doir$av3 == 0,0, (doir$IN/doir$av3)*30)
  
  doir <- doir %>% select(date, DOIr)
      
  
  mp <- inner_join(mp, doir, by=c("date"))
  
  mp$av3 <- NULL  
  mp <- gather(mp, type, i_var, 2:ncol(mp))
  mp <- spread(mp, date, i_var)
  
  names(mp)[2:ncol(mp)] <- format(as.Date(colnames(mp[2:ncol(mp)]), format = "%Y-%m-%d"), "%b/%y")
  
  datatable(
    data = mp,
    colnames = c('TYPE' = 'type'),
    extensions = 'FixedColumns',
    selection = 'single',
    rownames = F,
    options = list(
      dom = 't',
      ordering = F,
      scrollX = T,
      fixedColumns = list(leftColumns = 1)
      #autoWidth = T,
      #columnDefs = list(list(width = '65px', targets = "_all"))
      #pageLength = 10
    ) 
  ) %>% formatCurrency(2:ncol(mp), currency = i_currency, interval = 3, mark = ",", digits = i_digits) %>% 
    formatStyle('TYPE', target = 'row', backgroundColor = styleEqual(c("DOIe", "DOIr"), c("lightblue", "gold")))
}


###

CustomHeader <- dashboardHeader(title = "Prophet Data Management - MJN -")
CustomHeader$children[[3]]$children <- list(
  div(style="float:right;height:50px;margin-right:5px;padding-top:5px;", downloadButton("csv", "CSV")),
  div(style="float:right;height:50px;margin-right:5px;padding-top:5px;", downloadButton("tab", "TAB")) 
)

ui <- dashboardPage(skin = "black",
  CustomHeader,
  dashboardSidebar(
    sidebarMenu(
        dateRangeInput("dti",
                       label = h3('Date Range:'),
                       start = '2016-01-01', end = as.Date(getMaxDate("trans_data")$date),
                       min = '2013-01-01', max = as.Date(getMaxDate("trans_data")$date),
                       startview = 'months'
                       ),
        radioButtons("gobernor",
                     label = h3("Pos Data"),
                     choices = list("With Pos Info" = 1, "Without Pos Info" = 2), selected = 2
                     )
        )
  ),
  dashboardBody(
    tags$head(tags$style(HTML(mycss))),
    fluidRow(
      box(
        width = 6,
        title = "Product Hierarchy",
        status = "warning",
        solidHeader = T,
        collapsible = T,
        collapsed = T,
          column(6,
                 selectInput("brand",
                             label = "Brand",
                             choices = getUniqueValues("codes_master", "BRAND"),
                             selected = '%'
                 )
          ),
          column(6,
                 selectInput("format",
                             label = "Format",
                             choices = getUniqueValues("codes_master", "FORMAT"),
                             selected = '%'
                 )
          ),
          column(4,
                 selectInput("flavor",
                             label = "Flavor",
                             choices = getUniqueValues("codes_master", "FLAVOR"),
                             selected = '%'
                 )
          ),
          column(4,
                 selectInput("uom",
                             label = "Uom",
                             choices = getUniqueValues("codes_master", "UOM"),
                             selected = '%'
                 )
          ),
          column(4,
                 selectInput("sku",
                             label = "Sku",
                             choices = getUniqueValues("codes_master", "DESCRIPTION"),
                             selected = '%'
                 )
          )
        ),
      box(
        width = 6,
        title = "Client Hierarchy",
        status = "warning",
        solidHeader = T,
        collapsible = T,
        collapsed = T,
        column(6, 
          selectInput("channel",
                      label = "Channel",
                      multiple = T,
                      choices = getUniqueValuesSolo("pos_master", "CHANNEL"),
                      selected =c('DRUG WHOLESALERS ', 'PHARMACY CHAINS ', 'PRICE CLUBS', 'WHOLESALERS', 'GENERIC', 'SUPERMARKETS ')
                      )
              ),
        column(6,
          selectInput("client",
                      label = "Client",
                      choices = getUniqueValues("pos_master", "CLIENT"),
                      selected = '%'
                      )
              ),
        column(4,
          selectInput("city",
                      label = "City",
                      choices = getUniqueValues("pos_master", "CITY"),
                      selected = '%'
                      )
              ),
        column(4,
          selectInput("state",
                      label = "State",
                      choices = getUniqueValues("pos_master", "STATE"),
                      selected = '%'
                      )
              ),
        column(4,
          conditionalPanel(condition = "output.gobernor",
                           uiOutput("pos")
                           )
        )
      )
    ),
    fluidRow(
      tabBox(
        id = "raw_data",
        title = "Raw Data",
        #status = "primary",
        width = 12,
        tabPanel("Qty", div(DT::dataTableOutput("raw_qty"), style = "font-size: 90%")),
        tabPanel("Tons", div(DT::dataTableOutput("raw_ton"), style = "font-size: 90%")),
        tabPanel("Value", div(DT::dataTableOutput("raw_val"), style = "font-size: 90%"))
      )
    ),
    fluidRow(
      box(
        title = "Plot Data",
        status = "primary",
        width = 12,
        plotOutput("pto",
                   dblclick = "pto_doble_click",
                   brush = brushOpts(
                     id = "pto_brush",
                     resetOnNew = T
                     )
                   )
        ),busyIndicator()
    )
  )
)

#mp <- dataRetrival('ag_pos_data', 'COPIDROGAS')

server <- function(input, output, session) {
  
  
  output$gobernor <- reactive({
    if(input$gobernor == 1){
      TRUE
    } else {
      FALSE
    }
  })
  
  output$pos<- renderUI({
    selectInput("pos",
                label = "Pos Name",
                choices = getUniqueValuesWithPrecedent("pos_master", "pos", "client", input$client),
                selected = '%'
    )
  })
  
  mp <- reactive({
    
    gob_table <- 'ag_pos_data'
    if(input$gobernor == 2) { gob_table = 'ag_data'} 
    
    dataRetrival(gob_table, 
                 as.character(input$dti[1]), 
                 as.character(input$dti[2]), 
                 input$brand, 
                 input$format, 
                 input$flavor, 
                 input$uom,
                 input$sku,
                 input$city,
                 input$state,
                 input$channel,
                 input$client,
                 input$pos)
  })
  

  output$raw_qty <- DT::renderDataTable({
    tablesRetrival(input$raw_data, mp())
  })
  
  output$raw_ton <- DT::renderDataTable({
    tablesRetrival(input$raw_data, mp())
  })
  
  output$raw_val <- DT::renderDataTable({
    tablesRetrival(input$raw_data, mp())
  })
  
  output$pto <- renderPlot({

    #start <- input$dti[1]
    #end <- input$dti[2]
    
    #mp <- dataRetrival('ag_pos_data', input$client, as.Date(input$dti[1]), as.Date(input$dti[2]))
    
    i_var <- switch(input$raw_data,
                    "Qty" = "qty",
                    "Tons" = "tons",
                    "Value" = "value")
  
    mp <- mp() %>% 
      select(date, i_var = one_of(i_var), type) %>% 
      group_by(date, type) %>% 
      summarise(i_var = sum(i_var, na.rm = T)) %>% 
      spread(type, i_var) %>% 
      arrange(date)
    
    mp$av3 <- rollMeanRetrival(mp)
    mp$DOIe <- ifelse(mp$av3 == 0,0, (mp$IN/mp$av3)*30)
    
    mp$av3 <- NULL
    
    plotData(mp)

  })
  
  output$csv <- downloadHandler(
    filename = function(){
      paste('data', Sys.Date(), '.csv', sep = '')
    },
    content = function(file){
      write.csv(mp(), file, row.names = F)
    }
  )
  
  output$tab <- downloadHandler(
    filename = function(){
      paste('data', Sys.Date(), '.txt', sep = '')
    },
    content = function(con){
      write.table(mp(), con, sep="\t", row.names = F)
    }
  )
  
  outputOptions(output, "gobernor", suspendWhenHidden = FALSE)  
  
}


shinyApp(ui, server)
