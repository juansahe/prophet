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
library(prophet)


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
  sql <- sprintf("select date from %s order by date desc limit 1;", table)
  dbGetQuery(pool, sql)
}

getMaxDatenoIBP <- function(table){
  sql <- sprintf("select date from %s where type != 'IBP' order by date desc limit 1;", table)
  dbGetQuery(pool, sql)
}

getDoiTarget <- function(family, client) {
  
  if(dim(family)[1] > 1) {
    i_family <- 'ALL'
  } else {
    i_family <- family$family
  }
  
  if(dim(client)[1] > 1) {
    i_client <- 'ALL'
  } else {
    i_client <- client$client
  }
  
  sql <- sprintf("select target from doi_target where client = ?client and family = ?family")
  query <- sqlInterpolate(pool, sql, client = i_client, family = i_family)
  dbGetQuery(pool, query)
}

dataRetrival <- function(sql_table, start, end, i_category, i_brand, i_format, i_flavor, i_uom, i_sku, i_city, i_state, i_channel, i_client, i_pos_name, i_dfu) {
  
  if(length(i_channel) == 1) {
    #ff <- paste0('channel %like% ', paste0("'", i_channel, "'"))
    .dots = list(~channel %like% i_channel)
  } else {
    #ff <- paste0('channel %in% ', i_channel)
    .dots = list(~channel %in% i_channel)
  }
  
  
  if(is.null(i_pos_name)) { i_pos_name = '%'}
  if(sql_table == "ag_pos_data") {
    
    #i_channel <- noquote(sprintf("(%s)",toString(i_channel)))
    
    src_pool(pool) %>% tbl(sql_table) %>% 
      filter(date >= start & date <= end &
               family %like% i_category &
               brand %like% i_brand &
               format %like% i_format &
               flavor %like% i_flavor &
               uom %like% i_uom &
               description %like% i_sku &
               dfu %like% i_dfu & 
               city %like% i_city &
               state %like% i_state &
               client %like% i_client &
               #channel %in% i_channel &
               pos_name %like% i_pos_name) %>% 
      filter_(.dots = .dots) %>% collect(n = Inf)
  } else {
    
    src_pool(pool) %>% tbl(sql_table) %>% 
      filter(date >= start & date <= end &
               family %like% i_category &
               brand %like% i_brand &
               format %like% i_format &
               flavor %like% i_flavor &
               uom %like% i_uom &
               description %like% i_sku &
               dfu %like% i_dfu &
               city %like% i_city &
               state %like% i_state &
               #channel %in% i_channel &
               client %like% i_client)  %>% 
      filter_(.dots = .dots) %>% collect(n = Inf)
    
  }
  
  #data <- dbGetQuery(pool, query)
  #data <- tbl_df(data)
}

rollMeanRetrival <- function(i_data){
  if(is.null(i_data)){
    ro <- 0
  } else {
    ro <- rollmean(i_data, 3, align='right', fill = 0)
  }
}

plotData <- function(i_data){
  if(is.null(i_data$IN)){
    i_data$IN <- 0
  }
  if(is.null(i_data$SO)){
    i_data$SO <- 0
  }
  if(is.null(i_data$SI)){
    i_data$SI <- 0
  }
  
  a_end <- getMaxDatenoIBP('trans_data')
  
  if('yhat' %in% colnames(i_data)) {
    ggplot2::ggplot(i_data, aes(date)) + 
      geom_bar(aes(y = IN), stat = "identity", alpha = 0.6, fill = "black") + 
      geom_bar(aes(y = f_in), stat = "identity", alpha = 0.6, fill = "grey") +
      geom_line(aes(y = SO, colour = "Sell Out")) + 
      geom_line(aes(y = SI, colour = "Sell In")) +
      geom_line(aes(y= yhat, colour = "FCST Sell Out"), linetype = 2) +
      geom_line(aes(y = f_sellin, colour = "FCST Sell In"), linetype = 2) +
      geom_line(aes(y = IBP, colour = "IBP")) +
      geom_vline(xintercept = as.numeric(as.Date(a_end$date)), linetype = 2) +
      geom_ribbon(aes(ymin = yhat_lower, ymax = yhat_upper), fill = "darkmagenta", alpha = 0.2) +
      scale_x_date(name = "Dates", date_breaks = '1 month', date_labels = "%b %y") +
      scale_y_continuous(name = "", labels = comma, breaks = pretty_breaks(n=10)) + 
      theme(legend.title = element_blank())
  } else {
    ggplot2::ggplot(i_data, aes(date)) + 
      geom_bar(aes(y = IN), stat = "identity", alpha = 0.6, fill = "black") + 
      #geom_bar(aes(y = f_in), stat = "identity", alpha = 0.4, fill = "grey") +
      geom_line(aes(y = SO, colour = "Sell Out")) + 
      geom_line(aes(y = SI, colour = "Sell In")) +
      geom_line(aes(y = IBP, colour = "IBP")) +
      #geom_line(aes(y= yhat, colour = "FCST Sell Out"), linetype = 2) +
      #geom_line(aes(y = f_sellin, colour = "FCST Sell In"), linetype = 2 ) +
      #geom_vline(xintercept = as.numeric(as.Date(end$date)), linetype = 2) +
      #geom_ribbon(aes(ymin = yhat_lower, ymax = yhat_upper), fill = "darkmagenta", alpha = 0.1) +
      scale_x_date(name = "Dates", date_breaks = '1 month', date_labels = "%b %y") +
      scale_y_continuous(name = "", labels = comma, breaks = pretty_breaks(n=10)) + 
      theme(legend.title = element_blank())
  }
}

initSellInCalculation <- function(i_data, family, client, end){
  
  if(is.null(getDoiTarget(family, client)$target)) {
    i_data$goal <- 0
  } else {
    i_data$goal <- getDoiTarget(family, client)$target
  }
  
  i_data$av3 <- rollMeanRetrival(i_data$yhat)
  i_data <- mutate(i_data, temp = (av3/30)*goal)
  
  if(!'IN' %in% colnames(i_data)) {
    i_data$f_sellin <- 0
    i_data$f_in <- 0
  } else {
    i_data <- mutate(i_data, f_sellin = ifelse((temp-(lag(IN)-yhat))<0,0,(temp-(lag(IN)-yhat))))
    i_data <- mutate(i_data, f_in = lag(IN) + f_sellin - yhat)
    
    n <- which(i_data$date == end$date) + 1
    
    i_data$IN[n] <- i_data$f_in[n]
    i_data$SI[n] <- i_data$f_sellin[n]
    
    j <- n + 1
    for(i in j:nrow(i_data)) {
      
      i_data <- mutate(i_data, f_sellin = ifelse((temp-(lag(IN)-yhat))<0,0,(temp-(lag(IN)-yhat))))
      i_data <- mutate(i_data, f_in = lag(IN) + f_sellin - yhat)
      i_data$IN[i] <- i_data$f_in[i]
      i_data$SI[i] <- i_data$f_sellin[i]
      
    }
    
    i_data <- i_data %>% mutate(f_in = ifelse(date <= end$date, NA, f_in))
    i_data <- i_data %>% mutate(f_sellin = ifelse(date <= end$date, NA, f_sellin))
    
    i_data <- i_data %>% mutate(IN = ifelse(date > end$date, NA, IN))
    i_data <- i_data %>% mutate(SI = ifelse(date > end$date, NA, SI))
    
    
  }
  return(i_data)
}

#seqSellInCalculation <- function(n, i_data) {
#  if(!'IN' %in% colnames(i_data)) {
#    i_data$f_sellin <- 0
#    i_data$f_in <- 0
#  } else {
#    for(i in 1:n){
#      i_data <- mutate(i_data, f_sellin = ifelse((temp-(lag(f_in)-av3))<0,0,(temp-(lag(f_in)-av3))))
#      i_data <- mutate(i_data, f_in = lag(f_in) + f_sellin - av3)
#    }
#  }
#  return(i_data)
#} 

forecastEstimation <- function(i_data, doi_data, horizon){
  var <- 'SO'
  if(!var %in% colnames(i_data)) {
    var <- 'SI'
  } 
  
  a_end <- getMaxDatenoIBP('trans_data')
  
  fcst <- i_data %>% filter(date <= a_end$date) %>% select(ds=date, y=get(var)) #%>% mutate(cap = max(y)*1.1)  
  model <- prophet::prophet(fcst, growth = 'linear', yearly.seasonality = 'TRUE', weekly.seasonality = 'FALSE', n.changepoints = 12)
  future <- make_future_dataframe(model, periods = horizon, freq = "month") #make periods dynamic
  #future$cap <- max(fcst$cap)
  forecast <- predict(model, future)
  #plot(model, forecast)
  
  ff <- forecast %>% select(date = ds, yhat, yhat_lower, yhat_upper)
  d <- left_join(ff, i_data, by=c("date"))
  
  #end <- getMaxDate('trans_data')
  
  doi_client <- doi_data %>% distinct(client)
  doi_family <- doi_data %>% distinct(family)
  
  d <- initSellInCalculation(d, doi_family, doi_client, a_end)
  #n <- which(d$date == end$date) - 1
  #d <- seqSellInCalculation(n, d)
  
  return(d)
}

tablesRetrival <- function(raw_data, i_data, forecast_switch, horizon) {
  #start <- input$dti[1]
  #end <- input$dti[2]
  
  i_currency <- ""
  i_digits <- 1
  
  i_var <- switch(raw_data,
                  "Qty" = "qty",
                  "Tons" = "tons",
                  "Value" = "value")
  
  
  mp <- i_data %>% 
    select(date, i_var = one_of(i_var), type) %>% 
    group_by(date, type) %>% 
    summarise(i_var = sum(i_var, na.rm = T)) %>% 
    spread(type, i_var, fill = 0) %>% 
    arrange(date)
  
  mp$av3 <- rollMeanRetrival(mp$SO)
  mp$DOIe <- ifelse(mp$av3 == 0,0, (mp$IN/mp$av3)*30)
  
  #mp$DOIr <- 
  
  rpi <- i_data %>% 
    filter(regular_doi == 'Y' & cedi == 'WHS' & type == 'IN') %>% 
    select(date, i_var = one_of(i_var), type) %>% 
    group_by(date, type) %>% 
    summarise(i_var = sum(i_var, na.rm = T))
  if(dim(rpi)[1] == 0) {
    rpi <- 0
  } else {
    rpi <- rpi %>% spread(type, i_var) %>% arrange(date)
  }
  
  rpo <- i_data %>% 
    filter(regular_doi == 'Y' & type == 'SO') %>% 
    select(date, i_var = one_of(i_var), type) %>% 
    group_by(date, type) %>% 
    summarise(i_var = sum(i_var, na.rm = T))
  if(dim(rpo)[1] == 0) {
    rpo <- 0
  } else {
    rpo <- rpo %>% spread(type, i_var) %>% arrange(date)
  }
  
  if(rpi == 0 || rpo == 0) {
    doir <- 0
  } else {
    doir <- right_join(rpi, rpo, by=c("date")) %>% mutate(IN = ifelse(is.na(IN),0, IN))
    doir$av3 <- rollMeanRetrival(doir$SO)
    doir$DOIr <- ifelse(doir$av3 == 0,0, (doir$IN/doir$av3)*30)
    doir <- doir %>% select(date, DOIr)
    mp <- left_join(mp, doir, by=c("date"))
  }
  
  doi_data <- i_data %>% distinct(family, client)
  
  if(forecast_switch == "with_forecast") {
    
    d <- forecastEstimation(mp, doi_data, horizon)
    mp <- d %>% select(-yhat_lower, -yhat_upper, -av3, -goal, -temp, z_SO = yhat, z_SI = f_sellin, z_IN = f_in)
    
    a_end <- getMaxDatenoIBP('trans_data')
    
    mp[is.na(mp)] <- 0
    mp <- mp %>% mutate(z_SO = ifelse(z_SO < 0, 0, z_SO))
    mp <- mp %>% mutate(z_SO = ifelse(date <= a_end$date, 0, z_SO))
    if('SO' %in% colnames(mp)){
      mp$SO <- mp$z_SO + mp$SO
      mp$av3 <- rollMeanRetrival(mp$SO)
      mp$DOIee <- ifelse(mp$date >= a_end$date, (mp$z_IN/mp$av3)*30, 0)
    } else {
      mp$SO <- mp$z_SO
      mp$av3 <- rollMeanRetrival(mp$SO)
      mp$DOIee <- ifelse(mp$date >= a_end$date, (mp$z_IN/mp$av3)*30, 0)
    }
    
    #mp$SO <- mp$z_SO + mp$SO
    mp$SI <- mp$z_SI + mp$SI
    
    if('IN' %in% colnames(mp)) {
      mp$IN <- mp$z_IN + mp$IN
    } else {
      mp$IN <- mp$z_IN
    }
    
    mp$DOIe <- mp$DOIe + mp$DOIee
    
    if('DOIr' %in% colnames(mp)){
      p <- mp[which(mp$date <= a_end$date),] 
      p <- tail(p, 3)
      e_avg <- mean(p$DOIe)
      r_avg <- mean(p$DOIr)
      lp <- (r_avg/e_avg)
      mp$DOIrr <- mp$DOIee*lp
      mp$DOIrr <- ifelse(mp$date <= a_end$date, 0, mp$DOIrr)
      mp$DOIr <- mp$DOIr + mp$DOIrr
    } else {
      mp$DOIrr <- 0
    }
    
    
    #mp <- mp %>% select(-z_SO, -z_SI, -z_IN, -av3, -DOIee, -DOIrr)
    mp <- mp %>% select(-z_SO, -z_SI, -z_IN, -av3, -DOIee)
    
    
  } else {
    d <- mp
    mp <- d %>% select(-av3)
  }
  
  
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

CustomHeader <- dashboardHeader(title = "- MJN Prophet (beta) -")
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
                                       start = '2016-01-01', end = as.Date(getMaxDatenoIBP("trans_data")$date),
                                       min = '2013-01-01', max = as.Date(getMaxDatenoIBP("trans_data")$date),
                                       startview = 'months'
                        ),
                        radioButtons("gobernor",
                                     label = h3("Pos Data"),
                                     choices = list("With Pos Info" = 1, "Without Pos Info" = 2), selected = 2
                        ),
                        radioButtons("with_forecast",
                                     label = h3("Include Forecast"),
                                     choices = list("With Forecast" = "with_forecast", "WithOut Forecast" = "without_forecast"), selected = "without_forecast"
                        ),
                        uiOutput("forecast_horizon")
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
                          column(4,
                                 selectInput("category",
                                             label = "Category",
                                             choices = getUniqueValues("codes_master", "FAMILY"),
                                             selected = '%'
                                 )
                          ),
                          column(4,
                                 selectInput("brand",
                                             label = "Brand",
                                             choices = getUniqueValues("codes_master", "BRAND"),
                                             selected = '%'
                                 )
                          ),
                          column(4,
                                 selectInput("format",
                                             label = "Format",
                                             choices = getUniqueValues("codes_master", "FORMAT"),
                                             selected = '%'
                                 )
                          ),
                          column(3,
                                 selectInput("flavor",
                                             label = "Flavor",
                                             choices = getUniqueValues("codes_master", "FLAVOR"),
                                             selected = '%'
                                 )
                          ),
                          column(3,
                                 selectInput("uom",
                                             label = "Uom",
                                             choices = getUniqueValues("codes_master", "UOM"),
                                             selected = '%'
                                 )
                          ),
                          column(3,
                                 selectInput("sku",
                                             label = "Sku",
                                             choices = getUniqueValues("codes_master", "DESCRIPTION"),
                                             selected = '%'
                                 )
                          ),
                          column(3,
                                 selectInput("dfu",
                                             label = "DFU",
                                             choices = getUniqueValues("codes_master", "dfu"),
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
                          column(12, 
                                 selectInput("channel",
                                             label = "Channel",
                                             multiple = T,
                                             choices = getUniqueValuesSolo("pos_master", "CHANNEL"),
                                             selected =c('DRUG WHOLESALERS ', 'PHARMACY CHAINS ', 'PRICE CLUBS', 'WHOLESALERS', 'GENERIC', 'SUPERMARKETS ', 'HOSPITAL SALES ')
                                 )
                          ),
                          column(3,
                                 selectInput("client",
                                             label = "Client",
                                             choices = getUniqueValues("pos_master", "CLIENT"),
                                             selected = '%'
                                 )
                          ),
                          column(3,
                                 selectInput("city",
                                             label = "City",
                                             choices = getUniqueValues("pos_master", "CITY"),
                                             selected = '%'
                                 )
                          ),
                          column(3,
                                 selectInput("state",
                                             label = "State",
                                             choices = getUniqueValues("pos_master", "STATE"),
                                             selected = '%'
                                 )
                          ),
                          column(3,
                                 conditionalPanel(condition = "output.gobernor",
                                                  uiOutput("pos_name")
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
  
  output$pos_name<- renderUI({
    selectInput("pos_name",
                label = "Pos Name",
                choices = getUniqueValuesWithPrecedent("pos_master", "pos_name", "client", input$client),
                selected = '%'
    )
  })
  
  output$forecast_horizon<- renderUI({
    
    if(is.null(input$with_forecast))
      return()
    
    switch(input$with_forecast,
           
           "with_forecast" = sliderInput("forecast_horizon",
                                         label = "Forecast Horizon",
                                         min = 3, 
                                         max = 24, 
                                         value = 10)
    )
  })
  
  
  mp <- reactive({
    
    a_end <- getMaxDatenoIBP('trans_data')
    
    gob_table <- 'ag_pos_data'
    if(input$gobernor == 2) { gob_table = 'ag_data'}
    if(input$with_forecast == "with_forecast") { 
      final_date =  as.Date(a_end$date) %m+% months(input$forecast_horizon)
    } else { final_date = as.Date(input$dti[2]) }
    
    dataRetrival(gob_table, 
                 as.character(input$dti[1]), 
                 as.character(final_date), 
                 input$category,
                 input$brand, 
                 input$format, 
                 input$flavor, 
                 input$uom,
                 input$sku,
                 input$city,
                 input$state,
                 input$channel,
                 input$client,
                 input$pos_name,
                 input$dfu)
  })
  
  
  output$raw_qty <- DT::renderDataTable({
    tablesRetrival(input$raw_data, mp(), input$with_forecast, input$forecast_horizon)
  })
  
  output$raw_ton <- DT::renderDataTable({
    tablesRetrival(input$raw_data, mp(), input$with_forecast, input$forecast_horizon)
  })
  
  output$raw_val <- DT::renderDataTable({
    tablesRetrival(input$raw_data, mp(), input$with_forecast, input$forecast_horizon)
  })
  
  output$pto <- renderPlot({
    
    #start <- input$dti[1]
    #end <- input$dti[2]
    
    #mp <- dataRetrival('ag_pos_data', input$client, as.Date(input$dti[1]), as.Date(input$dti[2]))
    
    i_var <- switch(input$raw_data,
                    "Qty" = "qty",
                    "Tons" = "tons",
                    "Value" = "value")
    
    end <- getMaxDate('trans_data')
    a_end <- getMaxDatenoIBP('trans_data')
    
    mp <- mp() %>% 
      select(date, i_var = one_of(i_var), type) %>% 
      group_by(date, type) %>% 
      summarise(i_var = sum(i_var, na.rm = T)) %>% 
      spread(type, i_var, fill = 0) %>% 
      arrange(date)
    
    mp$av3 <- rollMeanRetrival(mp$SO)
    mp$DOIe <- ifelse(mp$av3 == 0,0, (mp$IN/mp$av3)*30)
    
    #mp$av3 <- NULL
    doi_data <- mp() %>% distinct(family, client)
    if (input$with_forecast == "with_forecast") {
      d <- forecastEstimation(mp, doi_data, input$forecast_horizon)
      
      d <- d %>% mutate(SO = ifelse(date >= end$date & SO < 0, 0, SO),
                        SI = ifelse(date >= end$date & SI < 0, 0, SI),
                        SO = ifelse(date > a_end$date, NA_real_, SO),
                        SI = ifelse(date > a_end$date, NA_real_, SI),
                        yhat = ifelse(date >= end$date &  yhat < 0, 0, yhat),
                        yhat_lower = ifelse(date >= end$date &  yhat_lower < 0, 0, yhat_lower),
                        yhat_upper = ifelse(date >= end$date &  yhat_upper < 0, 0, yhat_upper)
      )
    } else {
      d <- mp
    }
    
    d <- d %>% mutate(IBP = ifelse(date <= '2017-02-01', NA_real_, IBP))
    
    plotData(d)
    
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
  
  outputOptions(output, c("gobernor"), suspendWhenHidden = FALSE)
  #outputOptions(output, "with_forecast", suspendWhenHidden = FALSE)  
  
}


shinyApp(ui, server)