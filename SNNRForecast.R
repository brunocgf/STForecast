library(feather)
library(tidyverse)
library(lubridate)
library(readxl)
# library(foreach)
library(doParallel)
# library(data.table)
library(splines)

 #startDate <- make_date(2019,6,8)
 #endDate <- make_date(2019,6,8)

startDate <- Sys.Date()+2
endDate <- Sys.Date()+2

basedata <- read_feather('Data/MDALoads20190605.feather')%>%
  mutate(Date=as.Date(Date))%>%
  group_by(Zone,Date,Hour,System)%>%
  summarise(
    Direct=first(Direct),
    Indirect=first(Indirect),
    Total=first(Total)
  )%>%
  ungroup()

disDate <- distinct(basedata,Date)%>%
  filter(Date>=make_date(2017,1,1))%>%
  arrange(Date)
LastDate <- max(disDate$Date)
disDate <- bind_rows(disDate,tibble(Date=max(disDate$Date)+1:15))

Zones <-distinct(basedata,Zone)%>%
  filter(Zone!='WECC',Zone!='NO APLICA')

disTib <- tibble(Date=rep(disDate$Date,each=24),Hour=rep(1:24,length(disDate$Date)))

holidays <- read_excel('Data/Holidays.xlsx')%>%
  mutate(Date=make_date(Year,Month,Day),Holiday=1)%>%
  select(Date,Holiday)

data <- tibble(Zone=rep(Zones$Zone,each=nrow(disTib)),Date=rep(disTib$Date,108),Hour=rep(disTib$Hour,108))%>%
  left_join(basedata,c('Zone','Date','Hour'))%>%
  left_join(holidays,'Date')%>%
  arrange(Zone,Date,Hour)%>%
  group_by(Zone)%>%
  mutate(
    Indirect=if_else(is.na(Indirect),lag(Indirect,1),Indirect),
    Direct=if_else(is.na(Direct),lag(Direct,1),Direct),
    Total=if_else(is.na(Total),lag(Total,1),Total),
    Holiday = if_else(is.na(Holiday),0,Holiday),
    WDay = wday(Date),
    WDay = if_else(WDay==7,0,as.double(WDay)),
    WDay = if_else(WDay>1 & Holiday==1,1,WDay),
    WDayB = WDay,
    WDay = if_else(WDay>1&lead(WDayB,24)<=1,-1,WDay),
    WDay = if_else(WDay>1&lag(WDayB,24)<=1,-2,WDay),
    WDay = if_else(WDay>1,2,WDay),
    fWDay = as.factor(WDay),
    fHour = as.factor(Hour),
    Year = year(Date),
    # Year = as.integer(if_else(Year==2019,2018,as.double(Year))),
    fYear = as.factor(Year)
    
  )%>%ungroup()%>%
  filter(!is.na(WDay))%>%
  select(Date,Hour,Zone,Indirect,Direct,Total,fWDay,fHour,fYear,Holiday)


daysForecast <- 14
registerDoParallel()

ndays <- 45
ResListF <- foreach(d = 1:daysForecast)%do%{
  print(paste0('Day: ',d,' Time: ',Sys.time()))
  data <- group_by(data,Zone)%>%
    mutate(
      il =lag(Indirect,24*d),
      dl =lag(Direct,24*d))%>%
    ungroup()
  
  ResList <- foreach(ds=startDate:endDate, .packages=c('tidyverse','lubridate','splines'))%dopar%{
    date <- as_date(ds)
    print(paste0('Date: ',date,' Day: ',d,' Time: ',Sys.time()))
    
    estData <- filter(data, (Date>=date-ndays&Date<date)|(Date>=date-ndays-365&Date<=date-365+ndays+d-1))%>%
      mutate(diff=date-Date,diff=if_else(abs(diff-365)<abs(diff),diff-365,diff))
    foreData <- filter(data, Date==date+d-1)%>%mutate(`Date Created`=date-1)%>%
      mutate(diff=date-Date,diff=if_else(abs(diff-365)<abs(diff),diff-365,diff))
    
    nst <- ns(c(estData$diff,foreData$diff), 3,intercept = TRUE)
    ns3e <- data.frame(nst[1:nrow(estData),])
    colnames(ns3e)<-c('ns31','ns32','ns33')
    ns3f <- data.frame(nst[-1:-nrow(estData),])
    colnames(ns3f)<-c('ns31','ns32','ns33')
    
    estData <- bind_cols(estData,ns3e)
    foreData <- bind_cols(foreData,ns3f)
    
    forRes <- group_by(estData,Zone)%>%
      do({
        zfd <- filter(foreData,Zone==.$Zone[1])
        
        fitIndirect <- lm(Indirect~fWDay*fHour*il+fHour*fYear+fHour*Holiday+fWDay*(ns31+ns32+ns33),.)
        zfd$foreInd <- predict(fitIndirect,zfd)
        print(length(estData$Direct))
        if(sum(estData$Direct,na.rm = TRUE)>0){
          fitDirect <- lm(Direct~fWDay*fHour:dl-1,.)
          zfd$foreDir <- predict(fitDirect,zfd)
        }else{
          zfd$foreDir <- 0
        }
        
        zfd <- select(zfd, Date,`Date Created`,Hour,Zone,Indirect,foreInd,Direct,foreDir,Total)%>%
          mutate(foreDir=if_else(foreDir<0,0,foreDir),foreTotal=foreInd+foreDir)
        
        zfd
      })%>%ungroup()
  }
  
  Res <- rename(bind_rows(ResList),`Load Forecast`=foreTotal,`Load Zone`=Zone)%>%
    mutate(`Date Forecasted`=Date,`Day of Forecast`=d)%>%
    select(`Load Zone`,`Date Created`,`Date Forecasted`,`Day of Forecast`,Hour,`Load Forecast`)
  
  Res
}
ResultsTrend=bind_rows(ResListF)
ResultsTrend$Model <- 'Trend'


ndays <- 30
ResListF <- foreach(d = 1:daysForecast)%do%{
  print(paste0('Day: ',d,' Time: ',Sys.time()))
  data <- group_by(data,Zone)%>%
    mutate(
      il =lag(Indirect,24*d),
      dl =lag(Direct,24*d))%>%
    ungroup()
  
  ResList <- foreach(ds=startDate:endDate, .packages=c('tidyverse','lubridate','splines'))%dopar%{
    date <- as_date(ds)
    print(paste0('Date: ',date,' Day: ',d,' Time: ',Sys.time()))
    
    estData <- filter(data, (Date>=date-ndays&Date<date)|(Date>=date-ndays-365&Date<=date-365+ndays+d-1))%>%
      mutate(diff=date-Date,diff=if_else(abs(diff-365)<abs(diff),diff-365,diff))
    foreData <- filter(data, Date==date+d-1)%>%mutate(`Date Created`=date-1)%>%
      mutate(diff=date-Date,diff=if_else(abs(diff-365)<abs(diff),diff-365,diff))
    
    nst <- ns(c(estData$diff,foreData$diff), 3,intercept = TRUE)
    ns3e <- data.frame(nst[1:nrow(estData),])
    colnames(ns3e)<-c('ns31','ns32','ns33')
    ns3f <- data.frame(nst[-1:-nrow(estData),])
    colnames(ns3f)<-c('ns31','ns32','ns33')
    
    estData <- bind_cols(estData,ns3e)
    foreData <- bind_cols(foreData,ns3f)
    
    forRes <- group_by(estData,Zone)%>%
      do({
        zfd <- filter(foreData,Zone==.$Zone[1])
        
        fitIndirect <- lm(Indirect~fWDay*fHour*il+fHour*fYear+fHour*Holiday+fWDay*(ns31+ns32+ns33),.)
        zfd$foreInd <- predict(fitIndirect,zfd)
        print(length(estData$Direct))
        if(sum(estData$Direct,na.rm = TRUE)>0){
          fitDirect <- lm(Direct~fWDay*fHour:dl-1,.)
          zfd$foreDir <- predict(fitDirect,zfd)
        }else{
          zfd$foreDir <- 0
        }
        
        zfd <- select(zfd, Date,`Date Created`,Hour,Zone,Indirect,foreInd,Direct,foreDir,Total)%>%
          mutate(foreDir=if_else(foreDir<0,0,foreDir),foreTotal=foreInd+foreDir)
        
        zfd
      })%>%ungroup()
  }
  
  Res <- rename(bind_rows(ResList),`Load Forecast`=foreTotal,`Load Zone`=Zone)%>%
    mutate(`Date Forecasted`=Date,`Day of Forecast`=d)%>%
    select(`Load Zone`,`Date Created`,`Date Forecasted`,`Day of Forecast`,Hour,`Load Forecast`)
  
  Res
}
Results30=bind_rows(ResListF)
Results30$Model <- 'D30'

Results <- bind_rows(Results30,ResultsTrend)%>%
  group_by(`Load Zone`,`Date Created`,`Date Forecasted`,`Day of Forecast`,Hour)%>%
  summarise(`Load Forecast`=mean(`Load Forecast`))%>%
  ungroup()%>%
  mutate(`Load Zone`=as.character(`Load Zone`))%>%
  rename(
    Load_Zone=`Load Zone`,
    Date_Created=`Date Created`,
    Date_Forecasted=`Date Forecasted`,
    Day_of_Forecast=`Day of Forecast`,
    Load_Forecast=`Load Forecast`
  )
Results$Model <- 'LM v2'
write_csv(Results,'ForecastST.csv')