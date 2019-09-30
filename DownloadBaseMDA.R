library(lubridate)
library(readr)
library(dplyr)
library(tidyr)
library(RODBC)
library(feather)

myServer <- "seguroenergydb.database.windows.net"
myUser <- "seguroenergy"
myPassword <- #Agregar contraseña
myDatabase <- "seguroenergy"
myDriver <- "SQL Server" 

connectionString <- paste0(
  "Driver=", myDriver, 
  ";Server=", myServer, 
  ";Database=", myDatabase, 
  ";Uid=", myUser, 
  ";Pwd=", myPassword)
sqlQuery <- "SELECT * FROM CANTIDAD_ASIGNADA_ZONA_CARGA"
conn <- odbcDriverConnect(connectionString)
data <- sqlQuery(conn, sqlQuery)
close(conn)

datare <- rename(data,
                 Date=FECHA,
                 System=SISTEMA,
                 Hour=HORA,
                 Zone=ZONA_CARGA,
                 Direct=ENERGIA_DIRECT_MODELADA,
                 Indirect=ENERGIA_INDIRECT_MODELADA,
                 Total=ENERGIA_ASIGNADA_TOTAL)


write_feather(datare, "Data/MDALoads20190605.feather")
