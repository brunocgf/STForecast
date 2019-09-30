library(lubridate)
library(readr)
library(dplyr)
library(tidyr)
library(RODBC)
library(feather)

connectionString <- 'Driver=SQL Server;Server=seguroenergydb.database.windows.net,1433;Database=seguroenergy;Uid=seguroenergy;Pwd=SeGurO$67!;Encrypt=yes;'
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
