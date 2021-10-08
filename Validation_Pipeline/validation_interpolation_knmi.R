library(ncdf4)
library(raster)
library(BAMMtools)
library(RColorBrewer)
library(oceanmap)
library(rgeos)

# Set up the environment for processing in Rstudio
# rm(list = ls())
# path <- rstudioapi::getActiveDocumentContext()$path
# setwd(dirname(path))

RasterTemplate <- raster('nederland_1000.tif')
test_variable <- 'TN'
test_date <- '20140801'

file_path <- paste0('./',test_variable,'/',test_date,'/KNMITEST.nc')
pal <- colorRampPalette(c("blue", "red"))

knmi_stations <- nc2raster(file_path, "stations",layer=1)
names(knmi_stations) <- c('values')
vals_stations <- na.omit(c(as.matrix(knmi_stations$values)))


knmi_stations_values <- nc2raster(file_path, "stationvalues",layer=1)
names(knmi_stations_values) <- c('values')
vals_stations_values <- na.omit(c(as.matrix(knmi_stations_values$values)))

knmi_longitude <- nc2raster(file_path, "lon",layer=1)
names(knmi_longitude) <- c('values')
vals_longitude <- na.omit(c(as.matrix(knmi_longitude$values)))
vals_station_longitude <- vals_longitude[!c(as.matrix(is.na(knmi_stations_values)))]

knmi_latitude <- nc2raster(file_path, "lat",layer=1)
names(knmi_latitude) <- c('values')
vals_latitude <- na.omit(c(as.matrix(knmi_latitude$values)))
vals_station_latitude <- vals_latitude[!c(as.matrix(is.na(knmi_stations_values)))]

station_complete_from_knmi <- data.frame(vals_stations, vals_station_longitude, vals_station_latitude, vals_stations_values)
names(station_complete_from_knmi) <- c("STN", "LON", "LAT", "value")
P_2 <- SpatialPointsDataFrame(station_complete_from_knmi[,2:3],
                              station_complete_from_knmi,
                              proj4string = CRS("+proj=longlat +datum=WGS84"))

station_coordinates <- read.csv('StationCoordinates')
station_values <- read.csv(paste0('./',test_variable,'/',test_date,'/STATION_VALUES'))
station_complete <- merge(station_coordinates, station_values, by="STN")
p_temp <- data.frame(station_complete$STN, station_complete$LON, station_complete$LAT, station_complete$TG*0.1)
names(p_temp) <- c("STN", "LON", "LAT", "value")
p_temp <- p_temp[complete.cases(p_temp),]
P <- SpatialPointsDataFrame(p_temp[,2:3],
                            p_temp,
                            proj4string = CRS("+proj=longlat +datum=WGS84"))

RasterTemplate <- raster('nederland_1000.tif')
RasterTemplate_Proj <- projectRaster(RasterTemplate, crs = CRS("+proj=longlat +datum=WGS84"))
knmi_prediction <- nc2raster(file_path, "prediction",layer=1)

g <- as(!is.na(RasterTemplate_Proj), 'SpatialGridDataFrame')
P.idw <- gstat::idw(value ~ 1, P, newdata=g, idp=2.5)
Interpolated_raster  <- raster(P.idw)

mask_raster <- !is.na(knmi_prediction)
mask_raster[mask_raster==0]<- NA

# global_error <- c()
# for (blosck_size in seq(0,1,0.05)){
P_2.iwd <- gstat::idw(value ~ 1, P_2, newdata=g, idp=2.5, block=c(0.3, 0.3))
Interpolated_raster_2 <- raster(P_2.iwd)
Interpolated_raster_2_rs <- resample(Interpolated_raster_2, knmi_prediction)
Interpolated_raster_clipped <- mask(Interpolated_raster_2_rs, mask_raster)
sqr_error <- (Interpolated_raster_clipped-knmi_prediction)**2
error <- sum(na.omit(c(as.matrix(sqr_error))))
  # global_error <- c(global_error, error)
# }
# global_error

plot(Interpolated_raster_clipped)
plot(knmi_prediction)
plot(sqr_error)