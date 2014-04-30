library(ggplot2) 
library(zoo) 

# sets current directory
setwd("/home/andrea/Programming/code/hadoop/MapReduce")

# loads the mean temperatures
temp <- read.csv(file="results.txt", sep="\t", header=0)

# sets column names
names(temp) <- c("date","temperature")

# splits the date into year and month
ym <- as.yearmon(temp$date, format = "%m-%Y"); 
year <- format(ym, "%Y") 
month <- format(ym, "%m")

# plots it
ggplot(temp, aes(x=month, y=temperature, group=year)) + geom_line(aes(colour = year))

