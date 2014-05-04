# sets current directory
setwd("/home/andrea/Programming/code/hadoop/MapReduce/out/artifacts/Kmeans")

# loads the mean temperatures
points <- read.csv(file="final-data", sep="\t", header=0)
colnames(points)[1] <- "x"
colnames(points)[2] <- "y"

plot(points$x, points$y, col= points$V3+2)
