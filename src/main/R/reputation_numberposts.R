# sets current directory
setwd("/home/andrea/Programming/code/hadoop/MapReduce")

# loads the user reputation/no of posts data
users <- read.csv(file="part-r-00000",sep='\t', header=0)

# plots reputation on X axis and number of posts on Y axis
plot(users$V2, users$V3, xlab="Reputation", ylab="Number of posts", pch=19, cex=0.5)

# since there are a few outliers, removes them
users$V2[which(users$V2 > 10000,)] <- 0

# re-plot the cleaned data
plot(users$V2, users$V3, xlab="Reputation", ylab="Number of posts", pch=19, cex=0.4)
