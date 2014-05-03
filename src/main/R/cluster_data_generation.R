N <- 100
x <- rnorm(N) +1; y <- rnorm(N) +1
dat <-data.frame(x, y)

x <- rnorm(N) +5; y <- rnorm(N) +1
dat <- rbind(dat, data.frame(x, y))

x <- rnorm(N) +1; y <- rnorm(N) +5
dat <- rbind(dat, data.frame(x, y))

plot(dat)
write.table(dat, file="points.dat")
