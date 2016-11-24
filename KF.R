# Example inspired by
# https://idontgetoutmuch.wordpress.com/2014/09/09/fun-with-extended-kalman-filters-4/

# Logistic growth function
logistG <- function(r, p, k, t){
  k * p * exp(r*t) / (k + p * (exp(r*t) - 1))
}

k <- 100
p0 <- 0.1*k
r <- 0.2
deltaT <- 0.1

# Let's create some sample data:
set.seed(12345)

obsVariance <- 25
nObs = 250
nu <- rnorm(nObs, mean=0, sd=sqrt(obsVariance)) 
pop <- c(p0, logistG(r, p0, k, (1:(nObs-1))*deltaT)) + nu

Estimate <- data.frame(Rate=rep(NA, nObs),
                       Population=rep(NA,nObs))
                       
install.packages("numDeriv")
library(numDeriv)
a <- function(x, k, deltaT){
  c(r=x[1],
    logistG(r=x[1], p=x[2], k, deltaT))
}
G <- t(c(0, 1))

# Evolution error
Q <- diag(c(0, 0))
# Observation error
R <-  obsVariance
# Prior
x <- c(r, p0)
Sigma <-  diag(c(144, 25))

for(i in 1:nObs){
  # Observation
  xobs <- c(0, pop[i])
  y <- G %*% xobs
  # Filter  
  SigTermInv <- solve(G %*% Sigma %*% t(G) + R)
  xf <- x + Sigma %*% t(G) %*%  SigTermInv %*% (y - G %*% x)
  Sigma <- Sigma - Sigma %*% t(G) %*% SigTermInv %*% G %*% Sigma 
  
  A <- jacobian(a, x=x, k=k, deltaT=deltaT)   
  K <- A %*% Sigma %*% t(G) %*% solve(G %*% Sigma %*% t(G) + R)
  Estimate[i,] <- x
  
  # Predict
  x <- a(x=xf, k=k, deltaT=deltaT) + K %*% (y - G %*% xf)
  Sigma <- A %*% Sigma %*% t(A) - K %*% G %*% Sigma %*% t(A) + Q
}

# Plot output
op <- par(mfrow=c(2,1))
time <- c(1:nObs)*deltaT
plot(y=pop, x=time, t='l', main="Population growth", 
     xlab="Time", ylab="Population")
curve(logistG(r, p0, k, x),  from=0, to=max(time), col=2, add=TRUE, lwd=1)
lines(y=Estimate$Population, x=time, col="orange", lwd=2)
legend("bottomright", 
       legend=c("Data","Actual", "Estimate"), 
       bty="n",
       col=c("black", "red", "orange"),
       lty=1, lwd=2)
plot(y=Estimate$Rate, x=time, t='l', main="Estimated growth rate", 
     xlab="Time", ylab="Rate", col="orange", lwd=2)
abline(h=r, col=adjustcolor("red", alpha=0.5), lwd=2)
legend("topright", 
       legend=c("Actual", "Estimate"), 
       bty="n",
       col=c("red", "orange"),
       lty=1, lwd=2)
par(op)

