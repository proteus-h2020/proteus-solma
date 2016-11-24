# the main algorithm in the paper call " Random Projection ensemble classification"

# http://arxiv.org/abs/1504.04595


#########################################

install.packages("MASS")
install.packages("class")
install.packages("VGAM")
install.packages("mvtnorm")
install.packages("Rlab")
install.packages("stats4")
install.packages("splines")

library(MASS) # for mvrnorm

library(class) # for knn

library(VGAM) # for laplace

library(mvtnorm) # for t-distribution

library(Rlab) # for rben

#########################################

# functions using in main function


#########################################

# choice for alpha

choiceA=function(t){

  pi1=sum(y==1)/nt

  pi2=sum(y==2)/nt

  pi1*(sum(Vn[1:(round(nt*pi))]<t)/(round(nt*pi)))+pi2*(1-(sum(Vn[(round(nt*pi)+1):nt]<t)/(nt-round(nt*pi))))

}

# basic classifer KNN, LDA, and QDA 

KNN=function(X1,X,y,n){

  return(knn(X1,X,y,k=5,prob=T)[1:n])

}

QDA=function(X1,X,y,n){

  zq <- qda(X1, y)

  return(predict(zq, X)$class)

}



LDA=function(X1,X,y,n){

  zq <- lda(X1, y)

  return(predict(zq, X)$class)

}

# choosing best B1 number projection function

bestProjection<-function(p,d,x,y,B1,B2,FUN){

  # choose the best B1 number of projections 

  # using train data set to choose best B1 projections

  projectionB1<-matrix(nrow=B1*d,ncol=p)

  for(i in 1:B1){

    testErrorB2=NULL

    A=matrix(nrow=d*B2, ncol=p) # used for storing B2 number of projection

    for(j in 1:B2){

      # using Haar measure to generate projection 

      R=matrix(rnorm(d*p),d) # generate a matrix, where each entry is drawn from normal 

      projectionB2=t(svd(t(R))$u) # the projection (dimension:d*p) 

      A[((j-1)*d+1):(j*d),]=projectionB2

      X=matrix(nrow=n,ncol=d) # used to storing the projected x

      for(z in 1:nrow(x)){

        X[z,]=projectionB2%*%x[z,] # (d*p)%*%(p*1)=(d*1) 

      }

      testErrorB2[j]=sum(FUN(X,X,y,n)!=y)/nrow(X)

    } 

    j=which.min(testErrorB2)

    projectionB1[((i-1)*d+1):(i*d),]=A[((j-1)*d+1):(j*d),]

  }

  return(projectionB1)

}



#########################################

#

# simulated data function

#

#########################################

#1 two normal distribution 

normal=function(n,pi){

  x1=mvrnorm(n = round(n*pi), mu=rep(0,times=p), Sigma=diag(p))

  x2=mvrnorm(n = n-round(n*pi), mu=rep(1/8,times=p), Sigma=diag(p))

  return(rbind(x1,x2))

}



#2 lapace+normal distribution 

laplace=function(n,pi){

  x1=matrix(rlaplace(round(n*pi)*p,0,1), ncol=p)

  x2=mvrnorm(n = n-round(n*pi), mu=rep(1/8,times=p), Sigma=diag(p))

  return(rbind(x1,x2))

}



#3 multi t-distribution

tdistribution=function(n,pi){

  sigma1=diag(p)

  sigma2=diag(p)

  for(i in 1:p){

    for(j in 1:p){

      if(sigma2[i,j]==0){

        if(max(i,j)<=5){

          sigma2[i,j]=0.5

        }

      }

    }

  }

  x1=rmvt(round(n*pi), sigma = sigma1, df = 1, delta = rep(0, nrow(sigma1)),

          type = "shifted")

  x2=rmvt(n-round(n*pi), sigma = sigma2, df = 2, delta = 2*rep(c(1,0), c(5,p-5)),

          type ="shifted")

  return(rbind(x1,x2))

}



#4 multi-modal features

multi=function(n,pi){

  b=rbern(round(n*pi), 1/2)

  x1=matrix(ncol=p,nrow=round(n*pi))

  for(i in 1:round(n*pi)){

    if(b[i]==1){

      x1[i,]=mvrnorm(n = 1, mu=rep(c(1,0), c(5,p-5)), Sigma=diag(p))

    }

    else{

      x1[i,]=mvrnorm(n = 1, mu=-rep(c(1,0), c(5,p-5)), Sigma=diag(p))

    }

  }

  x2=matrix(nrow=n-round(n*pi), ncol=p)

  for(i in 1:(n-round(n*pi))){

    x2[i,]=c(rcauchy(5,0,1),rnorm(p-5,0,1))

  }

  return(rbind(x1,x2))

}



#########################################

#

# main algorithm (random projection enemble method) 

#

#########################################

B1=100 # we will have total B1 projection 

B2=100 # in each B2, we get the best projection 

n=200 # total data size

nt=1000

pi=0.5

p=50 # original dimension 

d=5 # projected dimension 

FUN=KNN # FUN is the function of base classifer (KNN,LDA,QDA)

simulation=multi # normal,laplace,tdistribution,multi

y=rep(c(1,2),times=c(round(n*pi),(n-round(n*pi)))) # respsonse variable for train data

yt=rep(c(1,2),times=c(round(nt*pi),(nt-round(nt*pi)))) # response variable for test data

x=simulation(n,pi)

xt=simulation(nt,pi)

start=Sys.time()

projectionB1=bestProjection(p,d,x,y,B1,B2,FUN)

Sys.time()-start

# Randan Projection Classifier 

# using test data set to calculate the test error 

c=matrix(nrow=B1,ncol=nrow(xt))

for(i in 1:B1){

  X=matrix(nrow=nrow(x),ncol=d)

  for(j in 1:nrow(x)){

    X[j,]=projectionB1[((i-1)*d+1):(i*d),]%*%x[j,]

  }

  Xt=matrix(nrow=nrow(xt),ncol=d)

  for(j in 1:nrow(xt)){

    Xt[j,]=projectionB1[((i-1)*d+1):(i*d),]%*%xt[j,]

  }

  c[i,]=FUN(X,Xt,y,nt)

}

c=ifelse(c==1,1,0)

c=as.data.frame(c)

Vn=sapply(c,mean)

alpha=optimize(choiceA, interval=c(0, 1), maximum = FALSE)$minimum

Cn=ifelse(Vn>alpha,1,2)

cat("final error:",(sum(Cn!=yt)/nt))





#########################################

#

# regual classification method

#

#########################################

p=50 # original dimension 

d=5 # projected dimension 

B1=100 # we will have total B1 projection 

B2=100 # in each B2, we get the best projection 

n=200 # total data size

nt=1000

pi=0.33

y=rep(c(1,2),times=c(round(n*pi),(n-round(n*pi)))) # respsonse variable for train data

yt=rep(c(1,2),times=c(round(nt*pi),(nt-round(nt*pi)))) # response variable for test data

FUN=QDA

simulation=laplace

final1=NULL

for(i in 1:100){

  x=simulation(n,pi)

  xt=simulation(nt,pi)

  final1[i]=sum(FUN(x,xt,y,nt)!=yt)/nt

}

mean(final1)

sd(final1)

