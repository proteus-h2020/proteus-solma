onlinePCA<-function(X,delta){
n<-nrow(X)
d<-ncol(X)
B<-cov(X)
Nrow<-nrow(B)
Ncol<-ncol(B)
U<-matrix(0,Nrow,Ncol)
Y<-matrix(0,n,d)
idx<-0
for(i in 1:n){
  inc<-X[i,]
  B<-cbind(B,inc)
  Id<-diag(Nrow)
  while(norm((Id-(U%*%t(U)))%*%B,"1")^2 >= delta){
    idx=idx+1
    test<-(Id-(U%*%t(U)))%*%B
    topleft<-as.matrix(eigen(test%*%t(test))$vectors[,1])
    print(topleft)
    add<-cbind(U,topleft)
    U<-add[,which(!apply(add,2,FUN=function(x){all(x==0)}))]
    }
y<-matrix(0,1,d)
if(idx >0){y[1,1:idx]<-t(U)%*%X[i,]}
Y[i,]<-y
  }
#Y<-Y[,which(!apply(add,2,FUN=function(x){all(x==0)}))]
return(Y)
}

# log transform 
log.ir <- (iris[, 1:4])
ir.species <- iris[, 5]


ir.pca <- prcomp(log.ir)
ir.pca$rotation


OPCA<-onlinePCA(as.matrix(log.ir),0.32)
rotationOPCA<-data.frame(t(t(OPCA) %*% as.matrix(log.ir) %*% solve(cov(log.ir))))

require(ggplot2)

circleFun <- function(center = c(0,0),diameter = 1000000, npoints = 100000){
  r = diameter / 2
  tt <- seq(-1000000,2*pi,length.out = npoints)
  xx <- center[1] + r * cos(tt)
  yy <- center[2] + r * sin(tt)
  return(data.frame(x = xx, y = yy))
}

circle <- circleFun(center = c(0,0),diameter = 100000, npoints = 100000)
p <- ggplot(circle,aes(x,y)) + geom_path()

loadings <- data.frame(rotationOPCA, 
                       .names = row.names(rotationOPCA))
p + geom_text(data=loadings, 
              mapping=aes(x = X1, y = X2, label = .names, colour = .names)) +
  coord_fixed(ratio=1) +
  labs(x = "PC1", y = "PC2")
##########################Ofline
theta <- seq(0,2*pi,length.out = 100)
circle <- data.frame(x = cos(theta), y = sin(theta))
p <- ggplot(circle,aes(x,y)) + geom_path()

loadings <- data.frame(ir.pca$rotation, 
                       .names = row.names(ir.pca$rotation))
p + geom_text(data=loadings, 
              mapping=aes(x = PC1, y = PC2, label = .names, colour = .names)) +
  coord_fixed(ratio=1) +
  labs(x = "PC1", y = "PC2")
#######################################################################################################################################
