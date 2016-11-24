#Following is a sort of a psudo-code for a object-oriented programmers.

#sketch<-function(A,ell){
#	if(ell%/%2==1){
#	stop('ell must be an even number')
#	}
#	n_samples<-nrow(A)
#	n_features<-ncol(A)
#	if(ell> n_samples){
#		stop('ell must be less than n_samples')
#	}
#	if(ell > n_features){
#		stop('ell must be less than n_features')
#	}
#	B<-matrix(0,ell,n_features)
#	for(i in 1:n_samples){
#		ind<-row(B)[which(rowSums(B)==round(0,7),arr.ind = T)]
#		zrows<-sort(ind)
#		if(length(zrows)>=1){
#			B[zrows[1],]<- A[idx+i,]	
#			}else{
#			U<-svd(B)$u
#			sigma<-svd(B)$d
#			V<-svd(B)$v
#			delta<-(sigma[floor(ell/2)])^2
#			sigma<-sqrt(pmax(diag(sigma,ell,ell)-(diag(ell)*delta),0))
#			B<-sigma%*%t(V)
#			}B
#			AA<-t(A)%*%A
#			BB<-t(B)%*%B
#			error<-norm(AA-BB,"1")
#			if(i==1){
#				error_first=error
#			}
#			print(error/error_first)
#			print(length(zrows)>=1)
#		}
#	 return(B)
#	}	
#######Implementation#############			
#set.seed(0)			
#mat_a<-matrix(rnorm(100*20,mean=38,sd=56), 1000, 200) 
#ell<-100
#mat_b<-sketch(mat_a,ell)

#######################################################################

#Being a functional programmer I following more intutive and faster.

Sketch<-function(rt,B,zero_row,ell){
	if(zero_row <= ell){
		B[zero_row,]<-rt
		return(B)
	}else{
		U<-svd(B)$u
		sigma<-svd(B)$d
		V<-svd(B)$v
		delta<-(sigma[floor(ell/2)])^2
		sigma<-sqrt(pmax(diag(sigma,ell,ell)-(diag(ell)*delta),0))
		B<-sigma%*%t(V)
		zr<-as.integer(ell/2)
		return(B)
		}
}

SimpleSketch<-function(A,ell){
	n<-nrow(A)
	d<-ncol(A)
	zero_row<-ell+1
	B<-matrix(0,ell,d)
	B[1:ell,]<-A[1:ell,]
	count<-ell+1
	for(i in count:n){
	  B<-Sketch(A[i,],B,zero_row,ell)
	}
	return(B)
}

set.seed(0)			
mat_a<-matrix(rnorm(100*20,mean=38,sd=56), 1000, 200) 
ell<-100
mat_b<-SimpleSketch(mat_a,ell)

AA<-t(mat_a)%*%mat_a
BB<-t(mat_b)%*%mat_b


#The following bound(aprox) is TRUE
norm(AA-BB,"1") <= (norm(mat_a,"f")^2)*ell

#For distributive implementation consider:
#1)local matrix A and C
#2)apply SimpleSketch on A and C
#3)SimpleSketch(cbind(SimpleSketch(A),SimpleSketch(C)))

#Idea can easily be extended to more machines...

#######################################
#Note:- A could be thought of an iterator instead of an array for the streaming case.

PCA2<-function(X,k,ep,w0){
	n<-nrow(X)
	d<-ncol(X)
	ell<-as.integer(ceiling(k/ep^3))
	I<-diag(d)
	U<-vector(mode="numeric",length=0)
	ell2<-as.integer(ceiling(k/ep^2))
	Z<-matrix(0,d,1)
	w<-0
	w_u<-vector(mode="numeric",length=0)
	idx<-0
	Y<-matrix(0,k,1)
	for(t in 1:n){
		x_t<-t(X[t,])
		w<-w+norm(x_t)^2
		r<-x_t-(U%*%t(U)%*%x_t)
		c1<-(I-(U%*%t(U)))
		C<-c%*%(Z%*%t(Z))%*%c1
		zerow_row<-1
		while(norm(C+(r%*%t(r)))>=(max(w0,w)/(k/(ep^2)))){
		lambda<-eigen(C)$values
		if( idx<ell){
			u<-eigen(C)$vectors
		    w_u<-rbind(w_u,Re(lambda[1]))
		    U<-cbind(U,Re(u))
		    idx<-idx+1
		    }else{
		    	idx<-order(w_u)[1]
		    	val<-w_u[idx]
		    	w_u[idx]<-lambda[1]
		    	U[,idx]<-u
		    	idx<-ell
		    }
		    c1<-(I-(U%*%t(U)))
		    C<-c1%*%(Z%*%t(Z))%*%c1
		    r<-x_t-((U%*%t(U))%*%x_t)
		}
		Z<-SimpleSketch(r,t(Z))
		Z<-t(Z)
		for(i in 1:idx){
			u<-U[,i]
			w_u[i]<-w_u[i]+((t(u)%*%x_t)[1])^2
		}
		y<-matrix(0,1,k)
		if(idx > 0){
			y[1,1:idx]<-t(U)%*%x_t
		}
		Y<-rbind(Y,y)
	}
	return(Y)
}


set.seed(0)			
mat_a<-matrix(rnorm(100*20,mean=38,sd=56), 1000, 200) 
ell<-100
mat_b<-PCA2(mat_a,10,0.3,3)

