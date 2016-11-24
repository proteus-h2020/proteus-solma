#CUR matrix decompositions for improved data analysis By M. Mahoney and P. Drineas
#http://www.boutsidis.org/software.html

install.packages("corpcor")
library(corpcor)#for pseudoinverse

AlgorithmCUR<-function(A,k,c,r){
	U<-svd(B)$d
	S<-svd(B)$u
	V<-svd(B)$v
	C<-ColumnSelect(A,k,c,v)
	R<-t(ColumnSelect(t(A),k,r,u))
	U<-pseudoinverse(C,0.05) %*% %*% pseudoinverse(R,0.05)
	List<-c(C,R,U)
	return(List)
}

ColumnSelect<-function(A,k,c,v){
	m<-nrow(A)
	n<-ncol(A)
	pi<-matrix(0,1,n)
	for(j in 1:n){
		pi[j]<-norm(as.matrix((v[j,]))^2)/k
	}
	indexA<-vector()
	for(j in 1:n){
		prob_j<-min(1,c*pi[j])
		prob_j<-prob_j[1]
		if(prob_j==1){
			indexA<-c(indexA,j)
			else if(prob_j > rand){
				indexA<-c(indexA,j)
			}
		}
	}
	C<-A[,indexA]
	return(A)
}