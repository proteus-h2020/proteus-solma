#Implementation of one of the Data from the conference Paper Aggregation Algorithm Vs Simple Average for time series

DailyTempAusMax<- read.csv("MaxTempAus.csv",T)
max_temp_aus<-ts(DailyTempAusMax,frequency=365,start=1980)

#########################################################################
#MAXIMUM DAILY TEMPERATURE PREDICTIONS FROM THE MODEL USED
#########################################################################

#Uncoment the following lines if you wish to run the code. The file PredictionsBigDataMax.csv reffers to these results.

#Cross-Validation for optimum value of k

ymax<-window(max_temp_aus,frquency=365,start=1980,end=c(1981,0))
xmax<-1:length(ymax)
qmax<-floor(365/2)-1
fit_gmax<-McSpatial::fourier(ymax~xmax,minq=1,maxq=qmax-1,crit="gcv")
fit_gmax$q

#Simple approach:
p<-0:2;d<-0:1;q<-0:2
combmax<-as.matrix(expand.grid(p,d,q))
fcmax<-matrix(0,nrow=3285,ncol=nrow(comb))
for (k in 1:(nrow(comb))){
     p<- comb[k,1];d<- comb[k,2];q<- comb[k,3]
     trainmax<-window(max_temp_aus,end=1980.999)
     fitmax<-Arima(trainmax, order=c(p,d,q),method="ML",xreg=forecast::fourier(trainmax,95))
     refitmax <-Arima(max_temp_aus, model=fitmax,xreg=forecast::fourier(max_temp_aus,95))
     fcmax[,k] <-window(fitted(refitmax),start=1981)
    }      
write.csv(fcmax,"PredictionsBigDataMax.csv",row.names=F)

#######################################################################################
#IMPLEMENTATION OF AA ON DAILY MAXIMUM TEMPERATURE DATA
#########################################################################################
#Algorithm preperation:
pre_max<- t(read.csv("PredictionsBigDataMax.csv",T))
pre_aus_max<- DailyTempAusMax[366:3650,1]
expertsPredictionsMax<- pre_max
outcomesMax<- t(pre_aus_max)
N<-nrow(expertsPredictionsMax)
col<-ncol(expertsPredictionsMax)
row<- nrow(expertsPredictionsMax)
AMax<-min(outcomesMax)
BMax<-max(outcomesMax)
etaMax<- 2 / ((BMax-AMax)^2)

#Substitition Function:
substitutionFunction<-function(p,ep){
	gAMax<- -(1/etaMax) * log(p %*% exp(-(etaMax) * t(ep - AMax)^2))
	gBMax<- -(1/etaMax) * log(p %*% exp(-(etaMax) * t(ep - BMax)^2))
 gammaMax<- (0.5*(BMax + AMax)) - ((gBMax - gAMax)/(2 * (BMax - AMax)))
	return(gammaMax)
}

#Aggregation Algorithm:
AAgpredictions<-function(expertsPredictions,outcomes){
	weights<-matrix(1,N,1)
	AApredictions<-matrix(1,col,1)
	for(t in 1:col){
	normalisedWeights<-weights/sum(weights) 
	    AApredictions[t]<-substitutionFunction(t(normalisedWeights),t(expertsPredictions[,t]))
	    weights1<-(normalisedWeights) * as.vector(exp(-etaMax * (expertsPredictions[,t] - outcomes[,t])^2))	
		weights<-weights1/sum(weights)
				}
		return(AApredictions)
}
predMax<-AAgpredictions(expertsPredictionsMax,outcomesMax)


outMax<- t(outcomesMax)
expertMax<-t(expertsPredictionsMax)
ExpertLossMax<-matrix(0,nrow=3285,ncol=18)
for(i in 1:18){
ExpertLossMax[,i]<-cumsum((expertMax[,i]-outMax)^2)
}
AALossMax<-cumsum((predMax - outMax)^2)
AvgLossMax<-cumsum(((as.matrix(rowSums(expertMax)/18)) - outMax)^2)
AALMax<-as.matrix(AALossMax)
AvgLMax<-as.matrix(AvgLossMax)

TotalAALossMax<-AALMax[3285,]
TotalAALossMax
TotalAvgLossMax<-AvgLMax[3285,]
TotalAvgLossMax
AggregationMinusExpertLossMax<-AALMax[3285,]-ExpertLossMax[3285,]
AggregationMinusExpertLossMax


