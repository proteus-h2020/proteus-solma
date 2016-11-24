ReservoirS <- function(DStream, R){  
  # DStream is the data stream 
  # R is the reservoir size
  k <- 1   
  ReserS <- NULL
  while (sum(is.na(DStream[k,]))==0) {    #  if Dstream is not exausted, keep the item and do the reservoir sampling
    DS <- DStream[k,]      
    if (k <= R){
      ReserS <- rbind(ReserS,DS)   # keep the first R items in the stream to the reservoir
    }
    else {
      j <- sample(1:k,1)    # random generate an integer j from 1 to k
      if (j < R)
        ReserS[j,] <- DS      # if j < R, then replace the jth item in the reservoir with the kth stream item
    }   
    k <- k+1
  }
  return(ReserS)
}
