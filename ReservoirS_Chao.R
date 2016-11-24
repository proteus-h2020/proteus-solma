ReservoirS_Chao <- function(DStream, w, R){  
  # DStream is the data stream 
  # W is the weight vector
  # R is the reservoir size
  k <- 1   
  ReserS <- NULL
  w_sum <- 0
  while (sum(is.na(DStream[k,]))==0) {    #  if Dstream is not exausted, keep the item and do the reservoir sampling
    DS <- DStream[k,]   
    if (k <= R){
      ReserS <- rbind(ReserS,DS)   # keep the first R items in the stream to the reservoir
      w_sum <- w_sum + w[k]/R
    }
    else {
      w_sum <- w_sum + w[k]/R
      p_k <- w[k]/w_sum       # calculate the probablity for the kth item
      T <- runif(1)
      if (p_k >= T)  {         # randomly decide if the kth item is inserted into the reservoir
         r <- sample(R,1)
         ReserS[r,] <- DS      # replace the item with the minimum value in the reservoir with the current item
      } 
    }
    k <- k+1
  }
  return(ReserS)
}
