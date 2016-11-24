ReservoirS_Res <- function(DStream, W, R){  
  # DStream is the data stream 
  # W is the weight vector
  # R is the reservoir size
  k <- 1   
  ReserS <- NULL
  W_R <- NULL
  while (sum(is.na(DStream[k,]))==0) {    #  if Dstream is not exausted, keep the item and do the reservoir sampling
    DS <- DStream[k,]      
    if (k <= R){
      ReserS <- rbind(ReserS,DS)   # keep the first R items in the stream to the reservoir
      W_R <- rbind(W_R, W[k])
    }
    else {
      u <- runif(R)       #calculate a random vector of R value between 0 and 1
      key <- u^(1/W_R)    # calcute the key value of each item in the reservoir
      T <- min(key)       # keep the minimum value of key
      r <- which(key == min(key))  
      key_k <- runif(1)^(1/W[k]) # the key value of the current item
      if (key_k > T){          # if the key value larger than the minimum value in the reservoir,
        ReserS[r,] <- DS      # replace the item with the minimum value in the reservoir with the current item
      }   
   }   
    k <- k+1
  }
  return(ReserS)
}
