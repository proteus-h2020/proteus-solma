ReservoirS_adapt = function(DStream, n, R, threshold) { 
  # DStream is the data stream
  # n is for controlling when to change the reservoir size (i.e. when k=10^(0,1..)*n*R) 
  # R is the original reservoir size
  # threshold is the uniformity confidence threshold (i.e. 0.99)
  k <- 1   
  ReserS <- NULL
  ReserS1 <- NULL
  ReserS2 <- NULL
  while (sum(is.na(DStream[k,]))==0) {    #  if Dstream is not exhausted, keep the item and do the reservoir sampling  
    DS <- DStream[k,] 
    if (k <= R){
      ReserS <- rbind(ReserS,DS)   # keep the first R items in the stream to the reservoir
    }
    else {
      j <- sample(1:k,1)      # random generate an integer j from 1 to k
      if (j < R)
        ReserS[j,] <- DS      # if j < R, then replace the jth item in the reservoir with the kth stream item
    }  
    k <- k+1
    if (k == n*R ) {    # when k==n*R, check if needed to change the reservoir size (can also depend on certain applications)
      r <- as.numeric(readline(prompt="Change the reservoir size (if not, press enter): "))   # enter the reservoir size 
      if (!is.na(r)){
        delta = r-R       
        if (delta < 0) {     # the reservoir size decrease 
          S_delta <- sample(R, abs(delta))
          ReserS <- ReserS[-S_delta,]  
        } 
        else{                # the reservoir size incerease
          m <- min_m(k,R,delta,threshold) # calculate the minimum size of incoming m items to exceed the Uniformity confidence threshold 
          x <- floor(runif(1)*R)      # randomly choose x items in the original reservoir to remain in the new reservoir
          S_x <- sample(R, x)
          ReserS1 <- ReserS[S_x,]   
          k0 <- k 
          while(sum(is.na(DStream[k,]))==0) {
            if (k < k0+R+delta-x){      # sampling the remaining R+delta-x items from the arriving m items using conventional reservoir sampling
              ReserS2 <- rbind(ReserS2,DStream[k,])
            }
            else {
              j <- sample((k+1):(k0+R+delta-x),1)   
              if (j < (R+delta-x))
                ReserS2[j,] <- DStream[k,]   
            } 
            k <- k+1
            if(k == k0+m) {   # break when k exceeds the extra arriving m items
              break
            }
          }
          ReserS <- rbind(ReserS1,  ReserS2)  
        }
      }    
      R <- r
      n <- n*10
    }
  }
  return(ReserS)
}

 





