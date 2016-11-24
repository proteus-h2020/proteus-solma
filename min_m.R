min_m = function(k,R,delta,threshold) { 
  # calcute the minimum m value that causes the Uniform Confidence exceed a threshold
  # k is the current item from the stream
  # R is the original reservoir size
  # delta is the discrepency between the changed reservoir size and the original reservoir size
  m <- delta
  UC <- 0  # the uniformity confidence defined in article Al-Kateb, et. al 2007
  x <- R   
  f1 <- factorial(k)*factorial(m)*factorial(R+delta)*factorial(k+m-R-delta)
  f2 <- factorial(x)*factorial(k-x)*factorial(R+delta-x)*factorial(m-R-delta+x)*factorial(k+m)
  UC = UC + f1/f2
  while(UC <= threshold){
    m <- m+1
    UC <- 0
    for (x in max(0,(R+delta-m)):R){
      f1 <- factorial(k)*factorial(m)*factorial(R+delta)*factorial(k+m-R-delta)
      f2 <- factorial(x)*factorial(k-x)*factorial(R+delta-x)*factorial(m-R-delta+x)*factorial(k+m)
      UC = UC + f1/f2 
    }
  }
  return(m) 
}
