train_data <- read.csv('../data/train.flann', header = FALSE, sep = " ")

dim(train_data)

train_matrix <- sqrt(data.matrix(train_data))



num_examples <- dim(train_matrix)[1]

AtA <- (t(train_matrix) %*% train_matrix)/num_examples

s <- svd(AtA)



num_sing_vectors = 100



lam <- 50

d <- s$d[1:num_sing_vectors]

weights <- sqrt(d/(d + lam))



r <- s$v[, 1:num_sing_vectors] %*% diag(weights)

reduced_data <- train_matrix %*% r



write.table(reduced_data, file='../data/train.flann.svd', quote=FALSE, col.names = FALSE, row.names = FALSE, sep = " ")

#result on training data: 97.5666666667



test_data <- read.csv('../data/test.flann', header = FALSE, sep = " ")

dim(test_data)

test_matrix <- sqrt(data.matrix(test_data))

reduced_test <- test_matrix %*% r

write.table(reduced_test, file='../data/test.flann.svd', quote=FALSE, col.names = FALSE, row.names = FALSE, sep = " ")
