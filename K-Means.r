# Databricks notebook source
iris_sdf <- createDataFrame(iris)

# COMMAND ----------

head(iris_sdf)

# COMMAND ----------

kmeans_model <- spark.kmeans(iris_sdf, ~Sepal_Length+Sepal_Width+Petal_Length+Petal_Width, k=3)

summary(kmeans_model)

# COMMAND ----------

install.packages("magrittr")
library(magrittr)

# COMMAND ----------

pred <- kmeans_model %>% fitted %>% collect

head(pred)

# COMMAND ----------

table(pred$prediction, iris$Species)

# COMMAND ----------

library(ggplot2)
qplot(pred$Petal_Length, pred$Petal_Width, col=as.factor(pred$prediction))
