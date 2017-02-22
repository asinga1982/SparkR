# Databricks notebook source
#Modelling using mtcars data set
m1 <- mtcars
m1$cyl <- as.factor(m1$cyl)
m1$gear <- as.factor(m1$gear)

# COMMAND ----------

mtcars_df <- as.DataFrame(m1)
head(mtcars_df)

# COMMAND ----------

model1 <- glm(mpg~gear+am+cyl+wt+hp, data=mtcars_df, family="gaussian")
summary(model1)

# COMMAND ----------

install.packages("lars")
library(lars)

data(diabetes)

# COMMAND ----------

diabetes_all <-  data.frame(cbind(diabetes$x, y=diabetes$y))
head(diabetes_all)

# COMMAND ----------

set.seed(100)
idx <- base::sample(nrow(diabetes_all), floor(0.8*nrow(diabetes_all)))

train <- diabetes_all[idx,]
test <- diabetes_all[-idx,]

# COMMAND ----------

train_sparkR_df <- as.DataFrame(train)
test_sparkR_df <- as.DataFrame(test)


# COMMAND ----------

model <- glm(y~ldl+hdl+age+sex+glu+tch+ltg, data=train_sparkR_df, family="gaussian")
summary(model)

# COMMAND ----------

test_pred <- predict(model, newData=train_sparkR_df)
names(test_pred)

# COMMAND ----------

out <- select(test_pred, "label", "prediction")
outR <- collect(out)
head(outR)

rmse <- sqrt(mean((outR$label- outR$prediction)^2))
print(rmse)
