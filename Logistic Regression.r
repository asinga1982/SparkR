# Databricks notebook source
titanic <- read.csv('http://math.ucdenver.edu/RTutorial/titanic.txt',sep='\t')

# COMMAND ----------

titanic$S1 <- ifelse(titanic$Survived == 1, "yes", "no")

titanic$S1 <- as.factor(titanic$S1)

str(titanic)
base::colSums(is.na(titanic))

# COMMAND ----------

#Extract titles
titanic$Title <- ifelse(grepl('Mr ',titanic$Name),'Mr',
                 ifelse(grepl('Mrs ',titanic$Name),'Mrs',
                 ifelse(grepl('Miss',titanic$Name),'Miss','Nothing')))

titanic$Title <- as.factor(titanic$Title)
head(titanic)

# COMMAND ----------

t1 <- titanic[,-c(1,5)]
t1$Age[is.na(t1$Age)] <- median(t1$Age, na.rm=T)
colSums(is.na(t1))

# COMMAND ----------

set.seed(100)
idx <- base::sample(nrow(t1), floor(0.8*nrow(t1)))
train <- t1[idx,]
test <- t1[-idx,]

train_spark_df <- as.DataFrame(train)

test_spark_df <- as.DataFrame(test)


str(train)

# COMMAND ----------

model <- glm(S1~PClass+Age+Sex+Title, data=train_spark_df, family="binomial")

summary(model)

# COMMAND ----------

pred <- predict(model, newData=test_spark_df)

head(pred)

# COMMAND ----------

# Extract the label and the predictions:
predictions_details <- select(pred, pred$label,
pred$prediction)

# make sql temp view
createOrReplaceTempView(predictions_details, "pred")

# COMMAND ----------

TP <- sql("SELECT count(label) FROM pred WHERE label = 1 AND prediction > 0.5")
TP <- collect(TP)[[1]]
TN <- sql("SELECT count(label) FROM pred WHERE label = 0 AND prediction < 0.5")
TN <- collect(TN)[[1]]
FP <- sql("SELECT count(label) FROM pred WHERE label = 0 AND prediction > 0.5")
FP <- collect(FP)[[1]]
FN <- sql("SELECT count(label) FROM pred WHERE label = 1 AND prediction < 0.5")
FN <- collect(FN)[[1]]

accuracy = (TP + TN)/(TP + TN + FP + FN) 

print(paste0(round(accuracy * 100,2),"%"))
