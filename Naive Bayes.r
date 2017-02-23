# Databricks notebook source
#Read wine datasets and conbine them
winequality_red <- read.csv('http://mlr.cs.umass.edu/ml/machine-learning-databases/wine-quality/winequality-red.csv', 
                            sep = ';')
winequality_white <- read.csv('http://mlr.cs.umass.edu/ml/machine-learning-databases/wine-quality/winequality-white.csv', 
                            sep = ';')
# bind both data sets together
wine_quality <- rbind(winequality_red, winequality_white)

# replace period in column name with underscore
names(wine_quality) <- gsub(names(wine_quality), pattern = '\\.', replacement = '_')

dim(wine_quality)

# COMMAND ----------

wine_quality$quality <- as.factor(wine_quality$quality)
str(wine_quality)

# COMMAND ----------

#Create a Spark DF and create a temp table
wine_df <- as.DataFrame(wine_quality)

createOrReplaceTempView(wine_df, "wine_df")
class(wine_df)

# COMMAND ----------

#Convert Numerical variables into Categorical vars
wine_sdf_quartiles <- sql("SELECT 
case when NTILE(2) OVER (ORDER BY alcohol) = 1 then 'low' else 'high' end AS alcohol,
case when NTILE(2) OVER (ORDER BY fixed_acidity) = 1 then 'low' else 'high' end AS fixed_acidity,
case when NTILE(2) OVER (ORDER BY citric_acid) = 1 then 'low' else 'high' end AS citric_acid,
case when NTILE(2) OVER (ORDER BY residual_sugar) = 1 then 'low' else 'high' end AS residual_sugar,
case when NTILE(2) OVER (ORDER BY chlorides) = 1 then 'low' else 'high' end AS chlorides,
case when NTILE(2) OVER (ORDER BY free_sulfur_dioxide) = 1 then 'low' else 'high' end AS free_sulfur_dioxide,
case when NTILE(2) OVER (ORDER BY total_sulfur_dioxide) = 1 then 'low' else 'high' end AS total_sulfur_dioxide,
case when NTILE(2) OVER (ORDER BY pH) = 1 then 'low' else 'high' end AS pH,
case when NTILE(2) OVER (ORDER BY sulphates) = 1 then 'low' else 'high' end AS sulphates,
                              quality FROM wine_df")

# COMMAND ----------

# Split train and test data in SparkR
train <- sample(wine_sdf_quartiles, withReplacement=FALSE, fraction=0.8, seed=100)
test <- except(wine_sdf_quartiles, train)

head(train)

# COMMAND ----------

#Train a Naive Bayes Model
nb_model <- spark.naiveBayes(train,quality~.) 

summary(nb_model)

# COMMAND ----------

#Predict on test
pred <- predict(nb_model,test[,-10])

#class(pred)
p1 <- collect(select(pred,pred$prediction))

a1 <- collect(test[,10])

table(p1$prediction, a1$quality)
