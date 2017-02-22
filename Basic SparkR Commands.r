# Databricks notebook source
dim(faithful)
#class(faithful)

# COMMAND ----------

# Creating a SparkR DataFrame
failthful_sp <- as.DataFrame(faithful)

# COMMAND ----------

head(failthful_sp)

# COMMAND ----------

install.packages("magrittr")
library(magrittr)

# COMMAND ----------

#Another way to create SparkR DataFrame
df <- createDataFrame(iris)
head(df)

# COMMAND ----------

head(select(df,"Sepal_Length", "Species"))
# Select specific cols
x <- select(filter(df, df$Sepal_Length >5), "Sepal_Length", "Species")
dim(x)


# COMMAND ----------

# Summarize using GroupBy, mean and count
head(summarize(groupBy(df, df$Species),mean(df$Sepal_Length), count=n(df$Species), max(df$Sepal_Length), min(df$Sepal_Length)  ) )

head(distinct(select(df,df$Species )))

# COMMAND ----------

#Sorting Data
head(arrange(df, desc(df$Species)))

# COMMAND ----------

#Pipelining commands
df1 <- filter(df, df$Sepal_Length > 7) %>% 
arrange("Sepal_Length", "Petal_Width") %>%
select(df$Species, df$Petal_Width, df$Sepal_Length)

head(df1,10)
