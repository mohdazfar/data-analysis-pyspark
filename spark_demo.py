from pyspark import SparkConf, SparkContext, SQLContext
import pyspark.sql.functions as F
from __future__ import division

conf = SparkConf().setMaster("local[*]")
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)

df = sqlContext.read.csv('athlete_events.csv', header=True)

# Gender representation by year
df1 = df.select(['Sex', 'Year'])
df2 = df1.filter(~df1['Sex'].isin(['M', 'F']) == False) # Filter uncleaned rows
df3 = df2.groupby(['Sex', 'Year']).count().orderBy('Sex', 'Year')


# Overall performance of teams
df_performance = df.select(['NOC', 'Medal']).filter(~df['Medal'].isin(['Gold', 'Silver', 'Bronze']) == False).groupby(['NOC', 'Medal']).count().orderBy('NOC', 'Medal')

# Athlete's BMI overtime
df_bmi = df.select(['Year','Weight','Height'])
df_bmi = df_bmi.filter(~df_bmi['Height'].isin(['NA']) == True) # Cleaning NA values
df_bmi = df_bmi.filter(~df_bmi['Year'].isin([x for x in range(1900, 2017)]) == False) # Cleaning Non Integer values
df_bmi_avg = df_bmi.withColumn("BMI", F.col('Weight')/(F.col('Height')/100)**2).select(['Year','BMI']).groupby('Year').avg()
df_bmi_avg = df_bmi_avg.orderBy('Year')
 


# Male female proportions overtime 
df_mf_prop = df.select(['Year','Sex','NOC'])
df_mf_prop = df_mf_prop.filter(~df_mf_prop['Sex'].isin(['M', 'F']) == False)
df_mf_prop = df_mf_prop.filter(~df_mf_prop['Year'].isin([x for x in range(1900, 2017)]) == False)
df_mf_prop = df_mf_prop.groupby(['Year','Sex', 'NOC']).count()

interval = 20
df_mf_prop = df_mf_prop.withColumn("range", df_mf_prop.Year - (df_mf_prop.Year % interval)).groupby("range", "Sex", "NOC").sum()

# df_mf_prop.withColumn("range", df_mf_prop.Year - (df_mf_prop.Year % interval)).groupby("range","Sex", # "NOC").withColumn.("YearRange", str(df_mf_prop.range)+" - " + str(df_mf_prop.range + 20))


# Analyzing Finland's sports performance
df_fin = df.filter(df.NOC == 'FIN')

# Compare Gender wise presence with global
gender_prop_fin =(df_fin.filter(df_fin.Sex == 'M').count()/df_fin.count())*100 # Give male's pct. For Female 100 - gender_prop_fin
gender_prop =(df.filter(df.Sex == 'M').count()/df.count())*100 # Give male's pct. For Female 100 - gender_prop


# Top Finnish sports
fin_performance = df_fin.filter(df_fin['Medal'].isin(['Gold', 'Silver', 'Bronze'])).groupby('Year', 'Sport','Medal').count().sort('Sport')


# Age distribution of Finnish olympians
# Finnish
age_dist_fin = df_fin.groupby('Age').count().sort('Age')
age_dist_fin = age_dist_fin.filter(~age_dist_fin['Age'].isin([x for x in range(1, 100)]) == False) # Cleaning non-integer ages

# Global
age_dist = df.filter(df.NOC != 'FIN').groupby('Age').count().sort('Age')
age_dist = age_dist.filter(~age_dist['Age'].isin([x for x in range(1, 100)]) == False) # Cleaning non-integer ages



df3.coalesce(1).write.csv('gender.csv')
df_bmi_avg.coalesce(1).write.csv('bmi.csv')
df_mf_prop.coalesce(1).write.csv('male_female.csv')
# gender_prop.write.csv('gender_prop.csv')
# gender_prop_fin.write.csv('gender_prop_fin.csv')
fin_performance.coalesce(1).write.csv('fin_performance.csv')
age_dist_fin.coalesce(1).write.csv('age_dist_fin.csv')
age_dist.coalesce(1).write.csv('age_dist.csv')