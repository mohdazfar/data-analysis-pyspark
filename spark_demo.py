from __future__ import division
from pyspark import SparkConf, SparkContext, SQLContext
import pyspark.sql.functions as F

import matplotlib.pyplot as plt
import pandas as pd

conf = SparkConf().setMaster("local[*]")
sc = SparkContext(conf=conf)
sqlContext = SQLContext(sc)

df = sqlContext.read.csv('athlete_events.csv', header=True)

################################
# Gender representation by year#
################################

df_gender = df.select(['Sex', 'Year'])
df_gender = df_gender.filter(~df_gender['Sex'].isin(['M', 'F']) == False) # Filter uncleaned rows
df_gender = df_gender.groupby(['Sex', 'Year']).count().orderBy('Sex', 'Year')




###############################
# Overall performance of teams#
###############################

df_performance = df.select(['NOC', 'Medal']).filter(~df['Medal'].isin(['Gold', 'Silver', 'Bronze']) == False).groupby(['NOC', 'Medal']).count().orderBy('NOC', 'Medal')



#########################
# Athlete's BMI overtime#
#########################

df_bmi = df.select(['Year','Weight','Height'])
df_bmi = df_bmi.filter(~df_bmi['Height'].isin(['NA']) == True) # Cleaning NA values
df_bmi = df_bmi.filter(~df_bmi['Year'].isin([x for x in range(1900, 2017)]) == False) # Cleaning Non Integer values
df_bmi_avg = df_bmi.withColumn("BMI", F.col('Weight')/(F.col('Height')/100)**2).select(['Year','BMI']).groupby('Year').avg()
df_bmi_avg = df_bmi_avg.orderBy('Year')
 
# Plot BMI average over the years
df_bmi_avg.columns = ['year',  'bmi_avg']
df_bmi_avg.set_index('year').plot.bar()
 
 
###################################
# Male female proportions overtime#
###################################

df_mf_prop = df.select(['Year','Sex','NOC'])
df_mf_prop = df_mf_prop.filter(~df_mf_prop['Sex'].isin(['M', 'F']) == False)
df_mf_prop = df_mf_prop.filter(~df_mf_prop['Year'].isin([x for x in range(1900, 2017)]) == False)
df_mf_prop = df_mf_prop.groupby(['Year','Sex', 'NOC']).count()

interval = 20
df_mf_prop = df_mf_prop.withColumn("range", df_mf_prop.Year - (df_mf_prop.Year % interval)).groupby("range", "Sex", "NOC").sum()

# plot overall performance of teams
df_mf_prop.columns = ['range', 'sex', 'noc', 'count', 'sum']
df_mf_prop = df_mf_prop[['range', 'sex', 'noc', 'count']]
df_mf_prop['range'] = df_mf_prop['range'].apply(lambda x: str(int(x))+' - '+ str(int(x)+20))
df_mf_prop = df_mf_prop.set_index(['range', 'noc', 'sex']).unstack().reset_index()
df_mf_prop = df_mf_prop.fillna(0)
df_mf_prop.columns = [''.join(col).strip() for col in df_mf_prop.columns.values]
colors = {'1900 - 1920':'red', '1920 - 1940':'blue', 
          '1940 - 1960':'green', '1960 - 1980':'black',
          '1980 - 2000':'cyan', '2000 - 2020': 'yellow'}

plt.scatter(x=df_mf_prop['countM'], y=df_mf_prop['countF'], marker='o', c= df_mf_prop['range'].apply(lambda x: colors[x]))
plt.legend(colors)
plt.xticks('Male Count in Olympics')
plt.yticks('Female Count in Olympics')
plt.show()



#########################################
# Analyzing Finland's sports performance#
#########################################
df_fin = df.filter(df.NOC == 'FIN')

# Compare Gender wise presence with global
gender_prop_fin =(df_fin.filter(df_fin.Sex == 'M').count()/df_fin.count())*100 # Give male's pct. For Female 100 - gender_prop_fin
gender_prop =(df.filter(df.Sex == 'M').count()/df.count())*100 # Give male's pct. For Female 100 - gender_prop

#####################
# Top Finnish sports#
#####################

fin_performance = df_fin.filter(df_fin['Medal'].isin(['Gold', 'Silver', 'Bronze'])).groupby('Year', 'Sport','Medal').count().sort('Sport')

# Analyze Finland's performance over the years
df_fin_performance.columns = ['year','sport','medal', 'count']
df_fin_performance = df_fin_performance[df_fin_performance['year'] > 1980] # Filter results from 1980
df_fin_performance = df_fin_performance.pivot_table(index='year', columns='medal', values='count' )
df_fin_performance.plot(kind='bar', stacked='true')
plt.show()




########################################
# Age distribution of Finnish olympians#
########################################

# Finnish
df_age_dist_fin = df_fin.groupby('Age').count().sort('Age')
df_age_dist_fin = df_age_dist_fin.filter(~age_dist_fin['Age'].isin([x for x in range(1, 100)]) == False) # Cleaning non-integer ages

# Global
df_age_dist = df.filter(df.NOC != 'FIN').groupby('Age').count().sort('Age')
df_age_dist = age_dist.filter(~age_dist['Age'].isin([x for x in range(1, 100)]) == False) # Cleaning non-integer ages

# Plot age distribution among athletes between Finland and Globally
df_merge = pd.merge(df_age_dist_fin, df_age_dist_global, how='inner', left_on = 0, right_on = 0)
df_merge.columns = ['age', 'fin_count', 'global_count']
df_merge['fin_count'] = df_merge['fin_count'] / df_merge['fin_count'].max()
df_merge['global_count'] = df_merge['global_count'] / df_merge['global_count'].max()
df_merge.set_index('age').plot.bar()
plt.show()


# Write files to CSV
df_gender.coalesce(1).write.csv('gender.csv')
df_bmi_avg.coalesce(1).write.csv('bmi.csv')
df_mf_prop.coalesce(1).write.csv('male_female.csv')
fin_performance.coalesce(1).write.csv('fin_performance.csv')
age_dist_fin.coalesce(1).write.csv('age_dist_fin.csv')
age_dist.coalesce(1).write.csv('age_dist.csv')