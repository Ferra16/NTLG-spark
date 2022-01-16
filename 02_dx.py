from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = (SparkSession.builder.appName('02 DZ').getOrCreate() )
spark.sparkContext.setLogLevel("ERROR")

print('Загрузка данных ...')
df = spark.read.option('header', True).option('sep', ',').option('inferSchema', True).csv('./owid-covid-data.csv')

print('-' * 20)
print('Задание 1. 15 стран с наибольшим процентом переболевших на 31 марта (2021 года):')
df = spark.read.option('header', True).option('sep', ',').option('inferSchema', True).csv('./owid-covid-data.csv')
columns_ = ['iso_code', 'location', 'date', 'population', 'total_cases', 'total_deaths']

date_ = '2021-03-31'

#df.select(columns_).show()
#df.select(columns_).groupBy('iso_code').max('total_deaths','total_cases').sort(F.col('total_deaths')).show()
#df_march = df.select(columns_).where(~F.col('location').isin('World','Europe','Africa', 'Asia')).where(F.col('date') == date_)
df_march = (df
            .select(columns_)
            .where(~F.col('location')
                   .isin('World','Europe','Africa', 'Asia'))
            .where(F.col('date') == date_))


# Предположим, что переболевшие это те кто не умер и выздоровел
result1 = (df_march
           .withColumn('recovered', F.col('total_cases') - F.col('total_deaths'))
           .withColumn('perc_of_recovered', F.round(F.col('recovered') / F.col('population') * 100, 2))
           .sort(F.col('perc_of_recovered').desc()).select('iso_code', 'location', 'perc_of_recovered'))

result1.show(15)


print('-' * 20)
print('Задание 2. Top 10 стран с максимальным зафиксированным' 
      ' кол-вом новых случаев за последнюю неделю марта 2021 в'
      ' отсортированном порядке по убыванию')

exclude_ = ['Asia', 'Africa', 'Europe', 'European Union', 'International', 'World', 'South America', 'North America']

df2 =  (df
        .select(F.to_date('date')
                .alias('date_'), 'location', 'new_cases')
        .where(~F.col('location').isin(exclude_))
        .where((F.col('date_') >= '2021-03-25') & (F.col('date_') <= '2021-03-31')))

 
from pyspark.sql import Window
w = Window.partitionBy('location')
 
df2_result = (df2
              .withColumn('max_cases', F.max('new_cases')
                          .over(w)).where(F.col('new_cases') == F.col('max_cases'))
              .drop(F.col('max_cases'))
              .sort(F.col('max_cases').desc()))

df2_result.show(10)


print('-' * 20)
print('Задание 3. Посчитайте изменение случаев относительно предыдущего'
      ' дня в России за последнюю неделю марта 2021.')

w = Window().orderBy('date_')
df3 = df.select(F.to_date('date').alias('date_'), 'new_cases').where(df.location == 'Russia')

(df3
.withColumn('yesterday_cases', F.lag('new_cases').over(w))
.withColumn('delta', F.col('new_cases') - F.col('yesterday_cases'))
.where((F.col('date_') >= '2021-03-25') & (F.col('date_') <= '2021-03-31')).show())


