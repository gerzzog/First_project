import sys

from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
import pyspark.sql.functions as F
import datetime
import sys
import os
from datetime import date as dt
import pathlib
import pyspark
from pyspark.sql import SparkSession
from pyspark import sql
import pandas as pd

import findspark 
import numpy as np
import datetime
import pyspark.sql.functions as F
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql import Window as W
import logging



#обозначаем диапазон, за который берем даты, так как слишком большой массив будет грузится долго
def input_event_paths(date, days_count, events_base_path):
    dt = datetime.datetime.strptime(date, '%Y-%m-%d')
    return [f"{events_base_path}/date={(dt - datetime.timedelta(days=x)).strftime('%Y-%m-%d')}" for x in
            range(days_count)]

def main():
    #дата старта забора данных
    date = sys.argv[1]
    #количество дней для вычислений
    days_count = sys.argv[2]
    #путь, где сохранены исходники
    events_base_path = sys.argv[3]
    #путь, где сохранены координаты городов
    cities_base_path = sys.argv[4]
    #путь, куда сохранять вьюхи    
    output_base_path = sys.argv[6]
    
    findspark.init()
    findspark.find()

    os.environ['HADOOP_CONF_DIR'] = '/etc/hadoop/conf'
    os.environ['YARN_CONF_DIR'] = '/etc/hadoop/conf'


    conf = SparkConf().setAppName(f"VerifiedTagsCandidatesJob333").set("spark.sql.legacy.timeParserPolicy","LEGACY")
    sc = SparkContext(conf=conf)
    spark = SQLContext(sc)
    
    logging.info('Session was successfully created')
    
    #берем диапазон на дату с глубиной 1 день
    reaction_paths = input_event_paths(date, days_count, events_base_path)
    #читаем файл с данными по координатам городов
    pandas_data_frame = pd.read_csv(cities_base_path, sep = ';')
    
    logging.info('Got cities coordinates from CSV file')

    #файл необходимо обработать, так как данные с формате str с запятыми вместо точек
    city_data_frame = spark.createDataFrame(pandas_data_frame)\
        .withColumnRenamed('lat', 'city_lat')\
        .withColumnRenamed('lng', 'city_lng')\
        .withColumn('city_lat', regexp_replace('city_lat', '\\.', ''))\
        .withColumn('city_lat', regexp_replace('city_lat', ',', '.'))\
        .withColumn('city_lng', regexp_replace('city_lng', '\\.', ''))\
        .withColumn('city_lng', regexp_replace('city_lng', ',', '.'))
    city_data_frame = city_data_frame\
        .withColumn('city_lng', city_data_frame['city_lng'].cast("float"))\
        .withColumn('city_lat', city_data_frame['city_lat'].cast("float"))

    #читаем датасет с определенными датами
    events_df = spark.read.parquet(*reaction_paths)

    #добавляем данные по расстоянию между всеми городами
    join_df = events_df\
        .crossJoin(city_data_frame) \
        .withColumn("dlon_start", radians(col("city_lng")) - radians(col("lon"))) \
        .withColumn("dlat_start", radians(col("city_lat")) - radians(col("lat"))) \
        .withColumn("haversine_dist_start", asin(sqrt(
                                             sin(col("dlat_start") / 2) ** 2 + cos(radians(col("lat")))
                                             * cos(radians(col("city_lat"))) * sin(col("dlon_start") / 2) ** 2
                                             )
                                        ) * 2 * 6371)\
        .drop("lat","lon","city_lat", "city_lng","dlon_start","dlat_start")
    #теперь выбираем кратчайшее расстояние до города
    Wi = W.partitionBy("event.message_id")
    result1 = join_df\
        .withColumn("min_dist_start", min('haversine_dist_start').over(Wi))\
        .filter(col("min_dist_start") == col('haversine_dist_start'))\
        .orderBy(F.desc("event.message_from"), F.desc("event.message_ts"))\
        .drop("haversine_dist_start").cache()

    logging.info('Reading source data and calculating distance completed')

    #выбираем первые даты и город для событий != сообщения, так как у них нет координат
    result_for_re = result1\
        .select(F.col("event.message_from").alias("user_id"),
                F.col("city").alias("home_city"),
                F.col("event.message_ts").alias("date_r"))\
        .groupby("user_id")\
        .agg(F.first("home_city"),F.first("date_r").alias("date_r"))

    #выбираем отдельно данные для регистраций. Тут нужно брать первые даты
    result_for_reg = result1\
        .select(F.col("event.message_from").alias("user_id"),
                F.col("id"),
                F.col("event.message_ts").alias("date_r"))\
        .groupby("user_id")\
        .agg(F.last("id").alias("id"),F.last("date_r").alias("date_reg"))

    #выбираем актуальный адрес
    view1_1 = result1\
        .select(F.col("event.message_from").alias("user_id"),
                F.col("city").alias("act_city"),
                F.col("event.message_ts").alias("datetime"))\
        .withColumn("rank", F.row_number().over(W.partitionBy("user_id")\
            .orderBy(F.desc("datetime"))))\
        .where("rank = 1")\
        .drop("rank")

    #выбираем текущее место жительства
    w_uf = W.partitionBy('user_id', 'home_city')
    view1_2 = result1\
       .where("event.message_ts is not null")\
        .select(F.col("event.message_from").alias("user_id"),
                F.col("city").alias("home_city"),
                F.col("event.message_ts").alias("date"))\
        .withColumn('max_date',  F.max((F.col('date'))).over(w_uf))\
        .withColumn('min_date',  F.min((F.col('date'))).over(w_uf))\
        .withColumn('diff_date',  datediff(col("max_date"),col("min_date")))\
        .where("diff_date > 2")\
        .drop("date",'max_date','min_date','diff_date')\
        .distinct()


    #выбираем количество посещенных городов
    view3 = result1.select(F.col("event.message_from").alias("user_id"), F.col("city")).groupby("user_id").agg(countDistinct("city"))

    #выбираем посещенные города
    df = result1.select(F.col("event.message_from").alias("user_id"), F.col("city")).groupby("user_id").agg(F.collect_set('city'))

    #формируем итоговую вьюху. Не получается сделать формулу по местному времени, так как ошибку выбивает,что
    #нет временной зоны типа Australia/Wollongong и еще другие города. Оставил Сидней по дефолту
    #в интернете есть модули с поиском по координатам таймзоны, но эти модули нельзя импортировать
    view = view1_1\
        .join(view1_2,['user_id'], 'left')\
        .join(view3,['user_id'], 'left')\
        .join(df,['user_id'], 'left')\
        .withColumn('local_time',  F.from_utc_timestamp(F.col("datetime"),(lit("Australia/Sydney"))))\
        .drop("datetime")
        #.withColumn('local_time',  F.from_utc_timestamp(F.col("datetime"),concat(lit("Australia/"), col("act_city"))))
    
    #сохраняем первую вьюху
    view.write.mode("overwrite").parquet(f"{output_base_path}/first_view/date={date}")
    
    logging.info('First view was written, going further')
    
    #подсчитываем сообщения понедельно и помесячно
    w_w = W.partitionBy("week","id")
    w_m = W.partitionBy("month","id")
    view_m = result1\
        .where("event_type='message'")\
        .select(date_format((F.col("event.message_ts")),"M").alias("month"),
                weekofyear(to_date(F.col("event.message_ts"))).alias("week"),
                F.col("id"))\
        .withColumn('week_message',  F.count('*').over(w_w))\
        .withColumn('month_message',  F.count('*').over(w_m))\
        .groupby("month","week","id")\
        .agg(F.first("week_message"),F.first("month_message"))

    #подсчитываем реакции понедельно и помесячно, берем последнюю дату сообщения
    result_for_re_view = result1\
        .join(result_for_re,result_for_re['user_id']==result1["event.reaction_from"], 'left')

    view_r = result_for_re_view\
        .where("event_type='reaction'")\
        .select(date_format((F.col("date_r")),"MM").alias("month"),
                weekofyear(to_date(F.col("date_r"))).alias("week"),
                F.col("id"))\
        .withColumn('week_reaction',  F.count('*').over(w_w))\
        .withColumn('month_reaction',  F.count('*').over(w_m))\
        .groupby("month","week","id")\
        .agg(F.first("week_reaction"),F.first("month_reaction"))

    #подсчитываем подписки понедельно и помесячно, берем первую дату сообщения
    result_for_sub_view = result1\
        .join(result_for_re,result_for_re['user_id']==result1["event.subscription_user"], 'left')

    view_sub = result_for_sub_view\
        .where("event_type='subscription'")\
        .select(date_format((F.col("date_r")),"MM").alias("month"),
                weekofyear(to_date(F.col("date_r"))).alias("week"),
                F.col("id"))\
        .withColumn('week_subscription',  F.count('*').over(w_w))\
        .withColumn('month_subscription',  F.count('*').over(w_m))\
        .groupby("month","week","id")\
        .agg(F.first("week_subscription"),F.first("month_subscription"))

    view_reg = result_for_reg\
        .select(date_format((F.col("date_reg")),"M").alias("month"),
                weekofyear(to_date(F.col("date_reg"))).alias("week"),
                F.col("id"))\
        .withColumn('week_reg',  F.count('*').over(w_w))\
        .withColumn('month_reg',  F.count('*').over(w_m))\
        .groupby("month","week","id")\
        .agg(F.first("week_reg"),F.first("month_reg"))

    #собираем все данные для второй вьюхи
    total_view = result1\
        .where("event_type='message'")\
        .select(
               date_format((F.col("event.message_ts")),"MM").alias("month"),
               weekofyear(to_date(F.col("event.message_ts"))).alias("week"),F.col("id"))\
        .groupby("id")\
        .agg(F.first("month").alias("month"),F.first("week").alias("week"))

    total_view = total_view.where("month is not null")
    total_view = total_view\
        .join(view_m,['id','month','week'],'left')
    total_view = total_view\
        .join(view_r,['id','month','week'],'left')
    total_view = total_view\
        .join(view_sub,['id','month','week'],'left')
    total_view = total_view\
        .join(view_reg,['id','month','week'],'left')

    #сохраняем вторую вьюху
    view.write.mode("overwrite").parquet(f"{output_base_path}/second_view/date={date}")
    
    logging.info('Second view was written, going further')
    
    #собираем координаты по всем пользователям
    w_l = W.partitionBy("user_id").orderBy(col("date_l").desc())
    view_last = events_df\
            .where("lon is not null")\
            .select(F.col("event.message_from").alias("user_id"),
                   F.col("lon"),
                   F.col("lat"),
                   F.col("event.message_ts").alias("date_l"))\
            .withColumn('lon_u',  F.max('lon').over(w_l))\
            .withColumn('lat_u',  F.max('lat').over(w_l))\
            .groupby("user_id")\
            .agg(F.first("lon").alias("lon"),F.first("lat").alias("lat"))

    #формируем списки по подпискм
    view_last_c = events_df\
        .select(F.col("event.subscription_channel").alias("channel"),
                   F.col("event.user").alias("user_id")).distinct()

    #выбираем уникальные пары пользователей
    new = view_last_c.join(view_last_c.withColumnRenamed('user_id', 'user_id2'), ['channel'], 'outer') \
      .filter('user_id < user_id2')

    #добавляем координаты
    user_list = new.join(view_last,['user_id'],'inner')\
        .withColumnRenamed('lon','lon_user1')\
        .withColumnRenamed('lat','lat_user1')
    user_list = user_list\
        .join(view_last,view_last['user_id']==user_list['user_id2'],'inner').drop(view_last['user_id'])\
        .withColumnRenamed('lon','lon_user2')\
        .withColumnRenamed('lat','lat_user2')

    #высчитываем расстояние
    distance = user_list\
        .withColumn("dlon_start", radians(col("lon_user1")) - radians(col("lon_user1"))) \
        .withColumn("dlat_start", radians(col("lat_user1")) - radians(col("lat_user2"))) \
        .withColumn("haversine_dist_start", asin(sqrt(
                                             sin(col("dlat_start") / 2) ** 2 + cos(radians(col("lat_user2")))
                                             * cos(radians(col("lat_user1"))) * sin(col("dlon_start") / 2) ** 2
                                             )
                                        ) * 2 * 6371)
    #итоговая вьюха
    result_dist = distance\
        .filter(col("haversine_dist_start") <= 1)\
        .join(view1_1,['user_id'],'left')\
         .withColumn("current_date",current_date())\
        .withColumnRenamed("act_city",'zone_id')\
         .withColumn('local_time',  F.from_utc_timestamp(current_timestamp(),(lit("Australia/Sydney"))))\
        .drop("channel","lon_user1","lat_user1","lon_user2","lat_user2","dlon_start","dlat_start","haversine_dist_start","datetime")
    result_dist.distinct().show()
    #.withColumn('local_time',  F.from_utc_timestamp(current_timestamp(),concat(lit("Australia/"), col("zone_id"))))

    #сохраняемт третью вьюху
    view.write.mode("overwrite").parquet(f"{output_base_path}/third_view/date={date}")
    
    logging.info('Third view was written. Finished ETL process')

if __name__ == "__main__":
    main()
