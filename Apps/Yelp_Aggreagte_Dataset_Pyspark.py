import sys
import re

from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split
from pyspark.sql.types import DateType,StringType
from pyspark.sql.functions import isnan, when, count, col,lower
from pyspark.sql.functions import udf
import pyspark.sql.functions as f
from pyspark.sql.functions import broadcast

spark = SparkSession\
        .builder\
        .master("local") \
        .appName("Yelp_Aggregate_dataSet_pyspark")\
        .getOrCreate()

#### Business Json data Load and cleaning 
df_Bu = spark.read.format('json').load('/yelp_academic_dataset_business.json')
df_BuNw = df_Bu.withColumn("state", when(col("state") == "XMS","XX").otherwise(df_Bu.state))
df_BuNw.createOrReplaceTempView("BusinessUnit")

def cityCleaning(str):
    str1 = re.sub("-","",(re.sub("[!@#$%^&*+?_=,.-:()_{}']","",str))).capitalize().strip()
    word_list = ["township", "park", "industrial", "beach", "city", "neighbors", "pa", "boro", "ap", "twp", "mall", 
                 "village", "hts", "county", "cbd", "heights", "place","balance","afb","manor","in","fl"
                ]
    repl_wrd = ""
    str2 = ' '.join([repl_wrd if idx in word_list else idx for idx in str1.split()])
    #k = initcap(regexp_replace(ltrim(rtrim(lower(str))),"[!@#$%^&*+?_=,<>/-,./:()_{}']",""))
    return str2.strip()

cityCleaningUDF = udf(lambda z:cityCleaning(z),StringType())     
df_BuNw = df_BuNw.withColumn("city",cityCleaningUDF(col("city")))
df_BuNw = df_BuNw.withColumn("business_id",lower(col("business_id")))
df_BuNw = df_BuNw.withColumn("latitude", f.round(df_BuNw["latitude"], 2))
df_BuNw = df_BuNw.withColumn("longitude", f.round(df_BuNw["longitude"], 2))
df_BuNw = df_BuNw.na.fill("unknownCategory",["categories"])
columns_to_drop = ['attributes', 'hours']
df_BuNw = df_BuNw.drop(*columns_to_drop)
df_BuNw.show(5)
df_BuNw.createOrReplaceTempView("BusinessUnit")
spark.sql("SELECT *  FROM BusinessUnit").show(5) 


#### Tip data Load and cleaning 
df_Tp = spark.read.format('json').load('/yelp_academic_dataset_tip.json')
df_Tp.createOrReplaceTempView("TPUnit")
df_Tp = spark.sql("SELECT distinct *  FROM TPUnit") 
df_Tp.createOrReplaceTempView("TPUnit")

df_TpNw = df_Tp
df_TpNw = df_TpNw.withColumn("user_id",lower(col("user_id")))
df_TpNw = df_TpNw.withColumn("business_id",lower(col("business_id")))
df_TpNw = df_TpNw.withColumn("date",col("date").cast("date"))
df_TpNw = df_TpNw.replace('None',None).na.fill("NA")
df_TpNw.show(5)
df_TpNw.createOrReplaceTempView("TPUnit")

#### Review data Load and cleaning
df_Rv = spark.read.format('json').load('/yelp_academic_dataset_review.json')
df_Rv.createOrReplaceTempView("ReviewUnit")
df_Rv = spark.sql("SELECT distinct *  FROM ReviewUnit") 
df_Rv.createOrReplaceTempView("ReviewUnit")
df_RvNw = df_Rv
df_RvNw = df_RvNw.withColumn("business_id",lower(col("business_id")))
df_RvNw = df_RvNw.withColumn("user_id",lower(col("user_id")))
df_RvNw = df_RvNw.withColumn("review_id",lower(col("review_id")))
df_RvNw = df_RvNw.replace('None',None).na.fill("NA")
df_RvNw = df_RvNw.withColumn("date",col("date").cast("date"))
df_RvNw.createOrReplaceTempView("ReviewUnit")
df_RvNw.show(5)
spark.sql("SELECT *  FROM ReviewUnit").show(5)

#### User data Load and cleaning
df_Usr = spark.read.format('json').load('/yelp_academic_dataset_user.json')
df_Usr.createOrReplaceTempView("UserUnit")
df_Usr = spark.sql("SELECT distinct *  FROM UserUnit") 
df_Usr.createOrReplaceTempView("UserUnit")
df_UsrNw = df_Usr
df_UsrNw = df_Usr.withColumn("user_id",lower(col("user_id")))
df_UsrNw = df_UsrNw.replace('None',None).na.fill("NA")
df_UsrNw.show(5)
df_UsrNw.createOrReplaceTempView("UserUnit")
spark.sql("SELECT *  FROM UserUnit").show(5)

#### check In  data Load and cleaning
df_chkIn = spark.read.format('json').load('/yelp_academic_dataset_checkin.json')
df_chkIn.createOrReplaceTempView("CheckInUnit")
df_chkIn = spark.sql("SELECT distinct *  FROM CheckInUnit") 
df_chkIn.createOrReplaceTempView("CheckInUnit")

df_chkIn = spark.sql("""select business_id, explode(split(date,',')) date FROM CheckInUnit""")
df_chkIn = df_chkIn.withColumn("date",col("date").cast("date"))
df_chkIn.show(5)
df_chkIn.createOrReplaceTempView("CheckInUnit")
spark.sql("SELECT *  FROM CheckInUnit").show(5)


df_BuRvTP = spark.sql("""SELECT /*+ BROADCAST(BusinessUnit) ,BROADCAST(TPUnit), BROADCAST(CheckInUnit)*/
                            BU.name,
                            BU.business_id,
                            BU.address,
                            BU.city,
                            BU.state,
                            BU.stars,
                            CU.date checkinDate,
                            UrU.name,
                            UrU.average_stars
                      FROM CheckInUnit CU,
                           TPUnit TU,
                           UserUnit UrU,
                           BusinessUnit BU
                    where lower(CU.business_id) = lower(TU.business_id)
                      and lower(BU.business_id) = lower(CU.business_id)
                      and CU.date = TU.date
                      and lower(UrU.user_id) = lower(Tu.user_id)
                        order by BU.name,CU.date
                          """)
df_BuRvTP.show(5)
try:
        df_BuRvTP.coalesce(1).write.format('json').save('/opt/spark-Aggregate/Template_df_BuRvTP.json')
except:
        print ("An error occurred while writing Template_df_BuRvTP.json")

df_BuTpCnt = spark.sql("""SELECT /*+ BROADCAST(BusinessUnit) ,BROADCAST(TPUnit), BROADCAST(CheckInUnit)*/
                            BU.name BusinessName,
                            BU.stars OverallStars,
                            CU.date  CheckinDate,
                            extract(week from CU.date) Week,
                            extract(year from CU.date) Year,
                            UrU.average_stars AverageStarGivenByUser,
                            count(*) CntCheckIn
                      FROM CheckInUnit CU,
                           TPUnit TU,
                           UserUnit UrU,
                           BusinessUnit BU
                    where lower(CU.business_id) = lower(TU.business_id)
                      and lower(BU.business_id) = lower(CU.business_id)
                      and CU.date = TU.date
                      and lower(UrU.user_id) = lower(Tu.user_id)
                       group by BU.name,BU.stars,CU.date,Week,Year,UrU.average_stars
                        order by BU.name,CU.date
                          """)
df_BuTpCnt.show(5)
try:
        df_BuTpCnt.coalesce(1).write.format('json').save('/opt/spark-Aggregate/Template_df_BuTpCnt.json')
except:
        print ("An error occurred while writing Template_df_BuTpCnt.json")


df_BuRvCnt = spark.sql("""SELECT /*+ BROADCAST(BusinessUnit), BROADCAST(CheckInUnit)*/
                            BU.name BusinessName,
                            BU.stars OverallStars, 
                            CU.date  CheckinDate,
                            extract(week from CU.date) Week,
                            extract(year from CU.date) Year,
                            RU.stars ReviewStars,
                            count(*) CntCheckIn
                      FROM CheckInUnit CU,
                           ReviewUnit RU,
                           BusinessUnit BU
                    where lower(CU.business_id) = lower(BU.business_id)
                      and CU.date = RU.date
                      and lower(CU.business_id) = lower(RU.business_id)
                       group by BU.name,BU.stars,CU.date,Week,Year,RU.stars
                        order by BU.name,CU.date
                          """)

df_BuRvCnt.show(5)
try:
        df_BuRvCnt.coalesce(1).write.format('json').save('/opt/spark-Aggregate/Template_df_BuRvCnt.json')
except:
        print ("An error occurred while writing Template_df_BuRvCnt.json")