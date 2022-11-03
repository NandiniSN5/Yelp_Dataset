import sys
import re

from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split
from pyspark.sql.types import DateType,StringType
from pyspark.sql.functions import isnan, when, count, col,lower
from pyspark.sql.functions import udf
import pyspark.sql.functions as f

def BusinessUnit(spark,BUnit):
    
    df_Bu = spark.read.format('json').load(BUnit)
    df_Bu.createOrReplaceTempView("BusinessUnit")
    BUDF = spark.sql("SELECT distinct *  FROM BusinessUnit") 
    BUDF.createOrReplaceTempView("BusinessUnit")

    ## validaing the data type check against the columns 
    BFdtTypCnt   = df_Bu.where((df_Bu["address"].cast("string").isNotNull()) &
                               (df_Bu["name"].cast("string").isNotNull()) &
                               (df_Bu["postal_code"].cast("string").isNotNull()) &
                               (df_Bu["business_id"].cast("string").isNotNull()) & 
                               (df_Bu["city"].cast("string").isNotNull()) & 
                               (df_Bu["is_open"].cast("integer").isNotNull()) &
                               (df_Bu["stars"].cast("float").isNotNull()) &
                               (df_Bu["latitude"].cast("float").isNotNull()) &
                               (df_Bu["longitude"].cast("float").isNotNull()) &
                               (df_Bu["review_count"].cast("integer").isNotNull()) &
                               (df_Bu["state"].cast("string").isNotNull())
                              ).count()

    BUBadrecorcds  = df_Bu.where((df_Bu["address"].cast("string").isNull()) &
                               (df_Bu["name"].cast("string").isNull()) &
                               (df_Bu["postal_code"].cast("string").isNull()) &
                               (df_Bu["business_id"].cast("string").isNull()) & 
                               (df_Bu["city"].cast("string").isNull()) & 
                               (df_Bu["is_open"].cast("integer").isNull()) &
                               (df_Bu["stars"].cast("float").isNull()) &
                               (df_Bu["latitude"].cast("float").isNull()) &
                               (df_Bu["longitude"].cast("float").isNull()) &
                               (df_Bu["review_count"].cast("integer").isNull()) &
                               (df_Bu["state"].cast("string").isNull())
                              ).count()

    if BFdtTypCnt == df_Bu.count():
       print('Data type of Business unit columns and datatype of the data are matching')
       print('Bad records count in Business Json file ' + str(BUBadrecorcds))
    
    
    ## is_open validating 
    is_open_chk = spark.sql("SELECT is_open  FROM BusinessUnit  where is_open not in (0,1)" ).count()
    if is_open_chk:
        print('records are not in (0,1)')
    else: 
        print('records are in (0,1)')

    ## business_id length and  dictinct records validating   
    spark.sql("select * from BusinessUnit where length(business_id)>22").show()
    spark.sql("select count(distinct(business_id)), count(business_id) from BusinessUnit").show()


    ## state validating and modifying
    Stchk = spark.sql("SELECT state FROM BusinessUnit where length(state)>2 ") 
    df_BuNw = df_Bu.withColumn("state", when(col("state") == "XMS","XX").otherwise(df_Bu.state))
    df_BuNw.createOrReplaceTempView("BusinessUnitNw")

    ## Normalising the data 
   
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

    ##Normalising the city column
    df_BuNw = df_BuNw.withColumn("city",cityCleaningUDF(col("city")))
    
    ##Normalising the business_id column 
    df_BuNw = df_BuNw.withColumn("business_id",lower(col("business_id")))

    ##Normalising the latitude column 
    df_BuNw = df_BuNw.withColumn("latitude", f.round(df_BuNw["latitude"], 2))

    ##Normalising the longitude column 
    df_BuNw = df_BuNw.withColumn("longitude", f.round(df_BuNw["longitude"], 2))


    ### finding the null columns in dataframe
    cols_check = df_BuNw.columns
    tyest = df_BuNw.select([
    (
        f.count(f.when((f.col(c).isNull()), c)) if t not in ("timestamp", "date")
        else 0
    ).alias(c)
    for c, t in df_BuNw.dtypes if c in cols_check
    ]).show()

    ### null values with unknownCategory for categories column in dataframe
    df_BuNw = df_BuNw.na.fill("unknownCategory",["categories"])

    ### make all columns from differnt nested struct types
    df_allCols = df_BuNw.select(f.col("address"), 
                                      f.col("attributes.*"),
                                      f.col("business_id"),
                                      f.col("categories"),
                                      f.col("city"),
                                      f.col("hours.*"),
                                      f.col("is_open"),
                                      f.col("latitude"),
                                      f.col("longitude"),
                                      f.col("name"),
                                      f.col("postal_code"),
                                      f.col("review_count"),
                                      f.col("stars"),
                                      f.col("state")
                                     ) 
    ### repalcing all the None and NUll value with NA    

    df_allCols.show(5)  
    spark.sql("select * from BusinessUnit where length(business_id)>22").show()                            
    df_allCols = df_allCols.replace('None',None).na.fill("NA") 
    try:
        df_allCols.coalesce(1).write.format('json').save('/opt/spark-Clean/Cleaned_BusinessUnit.json')
    except:
        print ("An error occurred while writing Business Unit")
    

    
    #print(BUDF.show(5))

def ChkInUnit(spark,ChkUnit):
    df_chkIn = spark.read.format('json').load(ChkUnit)
    df_chkIn.createOrReplaceTempView("CheckInUnit")
    df_chkIn = spark.sql("SELECT distinct *  FROM CheckInUnit") 
    df_chkIn.createOrReplaceTempView("CheckInUnit")
    
    """
    chkIndupRecCnt = spark.sql(select count(*)
                                   from ChekinUnit
                                group by business_id,date
                                  having count(*) > 1
                              ).count()
    """
    df_chkin = spark.sql("""select business_id, explode(split(date,',')) FROM CheckInUnit""")
    df_chkIn = df_chkIn.withColumn("date",col("date").cast("date"))
    df_chkIn.createOrReplaceTempView("CheckInUnit")
    
    #df_chkin.show(5)
    
    spark.sql("select * from CheckInUnit where length(business_id)>22").show()
    spark.sql("select count(distinct(business_id)) from CheckInUnit ").show()

    ## validaing the data type check against the columns
    """
    ChkIndtTypCnt     = df_chkin.where((df_chkin["business_id"].cast("string").isNotNull()) &
                                     (df_chkin["col"].cast(DateType()).isNotNull()))
    ChkIndNTtTypCnt   = df_chkin.where((df_chkin["business_id"].cast("string").isNull()) &
                                     (df_chkin["col"].cast(DateType()).isNull()))
    print('Bad records count in Checkin Json file  ' + str(ChkIndNTtTypCnt.count()))
    #print(CHKDF.show(5))
    """
    df_chkIn.show(5)
    try:
        df_chkIn.coalesce(1).write.format('json').save('/opt/spark-Clean/Cleaned_CheckInUnit.json')
    except:
        print ("An error occurred while writing Check In Unit")

def ReviewUnit(spark,RvUnit):
    df_Rv = spark.read.format('json').load(RvUnit)
    df_Rv.createOrReplaceTempView("ReviewUnit")
    df_Rv = spark.sql("SELECT distinct *  FROM ReviewUnit") 
    df_Rv.createOrReplaceTempView("ReviewUnit")

    RvwdtTypCnt   = df_Rv.where((df_Rv["review_id"].cast("string").isNotNull()) &
                                (df_Rv["user_id"].cast("string").isNotNull()) &
                                (df_Rv["business_id"].cast("string").isNotNull()) &
                                (df_Rv["stars"].cast("integer").isNotNull()) & 
                                (df_Rv["date"].cast(DateType()).isNotNull()) & 
                                (df_Rv["text"].cast("string").isNotNull()) &
                                (df_Rv["useful"].cast("integer").isNotNull()) &
                                (df_Rv["funny"].cast("integer").isNotNull()) &
                                (df_Rv["cool"].cast("integer").isNotNull())
                               ).count()

    RvwNtdtTypCnt  = df_Rv.where((df_Rv["review_id"].cast("string").isNull()) &
                                 (df_Rv["user_id"].cast("string").isNull()) &
                                 (df_Rv["business_id"].cast("string").isNull()) &
                                 (df_Rv["stars"].cast("integer").isNull()) & 
                                 (df_Rv["date"].cast(DateType()).isNull()) & 
                                 (df_Rv["text"].cast("string").isNull()) &
                                 (df_Rv["useful"].cast("integer").isNull()) &
                                 (df_Rv["funny"].cast("integer").isNull()) &
                                 (df_Rv["cool"].cast("integer").isNull())
                                ).count()
    print('Bad records count in Review Json file  ' + str(RvwNtdtTypCnt))
    

    """
    RvdupRecCnt = spark.sql(select count(*)
                                 from ReviewUnit
                              group by review_id,user_id,business_id,stars,
                                       date,text,useful,funny,cool
                                having count(*) > 1
                            ).count()
    """
    #RVDF = spark.sql("SELECT *  FROM ReviewUnit") 
    spark.sql("select * from ReviewUnit where length(business_id)>22 or length(user_id)>22 or length(business_id)>22").show()
    spark.sql("""select count(distinct(review_id)), 
                        count(review_id)
                  from ReviewUnit
              """)
    spark.sql("select count(distinct(business_id)) from ReviewUnit").show()
    spark.sql("select count(distinct(user_id)) from ReviewUnit").show()
    #print(RVDF.show(5))

    ##Normalising the business_id column 
    df_RvNw = df_Rv.withColumn("business_id",lower(col("business_id")))
    df_RvNw = df_Rv.withColumn("user_id",lower(col("user_id")))
    df_RvNw = df_Rv.withColumn("review_id",lower(col("review_id")))
    df_RvNw = df_Rv.withColumn("date",col("date").cast("date"))

    df_RvNw.createOrReplaceTempView("ReviewUnit")
    df_RvNw.show(5) 
    spark.sql("SELECT *  FROM ReviewUnit").show(5) 
    try:
        df_RvNw.coalesce(1).write.format('json').save('/opt/spark-Clean/Cleaned_ReviewUnit.json')
    except:
        print ("An error occurred while writing Review Unit")
    


def UserUnit(spark,UsrUnit):
    df_Usr = spark.read.format('json').load(UsrUnit)
    df_Usr.createOrReplaceTempView("UserUnit")
    df_Usr = spark.sql("SELECT distinct *  FROM UserUnit") 
    df_Usr.createOrReplaceTempView("UserUnit")

    UsrdtTypCnt   =  df_Usr.where((df_Usr["user_id"].cast("string").isNotNull()) &
                                  (df_Usr["name"].cast("string").isNotNull()) &
                                  (df_Usr["review_count"].cast("integer").isNotNull()) &
                                  (df_Usr["useful"].cast("integer").isNotNull()) & 
                                  (df_Usr["funny"].cast("integer").isNotNull()) & 
                                  (df_Usr["cool"].cast("integer").isNotNull()) &
                                  (df_Usr["fans"].cast("integer").isNotNull()) &
                                  (df_Usr["average_stars"].cast("float").isNotNull()) &
                                  (df_Usr["compliment_hot"].cast("integer").isNotNull()) &
                                  (df_Usr["compliment_more"].cast("integer").isNotNull()) &
                                  (df_Usr["compliment_profile"].cast("integer").isNotNull()) &
                                  (df_Usr["compliment_cute"].cast("integer").isNotNull()) &
                                  (df_Usr["compliment_list"].cast("integer").isNotNull()) &
                                  (df_Usr["yelping_since"].cast(DateType()).isNotNull()) &
                                  (df_Usr["compliment_plain"].cast("integer").isNotNull()) &
                                  (df_Usr["compliment_cool"].cast("integer").isNotNull()) &
                                  (df_Usr["compliment_funny"].cast("integer").isNotNull()) &
                                  (df_Usr["compliment_writer"].cast("integer").isNotNull()) &
                                  (df_Usr["compliment_photos"].cast("integer").isNotNull()) 
                                ).count()
    
    UsrdtNTTypCnt =  df_Usr.where((df_Usr["user_id"].cast("string").isNull()) &
                                  (df_Usr["name"].cast("string").isNull()) &
                                  (df_Usr["review_count"].cast("integer").isNull()) &
                                  (df_Usr["useful"].cast("integer").isNull()) & 
                                  (df_Usr["funny"].cast("integer").isNull()) & 
                                  (df_Usr["cool"].cast("integer").isNull()) &
                                  (df_Usr["fans"].cast("integer").isNull()) &
                                  (df_Usr["average_stars"].cast("float").isNull()) &
                                  (df_Usr["compliment_hot"].cast("integer").isNull()) &
                                  (df_Usr["compliment_more"].cast("integer").isNull()) &
                                  (df_Usr["compliment_profile"].cast("integer").isNull()) &
                                  (df_Usr["compliment_cute"].cast("integer").isNull()) &
                                  (df_Usr["compliment_list"].cast("integer").isNull()) &
                                  (df_Usr["yelping_since"].cast(DateType()).isNull()) &
                                  (df_Usr["compliment_plain"].cast("integer").isNull()) &
                                  (df_Usr["compliment_cool"].cast("integer").isNull()) &
                                  (df_Usr["compliment_funny"].cast("integer").isNull()) &
                                  (df_Usr["compliment_writer"].cast("integer").isNull()) &
                                  (df_Usr["compliment_photos"].cast("integer").isNull()) 
                                ).count()
    print('Bad records count in User Json file  ' + str(UsrdtNTTypCnt)) 

    """
    UsrdupRecCnt = spark.sql(select count(*)
                                 from UserUnit
                                 group by user_id,name,review_count,yelping_since,friends,
                                          useful,funny,cool,fans,elite,average_stars,compliment_hot,
                                          compliment_more,compliment_profile,compliment_cute,compliment_list,
                                          compliment_note,compliment_plain,compliment_cool,compliment_funny,
                                          compliment_writer,compliment_photos
                                 having count(*) > 1
                             ).count()
    """
    #USRDF = spark.sql("SELECT *  FROM UserUnit") 

    spark.sql("select user_id from UserUnit where length(user_id)>22 ").show()
    spark.sql("select count(user_id),count(distinct(user_id)) from UserUnit ").show()
    df_UsrNw = df_Usr.withColumn("user_id",lower(col("user_id")))
    df_UsrNw = df_UsrNw.replace('None',None).na.fill("NA")
    df_UsrNw.show(5)
    spark.sql("SELECT distinct *  FROM UserUnit").show(5)
    try:
        df_UsrNw.coalesce(1).write.format('json').save('/opt/spark-Clean/Cleaned_UserUnit.json')
    except:
        print ("An error occurred while writing User Unit")
    
    #print(USRDF.show(5))




def TipUnit(spark,TpUnit):
    df_Tp = spark.read.format('json').load(TpUnit)
    df_Tp.createOrReplaceTempView("TPUnit")
    df_Tp = spark.sql("SELECT distinct *  FROM TPUnit") 
    df_Tp.createOrReplaceTempView("TPUnit")
    
    TipDfTypCnt   =  df_Tp.where((df_Tp["text"].cast("string").isNotNull()) &
                                (df_Tp["date"].cast(DateType()).isNotNull()) &
                                (df_Tp["compliment_count"].cast("integer").isNotNull()) &
                                (df_Tp["business_id"].cast("string").isNotNull()) & 
                                (df_Tp["user_id"].cast("string").isNotNull())
                               ).count()
    
    TipNTDfTypCnt   =  df_Tp.where((df_Tp["text"].cast("string").isNull()) &
                                (df_Tp["date"].cast(DateType()).isNull()) &
                                (df_Tp["compliment_count"].cast("integer").isNull()) &
                                (df_Tp["business_id"].cast("string").isNull()) & 
                                (df_Tp["user_id"].cast("string").isNull())
                               ).count()
  
    print('Bad records count in Tip Json file  ' + str(TipNTDfTypCnt)) 

   

    TpdupRecCnt = spark.sql("""select text,date,compliment_count,business_id,user_id
                                 from TPUnit
                             group by text,date,compliment_count,business_id,user_id
                                 having count(*) > 1
                           """).show()

    #TPDF = spark.sql("SELECT *  FROM TPUnit") 
    spark.sql("select count(distinct(user_id)) from TPUnit ").show()
    spark.sql("""select count(distinct(tp.user_id)) 
               from TPUnit tp,
                    UserUnit Usr
               where tp.user_id = Usr.user_id
              """).show()
    df_TpNw = df_Tp.withColumn("user_id",lower(col("user_id")))
    df_TpNw = df_Tp.withColumn("business_id",lower(col("business_id")))
    df_TpNw = df_TpNw.replace('None',None).na.fill("NA")
    df_TpNw.createOrReplaceTempView("TPUnit")
    df_TpNw.show(5)
    spark.sql("SELECT *  FROM TPUnit").show(5)
    try:
        df_TpNw.coalesce(1).write.format('json').save('/opt/spark-Clean/Cleaned_TipUnit.json')
    except:
        print ("An error occurred while writing Tip Unit")
    

    #print(TPDF.show(5))


if __name__ == "__main__":

    
    print(sys.argv[0])
    print(sys.argv[1])
    print(sys.argv[2])
    print(sys.argv[3])
    print(sys.argv[4])
    print(sys.argv[5])
    print(len(sys.argv))
  
    BUnit    = sys.argv[1]
    ChkUnit  = sys.argv[2]
    RvUnit   = sys.argv[3]
    UsrUnit  = sys.argv[4]
    TpUnit  = sys.argv[5]
    
    print("BusinessUnit File " + BUnit)
    print("CheckInUnit File " + ChkUnit)
    print("Review File " + RvUnit)
    print("User File " + UsrUnit)
    print("Tip File " + TpUnit)
    
    
    spark = SparkSession\
        .builder\
        .master("local") \
        .appName("Yelp_dataset_pyspark")\
        .getOrCreate()

    
    BusinessUnit(spark,BUnit)
    ChkInUnit(spark,ChkUnit)
    ReviewUnit(spark,RvUnit)
    UserUnit(spark,UsrUnit)
    TipUnit(spark,TpUnit)
    