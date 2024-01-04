# Databricks notebook source
from pyspark.sql.session import SparkSession
from pyspark.sql.functions import split, explode, regexp_replace, to_timestamp, col, lit, current_timestamp, when, date_format, hour
from pyspark.sql.types import BooleanType
from pyspark.sql.functions import concat, lit
from pyspark.sql.functions import expr
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import MapType, StringType

# COMMAND ----------

files = dbutils.fs.ls('/FileStore/tables')
display(files)

# COMMAND ----------

df_business = spark.read.json('dbfs:/FileStore/tables/yelp_academic_dataset_business.json')
df_checkin = spark.read.json('dbfs:/FileStore/tables/yelp_academic_dataset_checkin.json')
df_review_1 = spark.read.json('dbfs:/FileStore/tables/yelp_academic_dataset_review_json.part1')
df_review_2 = spark.read.json('dbfs:/FileStore/tables/yelp_academic_dataset_review_json.part2')
df_review_3 = spark.read.json('dbfs:/FileStore/tables/yelp_academic_dataset_review_json.part3')
df_review_4 = spark.read.json('dbfs:/FileStore/tables/yelp_academic_dataset_review_json.part4')
df_tip = spark.read.json('dbfs:/FileStore/tables/yelp_academic_dataset_tip.json')
df_user_1 = spark.read.json('dbfs:/FileStore/tables/yelp_academic_dataset_user_json__1_.part1')
df_user_2 = spark.read.json('dbfs:/FileStore/tables/yelp_academic_dataset_user_json__1_.part2')

# COMMAND ----------

# Combine the four review DataFrames into one
df_review_combined = df_review_1.union(df_review_2).union(df_review_3).union(df_review_4)

# Combine the two user DataFrames into one
df_user_combined = df_user_1.union(df_user_2)

# COMMAND ----------

business = df_business.select(
    col("business_id"),
    col("name").alias("business_name"),
    col('categories'),
    col("city"),
    col("state"),
    col("postal_code"),
    col("latitude"),
    col("longitude"),
    col("stars"),
    col("review_count"),
    col("is_open"),
    col("hours.Monday").alias("Monday"),
    col("hours.Tuesday").alias("Tuesday"),
    col("hours.Wednesday").alias("Wednesday"),
    col("hours.Thursday").alias("Thursday"),
    col("hours.Friday").alias("Friday"),
    col("hours.Saturday").alias("Saturday"),
    col("hours.Sunday").alias("Sunday"),
    col("attributes.AcceptsInsurance").alias("AcceptsInsurance"),
    col("attributes.AgesAllowed").alias("AgesAllowed"),
    col("attributes.Alcohol").alias("Alcohol"),
    col("attributes.BYOB").alias("BYOB"),
    col("attributes.BYOBCorkage").alias("BYOBCorkage"),
    col("attributes.BikeParking").alias("BikeParking"),
    col("attributes.BusinessAcceptsBitcoin").alias("BusinessAcceptsBitcoin"),
    col("attributes.BusinessAcceptsCreditCards").alias("BusinessAcceptsCreditCards"),
    col("attributes.ByAppointmentOnly").alias("ByAppointmentOnly"),
    col("attributes.Caters").alias("Caters"),
    col("attributes.CoatCheck").alias("CoatCheck"),
    col("attributes.Corkage").alias("Corkage"),
    col("attributes.DietaryRestrictions").alias("DietaryRestrictions"),
    col("attributes.DogsAllowed").alias("DogsAllowed"),
    col("attributes.DriveThru").alias("DriveThru"),
    col("attributes.GoodForDancing").alias("GoodForDancing"),
    col("attributes.GoodForKids").alias("GoodForKids"),
    col("attributes.HappyHour").alias("HappyHour"),
    col("attributes.HasTV").alias("HasTV"),
    col("attributes.NoiseLevel").alias("NoiseLevel"),
    col("attributes.Open24Hours").alias("Open24Hours"),
    col("attributes.OutdoorSeating").alias("OutdoorSeating"),
    col("attributes.RestaurantsAttire").alias("RestaurantsAttire"),
    col("attributes.RestaurantsCounterService").alias("RestaurantsCounterService"),
    col("attributes.RestaurantsDelivery").alias("RestaurantsDelivery"),
    col("attributes.RestaurantsGoodForGroups").alias("RestaurantsGoodForGroups"),
    col("attributes.RestaurantsPriceRange2").alias("RestaurantsPriceRange2"),
    col("attributes.RestaurantsReservations").alias("RestaurantsReservations"),
    col("attributes.RestaurantsTableService").alias("RestaurantsTableService"),
    col("attributes.RestaurantsTakeOut").alias("RestaurantsTakeOut"),
    col("attributes.Smoking").alias("Smoking"),
    col("attributes.WheelchairAccessible").alias("WheelchairAccessible"),
    col("attributes.WiFi").alias("WiFi")
)
display(business)

# COMMAND ----------

def clean_string_column(column):
    return regexp_replace(column, "(^u'|^'|'$)", "")

def clean_all_columns(df):
    columns = df.columns
    for column in columns:
        df = df.withColumn(column, clean_string_column(col(column)))
    return df

business = clean_all_columns(business)
display(business)

# COMMAND ----------

from pyspark.sql.functions import split, explode, regexp_replace, to_timestamp
# ADD a Column with splitted dates
checkin = df_checkin.withColumn("all_dates", split("date", ", "))
# Flattern Dates
checkin = checkin.withColumn("checkin_dates", explode("all_dates")).select("business_id", "checkin_dates")
# Save cleaned Checkin Data to Delta format
checkin.write.format("delta").save("dbfs:/FileStore/tables/yelp_data/checkin_delta")
display(checkin)

# COMMAND ----------

tip = df_tip.withColumn("tip_date", to_timestamp("date", "yyyy-MM-dd HH:mm:ss"))
#tip.write.format("delta").save("dbfs:/FileStore/tables/yelp_data/tip_delta")
display(tip)

# COMMAND ----------

review = df_review_combined.drop("_corrupt_record").withColumn("review_date", to_timestamp("date", "yyyy-MM-dd HH:mm:ss"))
# Drop Null for review_id
review = review.filter(col("review_id").isNotNull())
columns_rename = ["cool", "funny", "stars", "text", "useful"]
for col_name in columns_rename:
    review = review.withColumnRenamed(col_name, "review_" + col_name)
# Save cleaned review data to Delta format
review.write.format("delta").save("dbfs:/FileStore/tables/yelp_data/review_delta")
display(review)

# COMMAND ----------

user = df_user_combined.withColumnRenamed("name", "user_name").withColumnRenamed("review_count", "user_review_count")
user = user.drop("_corrupt_record")
user.write.format("delta").save("dbfs:/FileStore/tables/yelp_data/user_delta")
display(user)

# COMMAND ----------

from pyspark.sql.functions import lit, current_timestamp, when, to_timestamp

# Assuming the data was read in on 2023/12/29
user = user.withColumn("start_date", lit("2023-12-29 20:00:00.000")).withColumn("end_date", lit("9999-99-99")).withColumn("is_current", lit(True))
user = user.withColumn("start_date", to_timestamp("start_date"))

# Daily update table named "user_new"
# Assuming we update the name of user with user_id "-TT5e-YQU9xLb1JAGCGkQw"
condition = (col("user_id") == "-TT5e-YQU9xLb1JAGCGkQw")
user_new = user.withColumn("user_name", when(condition, "Daniel_update").otherwise(col("user_name")))
user_new = user_new.withColumn("is_current", lit(True)).withColumn("start_date", current_timestamp())

# Identify any changes in user_new
changed_records = user.join(user_new, "user_id", "inner").filter(user["user_name"] != user_new["user_name"])
user_ids = [row.user_id for row in changed_records.select("user_id").collect()]

# Update start date and end date
user = user.withColumn("is_current", when(col("user_id").isin(user_ids), False).otherwise(col("is_current")))
user = user.withColumn("end_date", when(col("user_id").isin(user_ids), current_timestamp()).otherwise(col("end_date")))
user_update = user_new.filter(col("user_id").isin(user_ids))

userDf = user_update.union(user)
display(userDf)

# COMMAND ----------

# Assuming the data was read in on 2023/12/29
business = business.withColumn("start_date", lit("2023-12-29 20:00:00.000")).withColumn("end_date", lit("9999-99-99")).withColumn("is_current", lit(True))
business = business.withColumn("start_date", to_timestamp("start_date"))

# Daily update table named "business_new"
# Assuming we update the business_name of business with business_id "96752mk7VlAUtWg8o02Tvw"
condition = (col("business_id") == "96752mk7VlAUtWg8o02Tvw")
business_new = business.withColumn("business_name", when(condition, "Center City Pediatrics_update").otherwise(col("business_name")))
business_new = business_new.withColumn("is_current", lit(True)).withColumn("start_date", current_timestamp())

# Identify any changes in business_new
changed_records = business.join(business_new, "business_id", "inner").filter(business["business_name"] != business_new["business_name"])
business_ids = [row.business_id for row in changed_records.select("business_id").collect()]

# Update start date and end date
business = business.withColumn("is_current", when(col("business_id").isin(business_ids), False).otherwise(col("is_current")))
business = business.withColumn("end_date", when(col("business_id").isin(business_ids), current_timestamp()).otherwise(col("end_date")))
business_update = business_new.filter(col("business_id").isin(business_ids))

businessDf = business_update.union(business)


# COMMAND ----------

df = review.join(user, "user_id").join(business, "business_id")
display(df)
df.write.format("delta").save("dbfs:/FileStore/tables/yelp_data/yelp_full_delta")

# COMMAND ----------

da_df = df.select("business_id",
               "state",
               "city",
               "review_id",
               "user_id",
               "review_date")
display(da_df)
# add "weekday" to the da_df
da_df = da_df.withColumn("weekday", date_format("review_date", "E"))
# add "time_of_day" to the da_df
da_df = da_df.withColumn("time_of_day", when(hour("review_date") < 12, "morning").otherwise("evening"))
display(da_df)
