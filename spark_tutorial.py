import json
from math import exp
from datetime import datetime
import sklearn
import pickle
import re
from pyspark.sql import SQLContext # You need SQL context
from pyspark.sql.types import * # Export the type modules for schema
from pyspark.sql import Row  


# Instanciate SQL Context
sqc = SQLContext(sc)

############################################# SPARK CORE #############################################

#### PART 1: Creating an RDD

# We start by creating the 3 RDDs from the different datasets from Amazon product reviews.
# Note that it does not move the data at this stage due to the lazy evaluation nature.
fashion = sc.textFile('data/fashion.json')
electronics = sc.textFile('data/electronics.json')
sports = sc.textFile('data/sports.json')

# Let's do some data exploration.
print "fashion has {0} rows, electronics {1} rows and sports {2} rows".format(fashion.count(), electronics.count(), sports.count())
print "fashion first row:"
fashion.first()

# We can union them.
union_of_rdds = fashion.union(electronics).union(sports)
print union_of_rdds.first()

# We can now parse the file using the json library.
parsed_fashion = fashion.map(lambda x: json.loads(x))
parsed_fashion.first()

# Another way of loading files is by using a list of comma-separated file paths or a wildcard.
data = sc.textFile('data/fashion.json,data/electronics.json,data/sports.json').map(lambda x: json.loads(x))

# QUESTION: How many partitions does the rdd have?
data.getNumPartitions()

# Now let's imagine we want to know the number of lines in each partition.
# For that, we need to access the data in each single partition and run operations on them instead of on each row.
# For this, we will use mapPartitionsWithIndex which takes a partition index and an iterator over the data as arguments.
# Each function in the API is documented in: https://spark.apache.org/docs/1.3.1/api/python/pyspark.html#pyspark.RDD.
indexed_data = data.mapPartitionsWithIndex(lambda splitIndex, it: [(splitIndex, len([x for x in it]))])
indexed_data.collect()

#### PART 2: Reducers

#The next thing we have been tasked to do is to get the total number of reviews per product.
product_num = data.map(lambda x: (x['asin'], 1)).reduceByKey(lambda x,y: x+y)
# The rdd product_num will contain (product_asin, total_number_reviews)

# What are the maximum and minimum number of reviews?
max_num = product_num.map(lambda x: x[1]).max()
min_num = product_num.map(lambda x: x[1]).min()

print "Max number of reviews is {0}, min number of reviews is {1}".format(max_num, min_num)

# EXERCISE: what is the max score for each product?

#### PART 3: Joining multiple data sources

# We want to join the product reviews by users to the product metadata.
product_metadata = sc.textFile('data/sample_metadata.json').map(lambda x: json.loads(x))
print product_metadata.first()

# The categories are a list of lists, so we will make it easier to handle by 'flattening them out'.
def flatten_categories(line):
    old_cats = line['categories']
    line['categories'] = [item for sublist in old_cats for item in sublist]
    return line

product_metadata = product_metadata.map(lambda x: flatten_categories(x))
print product_metadata.first()

# We want to join the review data to the metadata about the product.
# We can use the 'asin' for that, which is a unique identifier for each product.
# In order to do a join, we need to turn each structure into key-value pairs.
key_val_data = data.map(lambda x: (x['asin'], x))
key_val_metadata = product_metadata.map(lambda x: (x['asin'], x))

print "We are joining {0} product reviews to {1} rows of metadata information about the products.\n".format(key_val_data.count(),key_val_metadata.count())
print "First row of key_val_data:"
print key_val_data.first()

print "number partitions key_val_data: ",
print key_val_data.getNumPartitions()
print "number partitions key_val_metadata: ",
print key_val_metadata.getNumPartitions()
joined = key_val_data.join(key_val_metadata)

# What is the number of output partitions of the join? To understand this,
# the best is to refer back to the Pyspark source code:
# https://github.com/apache/spark/blob/branch-1.3/python/pyspark/join.py

# QUESTION: what is the number of partitions in joined?
print "This RDD has {0} partitions.".format(joined.getNumPartitions())

joined.take(2)

# To make it easier to manipulate, we will change the structure of the joined rdd to be a single dictionary.
def merge_dictionaries(metadata_line, review_line):
    new_dict = review_line
    new_dict.update(metadata_line)
    return new_dict

nice_joined = joined.map(lambda x: merge_dictionaries(x[1][0], x[1][1]))
nice_joined.first()

# A couple of questions to probe your understanding of Spark
# Testing Spark understanding
# QUESTION: if I run this, what will be the title of the first row?
def change_title(line):
    line['title'] = 'this is the title'
    return line

categories = nice_joined.map(lambda x: change_title(x))

# ANSWER:
print categories.map(lambda x: x['title']).first()

# QUESTION: if I run this, what will be the title of the first row?
nice_joined.map(lambda x: x['title']).first()

def get_first_category(line):
    line['categories'] = line['categories'][0]
    return line

print "BEFORE"
print "the categories in the first 2 fields are: "
nice_joined.map(lambda x: x['categories']).take(2)

# QUESTION: if I run this, what will it print?
print "AFTER"
nice_joined.map(lambda x: get_first_category(x)).map(lambda x: x['categories']).take(2)

#### PART 4: GroupByKey

# Now that we have joined two data sources, we can start doing some ad-hoc analysis of the data!
# Let's start by counting the number of reviews per category. The categories are encoded as a list of categories,
# so we need to count 1 for each 'sub-category'.
# We want to get the distinct number of categories
all_categories = nice_joined.flatMap(lambda x: x['categories'])
print "all_categories.take(5): ",
print all_categories.take(5)
num_categories = all_categories.distinct().count()
print

print "There are {0} categories.".format(num_categories)

# We are going to take the categories in each review and count them as being reviewed once.
category_count = nice_joined.flatMap(lambda x: [(y,1) for y in x['categories']])
category_total_count = category_count.reduceByKey(lambda x,y: x+y)
print category_total_count.take(10)

sorted_categories = sorted(category_total_count.collect(), key=lambda x: x[1], reverse=True)
print "The top 5 categories are:"
print sorted_categories[:5]

# Next, we have been tasked to get the average product review length for each category.
# We can solve this using groupByKey!
category_review = nice_joined.flatMap(lambda x: [(y, len(x['reviewText'])) for y in x['categories']])
print "category_review.first(): " + str(category_review.first())
print

grouped_category_review = category_review.groupByKey().map(lambda x: (x[0], sum(x[1])/float(len(x[1]))))
print "grouped_category_review.first(): " + str(grouped_category_review.first())
print

### Now we can sort the categories by average product review length
print "The top 10 categories are: " + str(sorted(grouped_category_review.collect(),
                                                 key=lambda x: x[1], reverse=True)[:10])
# EXERCISE: Do the same thing, but this time you are not allowed to use groupByKey()!

#### Optional: Data skewness
def get_part_index(splitIndex, iterator):
    for it in iterator:
        yield (splitIndex, it)

def count_elements(splitIndex, iterator):
    n = sum(1 for _ in iterator)
    yield (splitIndex, n)

print "***Creating the large rdd***"
num_parts = 16
# create the large skewed rdd
skewed_large_rdd = sc.parallelize(range(0,num_parts), num_parts).flatMap(lambda x: range(0, int(exp(x)))).mapPartitionsWithIndex(lambda ind, x: get_part_index(ind, x)).cache()
print "first 5 items:" + str(skewed_large_rdd.take(5))
print "num rows: " + str(skewed_large_rdd.count())
print "num partitions: " + str(skewed_large_rdd.getNumPartitions())
print "The distribution of elements per partition is " + str(skewed_large_rdd.mapPartitionsWithIndex(lambda ind, x: count_elements(ind, x)).collect())
print

print "***Creating the small rdd***"
small_rdd = sc.parallelize(range(0,num_parts), num_parts).map(lambda x: (x, x))
print "first 5 items:" + str(small_rdd.take(5))
print "num rows: " + str(small_rdd.count())
print "num partitions: " + str(small_rdd.getNumPartitions())
print "The distribution of elements per partition is " + str(small_rdd.mapPartitionsWithIndex(lambda ind, x: count_elements(ind, x)).collect())

print

print "Joining them"
t0 = datetime.now()
result = skewed_large_rdd.leftOuterJoin(small_rdd)
result.count()
print "The direct join takes %s"%(str(datetime.now() - t0))
print "The joined rdd has {0} partitions and {1} rows".format(result.getNumPartitions(), result.count())

#### Optional: Integrating Spark with popular Python libraries

model = pickle.load(open('data/classifier.pkl', 'r'))
model_b = sc.broadcast(model)
fashion.map(lambda x: eval(x)['reviewText']).map(lambda x: (x, model_b.value.predict([x])[0])).first()

################################### Spark DataFrame API and Spark SQL ###################################

#  Part 5 : Loading data to spark
# We start by loading the files to spark
# First, load them as text file to validate
review_filepaths = 'Data/Reviews/*'
textRDD = sc.textFile(review_filepaths)
print 'number of reviews : {0}'.format(textRDD.count())
print 'sample row : \n{0}'.format(textRDD.first())

# You can let spark infer the schema of your DataFrame 
inferredDF = sqc.jsonFile(review_filepaths)
inferredDF.first()

# Or you can programmatically tell spark how the schema looks like
# Define Schema
REVIEWS_SCHEMA_DEF = StructType([
        StructField('reviewerID', StringType(), True),
        StructField('asin', StringType(), True),
        StructField('reviewerName', StringType(), True),
        StructField('helpful', ArrayType(
                IntegerType(), True), 
            True),
        StructField('summary', StringType(), True),
        StructField('reviewText', StringType(), True),
        StructField('reviewTime', StringType(), True),
        StructField('overall', DoubleType(), True),
        StructField('unixReviewTime', LongType(), True)
    ])
# View schema definition
print REVIEWS_SCHEMA_DEF

# Apply schema to data
appliedDF = sqlContext.jsonFile(review_filepaths,schema=REVIEWS_SCHEMA_DEF)
appliedDF.first()


# Part 6: DataFrame Operations 

# Spark DataFrame API allow you to do multiple operations on the Data. The primary advantage of using the DataFrame API is that you can do data transoformations with the high level API without having to use Python. Using the high level API has its advantages which will be explained later in the tutorial.

# DataFrame API have functionality similar to that of Core RDD API. For example: 
# + map                     : foreach, Select
# + mapPartition            : foreachPartition
# + filter                  : filter
# + groupByKey, reduceByKey : groupBy 

# 6.1 Selecting columns

# You can use SELECT statement to select columns from your dataframe

columnDF = appliedDF.select(appliedDF.asin,
                            appliedDF.overall,
                            appliedDF.reviewText,
                            appliedDF.reviewerID,
                            appliedDF.unixReviewTime)
columnDF.show()

# 6.2 Missing Values

# Similar to Pandas, DataFrames come equipped with functions to address missing data.
# + dropna function: can be used to remove observations with missing values
# + fillna function: can be used to fill missing values with a default value

# get null observations out
densedDF=columnDF.dropna(subset=["overall"]).fillna(0.0,subset=["helpful"]) 
densedDF.show()

# 6.3 Filtering Rows

# filter keywords allow you to filter rows in DFs
filteredDF=
# CODE WILL BE SHARED DURING THE TUTORIAL AS THIS IS PART OF AN EXERCISE

# 6.5 group by 

# Grouping is equivalent to the groupByKey in the core RDD API. You can transform the grouped values using a summary action such as:
# + count
# + sum
# + average
# + max and so on ...

grouped = filteredDF.groupBy("overall").count()
grouped.show()


# 6.5. Joining DataFrames together

# first, load the product dataset
product_filepaths = 'Data/Products/*'
productRDD = sc.textFile(product_filepaths)
productRDD.first()

# Load it as a dataframe
# Load Dataset2 : Amazon Product information
# First, define Schema for second Dataset
PRODUCTS_SCHEMA_DEF = StructType([
        StructField('asin', StringType(), True),
        StructField('title', StringType(), True),
        StructField('price', DoubleType(), True),
        StructField('categories', ArrayType(ArrayType(
            StringType(), True),True),True),
        StructField('related', MapType(StringType(), ArrayType(
                StringType(), True),True)),
        StructField('imUrl', StringType(), True),
        StructField('salesRank', MapType(StringType(), IntegerType(), True),True)
    ])

# Load the dataset
productDF = sqc.jsonFile(product_filepaths,PRODUCTS_SCHEMA_DEF)
# productDF.show()
# productDF.first()

"""
*QUESTION*: What do you think will happen if we remove some fields from this schema?

1. The schema fails
2. The schema works fine

ANSWER??? 

Now lets join the two datasets
"""

enrichedReviews = filteredDF.join(productDF, productDF.asin==filteredDF.asin).dropna(subset="title")
enrichedReviews.count()

enrichedReviews.show()


#  7. Saving your DataFrame
# Now that we have done some operations on the data, we can save the file for later use. Standard data formats are a 
# great way to opening up valuable data to your entire organization. Spark DataFrames can be saved in many different 
# formats including and not limited to JSON, parquet, Hive and etc... 

try:
    columnDF.saveAsParquetFile('Data/Outputs/reviews_filtered.parquet')
except:
    pass

print "Saved as parquet successfully"


#  8. Using Spark SQL

# Spark DataFrames also allow you to use Spark SQL to query from Petabytes of data. Spark comes with a SQL like query 
# language which can be used to query from Distributed DataFrames. A key advantage of using Spark SQL is that the 
# (https://databricks.com/blog/2015/04/13/deep-dive-into-spark-sqls-catalyst-optimizer.html) under the hood transforms
#  your SQL query to run it most efficiently. 

# Spark SQL can leverage the same functionality as the DataFrame API provides. In fact, it provides more functionality via SQL capabilities and HQL capabilities that are available to Spark SQL environment. 

# For the sake of time constrains, I will explain different functions available in Spark SQL environment by using examples that use multiple functions. This will benefit by:
# + Covering many functions that are possible via spark SQL
# + Giving an understanding about how to pipe multiple functions together

# Read the reviews parquet file
reviewsDF = sqc.parquetFile('Data/Outputs/reviews_filtered.parquet')

# Register the DataFrames to be used in sql
reviewsDF.registerAsTable("reviews")
productDF.registerAsTable("products")

print 'There are {0} reviews about {1} products'.format(reviewsDF.count(),productDF.count())


# NOW LET'S RUN A SQL QUERY

sql_query = """SELECT reviews.asin, overall, reviewText, price
            FROM reviews JOIN products ON  reviews.asin=products.asin
            WHERE price > 50.00
"""

result = sqc.sql(sql_query)
result.show()

# User defined functions
# Spark SQL also provides the functionality similar to User Defined Functions (UDF) offering in Hive. 
# Spark uses registerFunction() function to register python functions in SQLContext.

# user defined function
def transform_review(review):
    x1 = re.sub('[^0-9a-zA-Z\s]+','',review)
    return [x1.lower()]

# register table from above
result.registerAsTable("result")

# register function from above
sqc.registerFunction("to_lowercase", lambda x:transform_review(x),returnType=ArrayType(StringType(), True))

# use the registered function inside SQL 
sql_query_transform = """SELECT asin, reviewText, to_lowercase(reviewText) as cleaned
            FROM result
"""

result_transform = sqc.sql(sql_query_transform)
result_transform.show()

# FINALLY,  Mix and Match!!

# You can also mix DataFrames, RDDs and SparkSQL to make it work for you. 

# Scenario:
# We want to investigate the average rating of reviews in terms of the categories they belong to. In order to do this, we:
# + query the needed data using DataFrames API
# + classify the reviews into different categories using core RDD API
# + query the avearage rating for each category using Spark SQL

# load classifier and broadcast it
model = pickle.load(open('Data/classifiers/classifier.pkl', 'r'))
classifier_b = sc.broadcast(model) 

# DO CLASSIFICATION IN CORE RDD FORMAT
# fashion.map(lambda x: eval(x)['reviewText']).map(lambda x: (x, model_b.value.predict([x])[0])).first()
classifiedRDD = result_transform.map(lambda row: 
                                     (row.asin,row.reviewText,str(classifier_b.value.predict(row.cleaned)[0]))
                                    )

classifiedRDD.first()

# Transform the RDD into a DataFrame
CLASSIFIED_SCHEMA = StructType([
        StructField('asin', StringType(), True),
        StructField('review', StringType(), True),
        StructField('category', StringType(), True)
    ])

classifiedDF = sqc.createDataFrame(classifiedRDD,CLASSIFIED_SCHEMA)

classifiedDF.show()

# run a SQL query on the data
classifiedDF.registerAsTable('enrichedReviews')

sql_query_test = """SELECT category, avg(overall) as avgRating
            FROM reviews 
            JOIN products ON reviews.asin=products.asin 
            JOIN enrichedReviews ON products.asin=enrichedReviews.asin
            WHERE price > 50.0
            GROUP BY enrichedReviews.category
"""

resultTest = sqc.sql(sql_query_test)
resultTest.show()




