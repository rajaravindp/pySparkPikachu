# Databricks notebook source
# Import required modules for Spark
from pyspark import SparkConf, SparkContext
# Create a SparkConf object with an application name
conf = SparkConf().setAppName('pySparkPilot')
# Create a SparkContext object using the configuration
sc = SparkContext.getOrCreate(conf=conf)

# COMMAND ----------

# Read csv as RDD
rdd = sc.textFile('/FileStore/tables/StudentData.csv')
# Filter and drop header
header = rdd.first()
rdd = rdd.filter(lambda x: x!=header)
data = rdd.collect()
for row in data:
    print(row)

# COMMAND ----------

# Total number of students 
rdd.count()

# COMMAND ----------

# Total marks by gender
rdd = rdd.map(lambda x:x.split(','))
rdd1 = rdd
marks_by_gender = rdd1.map(lambda x: (x[1], x[5])).collect()
marks_female = []
marks_male = []
for i in marks_by_gender:
    if i[0] == 'Male':
        marks_male.append(int(i[1]))
    else:
        marks_female.append(int(i[1]))
print(f'Total marks scored by male students = {sum(marks_male)}')
print(f'Total marks scored by female students = {sum(marks_female)}')

# COMMAND ----------

# Atlernatively - Implement ReduceByKey method
marks_by_gender1 = rdd1.map(lambda x: (x[1], int(x[5])))
marks_by_gender1.reduceByKey(lambda x, y: x+y).collect()

# COMMAND ----------

# Total number of passed vs failed students - >50 = Pass else Fail
rdd2 = rdd
marks = rdd2.map(lambda x: int(x[5]))
mark = marks.collect()
passed = []
for i in mark:
    if i > 50:
        passed.append(i)
tot_passed = (len(passed))
tot_failed = rdd2.count()-tot_passed
print('tot_passed = ',tot_passed)
print('tot_failed = ', tot_failed)

# Alternatively - Implement filter method
total_passed = rdd2.filter(lambda x: int(x[5]) > 50).count()
total_failed = rdd2.filter(lambda x: int(x[5]) <= 50).count()
print(f"total number of students who passed = {total_passed}")
print(f"total number of students who failed = {total_failed}")

# COMMAND ----------

# Total number of courses offered
rdd3 = rdd
total_courses = rdd3.map(lambda x: (x[3])).distinct().count()
print(f'Total Number of Courses Offered = {total_courses}')

# COMMAND ----------

# Total enrollments per course
rdd4 = rdd
course_cnt = rdd4.map(lambda x: (x[3],1))
course_cnt.reduceByKey(lambda x, y: x+y).collect()

# COMMAND ----------

# Total marks per course
rdd5 = rdd
marks_per_course = rdd5.map(lambda x: (x[3], int(x[5])))
marks_per_course.reduceByKey(lambda x, y: x+y).collect()

# COMMAND ----------

# Avg marks scored per course
rdd6 = rdd
avg_marks = rdd6.map(lambda x: (x[3], (int(x[5]), 1)))
avg_marks = avg_marks.reduceByKey(lambda x, y: (x[0]+y[0], x[1]+y[1]))
avg_marks.map(lambda x: (x[0], round(x[1][0] / x[1][1], 2))).collect() #Rounded to 2 decimal places

# COMMAND ----------

# Alternatively - Implement the mapValues method
avg_marks_res = avg_marks.mapValues(lambda x: x[0]/x[1])
avg_marks_res.collect()

# COMMAND ----------

# Maximum marks scored by course
rdd7 = rdd
max_marks = rdd7.map(lambda x: (x[3], int(x[5])))
max_marks.reduceByKey(lambda x, y: x if x>y else y).collect()

# COMMAND ----------

# Least marks achieved by course
rdd8 = rdd
min_marks = rdd8.map(lambda x: (x[3], int(x[5])))
min_marks.reduceByKey(lambda x, y: x if x<y else y).collect()

# COMMAND ----------

# Average age of male students
rdd9 = rdd
age = rdd9.map(lambda x: (x[1], (int(x[0]), 1)))
age.reduceByKey(lambda x, y: (x[0]+y[0], x[1]+y[1])).collect()
