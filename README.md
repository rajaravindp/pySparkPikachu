# pySparkPilot
PySpark Project: Demonstrating Data Analysis and Processing using Apache Spark

The code showcases a myriad of pyspark ops performed on studentData dataset available down in the codebase. 

The project leverages PySpark, a powerful distributed data processing framework, to efficiently process large-scale data. It provides code snippets and examples for performing common data manipulation and analysis tasks.

Key features of the code base include:
- Reading a CSV file as an RDD (Resilient Distributed Dataset).
- Filtering and dropping the header from the RDD to ensure data integrity.
- Calculating various statistics and insights on the student data:
- Total number of students in the dataset.
- Total marks scored by gender, differentiating between male and female students.
- Counting the number of passed vs. failed students based on a threshold value.
- Determining the total number of courses offered.
- Analyzing enrollments per course to understand popularity.
- Aggregating total marks achieved per course.
- Calculating the average marks scored per course.
- Identifying the maximum and minimum marks achieved by course.
- Computing the average age of male students.

The provided code demonstrates the use of fundamental PySpark operations like map, reduceByKey, filter, distinct, and more. It serves as a valuable resource for learning PySpark and understanding how to efficiently process and analyze large datasets.

Feel free to explore the codebase, adapt it to your requirements, or use it as a reference to enhance your PySpark skills and tackle similar data analysis tasks.
