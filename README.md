# Finding-max-value-from-array  
write spark code, list of name of participants who has rank=1 most number of times)
# Input and Output
![Input data](https://github.com/user-attachments/assets/8753b149-75e8-4149-88c7-f25659bad9bc)  
data = [  
    ("a", [1, 1, 1, 3]),  
    ("b", [1, 2, 3, 4]),  
    ("c", [1, 1, 1, 1, 4]),  
    ("d", [3])  
]  
columns = ["name","rank"]  
df = spark.createDataFrame(data, schema = columns)  
df.show()  
df_counts = df.withColumn(  
    "rank1_count",  
    size(filter(col("rank"), lambda x : x == 1))  
)  
df_counts.show()  

max_count = df_counts.agg(max("rank1_count")).collect()[0][0]  

finaldf = df_counts.filter(col("rank1_count") == max_count).select("name")  


finaldf.show()
