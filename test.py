# import requests
# import time
# headers = {"User-Agent": "Mozilla/5.0 (X11; CrOS x86_64 12871.102.0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/81.0.4044.141 Safari/537.36"}
# BASE_URL = 'https://randomuser.me/api/?nat=gb'
# session = requests.Session()
# for i in range(1000):
#     print(i)
#     response = requests.get(BASE_URL, timeout=5)
#     response.raw.chunked = True  # Fix issue 1
#     response.encoding = 'utf-8'  # Fix issue 2
#     time.sleep(1)
#     if response.status_code == 200:
#         print(response)


# import requests
# session = requests.Session()
# url = "..."  # Add your URL here
# for _ in range(10):
#     session.get(url)

from pyspark.sql import SparkSession

spark_session = (SparkSession.builder
                 .appName('Spark Dataframe')
                 .config('spark.sql.warehouse.dir', '/home/nguyenthung/Desktop/Realtime Voting System/data')
                 .enableHiveSupport()
                 .getOrCreate())

orderDataframe = spark_session.read.csv('data/data.txt', header=True, inferSchema=True)
# orderDataframe.groupBy('order_status').count().show()
orderDataframe.createGlobalTempView('order_temp_table')
# spark_session.sql("select order_status, count(*) from global_temp.order_temp_table group by order_status order by order_status").show()
# top10Customer = orderDataframe.groupBy('customer_id').count().sort('count',ascending = False).limit(10)
# top10Customer.show()
# spark_session.sql("select customer_id, count(*) as count from global_temp.order_temp_table group by customer_id order by count desc limit 10").show()

# orderDataframe.select('customer_id').distinct().show()
# spark_session.sql("select distinct(customer_id) as customer_id from global_temp.order_temp_table").show()
orderDataframe.where("order_status == 'closed'").groupBy('customer_id').count().sort('count', ascending = False).limit(1).show()
spark_session.sql("select customer_id ,count(*) as count from global_temp.order_temp_table where order_status = 'closed' group by customer_id order by count desc limit 1").show()
