### Spark Anti Joins

### CDE Session Commands

```
cde session create --name "spark_antijoins" --type pyspark
```

```
cde session interact --name "spark_antijoins"
```

### Spark Dataframe

```
from pyspark.sql import Row
from datetime import datetime, date
```

Create DFs

```
df1 = spark.createDataFrame([
  "customer_id": [1,2,3,4,5],
  "customer_location": ["Los Angeles", "San Francisco", "San Diego", "Monterrey", "Santa Barbara"]
  ])
```

```
df2 = spark.createDataFrame([
  "customer_id": [1,4,5,6,7],
  "customer_age": [30,45,60,75,90]
  ])
```

Left Anti Join

```
df1.join(
  df2,
  df1.customer_id = df2.customer_id,
  "leftanti"
  ).show()
```

Right Anti Join

```
df2.join(
  df1,
  df1.customer_id = df2.customer_id,
  "rightanti"
  ).show()
```

### Spark SQL
