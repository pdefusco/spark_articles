## Instructions for Deploying the Scala Job to CDE

```
cde spark submit \
  --executor-memory "20g" \
  --executor-cores 10 \
  --driver-memory "5g" \
  --driver-cores 4 \
  target/scala-2.12/sparkparquetjoinjob_2.12-0.1.jar
```
