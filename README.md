This library helps to give a starting point for a spark job to get single JavaDStream<T>
from kafka topic event.

You just need to define how you want to process it at each RDD/Partion level.

Run TestApp by by providing --config test-config.yml

java TestApp --config test-config.yml

java -jar <test.jar> --config test-config.yml