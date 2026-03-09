
### Question 1
(week7) PS D:\Study_DE\DE-zoomcamp\week7> docker exec -it week7-redpanda-1 rpk --version
rpk version (Redpanda CLI): v25.3.9 (rev 836b4a36ef6d5121edbb1e68f0f673c2a8a244e2)

### Question 2
(week7) PS D:\Study_DE\DE-zoomcamp\week7> docker exec -it week7-redpanda-1 rpk topic create green-trips
TOPIC        STATUS
green-trips  OK

### Question 3

```python
	import json

	from kafka import KafkaProducer

	def json_serializer(data):
		return json.dumps(data).encode('utf-8')

	server = 'localhost:9092'

	producer = KafkaProducer(
		bootstrap_servers=[server],
		value_serializer=json_serializer
	)

	producer.bootstrap_connected()

True

### Question 4
