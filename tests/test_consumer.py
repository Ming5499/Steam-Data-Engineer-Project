from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    'price-updates',
    bootstrap_servers='localhost:29092',
    auto_offset_reset='earliest',
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

for message in consumer:
    print(f"""
    Timestamp: {message.timestamp}
    Operation: {message.value['op']}
    Before: {message.value.get('before')}
    After: {message.value.get('after')}
    """)