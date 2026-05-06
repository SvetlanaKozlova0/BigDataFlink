import csv
import json
import os
from pathlib import Path
from kafka import KafkaProducer

KAFKA_TOPIC = 'mock_data_input'
DATA_DIRECTORY = Path('/data')
KAFKA_BOOTSTRAP_SERVERS = os.getenv('BOOTSTRAP_SERVERS', 'kafka:29092')


def send_messages():
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(
            v, ensure_ascii=False).encode('utf-8')
    )
    csv_paths = sorted(DATA_DIRECTORY.glob('*.csv'))
    if not csv_paths:
        raise FileNotFoundError(f'No CSV files found in {DATA_DIRECTORY}')
    print(f'Detected {len(csv_paths)} csv-files.')
    total_messages = 0
    for index, csv_file in enumerate(csv_paths):
        print(f'Processing: {csv_file.name}, index = {index}')
        with open(csv_file, mode='r', encoding='utf-8-sig') as current_file:
            reader = csv.DictReader(current_file)
            for original_row in reader:
                original_id = int(original_row.get('id', 0))
                global_id = index * 1000000 + original_id
                enriched_row = {
                    **original_row,
                    'global_sale_id': str(global_id),
                    'file_name': csv_file.name,
                    'file_sequence': index
                }
                producer.send(KAFKA_TOPIC, value=enriched_row)
                total_messages += 1
    producer.flush()
    producer.close()
    print(f'Sent {total_messages} messages.')


if __name__ == '__main__':
    send_messages()
