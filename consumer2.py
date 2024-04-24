from pymongo import MongoClient
from kafka import KafkaConsumer
import json
from collections import defaultdict, Counter
import itertools
import hashlib


def hash_pair(pair):
    return int(hashlib.md5(f"{pair[0]}-{pair[1]}".encode()).hexdigest(), 16) % 1000


def pcy_algorithm(transactions, min_support, bitmap_size):
    item_counts = defaultdict(int)
    bucket_counts = defaultdict(int)
    for transaction in transactions:
        unique_items = sorted(set(transaction))
        for item in unique_items:
            item_counts[item] += 1
        for pair in itertools.combinations(unique_items, 2):
            bucket_index = hash_pair(pair)
            bucket_counts[bucket_index] += 1

    bitmap = [1 if count >= min_support else 0 for count in bucket_counts.values()]

    pair_counts = defaultdict(int)
    for transaction in transactions:
        unique_items = sorted(set(transaction))
        for pair in itertools.combinations(unique_items, 2):
            if bitmap[hash_pair(pair)]:
                pair_counts[pair] += 1

    frequent_pairs = {pair for pair,
                      count in pair_counts.items() if count >= min_support}
    return frequent_pairs


def main():
    # MongoDB connection setup
    client = MongoClient('localhost', 27017)
    db = client['PCY_db']
    collection = db['pcy_itemsets']

    consumer = KafkaConsumer(
        'bda3',
        bootstrap_servers=['localhost:9092'],
        auto_offset_reset='earliest',
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )

    window_size = 15
    transactions = []
    min_support = 8
    bitmap_size = 1000

    try:
        for message in consumer:
            transaction = message.value
            transactions.append(transaction)

            if len(transactions) > window_size:
                transactions.pop(0)

            if len(transactions) == window_size:
                frequent_pairs = pcy_algorithm(
                    transactions, min_support, bitmap_size)
                # Store frequent pairs in MongoDB
                for pair in frequent_pairs:
                    document = {'pair': list(pair), 'support': min_support}
                    collection.insert_one(document)
                print(f"Stored {len(frequent_pairs)
                                } frequent pairs to MongoDB.")

    except KeyboardInterrupt:
        print("Stopped by user.")
    finally:
        consumer.close()
        client.close()


if __name__ == "__main__":
    main()
