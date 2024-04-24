from pymongo import MongoClient
from kafka import KafkaConsumer
import json
from collections import defaultdict
import itertools

# Function to find frequent items


def find_frequent_itemsets(transactions, min_support):
    item_counts = defaultdict(int)
    for transaction in transactions:
        unique_items = set(transaction)
        for item in unique_items:
            item_counts[item] += 1

    # Items meeting the minimum support
    frequent_items = {item for item,
                      count in item_counts.items() if count >= min_support}
    return frequent_items

# Generate candidate itemsets of size k from frequent itemsets of size k-1


def generate_candidates(frequent_items, k):
    return set([
        frozenset(itemset) for itemset in itertools.combinations(frequent_items, k)
        if len(itemset) == k
    ])


def apriori(transactions, min_support):
    k = 1
    frequent_itemsets = []
    current_frequent_items = find_frequent_itemsets(transactions, min_support)

    while current_frequent_items:
        frequent_itemsets.append(current_frequent_items)
        k += 1
        candidate_itemsets = generate_candidates(current_frequent_items, k)
        valid_candidates = set()

        # Prune candidates that have any non-frequent subset
        for candidate in candidate_itemsets:
            if all(frozenset(subset) in current_frequent_items for subset in itertools.combinations(candidate, k - 1)):
                valid_candidates.add(candidate)

        current_frequent_items = {
            itemset for itemset in valid_candidates
            if sum(all(item in transaction for item in itemset) for transaction in transactions) >= min_support
        }

    return frequent_itemsets


def main():
    # MongoDB connection setup
    # Connect to the MongoDB server running on localhost default port
    client = MongoClient('localhost', 27017)
    db = client['APRIORI_db']  # Database name
    collection = db['itemsets']  # Collection name

    # Kafka consumer setup
    consumer = KafkaConsumer(
        'bda3',
        bootstrap_servers=['localhost:9092'],
        auto_offset_reset='earliest',
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )

    window_size = 100
    transactions = []
    min_support = 7

    try:
        for message in consumer:
            transaction = message.value
            transactions.append(transaction)

            if len(transactions) > window_size:
                transactions.pop(0)

            if len(transactions) == window_size:
                # Perform Apriori on the current window of transactions
                frequent_itemsets = apriori(transactions, min_support)
                # Store itemsets in MongoDB
                for itemset in frequent_itemsets:
                    # Convert frozenset to list for MongoDB storage
                    document = {'itemset': list(
                        itemset), 'support': len(itemset)}
                    collection.insert_one(document)
                print(f"Stored {len(frequent_itemsets)} itemsets to MongoDB.")

    except KeyboardInterrupt:
        print("Stopped by user.")
    finally:
        consumer.close()
        client.close()


if __name__ == "__main__":
    main()
