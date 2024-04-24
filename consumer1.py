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
    frequent_items = {item for item, count in item_counts.items() if count >= min_support}
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
        current_frequent_items = {
            itemset for itemset in candidate_itemsets
            if sum(all(item in transaction for item in itemset) for transaction in transactions) >= min_support
        }

    return frequent_itemsets

def main():
    consumer = KafkaConsumer(
        'bda3',
        bootstrap_servers=['localhost:9092'],
        auto_offset_reset='earliest',
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )

    window_size = 700
    transactions = []
    min_support = 50

    try:
        for message in consumer:
            transaction = message.value
            transactions.append(transaction)

            if len(transactions) > window_size:
                transactions.pop(0)

            if len(transactions) == window_size:
                # Perform Apriori on the current window of transactions
                frequent_itemsets = apriori(transactions, min_support)
                print("Frequent Itemsets:", frequent_itemsets)
                
    except KeyboardInterrupt:
        print("Stopped by user.")

if __name__ == "__main__":
    main()

