from kafka import KafkaConsumer
import json
from collections import defaultdict, deque
import statistics
from pymongo import MongoClient

# Configuration for Kafka Consumer
topic_name = 'bda3'
bootstrap_servers = ['localhost:9092']
consumer = KafkaConsumer(
    topic_name,
    bootstrap_servers=bootstrap_servers,
    auto_offset_reset='latest',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

# Initialize data tracking structures
product_views = defaultdict(int)
product_purchases = defaultdict(int)
window_size = 10  # Adjustable window size for moving average calculations
trend_data = defaultdict(lambda: defaultdict(lambda: deque(maxlen=window_size)))

def moving_average(data):
    return statistics.mean(data)

# MongoDB connection setup
client = MongoClient('localhost', 27017)
db = client['product_analysis_db']
views_collection = db['product_views']  # Collection for product views
purchases_collection = db['product_purchases']  # Collection for product purchases

# Consume messages and update product views and purchases
for message in consumer:
    data = message.value
    if isinstance(data, list):
        for item in data:
            if item.startswith('Also View:'):
                product_id = item.split(': ')[1]
                product_views[product_id] += 1
                trend_data[product_id]['views'].append(product_views[product_id])
            elif item.startswith('Also Buy:'):
                product_id = item.split(': ')[1]
                product_purchases[product_id] += 1
                trend_data[product_id]['purchases'].append(product_purchases[product_id])

        # Calculate moving averages and detect trends
        for product_id, trends in trend_data.items():
            if len(trends['views']) == window_size and len(trends['purchases']) == window_size:
                avg_views = moving_average(trends['views'])
                avg_purchases = moving_average(trends['purchases'])
                print(f"Product {product_id} has an average of {avg_views:.2f} views and {avg_purchases:.2f} purchases.")

                # Save data to MongoDB
                views_data = {'product_id': product_id, 'average_views': avg_views}
                purchases_data = {'product_id': product_id, 'average_purchases': avg_purchases}
                views_collection.insert_one(views_data)
                purchases_collection.insert_one(purchases_data)

                # Alert system based on heuristic ratios
                if avg_views / max(avg_purchases, 1) > 10:
                    print(f"Warning: High view-to-purchase ratio for {product_id}. Consider checking stock levels.")
                if avg_purchases / max(avg_views, 1) > 0.5:
                    print(f"Alert: High purchase-to-view ratio for {product_id}. Potential stockout risk.")

print("Current Product Views and Purchases have been saved to MongoDB.")

