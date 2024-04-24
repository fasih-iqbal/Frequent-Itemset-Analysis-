import json

class StreamHandler:
    def __init__(self, window_size):
        self.window_size = window_size
        self.transactions = []

    def process_transaction(self, product):
        transaction = set()
        if 'brand' in product:
            transaction.add(f"Brand: {product['brand']}")

        if 'category' in product:
            for cat in product['category']:
                if any(kw in cat for kw in ["Material:", "Size:", "Style:"]):
                    cat = cat.replace('.', ': ').title()  # Normalize the formatting
                    transaction.add(cat)
                elif ':' not in cat:
                    transaction.add(f"Category: {cat}")
                else:
                    key, value = cat.split(':', 1)
                    transaction.add(f"{key.strip().title()}: {value.strip()}")

        if 'features' in product:
            transaction.update(f"Feature: {feature}" for feature in product['features'] if feature)

        # Additional fields
        for key in ['also_buy', 'also_view']:
            if key in product:
                transaction.update(f"{key.replace('_', ' ').title()}: {item}" for item in product[key] if item)

        if len(self.transactions) >= self.window_size:
            self.transactions.pop(0)
        self.transactions.append(transaction)
        return transaction

def process_streaming_file(file_path, output_path, window_size):
    stream_handler = StreamHandler(window_size=window_size)
    buffer_size = 100  # Number of transactions to write in one go
    buffer = []
    
    with open(file_path, 'r') as file, open(output_path, 'w') as output:
        for line in file:
            try:
                product = json.loads(line.strip())
                transaction = stream_handler.process_transaction(product)
                buffer.append(json.dumps(list(transaction)) + '\n')
                if len(buffer) >= buffer_size:
                    output.writelines(buffer)
                    buffer = []
            except json.JSONDecodeError:
                print("Failed to decode JSON")
            except Exception as e:
                print(f"An error occurred: {e}")

        # Write any remaining transactions in the buffer
        if buffer:
            output.writelines(buffer)

# Usage example:
file_path = '50mb.json'
output_path = 'pp.json'
process_streaming_file(file_path, output_path, window_size=5)

