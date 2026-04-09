import socket
import time
import csv
import json
import random

HOST = "localhost"
PORT = 9999
CSV_FILE = "data/fraudTest.csv"
BATCH_SIZE = 500
DELAY = 4

print("Dang doc du lieu...")
fraud_rows = []
normal_rows = []
with open(CSV_FILE, "r") as f:
    reader = csv.DictReader(f)
    for row in reader:
        if row["is_fraud"] == "1":
            fraud_rows.append(row)
        else:
            normal_rows.append(row)

print(f"Fraud: {len(fraud_rows):,} | Non-fraud: {len(normal_rows):,}")

random.seed(42)
random.shuffle(fraud_rows)
random.shuffle(normal_rows)

# Tao cac batch: moi batch 500 dong, chen 1-5 fraud ngau nhien
all_batches = []
fi = 0
ni = 0
while ni < len(normal_rows) and fi < len(fraud_rows):
    n_fraud = random.randint(1, 5)
    n_fraud = min(n_fraud, len(fraud_rows) - fi)
    n_normal = min(BATCH_SIZE - n_fraud, len(normal_rows) - ni)

    batch = []
    for _ in range(n_fraud):
        batch.append(fraud_rows[fi])
        fi += 1
    for _ in range(n_normal):
        batch.append(normal_rows[ni])
        ni += 1

    random.shuffle(batch)
    all_batches.append(batch)

# Dong normal con thua
while ni < len(normal_rows):
    batch = normal_rows[ni:ni + BATCH_SIZE]
    ni += len(batch)
    all_batches.append(batch)

total = sum(len(b) for b in all_batches)
print(f"Tong: {total:,} giao dich, {len(all_batches)} batch")

# Socket server
server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
server_socket.bind((HOST, PORT))
server_socket.listen(1)
print(f"\nServer dang chay tren {HOST}:{PORT}...")
print(f"Cho ket noi tu Spark...")

client_socket, addr = server_socket.accept()
print(f"Ket noi tu {addr}\n")

try:
    for i, batch in enumerate(all_batches):
        for row in batch:
            line = json.dumps(row, ensure_ascii=False)
            client_socket.sendall((line + "\n").encode("utf-8"))

        n_fraud = sum(1 for r in batch if r["is_fraud"] == "1")
        print(f"Batch {i+1}: da gui {len(batch)} giao dich (fraud: {n_fraud})")
        time.sleep(DELAY)

    print(f"\nXong! Tong: {len(all_batches)} batch, {total:,} giao dich")
except KeyboardInterrupt:
    print("\nDung server...")
finally:
    client_socket.close()
    server_socket.close()
