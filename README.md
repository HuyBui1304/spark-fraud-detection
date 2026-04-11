# Credit Card Fraud Detection with Apache Spark

Detect fraudulent credit card transactions using PySpark MLlib, Structured Streaming, and GraphFrames.

**Course:** Big Data — HK2A, HUTECH  
**Dataset:** [Sparkov Credit Card Fraud Detection](https://www.kaggle.com/datasets/kartik2112/fraud-detection) (~1.85M transactions)

## Results

| Model | Accuracy | Precision | Recall | F1 | AUC-PR |
|-------|----------|-----------|--------|-----|--------|
| GBT (best) | 99.84% | 82.64% | 72.59% | 0.773 | 0.832 |
| Random Forest | 99.65% | 100% | 8.62% | 0.159 | 0.681 |
| Logistic Regression | 99.61% | 0% | 0% | 0 | 0.202 |

Best model: **Gradient Boosted Trees** with optimal threshold = 0.35

## Project Structure

```
.
├── eda.ipynb                # EDA — Spark SQL queries + visualizations
├── processing.ipynb         # Data preprocessing + feature engineering
├── training.ipynb           # Model training — 9 cases comparison + tuning
├── graphx.ipynb             # Graph analysis with GraphFrames
├── streaming_socket.py      # Socket server — sends transactions via TCP
├── streaming_spark.py       # Spark Streaming — receives + predicts fraud
├── data/
│   ├── fraudTrain.csv       # Training set (1.3M rows)
│   └── fraudTest.csv        # Test set (556K rows)
├── models/
│   ├── best_model/          # Saved GBT model + pipeline + threshold
│   └── preprocessing/       # Parquet files (raw + processed)
```

## Pipeline

### 1. EDA (`eda.ipynb`)
- Spark SQL queries: fraud distribution by category, merchant, time, geography
- Outlier detection, class imbalance analysis
- Train vs test comparison

### 2. Preprocessing (`processing.ipynb`)
- Outlier capping (IQR on `amt`)
- Feature engineering: `hour`, `day_of_week`, `month`, `age`, `distance_km` (Haversine)
- Correlation analysis + VIF multicollinearity check
- Class weight calculation (fraud:non-fraud = 1:172)

### 3. Training (`training.ipynb`)
- 9 cases: 3 datasets (Raw, Processed, Processed+Weight) x 3 models (LR, RF, GBT)
- 5-fold CrossValidator with pipeline inside (no data leakage)
- Hyperparameter tuning on best model (GBT)
- Threshold tuning (0.1-0.9) → optimal = 0.35
- Feature importance + error analysis (FN/FP)

### 4. Streaming (`streaming_socket.py` + `streaming_spark.py`)
- Socket-based: server sends JSON transactions via TCP port 9999
- Spark Structured Streaming receives, preprocesses, predicts in real-time
- Fraud alerts logged to `data/streaming/fraud_alerts.csv`
- Run:
  ```bash
  # Terminal 1
  python streaming_socket.py

  # Terminal 2
  spark-submit streaming_spark.py
  ```

### 5. Graph Analysis (`graphx.ipynb`)
- GraphFrames: customer-merchant bipartite graph (1,692 vertices, 1.85M edges)
- Degree analysis, PageRank, Connected Components
- Fraud subgraph, Triangle Count, Motif Finding
- Interactive visualization with pyvis (hover + drag nodes)

## Tech Stack

- **PySpark 4.1.1** — Spark SQL, MLlib, Structured Streaming
- **GraphFrames 0.11.0** — Graph algorithms (PageRank, Connected Components, etc.)
- **Matplotlib / Seaborn** — Static visualizations
- **pyvis** — Interactive graph visualization
- **Python 3.11**

## How to Run

```bash
# Install dependencies
pip install pyspark graphframes-py pyvis matplotlib seaborn networkx

# Run notebooks in order
jupyter notebook eda.ipynb
jupyter notebook processing.ipynb
jupyter notebook training.ipynb
jupyter notebook graphx.ipynb

# Run streaming demo
python streaming_socket.py      # Terminal 1
spark-submit streaming_spark.py # Terminal 2
```

## Dataset Schema

23 columns: transaction info (`trans_date_trans_time`, `amt`, `merchant`, `category`), customer info (`cc_num`, `gender`, `dob`, `job`, `city`, `state`, `lat`, `long`), merchant location (`merch_lat`, `merch_long`), target (`is_fraud`).

Heavily imbalanced: ~0.58% fraud rate.
