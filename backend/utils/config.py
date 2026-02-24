import os

BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BROKERS",
    "kafka-1:9092,kafka-2:9092,kafka-3:9092,kafka-4:9092"
).split(",")

TOPIC_RAW_EVENTS = "raw_events"
TOPIC_ML_PREDICTIONS = "ml_predictions"
TOPIC_PROMO_ACTIONS = "promo_actions"

GROUP_ID_ML = "ml_workers_group"
GROUP_ID_PROMO = "promo_engine_group"
GROUP_ID_UI = "streamlit_ui_group"

DATASET_PATH = "/backend/data/2020-Jan.csv"
MODELS_PATH = "/backend/models/pipeline.pkl"
