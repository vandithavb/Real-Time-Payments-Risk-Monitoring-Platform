from dotenv import load_dotenv
import os

load_dotenv()

STRIPE_SECRET_KEY = os.getenv("STRIPE_SECRET_KEY", "")
STRIPE_WEBHOOK_SECRET = os.getenv("STRIPE_WEBHOOK_SECRET", "")
APP_ENV = os.getenv("APP_ENV", "dev")
PORT = int(os.getenv("PORT", "8000"))
STRIPE_RAW_TOPIC = os.getenv("STRIPE_RAW_TOPIC", "stripe.raw")
STRIPE_PROCESSED_TOPIC = os.getenv("PROCESSED_TOPIC", "stripe.processed")
STRIPE_DLQ_TOPIC = os.getenv("DLQ_TOPIC", "stripe.dlq")
REDPANDA_BOOTSTRAP_SERVERS = os.getenv("REDPANDA_BOOTSTRAP_SERVERS", "localhost:19092")