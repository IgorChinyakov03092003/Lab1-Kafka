import time
import pandas as pd
from producers.producer import BaseProducer
from utils.config import DATASET_PATH, TOPIC_RAW_EVENTS
from utils.logger import logger


class CheckoutProducer(BaseProducer):
    def __init__(self, file_path: str = DATASET_PATH, chunksize: int = 500, start_delay: int = 5):
        super().__init__()
        self.file_path = file_path
        self.chunksize = chunksize
        self.start_delay = start_delay

    def stream_data(self):
        logger.info(f"Checkout producer sleeping for {self.start_delay} seconds to let raw data flow...")
        time.sleep(self.start_delay)
        logger.info(f"Starting CHECKOUT stream from {self.file_path}...")

        try:
            for chunk in pd.read_csv(self.file_path, chunksize=self.chunksize):
                checkout_events = chunk[chunk['event_type'] == 'purchase']
                for _, row in checkout_events.iterrows():
                    event = row.dropna().to_dict()
                    session_id = event.get('user_session')

                    if pd.isna(session_id) or not session_id:
                        continue

                    self.send_message(
                        topic=TOPIC_RAW_EVENTS,
                        value=event,
                        key=str(session_id)
                    )

                logger.info(f"Pushed {len(checkout_events)} checkout events (purchase)...")

                time.sleep(1.5)

        except FileNotFoundError:
            logger.error(f"Dataset not found at {self.file_path}. Check volume mounts.")
        except Exception as e:
            logger.error(f"Error while streaming checkout data: {e}")
        finally:
            self.close()


if __name__ == "__main__":
    producer = CheckoutProducer()
    producer.stream_data()
