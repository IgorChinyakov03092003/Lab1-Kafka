import time
import pandas as pd
from producers.producer import BaseProducer
from utils.config import DATASET_PATH, TOPIC_RAW_EVENTS
from utils.logger import logger


class RawDataProducer(BaseProducer):
    def __init__(self, file_path: str = DATASET_PATH, chunksize: int = 500):
        super().__init__()
        self.file_path = file_path
        self.chunksize = chunksize

    def stream_data(self):
        logger.info(f"Starting RAW DATA stream from {self.file_path}...")
        try:
            for chunk in pd.read_csv(self.file_path, chunksize=self.chunksize):

                events = chunk[chunk['event_type'].isin(['view', 'cart', 'remove_from_cart'])]

                for _, row in events.iterrows():
                    event = row.dropna().to_dict()
                    session_id = event.get('user_session')

                    if pd.isna(session_id) or not session_id:
                        continue

                    self.send_message(
                        topic=TOPIC_RAW_EVENTS,
                        value=event,
                        key=str(session_id)
                    )

                logger.info(f"Pushed {len(events)} raw events (view/cart/remove) to Kafka...")
                time.sleep(1)

        except FileNotFoundError:
            logger.error(f"Dataset not found at {self.file_path}. Check volume mounts.")
        except Exception as e:
            logger.error(f"Error while streaming: {e}")
        finally:
            self.close()


if __name__ == "__main__":
    producer = RawDataProducer()
    producer.stream_data()
