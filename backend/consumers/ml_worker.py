import joblib
import pandas as pd
from consumers.consumer import BaseConsumer
from producers.producer import BaseProducer
from utils.config import TOPIC_RAW_EVENTS, TOPIC_ML_PREDICTIONS, GROUP_ID_ML, MODELS_PATH
from utils.logger import logger


class MLWorker(BaseConsumer):
    def __init__(self):
        super().__init__(topic=TOPIC_RAW_EVENTS, group_id=GROUP_ID_ML)
        self.producer = BaseProducer()
        self.pipeline = joblib.load(MODELS_PATH)
        self.sessions = {}

    def process_message(self, message: dict):
        session_id = message.get('user_session')
        event_type = message.get('event_type')
        product_id = message.get('product_id', 'unknown')
        brand = message.get('brand', 'unknown')

        try:
            price = float(message.get('price', 0.0))
        except (ValueError, TypeError):
            price = 0.0

        if not session_id:
            return

        if event_type == 'purchase':
            self.producer.send_message(
                topic=TOPIC_ML_PREDICTIONS,
                value={
                    'user_session': session_id,
                    'status': 'organic_purchase',
                    'revenue': price,
                    'cart_items': [{'product_id': product_id, 'brand': brand, 'price': price}]
                },
                key=session_id
            )
            if session_id in self.sessions:
                del self.sessions[session_id]
            return

        if session_id not in self.sessions:
            self.sessions[session_id] = {
                'view_count': 0, 'cart_count': 0, 'remove_count': 0,
                'total_view_value': 0.0, 'total_cart_value': 0.0,
                'cart_items': []
            }

        state = self.sessions[session_id]
        if event_type == 'view':
            state['view_count'] += 1
            state['total_view_value'] += price
        elif event_type == 'cart':
            state['cart_count'] += 1
            state['total_cart_value'] += price
            state['cart_items'].append({'product_id': product_id, 'brand': brand, 'price': price})
        elif event_type == 'remove_from_cart':
            state['remove_count'] += 1

        if (state['view_count'] + state['cart_count'] + state['remove_count']) < 3:
            return

        features_df = pd.DataFrame([{
            'view_count': state['view_count'],
            'cart_count': state['cart_count'],
            'remove_count': state['remove_count'],
            'total_view_value': state['total_view_value'],
            'total_cart_value': state['total_cart_value']
        }])

        try:
            prob = float(self.pipeline.predict_proba(features_df)[0][1])
        except Exception as e:
            logger.error(f"Prediction failed for session {session_id}: {e}")
            return

        self.producer.send_message(
            topic=TOPIC_ML_PREDICTIONS,
            value={
                'user_session': session_id,
                'status': 'prediction',
                'probability': prob,
                'cart_count': state['cart_count'],
                'total_cart_value': state['total_cart_value'],
                'cart_items': state['cart_items']
            },
            key=session_id
        )

if __name__ == "__main__":
    worker = MLWorker()
    worker.start_consuming()
