import random
from consumers.consumer import BaseConsumer
from producers.producer import BaseProducer
from utils.config import TOPIC_ML_PREDICTIONS, TOPIC_PROMO_ACTIONS, GROUP_ID_PROMO


class PromoWorker(BaseConsumer):
    def __init__(self):
        super().__init__(topic=TOPIC_ML_PREDICTIONS, group_id=GROUP_ID_PROMO)
        self.producer = BaseProducer()
        self.discounted_sessions = set()

    def process_message(self, message: dict):
        session_id = message.get('user_session')
        status = message.get('status')

        if status != 'prediction' or session_id in self.discounted_sessions:
            return

        prob = message.get('probability', 0.0)
        cart_value = message.get('total_cart_value', 0.0)
        cart_items = message.get('cart_items', [])

        if 0.40 <= prob <= 0.70 and cart_value > 0:
            self.discounted_sessions.add(session_id)

            self.producer.send_message(
                topic=TOPIC_PROMO_ACTIONS,
                value={'action': 'discount_issued'}
            )

            uplift_chance = prob * 0.25

            if random.random() < uplift_chance:
                discounted_items = [
                    {'product_id': i['product_id'], 'brand': i['brand'], 'price': i['price'] * 0.9}
                    for i in cart_items
                ]

                self.producer.send_message(
                    topic=TOPIC_PROMO_ACTIONS,
                    value={
                        'action': 'simulated_purchase',
                        'revenue': cart_value * 0.9,
                        'cart_items': discounted_items
                    }
                )


if __name__ == "__main__":
    worker = PromoWorker()
    worker.start_consuming()
