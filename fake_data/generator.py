from faker import Faker
import random

fake = Faker()


def generate_user():
    return {
        "name": fake.name(),
        "email": fake.email(),
        "city": fake.city(),
    }


def generate_order(user_id):
    return {
        "user_id": user_id,
        "total_amount": round(random.uniform(10, 1000), 2),
        "status": random.choice([
            "pending",
            "paid",
            "cancelled"
        ])
    }