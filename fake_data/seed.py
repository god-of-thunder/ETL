from sqlalchemy import create_engine
from faker import Faker
import pandas as pd
import random

fake = Faker()

engine = create_engine(
    "postgresql://postgres:postgres@localhost:5432/ecommerce"
)

users = []

for i in range(1000):
    users.append({
        "name": fake.name(),
        "email": fake.email(),
        "country": fake.country(),
    })

users_df = pd.DataFrame(users)

users_df.to_sql(
    "bronze_users",
    engine,
    if_exists="replace",
    index=False
)

orders = []

for i in range(5000):
    orders.append({
        "user_id": random.randint(1, 1000),
        "total_amount": round(random.uniform(50, 5000), 2),
        "status": random.choice([
            "paid",
            "pending",
            "failed"
        ])
    })

orders_df = pd.DataFrame(orders)

orders_df.to_sql(
    "bronze_orders",
    engine,
    if_exists="replace",
    index=False
)

print("Seed completed")