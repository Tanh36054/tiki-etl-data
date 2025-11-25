# src/ingest/postgres_loader.py
from sqlalchemy import create_engine, text
import faker, random, os

DB_URL = os.environ.get('DB_URL', 'postgresql://dev:devpass@localhost:5433/ecommerce')
engine = create_engine(DB_URL)
fake = faker.Faker()

def insert_users(n=100):
    with engine.begin() as conn:
        for _ in range(n):
            conn.execute(text(
                "insert into users (name, email) values (:name, :email)"
            ), {"name": fake.name(), "email": fake.email()})
            
def insert_products(n=100):
    with engine.begin() as conn:
        for _ in range(n):
            conn.execute(text(
                "insert into products (vendor, vendor_product_id, title, category, price, currency) values (:v,:vp,:t,:c,:p,:cur) "
            ), {"v":"faker", "vp":fake.uuid4(), "t":fake.sentence(nb_words=5), "c":"electronics", "p":random.randint(10000, 1000000), "cur":"VND"})
            
def insert_carts(n=300):
    with engine.begin() as conn:
        for _ in range(n):
            conn.execute(text(
                "insert into carts (user_id, product_id, qty) values (:u, :p, :q)"
            ), {"u":random.randint(1,100), "p":random.randint(1,200), "q":random.randint(1,5)})
            
if __name__ == "__main__":
    insert_users(100)
    insert_products(200)
    insert_carts(300)
    print("Inserted fake data")