from fastapi import FastAPI, HTTPException
from sqlalchemy.orm import Session
from database import SessionLocal, engine
from models import Product
from schemas import ProductCreate
from database import Base

Base.metadata.create_all(bind=engine)

app = FastAPI()

@app.get("/")
def root():
    return {"message": "Ecommerce API Running"}

@app.post("/products")
def create_product(product: ProductCreate):

    db: Session = SessionLocal()

    db_product = Product(
        name=product.name,
        price=product.price
    )

    db.add(db_product)
    db.commit()
    db.refresh(db_product)

    return db_product

@app.get("/products")
def get_products():

    db: Session = SessionLocal()

    return db.query(Product).all()

@app.put("/products/{product_id}")
def update_product(product_id: int, product: ProductCreate):
    db = SessionLocal()

    db_product = db.query(Product).filter(Product.id == product_id).first()

    if not db_product:
        db.close()
        raise HTTPException(status_code=404, detail="Product not found")

    db_product.name = product.name
    db_product.price = product.price

    db.commit()
    db.refresh(db_product)
    db.close()

    return db_product

@app.delete("/products/{product_id}")
def delete_product(product_id: int):
    db = SessionLocal()

    db_product = db.query(Product).filter(Product.id == product_id).first()

    if not db_product:
        db.close()
        raise HTTPException(status_code=404, detail="Product not found")

    db.delete(db_product)
    db.commit()
    db.close()

    return {"message": "Product deleted successfully"}