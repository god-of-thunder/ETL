from fastapi import FastAPI, HTTPException
from sqlalchemy.orm import Session
from database import SessionLocal, engine, Base
from models import Product, User
from schemas import ProductCreate, UserCreate, UserLogin
from auth import hash_password, verify_password, create_access_token

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

@app.post("/register")
def register(user: UserCreate):
    db = SessionLocal()

    existing = db.query(User).filter(User.email == user.email).first()
    if existing:
        db.close()
        raise HTTPException(status_code=400, detail="Email already exists")

    new_user = User(
        email=user.email,
        hashed_password=hash_password(user.password)
    )

    db.add(new_user)
    db.commit()
    db.refresh(new_user)
    db.close()

    return {"message": "User created"}


@app.post("/login")
def login(user: UserLogin):
    db = SessionLocal()

    db_user = db.query(User).filter(User.email == user.email).first()

    if not db_user:
        db.close()
        raise HTTPException(status_code=400, detail="Invalid credentials")

    if not verify_password(user.password, db_user.hashed_password):
        db.close()
        raise HTTPException(status_code=400, detail="Invalid credentials")

    token = create_access_token({"sub": db_user.email})

    db.close()

    return {"access_token": token, "token_type": "bearer"}