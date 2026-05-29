from sqlalchemy.orm import Session

from app.models.product import Product



def create_product(db: Session, payload):
    product = Product(
        name=payload.name,
        sku=payload.sku,
        description=payload.description,
        category=payload.category,
        price=payload.price,
        stock=payload.stock
    )

    db.add(product)
    db.commit()
    db.refresh(product)

    return product



def get_products(db: Session):
    return db.query(Product).filter(
        Product.is_deleted == False
    ).all()



def update_product(db: Session, product_id: int, payload):
    product = db.query(Product).filter(
        Product.id == product_id
    ).first()

    if not product:
        return None

    for key, value in payload.dict(exclude_unset=True).items():
        setattr(product, key, value)

    db.commit()
    db.refresh(product)

    return product



def delete_product(db: Session, product_id: int):
    product = db.query(Product).filter(
        Product.id == product_id
    ).first()

    if not product:
        return None

    product.is_deleted = True

    db.commit()

    return {
        "message": "Product soft deleted"
    }