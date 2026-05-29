from sqlalchemy.orm import Session

from app.models.cart import CartItem
from app.models.product import Product



def add_to_cart(db: Session, user_id: int, payload):
    product = db.query(Product).filter(
        Product.id == payload.product_id
    ).first()

    if not product:
        return None

    cart_item = db.query(CartItem).filter(
        CartItem.user_id == user_id,
        CartItem.product_id == payload.product_id
    ).first()

    if cart_item:
        cart_item.quantity += payload.quantity
    else:
        cart_item = CartItem(
            user_id=user_id,
            product_id=payload.product_id,
            quantity=payload.quantity
        )

        db.add(cart_item)

    db.commit()
    db.refresh(cart_item)

    return cart_item


def get_cart_items(db: Session, user_id: int):
    return db.query(CartItem).filter(
        CartItem.user_id == user_id
    ).all()


def update_cart_item(
    db: Session,
    cart_item_id: int,
    user_id: int,
    payload
):
    cart_item = db.query(CartItem).filter(
        CartItem.id == cart_item_id,
        CartItem.user_id == user_id
    ).first()

    if not cart_item:
        return None

    cart_item.quantity = payload.quantity

    db.commit()
    db.refresh(cart_item)

    return cart_item


def remove_cart_item(
    db: Session,
    cart_item_id: int,
    user_id: int
):
    cart_item = db.query(CartItem).filter(
        CartItem.id == cart_item_id,
        CartItem.user_id == user_id
    ).first()

    if not cart_item:
        return None

    db.delete(cart_item)

    db.commit()

    return {
        "message": "Cart item removed"
    }