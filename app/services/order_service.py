from sqlalchemy.orm import Session
from datetime import datetime

from app.models.order import Order
from app.models.order_item import OrderItem
from app.models.product import Product
from app.models.cart import CartItem



def create_order(db: Session, user_id: int, payload):

    total_amount = 0

    order = Order(
        user_id=user_id,
        total_amount=0,
        shipping_address=payload.shipping_address,
        status="pending",
        payment_status="unpaid"
    )

    db.add(order)
    db.flush()

    for item in payload.items:

        product = db.query(Product).filter(
            Product.id == item.product_id
        ).first()

        if not product:
            continue

        subtotal = product.price * item.quantity

        total_amount += subtotal

        order_item = OrderItem(
            order_id=order.id,
            product_id=product.id,
            quantity=item.quantity,
            price=product.price,
            subtotal=subtotal
        )

        db.add(order_item)

        product.stock -= item.quantity

    order.total_amount = total_amount

    db.query(CartItem).filter(
        CartItem.user_id == user_id
    ).delete()

    db.commit()
    db.refresh(order)

    return order



def get_my_orders(db: Session, user_id: int):
    return db.query(Order).filter(
        Order.user_id == user_id
    ).all()



def get_order_detail(
    db: Session,
    order_id: int,
    user_id: int
):
    return db.query(Order).filter(
        Order.id == order_id,
        Order.user_id == user_id
    ).first()