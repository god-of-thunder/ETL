from sqlalchemy.orm import Session
from datetime import datetime
import uuid

from app.models.payment import Payment
from app.models.order import Order



def create_payment(
    db: Session,
    order_id: int,
    provider: str,
    amount: float
):

    transaction_id = str(uuid.uuid4())

    payment = Payment(
        order_id=order_id,
        provider=provider,
        amount=amount,
        status="paid",
        transaction_id=transaction_id,
        paid_at=datetime.utcnow()
    )

    db.add(payment)

    order = db.query(Order).filter(
        Order.id == order_id
    ).first()

    if order:
        order.payment_status = "paid"
        order.paid_at = datetime.utcnow()

    db.commit()
    db.refresh(payment)

    return payment