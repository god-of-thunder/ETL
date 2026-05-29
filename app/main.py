from fastapi import FastAPI

from app.routers import (
    auth,
    users,
    products,
    carts,
    orders
)

app = FastAPI(
    title="Production E-commerce API",
    version="1.0.0"
)

app.include_router(auth.router)
app.include_router(users.router)
app.include_router(products.router)
app.include_router(carts.router)
app.include_router(orders.router)


@app.get("/")
def root():
    return {
        "message": "E-commerce API Running"
    }