from fastapi import APIRouter

from app.api.endpoints import users, products, purchases, social, benchmark

api_router = APIRouter()

api_router.include_router(users.router, prefix="/users", tags=["users"])
api_router.include_router(products.router, prefix="/products", tags=["products"])
api_router.include_router(purchases.router, prefix="/purchases", tags=["purchases"])
api_router.include_router(social.router, prefix="/social", tags=["social"])
api_router.include_router(benchmark.router, prefix="/benchmark", tags=["benchmark"]) 