from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

# Import all models here to ensure they are registered with SQLAlchemy
from app.models.user import User
from app.models.product import Product
from app.models.purchase import Purchase
from app.models.follow import Follow 