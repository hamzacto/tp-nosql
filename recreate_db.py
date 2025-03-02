import asyncio
import sys
import os

# Add the current directory to the Python path
sys.path.append(os.path.abspath("."))

# Import the recreate_tables function
from app.db.recreate_tables import recreate_tables

if __name__ == "__main__":
    print("Recreating database tables...")
    asyncio.run(recreate_tables())
    print("Database tables recreated successfully.") 