
import os
import sys
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

# Ensure the backend directory is in the path
sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))

from database import DATABASE_URL
from models import Offer
from scrapers.utils import clean_text

def clean_all_entities():
    print(f"Connecting to database: {DATABASE_URL}")
    engine = create_engine(DATABASE_URL)
    Session = sessionmaker(bind=engine)
    session = Session()

    try:
        offers = session.query(Offer).all()
        total = len(offers)
        print(f"Found {total} offers. Starting entity cleanup...")

        changed_count = 0
        for i, offer in enumerate(offers):
            if not offer.description:
                continue
                
            old_desc = offer.description
            # We use preserve_newlines=True to keep the layout but fix entities and tags
            new_desc = clean_text(old_desc, preserve_newlines=True)
            
            if old_desc != new_desc:
                offer.description = new_desc
                changed_count += 1
            
            if (i + 1) % 500 == 0:
                print(f"Processed {i + 1}/{total} offers...")
                session.commit()

        session.commit()
        print(f"Finished! Cleaned {changed_count} offers out of {total}.")
    except Exception as e:
        session.rollback()
        print(f"Error during migration: {e}")
    finally:
        session.close()

if __name__ == "__main__":
    clean_all_entities()
