
import os
import sys
import httpx
import asyncio
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

# Ensure the backend directory is in the path
sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))

from database import DATABASE_URL
from models import Offer
from scrapers.utils import clean_text

async def backfill_descriptions():
    print(f"Connecting to database: {DATABASE_URL}")
    engine = create_engine(DATABASE_URL)
    Session = sessionmaker(bind=engine)
    session = Session()

    try:
        # Find LBA offers with missing/generic descriptions
        offers = session.query(Offer).filter(
            Offer.source == 'labonnealternance'
        ).filter(
            (Offer.description == None) | 
            (Offer.description == '') | 
            (Offer.description.like('%non disponible%'))
        ).all()
        
        total = len(offers)
        print(f"Found {total} LBA offers to backfill.")

        async with httpx.AsyncClient(timeout=10.0) as client:
            for i, offer in enumerate(offers):
                # source_id format: lba_{idea_type}_{offer_id}
                sid_parts = offer.source_id.split('_')
                if len(sid_parts) < 3:
                     continue
                
                idea_type = sid_parts[1]
                offer_id = sid_parts[2]
                
                print(f"[{i+1}/{total}] Fetching {idea_type} ID: {offer_id}...")
                
                new_desc = None
                
                # Fetch from LBA API v1
                # Different endpoint based on type
                if idea_type == "matcha":
                    url = f"https://labonnealternance.apprentissage.beta.gouv.fr/api/v1/jobs/matcha/{offer_id}"
                elif idea_type == "peJob":
                    url = f"https://labonnealternance.apprentissage.beta.gouv.fr/api/v1/jobs/job/{offer_id}"
                else:
                    url = f"https://labonnealternance.apprentissage.beta.gouv.fr/api/v1/jobs/job/{offer_id}"
                
                try:
                    resp = await client.get(url)
                    if resp.status_code == 200:
                        data = resp.json()
                        # Extract description from sub-result
                        sub_key = "matchas" if idea_type == "matcha" else ("peJobs" if idea_type == "peJob" else "partnerJobs")
                        results = data.get(sub_key, [])
                        if results:
                            raw_job = results[0]
                            job_data = raw_job.get("job", {})
                            new_desc = job_data.get("description", "").strip()
                            
                            if not new_desc:
                                # Fallback to ROME definition
                                rome_details = raw_job.get("romeDetails", {})
                                if isinstance(rome_details, dict):
                                    new_desc = rome_details.get("definition", "").strip()
                                    print("  Found ROME fallback description")
                                
                            if not new_desc:
                                # Second fallback
                                appellation = job_data.get("rome_appellation_label") or job_data.get("rome_label")
                                if appellation:
                                    new_desc = f"Poste en tant que {appellation}. (Description détaillée non disponible)"
                                    print("  Found ROME label fallback")
                except Exception as e:
                    print(f"  Error fetching {offer_id}: {e}")

                if new_desc:
                    offer.description = clean_text(new_desc, preserve_newlines=True)
                    print(f"  Updated: {offer.title[:30]}...")
                
                # Commit every 10
                if (i + 1) % 10 == 0:
                    session.commit()

        session.commit()
        print(f"Finished backfilling LBA descriptions.")
    finally:
        session.close()

if __name__ == "__main__":
    asyncio.run(backfill_descriptions())
