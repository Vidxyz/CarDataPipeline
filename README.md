# Car Data Pipeline

### Getting started
1. Download the latest [csv data set from here](https://www.fueleconomy.gov/feg/download.shtml)
2. `pip install` all required dependencies
3. Place `vehicles.csv` into the `data/` folder
4. Run `ingest_cardata.py`
    - `python ingest_cardata.py`
    - This script extracts data from the CSV and populates a postgres database
    - For more info on the database schema, [refer to the repo migrations listed here](https://github.com/Vidxyz/CarDataApp/tree/master/priv/repo/migrations)
    - Once complete, you should be able to view data for various cars
5. Run `scrape_images.py`
    - `python scrape_images.py`
    - This script extracts at most 5 images from google images' CDN
    - It is essential to have run `ingest_cardata.py` prior to running this, as it fetches data for vehicles that were loaded previously
    - This script can take quite some time (few hours), so run it wisely
    
---
