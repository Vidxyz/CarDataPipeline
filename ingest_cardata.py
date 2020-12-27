import pandas as pd
import psycopg2
import uuid
import re

def titlecase(s):
    return re.sub(r"[A-Za-z]+('[A-Za-z]+)?", lambda mo: mo.group(0).capitalize(), s)

def get_transmission_info(row):
    trans_id = uuid.uuid4()
    if pd.isnull(row['trans_dscr']):
        trans_dscr = None
    else:
        trans_dscr = str(row['trans_dscr'])
    trans_type = str(row['trany'])

    return trans_id, trans_dscr, trans_type

def get_dimensions_info(row):
    dimensions_id = uuid.uuid4()
    hlv = row['hlv']
    hpv = row['hpv']
    lv2 = row['lv2']
    lv4 = row['lv4']
    pv2 = row['pv2']
    pv4 = row['pv4']
    return dimensions_id, hlv, hpv, lv2, lv4, pv2, pv4

def get_fuel_emissions_info(row):
    fuel_emissions_id = uuid.uuid4()
    co2_1 = row['co2TailpipeGpm']
    co2_2 = row['co2TailpipeAGpm']
    gg_1 = row['ghgScore']
    gg_2 = row['ghgScoreA']
    if gg_1 == -1:
        gg_1 = None
    if gg_2 == -1:
        gg_2 = None
    return fuel_emissions_id, co2_1, co2_2, gg_1, gg_2

def get_vehicle_info(row):
    vehicle_id = uuid.uuid4()
    make = row['make']
    model = row['model']
    year = row['year']
    alt_fuel_type = row['atvType']
    ftype_1 = row['fuelType1']
    ftype_2 = row['fuelType2']
    ftype = row['fuelType']
    mfr_code = row['mfrCode']
    record_id = row['id']
    v_class = row['VClass']

    if pd.isna(alt_fuel_type) :
        alt_fuel_type = None
    if pd.isna(ftype_2):
        ftype_2 = None
    if pd.isna(mfr_code):
        mfr_code = None

    return vehicle_id, titlecase(make), titlecase(model), year, alt_fuel_type, ftype_1, ftype_2, ftype, mfr_code, record_id, v_class

def get_engine_info(row):
    engine_id = uuid.uuid4()
    cylinders = row['cylinders']
    displacement = row['displ']
    eng_id = row['engId']
    engine_descriptor = row['eng_dscr']
    ev_motor = row['evMotor']
    is_supercharged = row['sCharger']
    is_turbocharged = row['tCharger']
    drive_type = row['drive']

    if is_supercharged == 'S':
        is_supercharged = True
    else:
        is_supercharged = False

    if is_turbocharged == 'T':
        is_turbocharged = True
    else:
        is_turbocharged = False

    if pd.isna(drive_type):
        drive_type = None
    if pd.isna(cylinders):
        cylinders = None
    if pd.isna(displacement):
        displacement = None
    if pd.isna(engine_descriptor):
        engine_descriptor = None
    if pd.isna(ev_motor):
        ev_motor = None

    return engine_id, cylinders, displacement, eng_id, engine_descriptor, ev_motor, is_supercharged, is_turbocharged, drive_type


def get_fuel_economy_info(row):
    fuel_economy_id = uuid.uuid4()
    barrels08 = row['barrels08']
    barrelsA08 = row['barrelsA08']
    city08 = row['city08']
    cityA08 = row['cityA08']
    highway08 = row['highway08']
    highwayA08 = row['highwayA08']
    comb08 = row['comb08']
    combA08 = row['combA08']
    fuelCost08 = row['fuelCost08']
    fuelCostA08 = row['fuelCostA08']
    combE = row['combE']
    feScore = row['feScore']
    rangeA = row['rangeA']
    rangeCityA = row['rangeCityA']
    rangeHwyA = row['rangeHwyA']
    guzzler = row['guzzler']
    charge120 = row['charge120']
    charge240 = row['charge240']

    if feScore == -1:
        feScore = None
    if pd.isna(rangeA):
        rangeA = None
    if guzzler == 'G' or guzzler == 'T':
        guzzler = True
    else:
        guzzler = False

    return fuel_economy_id, barrels08, barrelsA08, city08, cityA08, highway08, highwayA08, comb08, combA08, \
           fuelCost08, fuelCostA08, combE, feScore, rangeA, rangeCityA, rangeHwyA, guzzler, charge120, charge240


# print(df['charge120'].value_counts())

db_host = 'localhost'
db_port = '6969'
db_user = 'postgres'
db_password = 'postgres'
db_name = 'postgres'
dataset_path = 'data/vehicles.csv'

def ingest_data():
    # dataset and definitions from https://www.fueleconomy.gov/feg/ws/index.shtml#fuelType1
    df = pd.read_csv(dataset_path)
    try:
        connection = psycopg2.connect(user=db_user, password=db_password, host=db_host, port=db_port, database=db_name)
        print("Connected to database successfully")
        cursor = connection.cursor()
        for index, row in df.iterrows():

            vehicle_id, make, model, year, alt_fuel_type, ftype_1, ftype_2, ftype, mfr_code, record_id, v_class = get_vehicle_info(row)
            cursor.execute("INSERT INTO vehicle VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
                           (str(vehicle_id), make, model, year, ftype_1, ftype_2, ftype,
                            mfr_code, record_id,
                            alt_fuel_type, v_class))

            trans_id, trans_dscr, trans_type = get_transmission_info(row)
            cursor.execute("INSERT INTO transmission VALUES (%s, %s, %s, %s)", (str(trans_id), trans_dscr, trans_type, str(vehicle_id)))

            dimensions_id, hlv, hpv, lv2, lv4, pv2, pv4 = get_dimensions_info(row)
            cursor.execute("INSERT INTO dimensions VALUES (%s, %s, %s, %s, %s, %s, %s, %s)", (str(dimensions_id), hlv, hpv, lv2, lv4, pv2, pv4, str(vehicle_id)))

            engine_id, cylinders, displacement, eng_id, engine_descriptor, ev_motor, is_supercharged, is_turbocharged, drive_type = get_engine_info(row)
            cursor.execute("INSERT INTO engine VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)", (str(engine_id), cylinders, displacement, eng_id,
                                                                                                      engine_descriptor, ev_motor, is_supercharged, is_turbocharged,
                                                                                                      drive_type, str(vehicle_id)))

            fuel_emissions_id, co2_1, co2_2, gg_1, gg_2 = get_fuel_emissions_info(row)
            cursor.execute("INSERT INTO fuel_emission VALUES (%s, %s, %s, %s, %s, %s)", (str(fuel_emissions_id), co2_1, co2_2, gg_1, gg_2, str(engine_id)))

            fuel_economy_id, barrels08, barrelsA08, city08, cityA08, highway08, highwayA08, comb08, combA08, \
            fuelCost08, fuelCostA08, combE, feScore, rangeA, rangeCityA, rangeHwyA, guzzler, charge120, charge240 = get_fuel_economy_info(row)
            cursor.execute("INSERT INTO fuel_economy VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)", (
                str(fuel_economy_id), barrels08, barrelsA08, city08, cityA08, highway08, highwayA08, comb08, combA08,
                fuelCost08, fuelCostA08, combE, feScore, rangeA, rangeCityA, rangeHwyA, guzzler, charge120, charge240, str(engine_id)))


            connection.commit()
            print(f"{round(float(index) * 100/df.shape[0], 2)}% complete")

        cursor.close()

    except (Exception, psycopg2.Error) as error:
        print("Error while connecting to PostgreSQL", error)


ingest_data()
