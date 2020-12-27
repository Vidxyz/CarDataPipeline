import itertools
import re
import os
import uuid
import psycopg2
from urllib.request import urlopen, Request
from datetime import datetime

from bs4 import BeautifulSoup

db_host = 'localhost'
db_port = '6969'
db_user = 'postgres'
db_password = 'postgres'
db_name = 'postgres'

REQUEST_HEADER = {
    'User-Agent': "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/43.0.2357.134 Safari/537.36"
}


def get_soup(url, header):
    response = urlopen(Request(url, headers=header))
    return BeautifulSoup(response, 'html.parser')

def get_query_url(query):
    return "https://www.google.com/search?q=%s&source=lnms&tbm=isch" % query

def extract_images_from_soup(soup):
    image_elements = soup.find_all("img", {"class": "rg_i", "data-src": re.compile('\\.+')}, limit=5)
    widths = [e['width'] for e in image_elements]
    lengths = [e['height'] for e in image_elements]
    imageURIs = [e['data-src'] for e in image_elements]
    link_type_records = ((i, w, l) for i, w, l in zip(imageURIs, widths, lengths))
    return link_type_records

def extract_images(query, num_images):
    url = get_query_url(query)
    soup = get_soup(url, REQUEST_HEADER)
    link_type_records = extract_images_from_soup(soup)
    return itertools.islice(link_type_records, num_images)

def get_raw_image(url):
    req = Request(url, headers=REQUEST_HEADER)
    resp = urlopen(req)
    return resp.read()

def main():
    try:
        connection = psycopg2.connect(user=db_user, password=db_password, host=db_host, port=db_port, database=db_name)
        print("Connected to database successfully")
        cursor = connection.cursor()
        query = 'select id, make, model, year from vehicle where id not in (select distinct vehicle_id from image) order by make, model, year '
        cursor.execute(query)
        results = cursor.fetchall()
        total_rows = len(results)
        for index, row in enumerate(results):
            vehicle_id = row[0]
            make = row[1]
            model = row[2]
            year = row[3]

            search_query = make + " " + model + " " + str(year)
            print("Search query is : " + str(search_query))
            images = extract_images('+'.join(search_query.split()), 5) # returns 5 top google search images

            for url, width, height in images:
                try:
                    raw_image = get_raw_image(url)
                    image_id = uuid.uuid4()
                    now = datetime.now()
                    cursor.execute("INSERT INTO image VALUES (%s, %s, %s, %s, %s, %s, %s)", (str(image_id), str(vehicle_id), raw_image, width, height, now, now))
                except (Exception) as error:
                    print("An error occurred while trying to get raw image from " + url)
                    print(error)


            connection.commit()
            print(f"{round(float(index) * 100 / total_rows, 3)}% complete")

        cursor.close()

    except (Exception, psycopg2.Error) as error:
        print("Error while connecting to PostgreSQL", error)


if __name__ == '__main__':
    main()