# Import libraries
import ssl, re, sys, csv, urllib.parse, urllib.error 
import numpy as np, pandas as pd, sqlite3, scipy, sklearn
from urllib.request import urlopen
from collections import Counter
from bs4 import BeautifulSoup
from datetime import datetime
from sklearn.impute import SimpleImputer

def pages_url_complier(search_page, num_pages):
    """
    Returns a list of house URLs using a input of the inital Zoopla search page and number of pages to scrap.
            Parameters:
                    search_page(str): A string of the url
                    num_page(int): An integer representing the number of pages of scrap
            
            Returns:
                    flat_list(list): A list containing strings, which are the urls of houses
    """
    pages_url = [search_page + '&pn=' +str(x) for x in range(1,num_pages+1)]

    def houses_url_complier(page_url): 
        """
        Returns a list of URLs for houses present in the current search page
            Parameters:
                    page_url(str): A string of the page url

            Returns:
                    houses_url(list): A list containing strings, which are the urls of houses
        """
        #Open current the search page and parse
        html = urlopen(page_url)
        soup = BeautifulSoup(html, "html.parser")

        #Extract the extension links for each house in the search page 
        anchors =[str(x.get('href')) for x in soup.find_all('a')] #get all anchor attributes
        res = [x for x in anchors if re.search('for-sale/details', x)] #get attributes with "for-sale" 
        #Create a set to ensure distinct values
        house_anchors = list(set(res))
        #Turn the extenstions to URLs and create the list of houses
        houses_url = ['https://www.zoopla.co.uk' + x for x in house_anchors] 
        return houses_url
       
    all_pages_url = [houses_url_complier(page_url) for page_url in pages_url] #A list containing each page as lists
    flat_list = [item for sublist in all_pages_url for item in sublist] #Flattened list
    return flat_list

def data_collection(pages_url_complier):
    """
    Takes the output of function pages_url_complier and returns an array of property data 
        Parameters:
                page_url_complier(list): A list containing strings of houses URL

        Returns:
                data(array): A 2D numpy array containing the scrapped data from each house
    """

    def house_data_extraction(house_url): 
        """
        Returns an numpy array containing the scrapped data from a Zoopla house URL
            Parameters:
                    house_url(list): A string of the house URL 

            Returns:
                    data(array): A 1D numpy array containing the extracted data from each house
        """
        # Open the house link to extract parameters
        html = urlopen(house_url)
        soup = BeautifulSoup(html, 'html.parser')
        pagesource = str(soup).split()

        try:
            # Number of beds
            bed_find = pagesource.index('num_beds:')
            beds = pagesource[bed_find+1][0]
            # Number of baths
            bath_find = pagesource.index('num_baths:')
            baths = pagesource[bath_find+1][0]
            # House price
            price_find = pagesource.index('price_actual:')
            price_rough = pagesource[price_find+1]
            price = price_rough.split(',')[0]  
            # Property type
            type_find = pagesource.index('property_type:')
            prop_type_rough = pagesource[type_find+1]
            prop_type = prop_type_rough.split('"')[1]
            #Lattitude
            lat_find = pagesource.index('"latitude":')
            lat_rough = pagesource[lat_find+1]
            lat = float(lat_rough.split(',')[0])
            #Longtitude
            lon_find = pagesource.index('"longitude":')
            lon_rough = pagesource[lon_find+1]
            lon = float(lon_rough.split(',')[0])
            # First published date
            price_history = pagesource.index('class="dp-price-history__item">')
            year = pagesource[price_history+4].split('<')[0]
            #Distance to nearsest attraction/train station
            station_find = pagesource.index('miles')
            station = float(pagesource[station_find-1])
        except:
            beds, baths, price,prop_type,lat,lon,year,station = np.nan()
        
        #Is there a mention of a Loft?
        if 'loft' in str(soup): loft = 1
        else: loft = 0

        #Is there a mention of a Garden?
        if 'garden' in str(soup): garden = 1
        else: garden = 0
        
        data_extract = np.array([price,beds,baths,loft,garden,station,year, lat, lon, prop_type])
        return data_extract
    
    data = np.array([house_data_extraction(house_url) for house_url in pages_url_complier])
    return data
    
# This isn't processing the last column
def data_processing(data_collection):
    """
    Takes the output of function data_collection and returns an array of property data 
        Parameters:
                data(array): A 2D numpy containing the raw scrapped data

        Returns:
                data(array): A 2D numpy array containing cleaned data without missing values
    """
    dataset = pd.DataFrame(data_collection)

    
    
    # Covert string columns to empty cell
    dataset = np.where(dataset == '"' ,np.nan, dataset)

    # Take care of missing numerical  data: Replace numerical values with mean
    imputer = SimpleImputer(missing_values=np.nan, strategy = 'median')
    imputer.fit(dataset[:,0:9])
    dataset[:, 0:9] = imputer.transform(dataset[:, 0:9])

    # Take care of missing string data: Replace string values with most frequent
    imputer2 = SimpleImputer(missing_values=np.nan, strategy = 'most_frequent')
    imputer2.fit(dataset[:, 9:])
    dataset[:,9:] = imputer2.transform(dataset[:, 9:])
    
    return dataset

def read_to_SQL(data_processing):
    """
    Takes the output of function dataprocessing and creates a SQLite file of the data 
        Parameters:
                data_processing(array): A 2D Numpy array containing processed data
    """
    data_df = pd.DataFrame(data_processing, columns= ['Price','Beds','Baths','Loft','Garden','Station_distance','Listing_year','Latitude','Longtitude','Property_type'])

    conn = sqlite3.connect('Zoopla_test.sqlite') # Connect to SQL file

    cur = conn.cursor()
    
    # Create SQL Table schema
    cur.executescript('''
    DROP TABLE IF EXISTS Location;
    CREATE TABLE Location (
        id  INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE,
        Price    INTEGER,
        Beds  INTEGER,
        Baths INTEGER,
        Loft INTEGER,
        Garden INTEGER,
        Station_distance FLOAT,
        Listing_year INTEGER,
        Latitude INTEGER,
        Longtitude INTEGER,
        Property_type VARCHAR(128));
    ''')

    data_df.to_sql('Location', con=conn, if_exists='append', index=False)

    conn.close()

if __name__ == "__main__":
    # Enter the first page  of the seach 
    search_page = input("Enter the search page:")
    # Enter the number of pages to search
    num_pages = int(input('How many pages are there?:'))
    # Create the list of page links
    read_to_SQL(data_processing(data_collection(pages_url_complier(search_page,num_pages))))
    sys.exit
