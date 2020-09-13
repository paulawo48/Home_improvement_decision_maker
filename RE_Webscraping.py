# Import libraries
import ssl, re, sys, csv, urllib.parse, urllib.error 
import numpy as np, pandas as pd, sqlite3, scipy, sklearn
from urllib.request import urlopen
from collections import Counter
from bs4 import BeautifulSoup
from datetime import datetime
from sklearn.impute import SimpleImputer
# import sklearn.impute import SimpleImputer

fname = input('Enter file name:')

#Create a list of all the Zoopla pages URL from input
def Read_all_pages():
    # Enter the page 1 of the seach URL
    search_page = input("Enter the search page:")

    # Enter the number of pages to search
    num_pages = int(input('How many pages are there?:'))

    # Create the list of page links
    pages =[]
    for i in range(1, num_pages+1):
        pages.append(search_page + '&pn=' +str(i))
    return(pages)

def Webpages_compositions():  
    # Input the search page from zoopla and read it using beautiful soup
    page_links = Read_all_pages()
    house_links = []
    for i in page_links:
        #Ignore SSL certificate errors
        ctx = ssl.create_default_context()
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE

        #Open current the search page and parse
        html = urlopen(i , context=ctx).read()
        soup = BeautifulSoup(html, "html.parser")

        #Extract the extension links for each house in the search page 
        links = []
        for i in soup.find_all('a'):
            links.append(str(i.get('href')))
        res = [x for x in links if re.search('for-sale/details', x)] 
        address = Counter(res)

        # Create a dictionary to ensure distinct values
        links_dic = dict(address)

        #Clean up dictionary to remove repetition from images
        links_copy = links_dic.copy()
        for k, v in links_copy.items():
            if v == 1 :
                del links_dic[k]

        # Turn the extenstions to URLs and create the list of houses
        houses = ['https://www.zoopla.co.uk' + ext for ext in links_dic.keys()]
        for i in houses:
            house_links.append(i)

    return house_links

def data_extraction(): 
    count = 1
    houses = Webpages_compositions()
    for i in houses:
        # A counter to update on status of extraction process
        print(count,"/", len(houses))
        count += 1

        # Open the first link within houses and extract parameters
        html2 = urlopen(i)
        soup2 = BeautifulSoup(html2, 'html.parser')
        pagesource = str(soup2).split()

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
        try:
            lat_find = pagesource.index('"latitude":')
            lat_rough = pagesource[lat_find+1]
            lat = float(lat_rough.split(',')[0])
        except:
            lat = np.nan

        #Longtitude
        try:
            lon_find = pagesource.index('"longitude":')
            lon_rough = pagesource[lon_find+1]
            lon = float(lon_rough.split(',')[0])
        except:
            lon = np.nan

        # First published date
        try:
            price_history = pagesource.index('class="dp-price-history__item">')
            date_search = pagesource[price_history+2]
            if len(date_search.split('>')[1]) == 4:
                day = date_search.split('>')[1][0:2]
            else:
                day = date_search.split('>')[1][0:1]
            month = pagesource[price_history+3]
            year = pagesource[price_history+4].split('<')[0]
            date_rough = day + ' ' + month + ' '+ year
            date = datetime.strptime(date_rough,'%d %b %Y').date()
        except:
            date = np.nan
        
        #Distance to nearsest attraction/train station
        try:
            station_find = pagesource.index('miles')
            station = float(pagesource[station_find-1])
        except:
            station= np.nan

        #Is there a mention of a Loft?
        if 'loft' in str(soup2): loft = 1
        else: loft = 0

        #Is there a mention of a Garden?
        if 'garden' in str(soup2): garden = 1
        else: garden = 0
        
        #Write data to csv file
        thewriter.writerow([price,beds,baths,loft,garden,station,date, lat, lon, prop_type])

def pre_processing():
    dataset = pd.read_csv(fname)
    dataset = dataset.iloc[:,:].values

    #Only take year listed
    dates = pd.DatetimeIndex(dataset[:,6]).year        
    dataset[:,6] = dates

    # Covert string columns to empty cellg
    dataset = np.where(dataset == '"' ,np.nan, dataset)

    # Take care of missing numerical  data: Replace numerical values with mean
    imputer = SimpleImputer(missing_values=np.nan, strategy = 'mean')
    imputer.fit_transform(dataset[:,0:9])
    dataset[:, 0:9] = imputer.transform(dataset[:, 0:9])

    # Take care of missing string data: Replace string values with most frequent
    imputer2 = SimpleImputer(missing_values=np.nan, strategy = 'most_frequent')
    imputer2.fit_transform(dataset[:, 9:])
    dataset[:, 9:] = imputer2.transform(dataset[:, 9:])

    return dataset

def read_to_SQL():
    dataset = pre_processing()
    data_df = pd.DataFrame(dataset, columns= ['Price','Beds','Baths','Loft','Garden','Station_distance','Listing_date','Latitude','Longtitude','Property_type'])
    
    conn = sqlite3.connect('Zoopla_data.sqlite')

    cur = conn.cursor()

    cur.executescript('''
    DROP TABLE IF EXISTS Zoopla;

    CREATE TABLE Zoopla (
        id  INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE,
        Price    INTEGER,
        Beds  INTEGER,
        Baths INTEGER,
        Loft INTEGER,
        Garden INTEGER,
        Station_distance FLOAT,
        Listing_date DATE,
        Latitude INTEGER,
        Longtitude INTEGER,
        Property_type VARCHAR(128));
    ''')

    data_df.to_sql('Zoopla', con=conn, if_exists='append', index=False)

    conn.close()

# Choose whether to collect data or process data to SQL Database
if __name__ == "__main__":
    execute = input('To collect data, print C. To process data to SQLite, print P: ').upper()
    if execute == 'C':
        # Choose whether to overwrite or append file
        modification = input('To overwrite file, print w.To append, print a:').lower()
        if modification == 'w':
            f = open(fname,modification, newline= '')
            thewriter = csv.writer(f)
            thewriter.writerow(['House price','Beds','Baths','Loft?','Garden?','Station_distance','Date_listed','Latitude', 'Longtitude','Property_type'])
            data_extraction()
        elif modification == 'a':
            f = open(fname,modification, newline= '')
            thewriter = csv.writer(f)
            data_extraction() 
        else:
            print("Run again and pick w or a ")
            sys.exit()
    #Process data and write to database
    elif execute == 'P':
        pre_processing()
        read_to_SQL()
    else:
        print('Run again and pick P or C')
        sys.exit
