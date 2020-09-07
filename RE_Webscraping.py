# Import libraries
from bs4 import BeautifulSoup
import json, ssl, csv, re, sys, urllib.parse, urllib.error 
from urllib.request import urlopen
from collections import Counter
 
#Enter URL of Search page to return list of url of houses
def Webpages_compositions():
    # Ignore SSL certificate errors
    ctx = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE
    # Input the search page from zoopla and read it using beautiful soup
    search_page = input("Enter the search page:")
    html = urlopen(search_page, context=ctx).read()
    soup = BeautifulSoup(html, "html.parser")
    # Extract the links for house in the results and create a dictionary to create distinct values
    links = []
    for all_link in soup.find_all('a'):
        links.append(str(all_link.get('href')))
    res = [x for x in links if re.search('for-sale/details', x)] 
    address = Counter(res)
    links_dic = dict(address)
    links_copy = links_dic.copy()
    #Clean up dictionary to remove repetition from images
    for k, v in links_copy.items():
        if v == 1:
            del links_dic[k]
    houses = ['https://www.zoopla.co.uk' + ext for ext in links_dic.keys()]
    return houses

def data_extraction(): 
    # Obtain weblinks of houses from Webcomposition function
    count = 1
    houses = Webpages_compositions()
    # Open each individual house link and obtain parameters
    for i in houses:
        print("House", count)
        count += 1
        html2 = urlopen(i)
        soup2 = BeautifulSoup(html2, 'html.parser')
        pagesource = str(soup2.find_all('script'))
        wds = pagesource.split()

        # Number of beds
        bed_find = wds.index('num_beds:')
        beds = wds[bed_find+1][0]
        # print("Number of beds:", beds)

        # Number of baths
        bath_find = wds.index('num_baths:')
        baths = wds[bath_find+1][0]
        # print("Number of baths:", baths)
        
        # House price
        price_find = wds.index('price_actual:')
        price_rough = wds[price_find+1]
        price = price_rough.split(',')[0]    
        # print("House price:", price)

        # Property type
        type_find = wds.index('property_type:')
        prop_type_rough = wds[type_find+1]
        prop_type = prop_type_rough.split('"')[1]
        # print("Property type:", prop_type)

        #Location by poscode
        pcode_find = wds.index('outcode:')
        pcode_rough = wds[pcode_find+1]
        pcode = pcode_rough.split('"')[1]
        # print("Property type:", prop_type)

        #Loft available?
        if 'loft' in str(soup2): loft = 1
        else: loft = 0
        # print("Loft:",loft)

        #Garden available?
        if 'garden' in str(soup2): garden = 1
        else: garden = 0
        # print("Garden: ",garden)
        
        #Write data to file
        thewriter.writerow([price,beds,baths,loft,garden,prop_type, pcode])

# Execute file, firstly chosing whether to overwrite or append file
if __name__ == "__main__":
    modification = input('To overwrite file, print w.To append, print a:').lower()
    if modification == 'w':
        f = open('DataTest.csv',modification, newline= '')
        thewriter = csv.writer(f)
        thewriter.writerow(['House price','Beds','Baths','Loft?','Garden?','Property type', 'Post Code'])
        data_extraction()
    elif modification == 'a':
        f = open('DataTest.csv',modification, newline= '')
        thewriter = csv.writer(f)
        data_extraction() 
    else:
        print("Run again and pick w or a ")
        sys.exit()
