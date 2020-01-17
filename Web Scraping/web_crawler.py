from bs4 import BeautifulSoup
import urllib.request
import re
import urllib.request
from os.path import basename
from os.path import dirname
import time


r = urllib.request.urlopen("http://insideairbnb.com/get-the-data.html").read()
soup = BeautifulSoup(r, "lxml")



# to find the pwd
# %pwd 
# http://data.insideairbnb.com/canada/bc/vancouver/


def scrape_data():

    #city list to scrape the data
    city_list = ["vancouver", "toronto", "montreal"]
    province_list = ["bc", "on", "qc"]

    # show rows of all the tables
    tables = soup.findAll('table')[1]
    rows = tables.findAll('tr')
    row_list = list()
    for tr in rows:
        td = tr.find_all('td')
        row = [i.text for i in td]
        row_list.append(row)
    #print(row_list)

    for i, item in enumerate(city_list):

        #show html doms (table) based on the class tag
        city_tag = "^" + item
        regex = re.compile(city_tag)
        content_lis = soup.find_all('table', attrs={'class': regex})
        #print(content_lis)


        table = content_lis[0]
        # There are 148 rows for vancouver
        len(table.find_all('tr'))

        table.findAll('a',attrs={'href': re.compile("^http")})

        #find the a tags -147
        len(table.findAll('a',attrs={'href': re.compile("^http")}))



        file = open('parsed_vancouver_data.txt', 'w')
        for link in table.findAll('a',attrs={'href': re.compile("^http")}):
            link_a = link['href']
            #print(link_a)
            download_url = link_a
            file_name = basename(link_a)
            dir_name = dirname(link_a)
            
            city_string = province_list[i] + "/" + item + "/"
            split_param = "http://data.insideairbnb.com/canada/" + city_string
            split_url = link_a.split("/")
            new_file_name_list = [split_url[-3], split_url[-2], split_url[-1]]
            new_file_name = "-".join(new_file_name_list)
            final_file_name = item + new_file_name
            urllib.request.urlretrieve(download_url, final_file_name)
            content_lis_link = str(link)
            file.write(content_lis_link)
            file.flush()
        file.close()

# invoke scrape data method to down load all the data from airbnb website for vancouver, Toranto and Montreal       
scrape_data()
        
