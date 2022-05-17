#!/usr/bin/env python
# coding: utf-8

# In[3]:


import requests
import json
from bs4 import BeautifulSoup
import re
import regex
from yelp.client import Client
import pandas as pd
import numpy as np
import os
import json
import urllib.request
import csv 
import time 
from random import randint
import sqlite3
from textblob import TextBlob
import matplotlib.pyplot as plt
from collections import Counter
import dataframe_image as dfi


# In[4]:


#scrape data from Yelp

def scrape_yelp(parameters):
    #define the API key, define the endpoint, and the Header
    #Use Endpoint: Business Search, path '/businesses/search' to locate a list
    API_KEY="vj5SsHLTfSO8qPIeq0UW7HTtw00zIbDqMSoUSxzKsZ4Txtsy_31BlUkF4EHZ6mJVzH9wywgvpgKwhKS8nZIhpyWDxoyzYOfu0bfpdJ4oo82335FSXQLjzN99DwddYnYx" #API key: 5000 per day
    ENDPOINT='https://api.yelp.com/v3/businesses/search' 
    HEADERS={'Authorization':'bearer %s' % API_KEY}
       
    #create each attribute their own list for df
    name_list=[]
    url_list=[]
    review_count_list=[]
    rating_list=[]
    price_list=[]
    city_list=[]
    location_list=[]
    id_list=[]
    review1=[]
    review2=[]
    review3=[]
    review=[]
    
    #Use Endpoint: Business Search, path '/businesses/search' to locate the list
    
    #make a request to the yelp API (max return per one request:50)
    response=requests.get(url=ENDPOINT,params=parameters,headers=HEADERS)
        
    #convert the JSON string to a Disctionary
    business_data=response.json()

    for place in business_data['businesses']:
        name_list.append(place['name'])
        url_list.append(place['url'])
        review_count_list.append(place['review_count'])
        rating_list.append(place['rating'])
        city_list.append(place['location']['city'])
        id_list.append(place['id'])
        try:
            price_list.append(place['price'])
        except:
            price_list.append('N/A')

    yelp_data={'Name':name_list,'url_place':url_list,'review count':review_count_list,'rating':rating_list,'price':price_list,'city':city_list,'yelpID':id_list}
    df=pd.DataFrame(yelp_data)
    
    #Use Endpoint: Reviews, path '/businesses/{id}/reviews' to get up to three review excerpts for a business.
    #locate the business with the ID of the business to get reviews
    #Yelp API allows users to get 3 reviews per restaurant
    #i=0
    for placeid in df['yelpID']:
        ENDPOINT_re=f'https://api.yelp.com/v3/businesses/{placeid}/reviews'
        HEADERS_re={'Authorization':'bearer %s' % API_KEY}
        response_re=requests.get(url=ENDPOINT_re,headers=HEADERS_re)
        business_review=response_re.json()
        #print(business_review)
        if len(business_review['reviews'])==3:
            review1.append(business_review['reviews'][0]['text'])
            review2.append(business_review['reviews'][1]['text'])
            review3.append(business_review['reviews'][2]['text'])
            review.append([business_review['reviews'][0]['text'],business_review['reviews'][1]['text'],business_review['reviews'][2]['text']])
        elif len(business_review['reviews'])==2:
            review1.append(business_review['reviews'][0]['text'])
            review2.append(business_review['reviews'][1]['text'])
            review3.append('N/A')
            review.append([business_review['reviews'][0]['text'],business_review['reviews'][1]['text'],'N/A'])
        elif len(business_review['reviews'])==1:
            review1.append(business_review['reviews'][0]['text'])
            review2.append('N/A')
            review3.append('N/A')
            review.append([business_review['reviews'][0]['text'],'N/A','N/A'])
        else:
            review1.append('N/A')
            review2.append('N/A')
            review3.append('N/A')
            review.append(['N/A','N/A','N/A'])

        #i+=1
        #print(i)
        
    #Use Endpoint: Details, path '/businesses/{id}' to get location for a business.
    #locate the business with the ID of the business to get location
    for placeid in df['yelpID']:
        ENDPOINT_re=f'https://api.yelp.com/v3/businesses/{placeid}'
        HEADERS_re={'Authorization':'bearer %s' % API_KEY}
        response_re=requests.get(url=ENDPOINT_re,headers=HEADERS_re)
        detaildata=response_re.json()
        address=detaildata['location']['display_address']
        modified_address=address[0]+', '+address[1] #as address comes in form like ['1200-1274 Miramar St', 'Los Angeles, CA 90026']
        location_list.append(modified_address)
    
    yelp_data={'name':name_list,
               'url':url_list,
               'rating':rating_list,
               'review_count':review_count_list,
               'price':price_list,
               'city':city_list,
               'location':location_list,
               'review':review, 
               'yelpID':id_list,}
    df=pd.DataFrame(yelp_data)
    
    return df

def webscrape_yelp():
    #change offset in parameters to get more results
    param1={'term':'japanese restaurant','limit':50,'sort_by':'rating','location':'Los Angeles','categories':'Japanese'}
    param2={'term':'japanese restaurant','limit':50,'offset':50,'sort_by':'rating','location':'Los Angeles','categories':'Japanese'}
    param3={'term':'japanese restaurant','limit':50,'offset':100,'sort_by':'rating','location':'Los Angeles','categories':'Japanese'}
    param4={'term':'japanese restaurant','limit':50,'offset':150,'sort_by':'rating','location':'Los Angeles','categories':'Japanese'}
    df_eats1=scrape_yelp(param1)
    df_eats2=scrape_yelp(param2)
    df_eats3=scrape_yelp(param3)
    df_eats4=scrape_yelp(param4)
    result = df_eats1.append(df_eats2).append(df_eats3).append(df_eats4)
    result.to_csv('Data_Yelp_API.csv')
    
    return result


# In[5]:


#scrape data from Tripadvisor

def scrape_loc_ta(url):
    response = requests.get(url, headers={'User-Agent': "Mozilla/5.0"})
    soup = BeautifulSoup(response.text, 'html.parser')
    #get location
    location=soup.find('a', {'class': 'fhGHT','href':'#MAPVIEW'}).text
    return location

#scrape 15 latest english reviews (sort by newest by default) using the url link on the restuarant list
def scrape_review(url): 
    response = requests.get(url, headers={'User-Agent': "Mozilla/5.0"})
    soup = BeautifulSoup(response.text, 'html.parser')
    review_list=[]

    for entry in soup.find_all('div',{'class':'ui_column is-9'}):
        review=''
        try:
            review_p1=entry.find('p',{'class':'partial_entry'}).text
            review+=review_p1
        except:
            pass
        try:
            review_p2=entry.find('span',{'class':'postSnippet'}).text
            review+=review_p2
        except:
            pass
        review_list.append(review)
        
    return review_list

def scrape_ta(url):
    #df_eats = pd.DataFrame(columns=["name","url_place","rating","price","location","review"])
    df_eats = pd.DataFrame(columns=["name","url_place","rating","review_count","price","location","review"])
    response = requests.get(url, headers={'User-Agent': "Mozilla/5.0"})
    soup = BeautifulSoup(response.text, 'html.parser')
    
    #things to note: Tripadvisor shows 30 restaurants per page OR 1 sponsor restaurant+30. 
    #Want to drop the sponsor restaurant

    #get names
    name=soup.find_all('a', {'class': 'bHGqj Cj b'})
    name_list=[]
    for i in name:
        try:
            name_list.append(i.text.split('.')[1]) #the name scrapped comes with ranking 
        except: #sponsored should not be included and sponsored one does not have ranking
            pass
    if len(name_list)==31:
        name_list.pop(0)
    df_eats['name']=name_list

    #get rating
    ratings=soup.find_all('svg', {'class': 'RWYkj d H0'})
    ratings_list=[]
    for i in ratings:
        try:
            rating=i.get('aria-label').split(' ')[0] #rating comes in form like "5.0 of 5 bubbles"
            ratings_list.append(rating) 
        except:
            pass
    if len(ratings_list)==31:
        ratings_list.pop(0)
    df_eats['rating']=ratings_list
    
    #get review_count
    review_count=soup.find_all('span', {'class': 'NoCoR'})
    review_count_list=[]
    for i in review_count:
        count=i.text.split(' ')[0] #review counts comes in form like "5 reviews"
        review_count_list.append(count) 
    if len(review_count_list)==31:
        review_count_list.pop(0)
    df_eats['review_count']=review_count_list
        
    #get price
    prices=soup.find_all('span', {'class': 'ceUbJ'})
    prices_list=[]
    count=0
    for i in prices:
        if '$' in i.text:
            count+=1
            price=i.text
            prices_list.append(price)
    if len(prices_list)==31:
        prices_list.pop(0)
    df_eats['price']=prices_list
    
    #get url_place
    url_places=soup.find_all('a', {'class': 'bHGqj Cj b'})
    url_places_list=[]
    locations_list=[]
    reviews_list=[]
    for i in url_places:
        url_place=i.get('href')
        url_places_list.append('https://www.tripadvisor.com/'+url_place) 
    if len(url_places_list)==31:
        url_places_list.pop(0)
    df_eats['url_place']=url_places_list
    
    #get location
    locations_list=[]
    for url_i in df_eats['url_place']:
        try:
            location=scrape_loc_ta(url_i)
            locations_list.append(location)  
        except:
            locations_list.append('N/A')  
    df_eats['location']=locations_list
    
    #get reviews
    reviews_list=[]
    for url_i in df_eats['url_place']:
        review=scrape_review(url_i)
        reviews_list.append(review)  
    df_eats['review']=reviews_list
        
    
    return df_eats

def webscrape_tripadvisor():
    #Best 30 Japanese Food in Los Angeles, CA
    df_eats1=scrape_ta('https://www.tripadvisor.com/Restaurants-g32655-c27-Los_Angeles_California.html')

    #Best 30-60 Japanese Food in Los Angeles, CA
    df_eats2=scrape_ta('https://www.tripadvisor.com/Restaurants-g32655-c27-oa30-Los_Angeles_California.html?pid=2#EATERY_LIST_CONTENTS')

    #Best 60-90 Japanese Food in Los Angeles, CA
    df_eats3=scrape_ta('https://www.tripadvisor.com/Restaurants-g32655-c27-oa60-Los_Angeles_California.html?pid=2#EATERY_LIST_CONTENTS')

    #Best 90-120 Japanese Food in Los Angeles, CA
    df_eats4=scrape_ta('https://www.tripadvisor.com/Restaurants-g32655-c27-oa90-Los_Angeles_California.html?pid=2#EATERY_LIST_CONTENTS')
    
    result = df_eats1.append(df_eats2).append(df_eats3).append(df_eats4)
    result.to_csv('Data_TripAdvisor.csv')
    
    return result


# In[6]:


#scrape data from OpenTable
#scrape 3 newest reviews (sort by newest by default) using the url link on the restuarant list
def scrape_review_ot(url): 
    html = urllib.request.urlopen(url).read()
    time.sleep(2) 
    soup = BeautifulSoup(html, 'html.parser', from_encoding="utf-8")
    review_list = ['N/A','N/A','N/A'] 
    try:
        i=0
        for entry in soup.find_all('div',{'class':'oc-reviews-5a88ccc3'}):
            time.sleep(1) 
            review=entry.find('p',{'lang':'en-US'}).text
            review_list[i]=review
            i+=1
            if i==3:
                break
    except:
        review_list=['N/A','N/A','N/A']
        
    if review_list==['N/A','N/A','N/A']: #sometime it needs multiple tries
        try:
            i=0
            for entry in soup.find_all('p',{'class':'t9JcvSL3Bsj1lxMSi3pz h_kb2PFOoyZe1skyGiz9 Ti64w3n01MDTYZb59n6Q'}):
                review=entry.text
                review_list[i]=review
                i+=1
                if i==3:
                    break
        except:
            review_list=['N/A','N/A','N/A']
            
    return review_list

def scrape_ot(url):
    # visit that url, and grab the html of said page
    html = urllib.request.urlopen(url).read()
    # convert this into a soup object
    soup = BeautifulSoup(html, 'html.parser', from_encoding="utf-8")
    #set up a dataframe
    df_eats = pd.DataFrame(columns=["name","url_place","rating","review_count","price","city","review"])

    #for loop for each restaurant entry
    #i=0
    for entry in soup.find_all('div', {'class':'rest-row-info'}):
        #print(entry)
        #get the name 
        name = entry.find('span', {'class': 'rest-row-name-text'}).text
    
        #get the rating
        try:
            ratingline=entry.find('div', {'class': 'star-rating-score'},{'role':"img"})
            rating=ratingline.get('aria-label', None)
            rating=rating.split(' ')[0] #rating comes in form like '4.6 stars out of 5'
        except:
            rating='N/A'
    
        #get the review_amount 
        try:
            review_count=entry.find('span', {'class': 'underline-hover'}).text
            review_count=int(review_count[1:-1])   #adjust it to integer
        except:
            review_count=0
        
        #get the price
        try:
            price=entry.find('i', {'class': 'pricing--the-price'}).text
        except:
            price='N/A'
    
        #get the url
        try:
            url_place=entry.find('a',{'class':'rest-row-name rest-name'}).get('href')
        except:
            url_place='N/A'
    
        #get the city
        try:
            city=entry.find('span',{'class':'rest-row-meta--location rest-row-meta-text sfx1388addContent'}).text
        except:
            city='N/A'
        
        #get the review
        if url_place!='N/A' and review_count!=0:
            r = requests.get(url_place)
            if r.status_code==200:
                try:
                    review=scrape_review_ot(url_place)
                except:
                    review=['N/A','N/A','N/A']
            else:
                review=['N/A','N/A','N/A']
                
        #print(i)
        #i+=1 
    
        #assemble the dataframe
        df_eats.loc[len(df_eats)]=[name,url_place,rating,review_count,price,city,review]
        
        #sometimes the website is sensitive and review info cannot be substract, so try again
        for i in range(len(df_eats)):
            if df_eats['review'].iloc[i]==['N/A','N/A','N/A'] and df_eats['url_place'].iloc[i]!='N/A' and df_eats['review_count'].iloc[i]!=0:
                r = requests.get(df_eats['url_place'].iloc[i])
                if r.status_code==200:
                    try:
                        review=scrape_review_ot(url_place)
                    except:
                        review=['N/A','N/A','N/A']
                else:
                    review=['N/A','N/A','N/A']
                
        
    #save the csv file to the folder
    df_eats.to_csv('Data_OpenTable.csv')
   
    return df_eats

def scrape_loc(url):
    if url=='N/A':
        return 'N/A'
    else: 
        try:
            html = urllib.request.urlopen(url).read() 
            soup = BeautifulSoup(html, 'html.parser', from_encoding="utf-8")
        except:
            return 'N/A'
        try:
            address=soup.find('p',{'class':'aAmRZnL9EescJ80holSh'}).text
        except:
            address='N/A'
    return address

def scrape_opentable():
    url = 'https://www.opentable.com/los-angeles-restaurant-listings&size=100&sort=Rating&cuisineids%5B%5D=7bf54b86-bd1f-4e35-bbca-f7cce982b4ed'
    #url2='https://www.opentable.com/los-angeles-restaurant-listings&size=100&sort=Rating&cuisineids%5B%5D=7bf54b86-bd1f-4e35-bbca-f7cce982b4ed&from=100'
    df=scrape_ot(url)
    return df

def webscrape_opentable_plusloc(df):
    location=[]
    for site in df['url_place']:
        location.append(scrape_loc(site))
    df['location']=location
    #save the csv file to the folder
    df.to_csv('Data_OpenTable.csv')
    return df


# In[7]:


#store the csv file into database
def csv_to_sql():
    conn = sqlite3.connect('finalproject.db', detect_types=sqlite3.PARSE_DECLTYPES|sqlite3.PARSE_COLNAMES)
    cur = conn.cursor()

    #feed 3 data csv files into database
    with open('Data_TripAdvisor.csv') as csvfile:
        csv_reader=csv.reader(csvfile, delimiter=',')
        cur.execute(f"DROP TABLE IF EXISTS TripAdvisor")
        cur.execute(f"CREATE TABLE TripAdvisor (TA_name TEXT, TA_url TEXT,TA_rating REAL,TA_review_count REAL, TA_price TEXT,TA_location TEXT, TA_review TEXT)")
        count=0
        for row in csv_reader:
            if count==0:
                count+=1
            else:
                cur.execute("INSERT INTO TripAdvisor values (?, ?,?,?,?,?,?)", (row[1].strip(), row[2].strip(),row[3].strip(),row[4].strip(),row[5].strip(),row[6].split(',')[0].strip(),row[7].strip()))
    
    with open('Data_OpenTable.csv') as csvfile:
        csv_reader=csv.reader(csvfile, delimiter=',')
        cur.execute(f"DROP TABLE IF EXISTS OpenTable")
        cur.execute(f"CREATE TABLE OpenTable (OT_name TEXT, OT_url TEXT,OT_rating REAL,OT_review_count REAL, OT_price TEXT, OT_city TEXT, OT_location TEXT,OT_review TEXT)")
        count=0
        for row in csv_reader:
            if count==0:
                count+=1
            else:
                cur.execute("INSERT INTO OpenTable values (?, ?,?,?,?,?,?,?)", (row[1].strip(), row[2].strip(),row[3].strip(),row[4].strip(),row[5].strip(),row[6].strip(),row[8].split('  ')[0].strip(),row[7].strip())) #split to get the address line 1

    with open('Data_Yelp_API.csv') as csvfile:
        csv_reader=csv.reader(csvfile, delimiter=',')
        cur.execute(f"DROP TABLE IF EXISTS Yelp")
        cur.execute(f"CREATE TABLE Yelp (Y_name TEXT, Y_url TEXT,Y_rating REAL,Y_review_count REAL, Y_price TEXT,Y_city TEXT,Y_location TEXT,Y_review TEXT)")
        count=0
        for row in csv_reader:
            if count==0:
                count+=1
            else:
                cur.execute("INSERT INTO Yelp values (?, ?,?,?,?,?,?,?)", (row[1].strip(), row[2].strip(),row[3].strip(),row[4].strip(),row[5].strip(),row[6].strip(),row[7].split(',')[0].strip(),row[8].strip())) #split to get the address line 1
    conn.commit()
    conn.close()

def merge_sqltables():
    #merge 3 tables based on location in sqlite
    conn = sqlite3.connect('finalproject.db', detect_types=sqlite3.PARSE_DECLTYPES|sqlite3.PARSE_COLNAMES)
    cur = conn.cursor()

    #Full outer join 2 table based on location
    cur.execute(f"DROP TABLE IF EXISTS combinedata")
    query1='''CREATE TABLE combinedata AS
        SELECT t.TA_name,y.Y_name, 
            t.TA_rating,y.Y_rating, 
            t.TA_review_count,y.Y_review_count,
            t.TA_price, y.Y_price,
            t.TA_location,y.Y_location,
            t.TA_review,y.Y_review
        FROM TripAdvisor t 
            LEFT JOIN Yelp y 
                ON LOWER(t.TA_location) LIKE LOWER(y.Y_location)
        UNION ALL
        SELECT t.TA_name,y.Y_name, 
            t.TA_rating,y.Y_rating, 
            t.TA_review_count,y.Y_review_count,
            t.TA_price, y.Y_price,
            t.TA_location,y.Y_location,
            t.TA_review,y.Y_review
        FROM Yelp y
            LEFT JOIN TripAdvisor t 
                ON LOWER(t.TA_location)=LOWER(y.Y_location)
        WHERE t.TA_location IS NULL'''
    cur.execute(query1)

    #coalesce 2 location columns into a new one called "location" (preparation for joining the 3rd table)
    cur.execute(f"DROP TABLE IF EXISTS combinedata1_2")
    query2='''CREATE TABLE combinedata1_2 AS
        SELECT
            TA_name,Y_name, 
            TA_rating,Y_rating, 
            TA_review_count,Y_review_count,
            TA_price, Y_price,
            TA_location,Y_location,
            TA_review,Y_review,
            COALESCE(TA_location,Y_location) AS TA_Y_location
        FROM combinedata
        WHERE COALESCE(TA_location,Y_location) IS NOT NULL '''
    cur.execute(query2)

    #coalesce 2 name columns into a new one called "name" (preparation for joining the 3rd table)
    cur.execute(f"DROP TABLE IF EXISTS combinedata1_3")
    cur.execute('''CREATE TABLE combinedata1_3 AS
        SELECT
            TA_name,Y_name, 
            TA_rating,Y_rating, 
            TA_review_count,Y_review_count,
            TA_price, Y_price,
            TA_Y_location,
            TA_review,Y_review,
            COALESCE(TA_name,Y_name) AS TA_Y_name
        FROM combinedata1_2
        WHERE COALESCE(TA_name,Y_name) IS NOT NULL ''')

    #join the 3rd table based on location
    cur.execute(f"DROP TABLE IF EXISTS combinedata2")
    query3='''CREATE TABLE combinedata2 AS
        SELECT c.TA_Y_name, o.OT_name,
            c.TA_rating,c.Y_rating, o.OT_rating,
            c.TA_review_count,c.Y_review_count,o.OT_review_count,
            c.TA_price, c.Y_price,o.OT_price,
            c.TA_Y_location,o.OT_location,
            c.TA_review,c.Y_review,o.OT_review
        FROM combinedata1_3 c 
            LEFT JOIN OpenTable o 
                ON LOWER(c.TA_Y_location) LIKE LOWER(o.OT_location)
        UNION ALL
        SELECT c.TA_Y_name, o.OT_name,
            c.TA_rating,c.Y_rating, o.OT_rating,
            c.TA_review_count,c.Y_review_count,o.OT_review_count,
            c.TA_price, c.Y_price,o.OT_price,
            c.TA_Y_location,o.OT_location,
            c.TA_review,c.Y_review,o.OT_review
        FROM OpenTable o
            LEFT JOIN combinedata1_3 c 
                ON LOWER(c.TA_Y_location)=LOWER(o.OT_location)
        WHERE c.TA_Y_location IS NULL'''
    cur.execute(query3)

    #coalesce 2 location columns into a new one
    cur.execute(f"DROP TABLE IF EXISTS combinedata2_2")
    cur.execute('''CREATE TABLE combinedata2_2 AS
        SELECT TA_Y_name,OT_name, 
            TA_rating,Y_rating, OT_rating,
            TA_review_count,Y_review_count,OT_review_count,
            TA_price, Y_price, OT_price,
            TA_Y_location,OT_location,
            TA_review,Y_review,OT_review,
            COALESCE(TA_Y_location,OT_location) AS location
        FROM combinedata2
        WHERE COALESCE(TA_Y_location,OT_location) IS NOT NULL ''')

    #coalesce 2 name columns into a new one
    cur.execute(f"DROP TABLE IF EXISTS combinedata2_3")
    cur.execute('''CREATE TABLE combinedata2_3 AS
        SELECT
            TA_Y_name,OT_name, 
            TA_rating,Y_rating, OT_rating,
            TA_review_count,Y_review_count,OT_review_count,
            TA_price, Y_price, OT_price,
            location,
            TA_review,Y_review,OT_review,
            COALESCE(TA_Y_name,OT_name) AS name
        FROM combinedata2_2
        WHERE COALESCE(TA_Y_name,OT_name) IS NOT NULL ''')
    
    #coalesce 3 price columns into a new one
    cur.execute(f"DROP TABLE IF EXISTS combinedata2_4")
    cur.execute('''CREATE TABLE combinedata2_4 AS
        SELECT
            name,
            TA_rating,Y_rating, OT_rating,
            TA_review_count,Y_review_count,OT_review_count,
            TA_price, Y_price, OT_price,
            location,
            TA_review,Y_review,OT_review,
            COALESCE(TA_price, Y_price, OT_price) AS price
        FROM combinedata2_3
        WHERE COALESCE(TA_price, Y_price, OT_price) IS NOT NULL ''')

    #clean the table 
    cur.execute(f"DROP TABLE IF EXISTS combinedata3")
    cur.execute('''CREATE TABLE combinedata3 AS
        SELECT
            name, 
            TA_rating,Y_rating, OT_rating,
            TA_review_count,Y_review_count,OT_review_count,
            price, 
            location,
            TA_review,Y_review,OT_review
        FROM combinedata2_4''')

    conn.commit()
    conn.close()


# In[8]:


#convert the merged table (table combinedata3) into dataframe to calculate overall review count and weighted rating
def cal_weighted_rating():
    conn = sqlite3.connect('finalproject.db', detect_types=sqlite3.PARSE_DECLTYPES|sqlite3.PARSE_COLNAMES)
    cur = conn.cursor()
    query4 = cur.execute("SELECT * From combinedata3")
    cols = [column[0] for column in query4.description]
    results= pd.DataFrame.from_records(data = query4.fetchall(), columns = cols)
    conn.commit()
    conn.close()

    #clean data (replace nan value with 0)
    results['Y_review_count']=results['Y_review_count'].fillna(0)
    results['TA_review_count']=results['TA_review_count'].fillna(0)
    results['OT_review_count']=results['OT_review_count'].fillna(0).replace('N/A',0)

    results['Y_rating']=results['Y_rating'].fillna(0).apply(lambda x: float(x))
    results['TA_rating']=results['TA_rating'].fillna(0).apply(lambda x: float(x))
    results['OT_rating']=results['OT_rating'].fillna(0).replace('N/A',0)

    #calculate the overall review count and weighted rating
    results['overall_review_count']=results['Y_review_count']+results['TA_review_count']+results['OT_review_count']
    results['weighted_rating_3ratings']=results['TA_rating']*(results['TA_review_count']/results['overall_review_count'])+results['Y_rating']*(results['Y_review_count']/results['overall_review_count'])+results['OT_rating']*(results['OT_review_count']/results['overall_review_count'])

    #sort the table by weighted rating, if the value is the same, sort by overall review count
    results=results.sort_values(by=['weighted_rating_3ratings','overall_review_count'],ascending=False)
    
    #drop rows that has 0 as weighted rating / has 0 reviews
    results.drop(results[results['weighted_rating_3ratings']==0].index, inplace = True)
    results.drop(results[results['overall_review_count']==0].index, inplace = True)  
    
    return results


# In[9]:


#find most repeated word in review
def find_most_repeated(df_result):
    repeated_list=[]
    for i in range(df_result.shape[0]):
        repeated=[]
        part1=df_result['TA_review'].iloc[i]
        part2=df_result['Y_review'].iloc[i]
        part3=df_result['OT_review'].iloc[i]
        #split the word in each review
        if part1!=None:
            split=part1[1:-1].split()
            for i in split:
                repeated.append(i.strip('\'').strip('\"').strip(',').lower())
        if part2!=None:
            split=part2[1:-1].split()
            for i in split:
                repeated.append(i.strip('\'').strip('\"').strip(',').lower())
        if part3!=None:
            split=part3[1:-1].split()
            for i in split:
                repeated.append(i.strip('\'').strip('\"').strip(',').lower())
        counter = Counter(repeated)
        #delete words: 'and','a','the','you','of'
        for word in ['and','a','the','of','i','you','is','an','had','our','to','my','in','was','on','can','it','this','always','as','for','have','so','very','but','at','were','me','&','here','but','not','with','we','are','they','been','has','if','n/a','be','their','no','that','which','he','such','also','do','by','that','am','-','there','too','could','will','never','would','first','then','when','some','ever','there','how','place','say',"i've",',','its','really','cannot',"n/a'",'most','or','try','one','up','just','oh','around','or','did','..','us','from','get','=','.','','seems','sometimes','after','4']:
            counter.pop(word, None)
        #find the 2 most common word for each place
        most_occur = counter.most_common(2)
        #make the most common words (occurance>1) into a list
        most_occur_list=[]
        if most_occur!=[]:
            for key,value in most_occur:
                if value>1:
                    most_occur_list.append(key)                  
        repeated_list.append(most_occur_list)
    df_result['keywords']=repeated_list
    return df_result


# In[10]:


#sentiment analysis on review text
def sentiment_analysis(df_result):
    # find the polarity score (from -1 to 1) and the sentiment of text using TextBlob
    df_result["TA_sentiment_score"] = df_result["TA_review"].apply(lambda x: TextBlob(str(x)).sentiment.polarity)
    # project the polarity score into rating
    # Positive is 5 stars, neutral is 4 stars, negative is the average between 1 to 3 stars, i.e. 2 stars. 
    df_result["TA_sentiment_rating"] = np.select([df_result["TA_sentiment_score"] < 0, df_result["TA_sentiment_score"] == 0, df_result["TA_sentiment_score"] > 0],
                           [2, 4, 5])

    # find the polarity score (from -1 to 1) and the sentiment of text using TextBlob
    df_result["Y_sentiment_score"] = df_result["Y_review"].apply(lambda x: TextBlob(str(x)).sentiment.polarity)
    # project the polarity score into rating
    # Positive is 5 stars, neutral is 4 stars, negative is the average between 1 to 3 stars, i.e. 2 stars. 
    df_result["Y_sentiment_rating"] = np.select([df_result["Y_sentiment_score"] < 0, df_result["Y_sentiment_score"] == 0, df_result["Y_sentiment_score"] > 0],
                           [2, 4, 5])

    # find the polarity score (from -1 to 1) and the sentiment of text using TextBlob
    df_result["OT_sentiment_score"] = df_result["OT_review"].apply(lambda x: TextBlob(str(x)).sentiment.polarity)
    # project the polarity score into rating
    # Positive is 5 stars, neutral is 4 stars, negative is the average between 1 to 3 stars, i.e. 2 stars. 
    df_result["OT_sentiment_rating"] = np.select([df_result["OT_sentiment_score"] < 0, df_result["OT_sentiment_score"] == 0, df_result["OT_sentiment_score"] > 0],[2, 4, 5])

    #find the weighted overall rating based on sentiment rating
    df_result['weighted_rating_sentiment']=df_result['TA_sentiment_rating']*(df_result['TA_review_count']/df_result['overall_review_count'])+df_result['Y_sentiment_rating']*(df_result['Y_review_count']/df_result['overall_review_count'])+df_result['OT_sentiment_rating']*(df_result['OT_review_count']/df_result['overall_review_count'])
    
    #sort the table by weighted rating, if the value is the same, sort by overall review count
    #df_result=df_result.sort_values(by=['weighted_rating_sentiment','overall_review_count'],ascending=False)
    
    #separate into restaurant details table and review table
    df_eats=df_result[['name','price','location','weighted_rating_3ratings','weighted_rating_sentiment','overall_review_count','keywords']]
    df_review=df_result[['name','overall_review_count','weighted_rating_3ratings','weighted_rating_sentiment','TA_rating','Y_rating','OT_rating','TA_review_count','Y_review_count','OT_review_count','TA_review','TA_sentiment_score','TA_sentiment_rating','Y_review','Y_sentiment_score','Y_sentiment_rating','OT_review','OT_sentiment_score','OT_sentiment_rating']]
    
    #sort the table by weighted rating, if the value is the same, sort by overall review count
    df_eats=df_eats.sort_values(by=['weighted_rating_3ratings','overall_review_count'],ascending=False)
    df_review=df_review.sort_values(by=['weighted_rating_3ratings','overall_review_count'],ascending=False)
    
    #store the dataframe as csv
    df_result.to_csv('ALL_Data_Final.csv')
    df_eats.to_csv('EATS_Data_Final.csv')
    df_review.to_csv('REVIEW_Data_Final.csv')
    
    #store the csv file into database as "Final"
    conn = sqlite3.connect('finalproject.db', detect_types=sqlite3.PARSE_DECLTYPES|sqlite3.PARSE_COLNAMES)
    cur = conn.cursor()

    with open('ALL_Data_Final.csv') as csvfile:
        csv_reader=csv.reader(csvfile, delimiter=',')
        cur.execute(f"DROP TABLE IF EXISTS ALL_Data_Final")
        cur.execute(f"CREATE TABLE ALL_Data_Final (name TEXT, TA_rating REAL, Y_rating REAL, OT_rating REAL, TA_review_count REAL, Y_review_count REAL,OT_review_count REAL, price TEXT, location TEXT, TA_review TEXT,Y_review TEXT,OT_review TEXT, overall_review_count REAL, weighted_rating_3ratings REAL, keywords,TA_sentiment_score,TA_sentiment_rating,Y_sentiment_score,Y_sentiment_rating,OT_sentiment_score,OT_sentiment_rating,weighted_rating_sentiment )")
        count=0
        for row in csv_reader:
            if count==0:
                count+=1
            else:
                cur.execute("INSERT INTO ALL_Data_Final values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", (row[1].strip(),row[2].strip(),row[3].strip(),row[4].strip(),row[5].strip(),row[6].strip(),row[7].strip(),row[8].strip(),row[9].strip(),row[10].strip(),row[11].strip(),row[12].strip(),row[13].strip(),row[14].strip(),row[15].strip(),row[16].strip(),row[17].strip(),row[18].strip(),row[19].strip(),row[20].strip(),row[21].strip(),row[22].strip()))

    with open('EATS_Data_Final.csv') as csvfile:
        csv_reader=csv.reader(csvfile, delimiter=',')
        cur.execute(f"DROP TABLE IF EXISTS EATS_Data_Final")
        cur.execute(f"CREATE TABLE EATS_Data_Final (name TEXT, price TEXT, location TEXT, weighted_rating_3ratings REAL, weighted_rating_sentiment REAL, overall_review_count REAL,keywords)")
        count=0
        for row in csv_reader:
            if count==0:
                count+=1
            else:
                cur.execute("INSERT INTO EATS_Data_Final values (?,?,?,?,?,?,?)", (row[1].strip(),row[2].strip(),row[3].strip(),row[4].strip(),row[5].strip(),row[6].strip(),row[7].strip()))

    with open('REVIEW_Data_Final.csv') as csvfile:
        csv_reader=csv.reader(csvfile, delimiter=',')
        cur.execute(f"DROP TABLE IF EXISTS REVIEW_Data_Final")
        cur.execute(f"CREATE TABLE REVIEW_Data_Final (name TEXT, overall_review_count REAL,weighted_rating_3ratings REAL, weighted_rating_sentiment REAL, TA_rating REAL, Y_rating REAL, OT_rating REAL, TA_review_count REAL, Y_review_count REAL,OT_review_count REAL,TA_review TEXT,TA_sentiment_score,TA_sentiment_rating,Y_review TEXT,Y_sentiment_score,Y_sentiment_rating,OT_review TEXT, OT_sentiment_score,OT_sentiment_rating )")
        count=0
        for row in csv_reader:
            if count==0:
                count+=1
            else:
                cur.execute("INSERT INTO REVIEW_Data_Final values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", (row[1].strip(),row[2].strip(),row[3].strip(),row[4].strip(),row[5].strip(),row[6].strip(),row[7].strip(),row[8].strip(),row[9].strip(),row[10].strip(),row[11].strip(),row[12].strip(),row[13].strip(),row[14].strip(),row[15].strip(),row[16].strip(),row[17].strip(),row[18].strip(),row[19].strip()))
    
    conn.commit()
    conn.close()
    
    return df_result,df_eats,df_review


# In[22]:


#investigate the correlations between the non-zero ratings on the three websites
def find_correlation(df_review):
    #yelp rating vs. opentable rating
    df_y_ot=df_review[['Y_rating','OT_rating']]
    df_y_ot_clean=df_y_ot[(df_y_ot['Y_rating'] != 0) & (df_y_ot['OT_rating'] != 0)]
    print('\nThe correlation between Yelp rating and OpenTable rating is: ')
    print(df_y_ot_clean.corr().iloc[1,0])

    #yelp rating vs. tripadvisor rating
    df_y_ta=df_review[['Y_rating','TA_rating']]
    df_y_ta_clean=df_y_ta[(df_y_ta['Y_rating'] != 0) & (df_y_ta['TA_rating'] != 0)]
    print('\nThe correlation between Yelp rating and TripAdvisor rating is: ')
    print(df_y_ta_clean.corr().iloc[1,0])
    
    #tripadvisor rating vs. opentable rating
    df_ta_ot=df_review[['TA_rating','OT_rating']]
    df_ta_ot_clean=df_ta_ot[(df_ta_ot['TA_rating'] != 0) & (df_ta_ot['OT_rating'] != 0.0)]
    print('\nThe correlation between TripAdvisor rating and OpenTable rating is: ')
    print(df_ta_ot_clean.corr().iloc[1,0])
    
    #weighted_rating_3ratings vs. weighted_rating_sentiment
    df_app_sentiment=df_review[['weighted_rating_3ratings','weighted_rating_sentiment']]
    df_app_sentiment_clean=df_app_sentiment[(df_app_sentiment['weighted_rating_3ratings'] != 0) & (df_app_sentiment['weighted_rating_sentiment'] != 0)]
    print('\nThe correlation between weighted rating using 3 platform ratings and weighted rating using sentiment analysis on reviews is: ')
    print(df_app_sentiment_clean.corr().iloc[1,0])
    plt.scatter(df_app_sentiment_clean['weighted_rating_3ratings'],df_app_sentiment_clean['weighted_rating_sentiment'])
    plt.xlabel("weighted rating using 3 platform ratings")
    plt.ylabel("weighted rating using sentiment analysis on reviews")
    plt.title('platform rating vs. sentiment rating')
    plt.savefig('figure1.png')
    plt.show()


# In[23]:


#putting everything together
#scrape data
def scrape_data():
    df=scrape_opentable()
    df_opentable=webscrape_opentable_plusloc(df)
    df_yelp=webscrape_yelp()
    df_tripadvisor=webscrape_tripadvisor()
    #store the csv file into sqlite database
    csv_to_sql()
    return df_opentable,df_yelp,df_tripadvisor

#ORGANIZE AND MERGE DATA, ANALYSIS
def organize_analysis():
    #merge 3 tables based on location in sqlite
    merge_sqltables()

    #ANALYSIS
    #convert the merged table (table combinedata2_3) into dataframe to calculate overall review count and weighted rating
    df_weighted_3ratings=cal_weighted_rating()
    #find most repeated word in review
    df_weighted_3ratings=find_most_repeated(df_weighted_3ratings)
    #sentiment analysis using the dataframe
    df_result,df_eats,df_review=sentiment_analysis(df_weighted_3ratings)
    #USING DF_EATS: report the top 10 
    print('The top 10 Japanese restaurants in LA are: ')
    print(df_eats.head(10))
    dfi.export(df_eats.head(10), 'top10.png')
    #USING DF_REVIEW: investigate the correlations between the non-zero ratings on the three websites and sentiment rating
    find_correlation(df_review)


# In[24]:


#df_opentable,df_yelp,df_tripadvisor=scrape_data()


# In[25]:


#organize_analysis()


# In[15]:


import sys

if __name__ == "__main__":
    if len(sys.argv)==1:
        df_opentable,df_yelp,df_tripadvisor=scrape_data()
        organize_analysis()
        
    elif len(sys.argv)==2 and 'static' in str(sys.argv[1]):   
        organize_analysis()
    else:
        print('try another command') 


# In[ ]:




