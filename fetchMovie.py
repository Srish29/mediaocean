# IMPORTS
from sqlalchemy import create_engine
from sqlalchemy import Column, Integer, String
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
Base = declarative_base()

import datetime
import requests
import json
HEADERS = {'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.116 Safari/537.36'}


# Database Credentials
DB = 'mysql'
DB_URL = '127.0.0.1:3306'
DB_USER = 'root'
DB_PASSWORD = 'password'
DB_NAME = 'media_ocean'

# Vendor API Credentials
API_SECRET = '97jc9rpjcna945pqn4t4hw9m'


class Movietheaters(Base):
    __tablename__ = 'movie_theaters'

    id = Column(Integer, primary_key=True)
    title = Column(String(500))
    release_year = Column(Integer)
    genres = Column(String(100))
    description = Column(String(500))
    theater = Column(String(500))

    def __init__(self, title, release_year, genres, description, theater):
        self.title = title
        self.release_year = release_year
        self.genres = genres
        self.description = description
        self.theater = theater

class Movietv(Base):
    __tablename__ = 'movie_tv'

    id = Column(Integer, primary_key=True)
    title = Column(String(500))
    release_year = Column(Integer)
    genres = Column(String(100))
    description = Column(String(500))
    channel = Column(String(500))

    def __init__(self, title, release_year, genres, description, channel):
        self.title = title
        self.release_year = release_year
        self.genres = genres
        self.description = description
        self.channel = channel

# connect and create database
db_engine = create_engine(DB+"://"+DB_USER+":"+DB_PASSWORD+"@"+DB_URL+"/"+DB_NAME)
Session = sessionmaker(bind=db_engine)
Base.metadata.drop_all(db_engine)
Base.metadata.create_all(db_engine)


ZIPCODE = '78701'
START_DATE = (datetime.datetime.now()).strftime('%Y-%m-%d')
url_movie_theater = 'http://data.tmsapi.com/v1.1/movies/showings?startDate='+START_DATE+'&zip='+ZIPCODE+'&api_key='+API_SECRET

LINEUP_ID = 'USA-TX42500-X'
START_DATE_TIME = (datetime.datetime.now()).strftime('%Y-%m-%dT%I:%MZ') #2021-03-12T04:30Z
url_movie_tv = 'http://data.tmsapi.com/v1.1/movies/airings?lineupId='+LINEUP_ID+'&startDateTime='+START_DATE_TIME+'&api_key='+API_SECRET


session = Session()
# FETCH movie theaters
response = requests.get(url_movie_theater, headers=HEADERS)
jsonData = json.loads(response.text)
for i in jsonData:
    title = i['title']
    if 'genres' in i:
        genres = i['genres'][0]
    if 'releaseYear'  in i:
        release_year = i['releaseYear']
    if 'shortDescription'  in i:
        description = i['shortDescription']
    if 'showtimes'  in i:
        theater = i['showtimes'][0]['theatre']['name']
    movie = Movietheaters(title, release_year, genres, description, theater)
    session.add(movie)


# FETCH movie tv
response = requests.get(url_movie_tv, headers=HEADERS)
jsonData = json.loads(response.text)
for i in jsonData:
    if 'program' in i:
        program = i['program'] 
        title = program['title']
        if 'genres' in program:
            genres = program['genres'][0]
        if 'releaseYear' in program:
            release_year = program['releaseYear']
        if 'shortDescription' in program:
            description = program['shortDescription']
        if 'channels' in i:
            channel = ', '.join(i['channels'])
        movie = Movietv(title, release_year, genres, description, channel)
        session.add(movie)
session.commit()
session.close()
