
# IMPORTS
from sqlalchemy import create_engine
from sqlalchemy import Column, Integer, String
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
Base = declarative_base()

import pandas as pd

# Database Credentials
DB = 'mysql'
DB_URL = '127.0.0.1:3306'
DB_USER = 'root'
DB_PASSWORD = 'password'
DB_NAME = 'media_ocean'


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
session = Session()

list_movie_tv = []
for class_instance in session.query(Movietv).all():
    a = vars(class_instance)
    tmp = {}
    tmp['title'] = a['title']
    tmp['year'] = a['release_year']
    tmp['genres'] = a['genres']
    tmp['description'] = a['description']
    tmp['channel'] = a['channel']
    list_movie_tv.append(tmp)

list_movie_theater = []
for class_instance in session.query(Movietheaters).all():
    a = vars(class_instance)
    tmp = {}
    tmp['title'] = a['title']
    tmp['year'] = a['release_year']
    tmp['genres'] = a['genres']
    tmp['description'] = a['description']
    tmp['theater'] = a['theater']
    list_movie_theater.append(tmp)

session.close()

df_movie_tv = pd.DataFrame(list_movie_tv)
df_movie_theater = pd.DataFrame(list_movie_theater)


df_movie = df_movie_tv.merge(df_movie_theater, on='genres')
df_movie_genres_descending = df_movie.groupby(['genres']).size().sort_values(ascending=False)
df_movie_genres_top5 = df_movie_genres_descending[:5]
for genre in df_movie_genres_top5.index:
    df_genre_movie = df_movie[df_movie['genres'] == genre]
    df_genre_movie.to_csv(genre+'.csv')
