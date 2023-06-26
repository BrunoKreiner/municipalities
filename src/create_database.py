from sqlalchemy import Column, Integer, String, ForeignKey, create_engine, Float
from sqlalchemy.orm import sessionmaker, relationship, declarative_base

Base = declarative_base()

class Municipality(Base):
    __tablename__ = 'municipality'

    id = Column(Integer, primary_key=True)
    name = Column(String)
    country = Column(String)
    email = Column(String)
    population = Column(Integer)
    pop_d = Column(Float)
    status = Column(String)
    pages = relationship("Page", back_populates="municipality")

class Page(Base):
    __tablename__ = 'pages'

    id = Column(Integer, primary_key=True)
    url = Column(String)
    content = Column(String)
    municipality_id = Column(Integer, ForeignKey('municipality.id'), nullable=True)
    municipality = relationship("Municipality", back_populates="pages")

class Link(Base):
    __tablename__ = 'links'

    id = Column(Integer, primary_key=True)
    source_id = Column(Integer, ForeignKey('pages.id'))
    destination_id = Column(Integer, ForeignKey('pages.id'))
    depth = Column(Integer)
    municipality_url = Column(String)

engine = create_engine('sqlite:///municipalities.db')
Base.metadata.create_all(engine)
Session = sessionmaker(bind=engine)

session = Session()

"""import csv
from collections import namedtuple
import os
import os.path as path
import pandas as pd

class CSVDatabase:
    def __init__(self):

        if path.exists('municipality.csv'):
            self.municipality = pd.read_csv('municipality.csv', header=0)
        else:
            columns = ['id', 'name', 'country', 'email', 'population', 'pop_d', 'status']
            self.municipality = pd.DataFrame(columns=columns)

        if path.exists('../data/page.csv'):
            self.punicipality = pd.read_csv('page.csv', header=0)
        else:
            columns = ['id', 'url', 'content', 'municipality_id']
            self.page = pd.DataFrame(columns=columns)
        
        if path.exists('../data/link.csv'):
            self.link = pd.read_csv('link.csv', header=0)
        else:
            columns = ['id', 'source_id', 'destination_id', 'depth', 'municipality_url']
            self.link = pd.DataFrame(columns=columns)
    
    def add_municipality(self, municipalities):
        self.municipality = self.municipality.append(pd.DataFrame(municipalities), ignore_index=True)

    def add_page(self, pages):
        self.page = self.page.append(pd.DataFrame(pages), ignore_index=True)

    def add_link(self, result, page, subpage, depth):
        self.link = self.link.append(pd.DataFrame(), ignore_index=True)
        link = self.Link(
            id=self.link_id,
            source_id=page.id,
            destination_id=subpage.id,
            depth=depth,
            municipality_url=result['url'],
        )
        self.link_writer.writerow(link)
        self.link_id += 1
        return link

    def close_files(self):
        # Close the CSV files
        self.municipality_file.close()
        self.page_file.close()
        self.link_file.close()
        
csv_db = CSVDatabase()
        municipalities = []
        pages = []
        links = []
        for row, result in zip(random_rows.itertuples(), results):
            municipality = {
                "id":row.index,
                "name":row.municipality,
                "country":row.country,
                "email":row.email,
                "population":row.population,
                "pop_d":row.pop_d,
                "status":row.status,
            }
            municipalities.append(municipality)        
            page = {
                "id":self.page_id,
                "url":result['url'],
                "content":str(result['content']),
                "municipality_id":municipality.id,
            }
            pages.append(page)
            for sublink in result['sublinks']:
                subpage = {
                    "id":self.page_id,
                    "url":result['url'],
                    "content":str(result['content']),
                    "municipality_id":municipality.id,
                }
                pages.append(subpage)
                link = {
                    "id":self.link_id,
                    "source_id":page.id,
                    "destination_id":subpage.id,
                    "depth":depth,
                    "municipality_url":result['url'],
                }
                links.append(link)
        
        csv_db.add_municipality(municipalities)
        csv_db.add_page(pages)
        csv_db.add_link(links)

        csv_db.save()"""

# Usage