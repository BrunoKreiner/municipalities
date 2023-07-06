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
    content = Column(String)
    url = Column(String)
    scripts = Column(String)
    css_files = Column(String)
    sublinks_count = Column(Integer)
    pages = relationship("Page", back_populates="municipality")

class Page(Base):
    __tablename__ = 'pages'

    id = Column(Integer, primary_key=True)
    url = Column(String)
    content = Column(String)
    depth = Column(Integer)
    municipality_id = Column(Integer, ForeignKey('municipality.id'), nullable=True)
    municipality = relationship("Municipality", back_populates="pages")
    source_id = Column(Integer, ForeignKey('pages.id'))


engine = create_engine('sqlite:///../data/municipalities.db')
Base.metadata.create_all(engine)
Session = sessionmaker(bind=engine)

session = Session()