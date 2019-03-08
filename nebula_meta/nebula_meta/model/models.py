#!/usr/bin/env python
# -*- coding: utf-8 -*-

from sqlalchemy import Column
from sqlalchemy.types import CHAR, Integer, String
from sqlalchemy.ext.declarative import declarative_base

__author__ = 'lw'

BaseModel = declarative_base()


class User(BaseModel):
    __tablename__ = 'eventmeta'

    id = Column(Integer, primary_key=True)
    name = Column(CHAR(100)) 
    email = Column(CHAR(100))
    password = Column(CHAR(40))
    is_super = Column(Integer)
    attrs = Column(CHAR(3000))
    last_login = Column(Integer,) #上次登陆时间
    date_joined = Column(Integer,)    #注册时间
    is_active = Column(Integer)#是否激活，允许登陆
    
    
def init_db():
    BaseModel.metadata.create_all()

def drop_db():
    BaseModel.metadata.drop_all()



    
if __name__ == "__main__":
    init_db()