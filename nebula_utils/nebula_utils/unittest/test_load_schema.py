# -*- coding: utf-8 -*-

from nebula_utils.persist_utils import utils

def test_load_schema():
    db_path = './'
    assert utils.name2_schema is None
    utils.load_schema(db_path)
    print utils.name2_schema
    assert utils.name2_schema is not None
    
def test_load_header_version():
    db_path = './'
    assert utils.Header_Version is None
    utils.load_header_version(db_path)
    print utils.Header_Version
    assert utils.Header_Version is not None