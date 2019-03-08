#!/usr/bin/env python
# -*- encoding: utf-8 -*-

import sys
import os

reload( sys )
sys.setdefaultencoding( "utf8" )

bank_bin_info = {}

def get_source_data():

    fp = open( os.path.join( os.path.dirname( __file__), "data/bank_bin_info.csv" ), "r" )
    lines = fp.readlines()
    fp.close()
    for line in lines:
        if len( line.split( "," ) ) == 3:
            line_splited = line.split( "," )
            bank_bin_info[ str( line_splited[ 2 ].strip() ) ] = ( 
                    line_splited[ 0 ].strip(), line_splited[ 1 ].strip() )

def get_issue_bank( card_no ):

    issue_bank = bank_bin_info.get( str( card_no ).strip(), None )

    if issue_bank == None:
        return issue_bank
    else:
        return issue_bank[ 0 ].strip()

def get_card_type( card_no ):

    card_type = bank_bin_info.get( str( card_no ).strip(), None )

    if card_type == None:
        return card_type
    else:
        return card_type[ 1 ].strip()

get_source_data()
