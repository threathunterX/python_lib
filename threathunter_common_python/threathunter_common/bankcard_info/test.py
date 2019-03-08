from bankcard_bin import get_issue_bank, get_card_type

if __name__ == "__main__":

    fp = open( "data/bank_bin_info.csv", "r" )
    lines = fp.readlines()
    fp.close()

    all_count = 0
    matched_count = 0
    not_matched_count = 0

    print "\n"
    for line in lines:
        if len( line.split( "," ) ) == 3:
            all_count += 1
            info = line.split( "," )

            if info[ 0 ].strip() == get_issue_bank( info[ 2 ].strip() ):
                if info[ 1 ].strip() == get_card_type( info[ 2 ].strip() ):
                    # print line.strip() + "\t\t\tMatched"
                    matched_count += 1
                else:
                    print line.strip() + "\t\t\tNot Matched"
                    not_matched_count += 1
            else:
                print line.strip() + "\t\t\tNot Matched"
                not_matched_count += 1

    print "\nAll records: \t\t" + str( all_count )
    print "Matched records: \t\t" + str( matched_count )
    print "Not Matched records: \t\t" + str( not_matched_count )
    print "\n"
