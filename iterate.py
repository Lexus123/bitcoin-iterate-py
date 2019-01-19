import sys
from pymongo import MongoClient
import json
from datetime import datetime

# INIT ALL MONGODB STUFF
client = MongoClient('localhost', 27017)
db = client.iterate
col = db.utxotenplus

# CREATE LIST FOR ALL THE JSON
rawutxolist = []

# GET DATA FROM PIPE
bitcoiniterateoutput = sys.stdin

# LOOP THROUGH DATA
for utxo in bitcoiniterateoutput:
    # TURN EACH LINE FROM DATA INTO JSON AND APPEND IT TO DATA LIST
    rawutxolist.append(json.loads(utxo))

# GET THE FIRST UTXO FROM RAW DATA
firstutxo = rawutxolist[0]

# THE FIRST BLOCKHEIGHT AND TIMESTAMP OR THE FROM PREVIOUS UTXO
previousheight = firstutxo["blockheight"]
previoustimestamp = firstutxo["blocktimestamp"]

# APPEND A UTXO TO TRIGGER THE END OF LOOP
rawutxolist.append({'blockheight': 0, 'blocktimestamp': 0, 'utxoage': 0, 'utxosatoshis': 0})

# DECLARE THE LIST
utxodata = []

# DICT FOR COUNTED DATA
singleblockdata = {
    "blockheight": firstutxo["blockheight"],
    "blocktimestamp": datetime.utcfromtimestamp(firstutxo["blocktimestamp"]).strftime('%Y-%m-%d %H:%M:%S'),
    "totalamount": 0,
    "totalutxos": 0,
    "nuytday": 0,
    "nuytweek": 0,
    "nuytmonth": 0,
    "nuytthreemonth": 0,
    "nuytsixmonth": 0,
    "nuytyear": 0,
    "nuytonehalfyear": 0,
    "nuyttwoyear": 0,
    "nuytthreeyear": 0,
    "nuytfouryear": 0,
    "nuytsixyear": 0,
    "nuyteightyear": 0,
    "nuyttenyear": 0,
    "nuottenyear": 0,
    "puytday": 0,
    "puytweek": 0,
    "puytmonth": 0,
    "puytthreemonth": 0,
    "puytsixmonth": 0,
    "puytyear": 0,
    "puytonehalfyear": 0,
    "puyttwoyear": 0,
    "puytthreeyear": 0,
    "puytfouryear": 0,
    "puytsixyear": 0,
    "puyteightyear": 0,
    "puyttenyear": 0,
    "puottenyear": 0,
    "ncytday": 0,
    "ncytweek": 0,
    "ncytmonth": 0,
    "ncytthreemonth": 0,
    "ncytsixmonth": 0,
    "ncytyear": 0,
    "ncytonehalfyear": 0,
    "ncyttwoyear": 0,
    "ncytthreeyear": 0,
    "ncytfouryear": 0,
    "ncytsixyear": 0,
    "ncyteightyear": 0,
    "ncyttenyear": 0,
    "ncottenyear": 0,
    "pcytday": 0,
    "pcytweek": 0,
    "pcytmonth": 0,
    "pcytthreemonth": 0,
    "pcytsixmonth": 0,
    "pcytyear": 0,
    "pcytonehalfyear": 0,
    "pcyttwoyear": 0,
    "pcytthreeyear": 0,
    "pcytfouryear": 0,
    "pcytsixyear": 0,
    "pcyteightyear": 0,
    "pcyttenyear": 0,
    "pcottenyear": 0
}

# AGE LISTS SECONDS
day = 86400
week = 604800
month = 2628002
threemonth = 7884006
sixmonth = 15768012
year = 31536024
onehalfyear = 47304036
twoyear = 63072048
threeyear = 94608072
fouryear = 126144096
sixyear = 189216144
eightyear = 252288192
tenyear = 315360240

print("JUST BEFORE PYTHON LOOP: ", datetime.now())

# LOOP THROUGH
for utxo in rawutxolist:
    if utxo["blockheight"] == previousheight:
        singleblockdata["totalamount"] = singleblockdata["totalamount"] + (utxo["utxosatoshis"] / 100000000)
        singleblockdata["totalutxos"] = singleblockdata["totalutxos"] + 1
        
        if utxo["utxoage"] <= day:
            singleblockdata["nuytday"] = singleblockdata["nuytday"] + 1
            singleblockdata["ncytday"] = singleblockdata["ncytday"] + (utxo["utxosatoshis"] / 100000000)
        elif day < utxo["utxoage"] <= week:
            singleblockdata["nuytweek"] = singleblockdata["nuytweek"] + 1
            singleblockdata["ncytweek"] = singleblockdata["ncytweek"] + (utxo["utxosatoshis"] / 100000000)
        elif week < utxo["utxoage"] <= month:
            singleblockdata["nuytmonth"] = singleblockdata["nuytmonth"] + 1
            singleblockdata["ncytmonth"] = singleblockdata["ncytmonth"] + (utxo["utxosatoshis"] / 100000000)
        elif month < utxo["utxoage"] <= threemonth:
            singleblockdata["nuytthreemonth"] = singleblockdata["nuytthreemonth"] + 1
            singleblockdata["ncytthreemonth"] = singleblockdata["ncytthreemonth"] + (utxo["utxosatoshis"] / 100000000)
        elif threemonth < utxo["utxoage"] <= sixmonth:
            singleblockdata["nuytsixmonth"] = singleblockdata["nuytsixmonth"] + 1
            singleblockdata["ncytsixmonth"] = singleblockdata["ncytsixmonth"] + (utxo["utxosatoshis"] / 100000000)
        elif sixmonth < utxo["utxoage"] <= year:
            singleblockdata["nuytyear"] = singleblockdata["nuytyear"] + 1
            singleblockdata["ncytyear"] = singleblockdata["ncytyear"] + (utxo["utxosatoshis"] / 100000000)
        elif year < utxo["utxoage"] <= onehalfyear:
            singleblockdata["nuytonehalfyear"] = singleblockdata["nuytonehalfyear"] + 1
            singleblockdata["ncytonehalfyear"] = singleblockdata["ncytonehalfyear"] + (utxo["utxosatoshis"] / 100000000)
        elif onehalfyear < utxo["utxoage"] <= twoyear:
            singleblockdata["nuyttwoyear"] = singleblockdata["nuyttwoyear"] + 1
            singleblockdata["ncyttwoyear"] = singleblockdata["ncyttwoyear"] + (utxo["utxosatoshis"] / 100000000)
        elif twoyear < utxo["utxoage"] <= threeyear:
            singleblockdata["nuytthreeyear"] = singleblockdata["nuytthreeyear"] + 1
            singleblockdata["ncytthreeyear"] = singleblockdata["ncytthreeyear"] + (utxo["utxosatoshis"] / 100000000)
        elif threeyear < utxo["utxoage"] <= fouryear:
            singleblockdata["nuytfouryear"] = singleblockdata["nuytfouryear"] + 1
            singleblockdata["ncytfouryear"] = singleblockdata["ncytfouryear"] + (utxo["utxosatoshis"] / 100000000)
        elif fouryear < utxo["utxoage"] <= sixyear:
            singleblockdata["nuytsixyear"] = singleblockdata["nuytsixyear"] + 1
            singleblockdata["ncytsixyear"] = singleblockdata["ncytsixyear"] + (utxo["utxosatoshis"] / 100000000)
        elif sixyear < utxo["utxoage"] <= eightyear:
            singleblockdata["nuyteightyear"] = singleblockdata["nuyteightyear"] + 1
            singleblockdata["ncyteightyear"] = singleblockdata["ncyteightyear"] + (utxo["utxosatoshis"] / 100000000)
        elif eightyear < utxo["utxoage"] <= tenyear:
            singleblockdata["nuyttenyear"] = singleblockdata["nuyttenyear"] + 1
            singleblockdata["ncyttenyear"] = singleblockdata["ncyttenyear"] + (utxo["utxosatoshis"] / 100000000)
        elif tenyear < utxo["utxoage"]:
            singleblockdata["nuottenyear"] = singleblockdata["nuottenyear"] + 1
            singleblockdata["ncottenyear"] = singleblockdata["ncottenyear"] + (utxo["utxosatoshis"] / 100000000)
    else:
        # SOME CALC FOR PERCENTAGE OF TOTAL

        totalutxos = singleblockdata["totalutxos"] / 100
        totalcoins = singleblockdata["totalamount"] / 100

        singleblockdata["puytday"] = singleblockdata["nuytday"] / totalutxos
        singleblockdata["puytweek"] = singleblockdata["nuytweek"] / totalutxos
        singleblockdata["puytmonth"] = singleblockdata["nuytmonth"] / totalutxos
        singleblockdata["puytthreemonth"] = singleblockdata["nuytthreemonth"] / totalutxos
        singleblockdata["puytsixmonth"] = singleblockdata["nuytsixmonth"] / totalutxos
        singleblockdata["puytyear"] = singleblockdata["nuytyear"] / totalutxos
        singleblockdata["puytonehalfyear"] = singleblockdata["nuytonehalfyear"] / totalutxos
        singleblockdata["puyttwoyear"] = singleblockdata["nuyttwoyear"] / totalutxos
        singleblockdata["puytthreeyear"] = singleblockdata["nuytthreeyear"] / totalutxos
        singleblockdata["puytfouryear"] = singleblockdata["nuytfouryear"] / totalutxos
        singleblockdata["puytsixyear"] = singleblockdata["nuytsixyear"] / totalutxos
        singleblockdata["puyteightyear"] = singleblockdata["nuyteightyear"] / totalutxos
        singleblockdata["puyttenyear"] = singleblockdata["nuyttenyear"] / totalutxos
        singleblockdata["puottenyear"] = singleblockdata["nuottenyear"] / totalutxos

        singleblockdata["pcytday"] = singleblockdata["ncytday"] / totalcoins
        singleblockdata["pcytweek"] = singleblockdata["ncytweek"] / totalcoins
        singleblockdata["pcytmonth"] = singleblockdata["ncytmonth"] / totalcoins
        singleblockdata["pcytthreemonth"] = singleblockdata["ncytthreemonth"] / totalcoins
        singleblockdata["pcytsixmonth"] = singleblockdata["ncytsixmonth"] / totalcoins
        singleblockdata["pcytyear"] = singleblockdata["ncytyear"] / totalcoins
        singleblockdata["pcytonehalfyear"] = singleblockdata["ncytonehalfyear"] / totalcoins
        singleblockdata["pcyttwoyear"] = singleblockdata["ncyttwoyear"] / totalcoins
        singleblockdata["pcytthreeyear"] = singleblockdata["ncytthreeyear"] / totalcoins
        singleblockdata["pcytfouryear"] = singleblockdata["ncytfouryear"] / totalcoins
        singleblockdata["pcytsixyear"] = singleblockdata["ncytsixyear"] / totalcoins
        singleblockdata["pcyteightyear"] = singleblockdata["ncyteightyear"] / totalcoins
        singleblockdata["pcyttenyear"] = singleblockdata["ncyttenyear"] / totalcoins
        singleblockdata["pcottenyear"] = singleblockdata["ncottenyear"] / totalcoins

        utxodata.append(singleblockdata)

        # RESET BLOCK DATA
        singleblockdata = {
            "blockheight": utxo["blockheight"],
            "blocktimestamp": datetime.utcfromtimestamp(utxo["blocktimestamp"]).strftime('%Y-%m-%d %H:%M:%S'),
            "totalamount": (utxo["utxosatoshis"] / 100000000),
            "totalutxos": 1,
            "nuytday": 0,
            "nuytweek": 0,
            "nuytmonth": 0,
            "nuytthreemonth": 0,
            "nuytsixmonth": 0,
            "nuytyear": 0,
            "nuytonehalfyear": 0,
            "nuyttwoyear": 0,
            "nuytthreeyear": 0,
            "nuytfouryear": 0,
            "nuytsixyear": 0,
            "nuyteightyear": 0,
            "nuyttenyear": 0,
            "nuottenyear": 0,
            "ncytday": 0,
            "ncytweek": 0,
            "ncytmonth": 0,
            "ncytthreemonth": 0,
            "ncytsixmonth": 0,
            "ncytyear": 0,
            "ncytonehalfyear": 0,
            "ncyttwoyear": 0,
            "ncytthreeyear": 0,
            "ncytfouryear": 0,
            "ncytsixyear": 0,
            "ncyteightyear": 0,
            "ncyttenyear": 0,
            "ncottenyear": 0
        }

        if utxo["utxoage"] <= day:
            singleblockdata["nuytday"] = singleblockdata["nuytday"] + 1
            singleblockdata["ncytday"] = singleblockdata["ncytday"] + (utxo["utxosatoshis"] / 100000000)
        elif day < utxo["utxoage"] <= week:
            singleblockdata["nuytweek"] = singleblockdata["nuytweek"] + 1
            singleblockdata["ncytweek"] = singleblockdata["ncytweek"] + (utxo["utxosatoshis"] / 100000000)
        elif week < utxo["utxoage"] <= month:
            singleblockdata["nuytmonth"] = singleblockdata["nuytmonth"] + 1
            singleblockdata["ncytmonth"] = singleblockdata["ncytmonth"] + (utxo["utxosatoshis"] / 100000000)
        elif month < utxo["utxoage"] <= threemonth:
            singleblockdata["nuytthreemonth"] = singleblockdata["nuytthreemonth"] + 1
            singleblockdata["ncytthreemonth"] = singleblockdata["ncytthreemonth"] + (utxo["utxosatoshis"] / 100000000)
        elif threemonth < utxo["utxoage"] <= sixmonth:
            singleblockdata["nuytsixmonth"] = singleblockdata["nuytsixmonth"] + 1
            singleblockdata["ncytsixmonth"] = singleblockdata["ncytsixmonth"] + (utxo["utxosatoshis"] / 100000000)
        elif sixmonth < utxo["utxoage"] <= year:
            singleblockdata["nuytyear"] = singleblockdata["nuytyear"] + 1
            singleblockdata["ncytyear"] = singleblockdata["ncytyear"] + (utxo["utxosatoshis"] / 100000000)
        elif year < utxo["utxoage"] <= onehalfyear:
            singleblockdata["nuytonehalfyear"] = singleblockdata["nuytonehalfyear"] + 1
            singleblockdata["ncytonehalfyear"] = singleblockdata["ncytonehalfyear"] + (utxo["utxosatoshis"] / 100000000)
        elif onehalfyear < utxo["utxoage"] <= twoyear:
            singleblockdata["nuyttwoyear"] = singleblockdata["nuyttwoyear"] + 1
            singleblockdata["ncyttwoyear"] = singleblockdata["ncyttwoyear"] + (utxo["utxosatoshis"] / 100000000)
        elif twoyear < utxo["utxoage"] <= threeyear:
            singleblockdata["nuytthreeyear"] = singleblockdata["nuytthreeyear"] + 1
            singleblockdata["ncytthreeyear"] = singleblockdata["ncytthreeyear"] + (utxo["utxosatoshis"] / 100000000)
        elif threeyear < utxo["utxoage"] <= fouryear:
            singleblockdata["nuytfouryear"] = singleblockdata["nuytfouryear"] + 1
            singleblockdata["ncytfouryear"] = singleblockdata["ncytfouryear"] + (utxo["utxosatoshis"] / 100000000)
        elif fouryear < utxo["utxoage"] <= sixyear:
            singleblockdata["nuytsixyear"] = singleblockdata["nuytsixyear"] + 1
            singleblockdata["ncytsixyear"] = singleblockdata["ncytsixyear"] + (utxo["utxosatoshis"] / 100000000)
        elif sixyear < utxo["utxoage"] <= eightyear:
            singleblockdata["nuyteightyear"] = singleblockdata["nuyteightyear"] + 1
            singleblockdata["ncyteightyear"] = singleblockdata["ncyteightyear"] + (utxo["utxosatoshis"] / 100000000)
        elif eightyear < utxo["utxoage"] <= tenyear:
            singleblockdata["nuyttenyear"] = singleblockdata["nuyttenyear"] + 1
            singleblockdata["ncyttenyear"] = singleblockdata["ncyttenyear"] + (utxo["utxosatoshis"] / 100000000)
        elif tenyear < utxo["utxoage"]:
            singleblockdata["nuottenyear"] = singleblockdata["nuottenyear"] + 1
            singleblockdata["ncottenyear"] = singleblockdata["ncottenyear"] + (utxo["utxosatoshis"] / 100000000)

    previousheight = utxo["blockheight"]
    previoustimestamp = utxo["blocktimestamp"]

print("START DATA INSERT AT: ", datetime.now())

# INSERT ALL THE DATA AT ONCE
col.insert_many(utxodata)

print("UTXO DATA INSERTED AT: ", datetime.now())

print("LAST BLOCK HEIGHT: ", utxodata[-1]["blockheight"])

# CLOSE PYMONGO CLIENT
client.close()