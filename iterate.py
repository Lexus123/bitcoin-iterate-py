import sys
from pymongo import MongoClient
import json

# INIT ALL MONGODB STUFF
client = MongoClient('localhost', 27017)
db = client.iterate
col = db.utxo

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
    "blocktimestamp": firstutxo["blocktimestamp"],
    "totalamount": 0,
    "totalutxos": 0,
    "youngerthenday": 0,
    "youngerthenweek": 0,
    "youngerthenmonth": 0,
    "youngerthenthreemonth": 0,
    "youngerthensixmonth": 0,
    "youngerthenyear": 0,
    "youngerthenonehalfyear": 0,
    "youngerthentwoyear": 0,
    "youngerthenthreeyear": 0,
    "youngerthenfouryear": 0,
    "youngerthenfiveyear": 0,
    "youngerthensixyear": 0,
    "olderthensexyear": 0
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
fiveyear = 157680120
sixyear = 189216144

# LOOP THROUGH
for utxo in rawutxolist:
    if utxo["blockheight"] == previousheight:
        singleblockdata["totalamount"] = singleblockdata["totalamount"] + (utxo["utxosatoshis"] / 1000000)
        singleblockdata["totalutxos"] = singleblockdata["totalutxos"] + 1
        
        if utxo["utxoage"] <= day:
            singleblockdata["youngerthenday"] = singleblockdata["youngerthenday"] + 1
        elif day < utxo["utxoage"] <= week:
            singleblockdata["youngerthenweek"] = singleblockdata["youngerthenweek"] + 1
    else:
        utxodata.append(singleblockdata)

        singleblockdata = {
            "blockheight": utxo["blockheight"],
            "blocktimestamp": utxo["blocktimestamp"],
            "totalamount": (utxo["utxosatoshis"] / 1000000),
            "totalutxos": 1
        }

    previousheight = utxo["blockheight"]
    previoustimestamp = utxo["blocktimestamp"]

print(utxodata)

# INSERT ALL THE DATA AT ONCE
# col.insert_many(datalist)

# CLOSE PYMONGO CLIENT
client.close()