import sys
from pymongo import MongoClient
import json

# INIT ALL MONGODB STUFF
client = MongoClient('localhost', 27017)
db = client.iterate
col = db.utxo

# AGE LISTS
zero_day = [0, 86400]
day_week = [86400, 604800]
week_month = [604800, 2628002]
month_threemonth = [2628002, 7884006]
threemonth_sixmonth = [7884006, 15768012]
sixmonth_year = [15768012, 31536024]
year_onehalfyear = [31536024, 47304036]
onehalfyear_twoyear = [47304036, 63072048]
twoyear_threeyear = [63072048, 94608072]
threeyear_fouryear = [94608072, 126144096]
fouryear_fiveyear = [126144096, 157680120]
fiveyear_sixyear = [157680120, 189216144]

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

# APPEND A UTXO TO TRIGGER THE END OF LOOP
rawutxolist.append({'blockheight': 0, 'blocktimestamp': 0, 'utxoage': 0, 'utxosatoshis': 0})

# DECLARE THE LIST
utxodata = []

# DICT FOR COUNTED DATA
singleblockdata = {
    "blockheight": firstutxo["blockheight"],
    "blocktimestamp": firstutxo["blocktimestamp"],
    "totalamount": 0,
    "totalutxos": 0
}

# THE FIRST BLOCKHEIGHT AND TIMESTAMP OR THE FROM PREVIOUS UTXO
previousheight = firstutxo["blockheight"]
previoustimestamp = firstutxo["blocktimestamp"]

for utxo in rawutxolist:
    if utxo["blockheight"] == previousheight:
        singleblockdata["totalamount"] = singleblockdata["totalamount"] + (utxo["utxosatoshis"] / 1000000)
        singleblockdata["totalutxos"] = singleblockdata["totalutxos"] + 1
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