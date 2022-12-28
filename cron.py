# mark is_first as false in the 2nd run 

#
# import pyodbc

#  matrix:cIMqGmO1wEjKJYtIFa8Z@tcp(zion.cemxm0xwsaao.ap-south-1.rds.amazonaws.com:3306)/laxmi?charset=utf8mb4&collation=utf8mb4_unicode_ci&parseTime=True

# Trusted Connection to Named Instance
# connection = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER=.\SQL2K19;DATABASE=SampleDB;Trusted_Connection=yes;')
# connection = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER=.\SQL2K19;DATABASE=SampleDB;Trusted_Connection=yes;')

# cursor=connection.cursor()
# cursor.execute("SELECT @@VERSION as version")



#####
# moralis api to fetch data from blockchaian

from numpy import insert
from moralis import evm_api
from datetime import datetime

api_key = "g5DtsCLrxyGdfZNMIWN2N1yKhzz07g7T9afsDlQ7DLcarQBQUxjmhR9vIXH2uJKp"
params = {
    "address": "0x97CaBeb44e1951ED0d7806A62A3525022691e498", 
    "chain": "polygon", 
    "format": "decimal", 
    "limit": 100, 
    "cursor": "", 
    "normalizeMetadata": True, 
}

result = evm_api.nft.get_nft_owners(
    api_key=api_key,
    params=params,
)
print("fetching NFT data from blockchain")
nft_data = result.keys()

# print(nft_data)
for i in range(result['total']):
    print(result['result'][i]['owner_of'], " -> ", result['result'][i]['token_id'])
    # print(result['result'])
    # print(" ")

print("parsing NFT data")
nft_data_parsed = {}
for i in range(result['total']):
    nft_data_parsed[result['result'][i]['token_id']] = result['result'][i]['owner_of']

for i,j in nft_data_parsed.items():
    print("_--->", i,j) #token -> wallet


###
# export PYTHONPATH=.
import pymysql

# host = 'zion.cemxm0xwsaao.ap-south-1.rds.amazonaws.com'
# user = 'matrix'
# password = 'cIMqGmO1wEjKJYtIFa8Z'
# database = 'laxmi-dev'

# connection = pymysql.connect(host, user, password, database)
# with connection:
#     cur = connection.cursor()
#     cur.execute("SELECT VERSION()")
#     version = cur.fetchone()
#     print("Database version: {} ".format(version[0]))

# db = pymysql.connect(host, user, password)
# # you have cursor instance here
# cursor = db.cursor()
# cursor.execute("select version()")
# #now you will get the version of MYSQL you have selected on instance
# data = cursor.fetchone()

# import pymysql

# Connect to the database
connection = pymysql.connect(host='zionverse-v8.cemxm0xwsaao.ap-south-1.rds.amazonaws.com',
                             user='admin',
                             password='4325964d67f44324f78c744a41e259a1',
                             database='production',
                             charset='utf8mb4',
                             cursorclass=pymysql.cursors.DictCursor)

cursor = connection.cursor()
cursor.execute("SELECT VERSION()")
data = cursor.fetchone()
print ("Database version : %s " % data)

# if(exist in onchain):
#     check if consistent w wallet:
#         nothing
#     else:
#         if(exist in metamask):
#             update progress and flag older
#         else:
#             do nothing as the newwer user didnt come to the eplatform
    
# else: #not exist in onchain
#     check if in metamask:
#         update
#     else:
#         nothing

def fetch_address_metamask(addr):
    query = f'''SELECT * FROM MetamaskWallet Where wallet_address = "{addr}" AND state = 1;''' # state =1, datetime
    print(query)
    cursor.execute(query)
    return cursor.fetchone()

def fetch_address_metamask(addr):
    query = f'''SELECT * FROM MetamaskWallet Where wallet_address = "{addr}";'''
    # print(query)
    cursor.execute(query)
    return cursor.fetchone()

def fetch_address_onchain(addr):
    query = f'''SELECT * FROM CollectionOnchain Where wallet_address = "{addr}";'''
    # print(query)
    cursor.execute(query)
    return cursor.fetchone() # or fetchall?

def inMetamask(addr):
    query = f'''COUNT * FROM MetamaskWallet Where wallet_address = "{addr}";'''
    cursor.execute(query)
    return cursor.fetchone()

def fetch_address_onchain_byid(tokenid):
    query = f'''SELECT * FROM CollectionOnchain Where item_id = "{tokenid}";'''
    cursor.execute(query)
    return cursor.fetchone()

def  insertToCollectionOnchain(addr, tokenid, collectionid, userid):
    now = datetime.now()
    formatted_date = now.strftime('%Y-%m-%d %H:%M:%S')
    query = f'INSERT INTO CollectionOnchain VALUES ({collectionid}, {tokenid}, {userid}, "{addr}", 1, "{formatted_date}","{formatted_date}");'

    print("insert query", addr, tokenid, collectionid, userid)

    cursor.execute(query)
    return # check commit?

def getuserid(addr):
    query = f'''SELECT user_id FROM MetamaskWallet WHERE wallet_address = "{addr}";'''
    cursor.execute(query)
    return cursor.fetchone() # check commit?

def getcollectionid(tokenid): 
    query = f'''SELECT collection_id FROM Collection WHERE item_id= "{tokenid}";'''
    cursor.execute(query)
    return cursor.fetchone() # check commit?

def flag(tokenid):
    query = f'''UPDATE CollectionOnchain SET verified = 0 WHERE item_id= "{tokenid}";'''
    cursor.execute(query)
    
    print("flagged- ", tokenid)
    return 

# def getUserId(addr):

# def insertToOnchain():



# main script 
# print(fetch_address_metamask("0xb6e266fec99a4092c5afbee3900b95079ec81fe7"))


# if present in metamsk -> either not present in onchain, or consistent, or non consistent

is_first = False
print("main loop")

for tokenid, addr in nft_data_parsed.items(): #multiple nft in one wallet
    
    if(is_first):
        #capture state
        metamask_entry =  fetch_address_metamask(addr) # fetch using addr
        print("metamask entry", metamask_entry)
        if(metamask_entry is None):
            continue
        else:
            print("inserting")
            userid = getuserid(addr)
            collectionid = getcollectionid(tokenid)
            # print("user collection id -> " , userid, collectionid)

            insertToCollectionOnchain(addr, tokenid, collectionid["collection_id"], userid["user_id"]) # multiple nfts
            #insert to gME   

    else:
        #check state
        onchain_entry = fetch_address_onchain_byid(tokenid) # fetch using token
        if(onchain_entry and onchain_entry['metamask_wallet'] == addr): ## is and ==
            continue
        elif(onchain_entry):
            flag(tokenid)
            print("new address found was- ",  addr)
is_first = False





    ####################################

    # inM = inMetamask(add)
    # metamask_entry =  fetch_address_metamask(add)
    
    # if(metamask_entry is None):
    #     continue # if new user comes to platform, he'd make acc 
    # else:
    #     onchain_entry = fetch_address_onchain(add)

    #     if(onchain_entry is None):
    #         #insert ## only first time
    #         userid= getUserId()

    #         insertToOnchain()
            
    #     elif(onchain_entry['item_id' == tokenid]):
    #         # update progress?? no
    #         updateprogress()

    #     else:






    

#Lets select the data from above added table

# sql = '''select * from CollectionOnchain'''
# sql = ''' INSERT INTO CollectionOnchain (collection_id, item_id, user_id) VALUES (1, 2, 3)'''
sql = ''' INSERT INTO CollectionOnchain VALUES (1, 2, 3, "0x", 4, "df","sd");'''

print("excuting update query")
# cursor.execute(sql)
# print(cursor.fetchone())
# print(cursor.fetchmany(5))

print("fetching the records from the table")
sql2 = '''select * from CollectionOnchain'''
print("collectiononchain data")
cursor.execute(sql2)
# print(cursor.fetchone())
print(cursor.fetchmany(10))

connection.commit()

