'''
collect tweets and insert collected tweets into the dbf table
'''



import twitter
import dbf




FieldList = ["TweetID","Text","TweetTime","Lat","Lon","UserID","UserName"]
# field list


'''
OAUTH
'''

CONSUMER_KEY = "fill your own CONSUMER_KEY "
CONSUMER_SECRET = "fill your own CONSUMER_SECRET"
OAUTH_TOKEN = "fill your own OAUTH_TOKEN"
OATH_TOKEN_SECRET = "fill your own OATH_TOKEN_SECRET"



auth = twitter.oauth.OAuth(OAUTH_TOKEN,OATH_TOKEN_SECRET,CONSUMER_KEY,CONSUMER_SECRET)
twitter_api = twitter.Twitter(auth=auth)


def GetTweetByContent():

    '''
    search tweet by content
    q defines the content
    geo defines the location
    '''
    try:
        tweet_id_list = []   
        tweet_text_list = []
        tweet_time_list = []
        tweet_lat_list = []
        tweet_log_list = []         
        tweet_user_id = []
        tweet_user_name = []
        tweet_user_location = []
        
         
        q= "Chicago"
        count = 100
        lang = "en"

        geocode = "33.7550,-84.3900,20mi"     

        search_results = twitter_api.search.tweets(count=count,lang=lang,geocode=geocode,q=q)

        statuses = search_results["statuses"]
        i = 0
        for statue in statuses:
            print ("---------------" + str(i))
            print ("ID      | " + str(statue["id"]))
            print ("Text    | " + str(statue["text"].encode('gbk','ignore')))
            print ("Time    | " + str(statue["created_at"]))
            print ("Geo     | " + str(statue["geo"]))
            print ("User ID | " + str(statue["user"]["id"]))
            print ("Name    | " + str(statue["user"]["screen_name"].encode('gbk','ignore')))
            print ("Location| " + str(statue["user"]["location"].encode('gbk','ignore')))
            tweet_id_list.append(str(statue["id"]))
            tweet_text_list.append(str(statue["text"].encode('gbk','ignore')))
            tweet_time_list.append(str(statue["created_at"]))  
            tweet_user_id.append(str(statue["user"]["id"]))
            tweet_user_name.append(str(statue["user"]["screen_name"].encode('gbk','ignore')))
            tweet_user_location.append(str(statue["user"]["location"].encode('gbk','ignore')))  
            if statue["geo"]!=None:
                print ("Lat     | " + str(statue["geo"]["coordinates"][0]))
                print ("Lon     | " + str(statue["geo"]["coordinates"][1]))
                tweet_lat_list.append(str(statue["geo"]["coordinates"][0]))
                tweet_log_list.append(str(statue["geo"]["coordinates"][1]))
            else:
                tweet_lat_list.append("-9999.99")
                tweet_log_list.append("-9999.99")
            i = i+1
        return tweet_id_list,tweet_text_list,tweet_time_list,tweet_lat_list,tweet_log_list, tweet_user_id, tweet_user_name , tweet_user_location
        
 
    except :
        print ("get twitter error")

TweetIDList,TweetTextList,TweetTimeList,TweetLatList,TweetLonList,UserIDList,UserNameList,UserLocList = GetTweetByContent()

'''
try using dbf library to export tweets into the dbf table
'''
try:
    table = dbf.Table("Tweet.dbf")
    table.open()
    
    for i in range(len(TweetIDList)):
        try:
            datum = (TweetIDList[i],TweetTextList[i],TweetTimeList[i],TweetLatList[i],TweetLonList[i],UserIDList[i],UserNameList[i],UserLocList[i])
            table.append(datum)
        except UnicodeDecodeError:
            continue
        except :
            print ("insert table error")
    
    table.close()
except :
    print ("update table error")
