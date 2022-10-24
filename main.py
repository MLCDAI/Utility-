    
def twitter_lookup():
    from src.twitter_lookup import Tweets_Lookup
    crawler = Tweets_Lookup()
    result = crawler.get_tweets() # INPUT A list of Tweets ID
    return result


if __name__ == '__main__':
    result = twitter_lookup()


