'''fetch Twitter by given Tweets IDs'''
from unicodedata import name
from pathlib import Path
import time,os
from dotenv import load_dotenv;
from typing import Optional, Union, Dict, List
import pandas as pd 
import logging
from tqdm import tqdm
root_dir = Path(__file__).parent.parent

load_dotenv(root_dir /".env")

logger = logging.getLogger(__name__)

import requests

class Tweets_Lookup:
    '''Wrapper for the Twitter API'''
    
    base_url = 'https://api.twitter.com/2/tweets'
    
    def __init__(self) -> None:
        api_key = os.getenv('TWITTER_API_KEY')
        self.headers = dict(Authorization=f'Bearer {api_key}')
        self.tweet_fields = [
        "attachments",
        "author_id",
        "context_annotations",
        "conversation_id",
        "created_at",
        "entities",
        "geo",
        "id",
        "in_reply_to_user_id",
        "lang",
    #    "non_public_metrics",
    #    "organic_metrics",
    #    "possibly_sensitive",
    #    "promoted_metrics",
        "public_metrics",
        "referenced_tweets",
        "reply_settings",
        "source",
        "text",
        "withheld"
    ]
        self.expansions = [
        "attachments.media_keys",
        "attachments.poll_ids",
        "author_id",
        "entities.mentions.username",
        "geo.place_id",
        "in_reply_to_user_id",
        "referenced_tweets.id",
        "referenced_tweets.id.author_id"
    ]
        self.media_fields =  [
        "alt_text",
        "duration_ms",
        "height",
        "media_key",
        "non_public_metrics",
        "organic_metrics",
        "preview_image_url",
        "promoted_metrics",
        "public_metrics",
        "type",
        "url",
        "variants",
        "width"
    ]
        self.user_fields =  [
        "created_at",
        "description",
        "entities",
        "id",
        "location",
        "name",
        "pinned_tweet_id",
        "profile_image_url",
        "protected",
        "public_metrics",
        "url",
        "username",
        "verified",
        "withheld"
    ]
        self.place_fields =  [
        "contained_within",
        "country",
        "country_code",
        "full_name",
        "geo",
        "id",
        "name",
        "place_type"
    ]
    
    def get_tweets(self, tweet_id: Union[int, List[int]]):
        # Make sure `tweet_id` is a list
        if isinstance(tweet_id, str) or isinstance(tweet_id, int):
            tweet_ids = [tweet_id]
        else:
            tweet_ids = tweet_id
        # Initialise parameters used in the GET requests
        params = {'tweet.fields': ','.join(self.tweet_fields),
                  'expansions': ','.join(self.expansions),
                  'place.fields': ','.join(self.place_fields),
                  'user.fields' : ','.join(self.user_fields),
                  'media.fields': ','.join(self.media_fields),
                  
                  }
        params = {'tweet.fields': ','.join(self.tweet_fields)}
        # Initialise dataframes
        tweet_df = pd.DataFrame()
        user_df = pd.DataFrame()
        media_df = pd.DataFrame()
        reply_df = pd.DataFrame()
        place_df = pd.DataFrame()
        meta_df = pd.DataFrame()
        for tweet_id in tqdm(tweet_ids):
            url = f"{self.base_url}/{tweet_id}"
            # Perform the GET request
            response = requests.get(url,
                                        params=params,
                                        headers=self.headers)
            while response.status_code in [429, 503]:
                logger.debug('Request limit reached. Waiting...')
                time.sleep(1)
                response = requests.get(url,
                                        params=params,
                                        headers=self.headers)
            if response.status_code != 200:
                    raise RuntimeError(f'[{response.status_code}] {response.text}')
            
            

            # Convert the response to a dict
            data_dict = response.json()
            
            # If the query returned errors, then raise an exception
            if 'data' not in data_dict and 'errors' in data_dict:
                error = data_dict['errors'][0]
                raise RuntimeError(error["detail"])
            # Tweet dataframe
            if 'data' in data_dict:
                df = pd.json_normalize(data_dict['data'])
                df.set_index('id', inplace=True)
                tweet_df = pd.concat((tweet_df, df))
            # User dataframe
            if 'includes' in data_dict and 'users' in data_dict['includes']:
                users = data_dict['includes']['users']
                df = pd.json_normalize(users)
                df.set_index('id', inplace=True)
                user_df = pd.concat((user_df, df))
            # Media dataframe
            if 'includes' in data_dict and 'media' in data_dict['includes']:
                media = data_dict['includes']['media']
                df = pd.json_normalize(media)
                df.set_index('media_key', inplace=True)
                media_df = pd.concat((media_df, df))
            # Reply dataframe
            if 'includes' in data_dict and 'tweets' in data_dict['includes']:
                replies = data_dict['includes']['tweets']
                df = pd.json_normalize(replies)
                df.set_index('id', inplace=True)
                reply_df = pd.concat((reply_df, df))
            # Places dataframe
            if 'includes' in data_dict and 'places' in data_dict['includes']:
                places = data_dict['includes']['places']
                df = pd.json_normalize(places)
                df.set_index('id', inplace=True)
                place_df = pd.concat((place_df, df))
        
            all_dfs = dict(tweets=tweet_df, authors=user_df, media=media_df,
                        replies=reply_df, places=place_df,
                        metadata=meta_df)
        return all_dfs
    

    


    