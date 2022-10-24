'''Translator classes'''

import requests
from requests.exceptions import ConnectionError
import numpy as np
from typing import Union, Sequence, List
from tqdm.auto import tqdm
import logging
from dotenv import load_dotenv; load_dotenv()
import os
import time


logger = logging.getLogger(__name__)


class GoogleTranslator:
    '''A wrapper for the Google Translation API'''
    base_url = 'https://translation.googleapis.com/language/translate/v2'

    def __init__(self):
        self.api_key = os.getenv('GOOGLE_API_KEY')

    def __call__(self,
                 text: Union[str, List[str]],
                 target_lang: str = 'en') -> Union[str, List[str]]:
        '''Translate text.

        Args:
            text (str or list of str): The text to translate.

        Returns:
            str or list of str: The translated English text.
        '''
        # Ensure that `text` is a list
        if isinstance(text, str):
            text = [text]

        logger.debug(f'Translating {len(text)} documents...')

        # Set up the POST request parameters, except the query
        data = dict(target=target_lang, format='text', key=self.api_key)

        # Split into chunks
        num_batches = (len(text) // 50) + 1
        batches = np.array_split(text, num_batches)
        logger.debug(f'Split the texts into {num_batches} batches.')

        # Translate every batch and collect all the translations
        all_translations = []
        for batch_idx, batch in enumerate(batches):
            logger.debug(f'Translating batch {batch_idx}...')

            # Update the query to the batch
            data['q'] = batch

            # Perform the POST request
            while True:
                try:
                    response = requests.post(self.base_url, data=data)
                    logger.debug(f'POST request resulted in status code '
                                 f'{response.status_code}.')
                    break
                except ConnectionError:
                    logger.debug('Connection error encountered. Trying again.')
                    continue

            # If we have reached the translation quota, then wait and try again
            while response.status_code in [403, 503]:

                if response.status_code == 403:
                    logger.debug('Translation quota reached. Waiting...')
                elif response.status_code == 503:
                    logger.debug('Service currently unavailable. Waiting...')

                time.sleep(10)
                response = requests.post(self.base_url, data=data)
                logger.debug(f'POST request resulted in status code '
                             f'{response.status_code}.')

            # Convert the response into a dict
            data_dict = response.json()

            # Give informative error if the request failed
            if 'data' not in data_dict and 'error' in data_dict:
                error = data_dict['error']
                status_code = error.get('code')
                message = error.get('message')
                exception_message = ''
                if status_code is not None:
                    exception_message += f'[{status_code}] '
                if message is not None:
                    exception_message += message
                raise RuntimeError(exception_message)

            # Pull out the translations
            translations = [dct['translatedText']
                            for dct in data_dict['data']['translations']]

            # Update `all_translations` with the new translations
            all_translations.extend(translations)
            logger.debug(f'Have now translated {len(all_translations)} '
                         f'documents')

        # If a string was inputted, make sure to also output a string
        if len(all_translations) == 1:
            all_translations = all_translations[0]

        # Return the list of all the translations
        return all_translations
