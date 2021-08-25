

"""
> Class: jsonConverter
> Author: Wyatt Marciniak
> |
-- Notes --
This class is used as a pipeline for jsonizing data objects. Data structures 
are re-formatted for compatibility with JSON data structures and written
to / read from < .json > files in memory. The packed / unpacked / data (original)
data sets are all accessable + access to directory & file path control, toggling of 
local returns and unpacking functionality to handle input parameters that are 
< .json > file paths (natively). This class was designed primarily 
for use in the Financial Analysis Platform app project (session cache handling + backend)
but it is built for general use cases as well.

"""

import json
import pandas as pd
from helper import sysout


class JsonConverter:

    packed = None
    unpacked = None

    def __init__(self):

        sysout('\n> JSON Converter ready...')


    ## Adjust data types to JSON Serializable types

    def clean(self,d=None):

        try:

            if isinstance(d, pd.DataFrame):
                return d.to_dict(orient='list')

            elif isinstance(d, dict):

                edict = {}

                for k, v in d.items():
                    edict[k] = self.clean(v)

                return edict.copy()

            else:
                return d

        except Exception:
            sysout('\n> [!] (Error) JSON Cleaning: Type < {} > could not be cleaned'.format(type(d)))
            return d


    ## Format object into JSON

    def pack(self,d=None):


        try: self.__packed = json.dumps(self.clean(d))

        except Exception:
            sysout('\n> [!] (Error) JSON Packing: Type < {} > could not be packed'.format(type(d)))
            return d

        else:
            return self.__packed


    ## Format object into Python (non-string object)

    def unpack(self,d=None):

            try: self.__unpacked = json.loads(d)

            except Exception:
                sysout('\n> [!] (Error) JSON Unpacking: Type < {} > could not be unpacked'.format(type(d)))
                return d

            else:
                return self.__unpacked.copy()
