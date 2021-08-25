
"""
>> Module: FinAppWorkerMain
>> Author: Wyatt Marciniak

>> USE:

    > Main Application Worker
    > Handles most cache/log/table operations
    > Handles all data request-fetch ops


                                                                """

from FinAppBaseWorker import *


class FinAppWorkerMain(FinAppBaseWorker):

    cn = 'FinAppWorkerMain'

    DEVOPS_NOFETCH =    False
    allow_updates =     True
    spyder =            None
    req_assets =        {}

    ticker_universe_locker_fp = 'finapp_assets/operation_depo/__universe_locker/'


    def __init__(self, devprint=True, allow_updates=True, maxproc=40):
        super().__init__()

        self.spyder = FinDataSpyder()
        self.spyder.check_update = allow_updates
        self.spyder.maxproc = maxproc

        self.PNOTICE = devprint
        self.allow_updates = allow_updates
        self.maxproc = maxproc

        self.__reset()

        sysout('\n>> [FINAPP WORKER READY]')


    def __reset(self):

        self.cache = {}
        self.log = kv2df(['ticker_id', 'ac', 'selected'], [[], [], []])
        self.curr = []
        self.req_assets = {}

    def __handle_fetching(self):

        self.fxn = self.getfxn()

        if isok(self.req_assets) and not self.req_assets == {} and any(
            [len(v) > 0 for k, v in self.req_assets.items()]):

            self.spyder.process_req(self.req_assets,update=self.do_update,maxproc=self.maxproc)
            temp = self.spyder.data.copy()

            if not isok(temp) or not isinstance(temp, dict) or len(temp.keys()) == 0 or temp == {}:

                self.error('[!] Fetch operation failed - check spyder (sub call object maintainer)')
                self.__reset()
                return False

            else:

                self.po('\n> [*] App request was VALID - updating session data accordingly')

                try:
                    self.cache = temp.copy()
                    self.log, self.curr = self.cache_to_log(self.cache.copy(), self.log.copy(), self.curr.copy(), [])
                    self.req_assets = {}

                except Exception:

                    self.error('Issue with setting attributes - post fetch')
                    self.__reset()
                    return False

                else: return True

        else:
            self.po('\n> [!] App request was INVALID - session data unchanged')
            self.__reset()
            return False


    ## > UI Core Backend Processes

    def genStyle_dataCond(self, log, cs=[], maxlen=None):

        self.fxn = self.getfxn()
        self.devout('RUN', '...')
        self.__reset()

        try:

            cs = self.chkpack(cs)
            sr = [x[0] for x in cs if isinstance(x, (list, tuple))]

            log = self.chkpack(log)
            logt = [x for x in list(log['ticker_id'])]




        except Exception:

            self.error('Bad parsing of selected rows from cs ({})'.format(type(cs)))
            return []

        else:

            if not isok(sr) or not isinstance(sr, list) or not all([isinstance(x, int) for x in sr]):

                self.error('Bad type ({}) for param sr'.format(type(sr)))
                return []

            elif not isok(maxlen) or not isinstance(maxlen, int) or maxlen <= 0:

                self.error(
                    'Unable to generate conditions for rows (bad maxlen type ({}) or value)'.format(type(maxlen)))
                return []

            else:

                try:

                    row_selected = [

                        {

                            'if': {'row_index': x},
                            'backgroundColor': 'rgb(196, 242, 200)'

                        } for x in range(maxlen) if x in sr and logt[x] in [x[1] for x in cs]]

                    row_unselected = [

                        {

                            'if': {'row_index': x},
                            'backgroundColor': 'rgb(247, 173, 173)'

                        } for x in range(maxlen) if not x in sr or not logt[x] in [x[1] for x in cs]]


                except Exception:

                    self.error('Failed to create selected/not conditional lists')
                    return []


                else:

                    self.po(' [*] OK')
                    return row_selected + row_unselected

    def update_cache(self, cnew=None):

        self.fxn = self.getfxn()
        self.devout('RUN', '...')

        if not isok(self.cache) or not isinstance(self.cache, dict) or self.cache == {}:
            self.error('Local cache: type ({}) or size is invalid'.format(type(self.cache)))
            return False

        elif not isok(cnew) or not isinstance(cnew, dict) or cnew == {}:
            self.error('Param (@cnew): type ({}) or size is invalid'.format(type(cnew)))
            return False

        else:

            try:

                for a, tl in cnew.items():

                    if a in self.cache.keys():

                        self.tkeys = [_tl.keys() for _a,_tl in self.cache.items() if _a == a]

                        for t in tl.keys():

                            if t in self.tkeys: self.cache[t].update(cnew[a][t])
                            else:               self.cache[a][t] = cnew[a][t]
                    else: self.cache[a] = cnew[a]

            except Exception:
                self.error('Updating nested cache (dict) was invalid')
                return False

            else:

                if not isok(self.cache) or not isinstance(self.cache, dict) or self.cache == {}:
                    self.error('Local cache: post-update type ({}) or size is invalid'.format(type(self.cache)))
                    return False

                else:
                    self.po(' [*] Cache Updated - OK')
                    return True

    def drop_cache_elems(self, cache=None, sel=None):

        self.fxn = self.getfxn()
        self.devout('RUN', '...')

        if not isok(cache) or not isinstance(cache, (dict, str)) or cache == {}:
            self.error('Param @cache: type ({}) or size is invalid'.format(type(cache)))
            return False

        elif not isok(sel) or not isinstance(sel, (list, str)) or sel == []:
            self.error('Param @sel: type ({}) or size is invalid'.format(type(sel)))
            return False

        else:

            cache = self.chkpack(cache)     # Should now be a dict
            sel = self.chkpack(sel)         # Should now be a list

            if not isok(cache) or not isok(sel):
                self.error('Bad unpacked type(s) for cache / sel :: (NONE)')
                return None

            # Else

            try:

                drop_t =        unique([x[1] for x in sel])
                self.cache =    {}

                for i,ca in enumerate(cache.keys()):
                    self.cache[ca] = {}

                    for t in cache[ca].keys():
                        if not t in drop_t: self.cache[ca][t] = cache[ca][t]

            except Exception:
                self.error('Cache (type {}) parsing invalid for @sel of type ({})'.format(type(cache), type(sel)))
                return False

            else:

                if not isok(self.cache) or not isinstance(self.cache, dict) or self.cache == {}:
                    self.error('New Cache post-update type ({}) or size is invalid'.format(type(self.cache)))
                    return False

                else:
                    self.po(' [*] Cache Updated - OK')
                    return True


    ## > Request-Fetch Handlers

    def fetchData_inapp(self, to_try=None, log=None, allow_updates=True):

        ### INPUT FROM APP MUST BE IN PROPER ORDER OR ELSE THIS WILL ERROR

        self.fxn = self.getfxn()
        self.devout('RUN', '...')
        self.__reset()

        if isok(allow_updates): self.do_update = allow_updates

        ## Dev option - Disallow in-app fetch requests (testing/debug - Default: False)

        if self.DEVOPS_NOFETCH:

            self.devout('DEVOP_NOFETCH', 'Fecthing is disallowed from within the app (dev options)')
            self.__reset()
            return False


        ## Checks to_try param

        elif not isok(to_try) or not isinstance(to_try, list) or len(to_try) < len(self.valid_assets):

            self.error('Bad type ({}) or value(s) for request trying - No effect'.format(type(to_try)))
            self.__reset()
            return False


        ## Checks log param

        elif (not isok(log) or not isinstance(log, (str, list))) or (
            isinstance(log, list) and len(to_try) < len(self.valid_assets)):

            self.error('Bad type ({}) or value(s) for request trying - No effect'.format(type(log)))
            self.__reset()
            return False


        ## Valid params

        else:

            if isok(allow_updates): self.spyder.check_update = allow_updates

            log = self.unpack(log)

            ## Iterate over inputs to try and parse assets for request handling

            for i, raw in enumerate(to_try):

                if isok(raw) and not raw == '':

                    self.po('\n   > {}:'.format(self.valid_assets[i]))

                    try:

                        """| Ticker filter: 4 chars (normal) or be fx (7 chars with '/')                       |
                           |> ALSO: skip processing if cache already holds ticker and updates are not occuring |"""

                        temp = [x for x in raw.split(',') if (
                            (not x == '' and len(x) in range(1, 5)) or '/' in x)
                                and not (x in list(log['ticker_id']) and not self.spyder.check_update)]

                    except Exception:

                        self.po(' 0* (error) valid requests found')

                    else:

                        self.req_assets[self.valid_assets[i]] = temp
                        self.po(' {} valid requests found: {}'.format(len(temp), temp))

            if self.__handle_fetching():
                self.po('\n>> [DONE - OK]')
                return True

            else:
                self.po('\n>> [DONE - FAIL]')
                return False

    def fetchData_base(self, allow_updates=True, maxproc=25):

        self.fxn = self.getfxn()
        self.devout('RUN', '...')
        self.__reset()

        if isok(allow_updates):  self.spyder.check_update = allow_updates
        if isok(maxproc):       self.spyder.maxproc = maxproc

        try:
            self.req_assets = {

                'etf': [v for k, v in self.sector_etfs.items()],
                'fx': [v for k, v in self.major_currencies.items()],
                'indices': [v for k, v in self.major_indices.items()]

            }

        except Exception:
            self.req_assets = {}
            self.error('Failed to create req_assets dict')

        else:

            if self.__handle_fetching():
                self.po('\n>> [DONE - OK]')
                return True

            else:
                self.po('\n>> [DONE - FAIL]')
                return False

    def fetchData_explicit(self, req_assets=None, allow_updates=True, maxproc=25):

        self.fxn = self.getfxn()
        self.devout('RUN', '...')
        self.__reset()

        if isok(allow_updates):
            self.spyder.check_update = allow_updates
            self.do_update = allow_updates

        if isok(maxproc):       self.spyder.maxproc = maxproc
        if isok(req_assets):    self.req_assets = req_assets.copy()

        valid_fetch = self.__handle_fetching()

        return valid_fetch

    def fetchData_fromFile(self, req_path=None, check_update=True, maxproc=25):

        self.fxn = self.getfxn()
        self.devout('RUN', '...')
        self.__reset()

        if not isok(req_path) or not isinstance(req_path, str):
            self.error('Request path missing or invalid')
            return False

        elif not os.path.isfile(req_path):
            self.error('Requested file does not exist')
            return False

        else:

            if isok(check_update):  self.spyder.check_update = check_update
            if isok(maxproc):       self.spyder.maxproc = maxproc

            ## Collect and parse request assets from file

            sysout('\n-----\n> Coming soon...\n-----\n')

            # valid_fetch = self.__handle_fetching()
            #
            # return valid_fetch
