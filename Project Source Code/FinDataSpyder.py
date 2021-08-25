
"""
> Module:   FinDataSypder
> Author:   Wyatt Marciniak
...


"""


from ClassNoticeHandler import *
from RequestEngine import *
import os


def ptime(t,rf=3):

    rns = [1,60,60,24,7,4.5,11.5]
    rts = ['sec','min','hr','day(s)','week(s)','month(s)','years(s)']

    i = 0
    div_by = 1
    x = 60

    while i < len(rns) and t > x:

        if i == 0:

            div_by = x
            x *= rns[i]
            i += 1

        else:

            if t < x * rns[min(i+1,len(rns)-1)]:
                break

            else:

                i += 1
                x *= rns[i]
                div_by = x

    i = min(i,(len(rts)-1))
    return np.round((t / div_by), rf), rts[i]



class FinDataSpyder(ClassNoticeHandler):

    """

    [CLASS]: FinDataSpyder

    Def: Main Financial Data Application API controller


    > TODO: ** Currently working for market data and intraday data sets (Expansion - WIP)

    > Takes in asset request dict { asset_class : [list, of, symbols], ... }

    > Creates 'robjs' (Request Objects) which are just 'sets' or dictionaries to be passed and altered for return

    > Passes list (@self.robjs) to a @SpyderWeb object (locally instantiated)

    ...


    """


    cn = 'FDSpyder'


    # region > Member Variables (Attributes)


    data = {}               # Resulting data set from processing


    log_file = None         # Local copy of current log_file
    update_file = None      # ... update_file

    requested_assets = {}   # Dictionary of asset (key) / ticker list (value) pairs
    # __curr_ac = None        # [PRIVATE] Current requested asset class
    # __curr_ticker = None    # [PRIVATE] Current requested ticker (of requested asset class)


    robjs = []              # 'Request' objects (list of dictionaries, 1 per ticker| asset/ticker tags are elems)
    fobjs = []              # 'Fetch' objects (extended robjs list, 1 per (every) request path)| all robj is contained)

    __robj = {}             # [PRIVATE] Temp attribute used as variable carrying current request object being converted

    check_update = False    # Defaults to false - will only pull existing data (no auto-update - option change is valid)
    spyderweb = None        # MT request engine - Defined in constructor
    maxproc = 25            # Max number of threads to run in MT (Maximum number of processes) - mutable

    __reqpaths = {}         # Dictionary of paths for data fetching (/.data_paths)


    __op_cache = {}         # Captures last exrtaction process results (for testing)
    __all_urls = {}         # Captures all parsed urls (for easy access - ** NOT CURRENTLY USED)


    # Path URLs

    __warehouse_data_fp = 'warehouse/{}/{}.xlsx'
    __log_fp = 'warehouse/logfile.xlsx'
    __upd_fp = 'warehouse/updatefile.xlsx'
    __reqpaths_fp = 'spyder_assets/config/.data_paths'

    __intraday_archive_fp_base = 'warehouse/__archive/__intraday_mkt_data/{}.intradaydata'


    # Time limiter (required wait time between data fetch requests (per data type, all assets - can ALL be unique)

    __time_delays = {

        'keymetrics':   900,            # Default: 15 min (This is also for asset news - pulled with keymetrics)
        'mkt_d':        3600 * 2,       # Default: 2 hr
        'mkt_w':        86400 * 5,      # Default: 5 days
        'mkt_m':        86400 * 25,     # Default: 25 days
        'intraday':     86400 * 1,      # Default: 1 day
        'div':          86400 * 90,     # Default: 90 days (3 months or 1 quarter approx.)


        ## Implementation in progress...

        'etfexposure':  86400 * 5,      # Default: 5 days
        'comps':        86400 * 5,      # Default: 5 days
        'etfinfo':      86400 * 5,      # Default: 5 days
        'etfbreakdown': 86400 * 5,      # Default: 5 days


        ## Market related metrics

        'rates':    86400 * 1,
        'yc_ust':   86400 * 1,
        'yc_corp':  86400 * 1,
        'yc_aaa':   86400 * 1,
        'yc_junk':  86400 * 1

    }

    FLAG_BAD_ASSET = False

    # endregion


    ### Constructor - Handles all file creation and loading, as well as creation of SpyderWeb fetching object

    def __init__(self):

        sysout('\n> [FinDataSypder] Spinning up...')


        # [I] Handle creation of backend directories

        if not os.path.isdir('warehouse'):
            self.__create_warehouse()


        else:

            if not os.path.isfile(self.__log_fp):
                self.__create_logfile()


            if not os.path.isfile(self.__upd_fp):
                self.__create_updfile()


        # [II] Handle local backend import

        try:

            self.log_file = self.load_log_file()
            self.update_file = self.load_update_file()
            self.__get_reqpaths()

        except Exception:
            self.error('Bad instantiation - Check backed resources\n')

        else:

            if not isok(self.log_file) or not isok(self.update_file) or not isok(self.__reqpaths):
                self.error('Bad instantiation - Check backed resources\n')

            else:

                self.spyderweb = SpyderWeb()
                sysout('\n> [FinDataSpyder] Ready\n')


    ### Main call for class

    def process_req(self, req_assets=None,update=None,maxproc=25):

        """| Handles requested asset(s) operations - Load / update / fetch |"""

        self.fxn = self.getfxn()


        # [0] Initial pre-processing param clean up

        if isok(req_assets):            self.requested_assets = req_assets
        if isok(update):                self.check_update =     update
        if not isok(self.__reqpaths):   self.__get_reqpaths()


        self.robjs =    []      # Holds fetch request objects (to be fed to Spyder)
        self.__robj =   None    # Holds current request object (url parsers call this attribute and update it directly)
        self.data =     {}      # Holds resulting data set from the current process request



        # [1] Check if valid requests are present (can be given as param or set externally '.requested_assets')

        if not (isok(self.requested_assets) and isinstance(self.requested_assets, dict) and all(
            [isinstance(v, list) for k,v in self.requested_assets.items()])):

            self.error('No asset requests found (They muct be given as a parameter or explicitly set in obj)')


        # [2] Valid requests found

        else:

            sysout('\n> Requested assets are valid - moving on to processing...')

            if isok(maxproc) and isinstance(maxproc,int) and maxproc >= 1: self.maxproc = maxproc


            # [3] Iterate over request objs to determine actions needed

            for ac,tickers in self.requested_assets.items():

                if not ac in self.data.keys() or self.data == {}:
                    self.data[ac] = {}


                for t in tickers:

                    sysout('\n> Current target: {} - {}'.format(ac, t))

                    temp_t = t.lower()
                    if ac == 'fx': temp_t = temp_t.replace('/', '_')    # Adjust for FX file paths

                    self.__robj = None                                  # Reset current robj to operate on


                    ## Check if asset logged (try updates if allowed) -- else fetch full profile

                    if self.__check_logfile(t):

                        sysout(' | Loading current profile from memory...')

                        self.data[ac][t] = xb2py(self.__warehouse_data_fp.format(ac.lower(),temp_t))
                        self.__check_updfile(ac, t, 'ticker_id')


                    else:

                        sysout(' | Fetching full profile...'.format(ac,t))

                        self.data[ac][t] =  {}
                        self.__format_req_asset(ac, t, is_update=False)
                        # self.__robj =


                        if self.__robj['asset_id'] in ['equity', 'etf']:

                            for dtype in ['keymetrics','div']:
                                self.__build_url(dtype)


                        if self.__robj['asset_id'] in ['equity','etf','fx','indices']:

                            for dtype in ['mkt_d','mkt_w','mkt_m','intraday']:
                                self.__build_url(dtype)

                        self.robjs.append(self.__robj)


            # [4] Run final conversion to fetch objects, then feed to spyder

            self.__handle_fetch_objects()



    # region > Handlers for backend make/read/write/clean


    def __create_warehouse(self):

        sysout('\n> Building warehouse backend...')


        # Base directory

        os.mkdir('warehouse')


        # Warehouse sub-directories

        for x in ['econ_data', 'equity', 'etf', 'fx', 'indices', 'options']:
            os.mkdir('warehouse/{}'.format(x))

        self.__create_logfile()
        self.__create_updfile()

        sysout(' | Complete')

    def __get_reqpaths(self):
        self.fxn = self.getfxn()

        try:
            conn = open(self.__reqpaths_fp, 'r')
            rp_raw = [x.strip('\n') for x in conn.readlines()]
            conn.close()

            self.__reqpaths = mkdict(
                k=[x.split(',')[0] for x in rp_raw],
                v=[x.split(',')[1] for x in rp_raw]
            )

        except Exception:
            self.error('Bad request (local) for req path /.data_paths - Fetching will not work if unfixed')


    def load_log_file(self):

        try: temp = xb2py(self.__log_fp)

        except Exception:
            self.fxn = self.getfxn()
            self.error('Bad request (local) for logfile')
            return None

        else:
            self.log_file = temp
            return temp

    def __create_logfile(self):

        sysout('\n> Creating Log File... ')

        chead = ['ticker_id', 'ac']

        self.__logfile = kv2df(k=chead, v=[[] for i in range(len(chead))])
        self.__logfile.to_excel(self.__log_fp, 'log', index=False)

        sysout(' | Complete')

    def __update_log_file(self):

        try: self.log_file.copy().to_excel(self.__log_fp,sheet_name='log',index=False,header=True)

        except Exception:
            self.fxn = self.getfxn()
            self.error('Bad write attempt for log file')
            return False

        else:
            return True


    def load_update_file(self):

        try:
            temp = xb2py(self.__upd_fp)

        except Exception:
            self.fxn = self.getfxn()
            self.error('Bad request (local) for update file')
            return None

        else:
            self.update_file = temp
            return temp

    def __create_updfile(self):

        sysout('\n> Creating Update Tracking File...')

        chead_base = ['ticker_id','keymetrics', 'mkt_d', 'mkt_w', 'mkt_m','intraday','div']

        self.__updatefile = kv2df(k=chead_base, v=[[] for i in range(len(chead_base))])
        self.__updatefile.to_excel(self.__upd_fp,'update_log',index=False)

        sysout(' | Complete')

    def __update_upd_file(self):

        try:
            self.update_file.copy().to_excel(self.__upd_fp, sheet_name='update_log',index=False,header=True)

        except Exception:
            self.fxn = self.getfxn()
            self.error('Bad write attempt for update file')
            return False

        return True



    ## TODO [3] Clean log and update files (scan directories in /warehouse - re-write on change (dropped, added, etc..)

    def __clean_log_file(self):

        print(1)

    def __clean_update_file(self):

        print(1)


    # endregion


    # region > Handlers for data extraction (from responses)


    def __h_clean_dates(self, dates, dform='%b-%d-%y %I:%M%p'):

        """| Handles date formatting for asset news data sourcing |"""

        # [0] Initialize lists for the date (char/unix) results

        ds, du = [], []

        for i, x in enumerate(dates):

            ## [1] Handle same-date missing tags (entries on the same day only post a date for the first in the list)

            if x.find(' ') == -1 and i > 0:
                dates[i] = str(dates[i - 1].split(' ')[0] + ' ' + x)
                x = dates[i]

            ## [2] If strptime fails, the entry is a sub-entry (add parent date)

            try:
                temp = dtt.strptime(x, dform)

            except Exception:

                ds.append(str(x))
                du.append(-1)

            else:

                ds.append(str(temp))
                du.append(int(temp.timestamp()))

        ## Local class return

        return ds, du

    def __handle_news(self, resp):

        """| Collects news headlines from parent and/or unique urls -> appends to .assetnews files |"""

        self.fxn = self.getfxn()

        if not isok(resp):

            sysout('\n> No resp with source html given - News not retrieved')

        else:

            source_html = BeautifulSoup(resp['resp'].content, features='lxml')

            try:

                news_table = pd.read_html(str(source_html.select('table.fullview-news-outer')[0]))[0]
                raw_date = list(news_table[0])

                for i, v in enumerate(raw_date):

                    if (v.find(' ') == -1 or len(v) < 10) and i > 0:
                        raw_date[i] = str(raw_date[(i - 1)].split(' ')[0] + ' ' + v)

                news_table[2] = [x.get('href') for x in source_html.select('a.tab-link-news')]
                news_table[0], news_table[3] = self.__h_clean_dates(raw_date)
                news_table.columns = ['datec', 'article_title', 'article_url', 'dateu']

            except Exception:

                self.error('Asset news retrieval failed - no effect')
                return


            else:

                temp_fp = 'warehouse/asset_news/{}.news'.format(resp['ticker_id'])

                ## Try to pull old file (to append unique values only) - else, normal 'a+' append/create operation

                try: temp = pd.read_csv(temp_fp, sep='|')
                except Exception:
                    news_table.to_csv(temp_fp, header=True, index=None, sep='|', mode='a+')

                else:

                    news_table = news_table.append(temp)
                    news_table = news_table.drop_duplicates('article_title')

                    news_table.to_csv(temp_fp, header=True, index=None, sep='|', mode='w+')

                # sysout('\n> Asset news \t| OK')

    def __handle_keymetrics(self, resp):

        """| Handles data extraction for key metrics data (as well as asset news - chained here) |"""

        self.fxn = self.getfxn()

        resp = sorted(resp, key=lambda x: x['req_id'].split('-')[-1])


        df_m = []
        df_v = []
        result = None

        # [1] Extract data from url 1 (Stockrow)

        try:
            temp = [x for x in resp if x['req_id'].split('-')[-1] == 'keymetrics_2'][0]

        except Exception:

            self.error('Keymetrics_2 not found in given responses - No effect')
            return None

        else:

            try:

                ## Extract key metric data

                raw_data = temp['resp'].json()


                df_m += [k for k in raw_data.keys()][:-2]
                df_v += [v for k, v in raw_data.items()][:-2]


            except Exception:

                self.error('Keymetrics_2 - issue converting response to df - No effect')
                return None


            else:

                # [2] Extract data from url 2 (finviz) & handle news source data (collected when keymetrics are fetched)

                temp = [x for x in resp if x['req_id'].split('-')[-1] == 'keymetrics_1'][0]



                # Extract key metric data

                try:

                    raw_data = BeautifulSoup(temp['resp'].content, features='lxml')

                    df_m += [x.get_text() for x in raw_data.select('.snapshot-td2-cp')]
                    df_v += conv_s2n([x.get_text() for x in raw_data.select('.snapshot-td2')],['AMC','AMO','BMO','BMC'])


                except Exception:

                    self.error('Keymetrics_1 - issue(s) converting response to df - No effect')
                    return None


                else:

                    ## Extract asset news

                    self.__handle_news(temp)



                    ## Create & return DF

                    try:
                        return kv2df(

                            k = ['metrics','values'],
                            v = [df_m,df_v]
                        )


                    except Exception:

                        self.error('Bad DF build - returning None')
                        return None

    def __h_create_datec(self, dates):

        ## If single date, put into list (as int) else create list from dates list (of ints)

        if not isinstance(dates, list):
            d = [int(dates)]

        else:
            d = [int(x) for x in dates]

        ## Return list of formatted dates --> datec

        return [

            dtt.strptime(
                time.ctime(x),
                '%a %b %d %H:%M:%S %Y').strftime('%Y-%m-%d_%H:%M:%S')
            for x in d]

    def __handle_mktdata(self, resp):

        """| Handle extraction of market data (OHLCVA) from responses |"""

        self.fxn = self.getfxn()

        try: r = resp['resp'].json()
        except Exception:
            self.error('Mkt data response was invalid (error on resp.json()) - No effect')
            return None

        else:

            try:

                dateu = r['chart']['result'][0]['timestamp']        # unix (int) date num
                datec = self.__h_create_datec(dateu)                # char date elem

                raw = r['chart']['result'][0]['indicators']         # Raw contents
                adj = raw['adjclose'][0]['adjclose']                # Adjusted Close
                prc = raw['quote'][0]                               # OHLCV data

                keys = ['dateu', 'datec', 'open', 'high', 'low', 'close', 'volume', 'adjusted']
                vals = [dateu, datec, np.array(prc['open']), np.array(prc['high']),
                        np.array(prc['low']), np.array(prc['close']), np.array(prc['volume']),
                        np.array(adj)]

            except Exception:
                self.error('Mkt data extraction failed (error in parsing resp.json()) - No effect')
                return None

            else:
                return pd.DataFrame(dict(zip(keys,vals)))

    def __handle_intraday(self,resp):

        """| Handles extracting intrdday data from responses |"""

        self.fxn = self.getfxn()


        resp = sorted(resp, key=lambda x: x['req_id'].split('-')[-1])

        df_data = []

        df_ret = None


        ## Iterate over intraday response elements - try to extract data & append to @df_data

        for i,curr_resp in enumerate(resp):


            # [1] Extract currrent response data

            try:

                r = curr_resp['resp'].json()

                dateu = r['chart']['result'][0]['timestamp']
                datec = self.__h_create_datec(dateu)

                raw = r['chart']['result'][0]['indicators']['quote'][0]

                keys = ['dateu', 'datec', 'open', 'high', 'low', 'close', 'volume']
                vals = [dateu, datec,
                        np.array(raw['open']),
                        np.array(raw['high']),
                        np.array(raw['low']),
                        np.array(raw['close']),
                        np.array(raw['volume'])]

            except Exception:

                self.error('Intraday data extraction ({}/{}) failed - Not added'.format(i,len(resp)))

            else:

                df_data.append(dict(zip(keys, vals)))


        ## Check list of data and if valid - create DF and return (else return None)

        if len(df_data) > 0:

            df_ret = pd.concat([pd.DataFrame(x) for x in df_data],copy=False,ignore_index=True).sort_values(
                by=['dateu'],ascending=True)

            return df_ret

        else:
            return None

    def __handle_dividend(self, resp):

        """| Handle extraction of dividend data from responses |"""

        self.fxn = self.getfxn()

        try:
            r = resp['resp'].json()


        except Exception:
            self.error('Dividend response was invalid (error on resp.json()) - No effect')
            return None


        else:

            try:
                raw = r['chart']['result'][0]['events']['dividends']  # ['splits']


            except Exception:

                # self.error('Dividend extraction failed (error in parsing resp.json()) - No effect')
                return None


            else:


                keys = ['dateu', 'datec', 'amount']

                values = [
                    np.array([raw[x]['date'] for x in sorted(raw)]),
                    self.__h_create_datec([raw[x]['date'] for x in sorted(raw)]),
                    np.array([raw[x]['amount'] for x in sorted(raw)])
                ]

                to_return = pd.DataFrame(dict(zip(keys,values)))
                to_return = to_return.sort_values(by=['dateu'],ascending=True)


                return to_return


    # endregion


    # region > Handlers for processing requests


    ## Handles log / upd file searching for assets at runtime (* See main processor and handlers)

    def __check_logfile(self,t):

        """| Check if current requested asset (@t) is in the logfile |"""

        if len(self.log_file.index) == 0: return False
        else:

            self.log_file.index = self.log_file['ticker_id']
            targ_isin = inList(t, list(self.log_file.index))

            return targ_isin['isin']

    def __check_updfile(self, ac, targ, df_index):

        """| Check update file for last update timestamp to see if another is possible at call time |"""


        # Initial check to allow updates

        if not self.check_update:
            sysout('| Update checker is off - No new data will be added')
            return

        else:
            sysout(' | Checking for updates...\n-----')

            self.update_file.index = self.update_file[df_index]


            ## Iterate over key:value pairs to check only data sets in the profile (asset specific)

            for dtype, dvalue in self.data[ac][targ].items():


                last_update = int(self.update_file.at[targ, dtype])

                if last_update == -1:
                    sysout('\n    > Update DENIED: [{}] Data NA (OK)'.format(dtype))
                    continue


                time_diff =     int(dtt.today().timestamp()) - last_update
                time_check =    self.__time_delays[dtype]
                temp_d0 =       None

                if time_diff < time_check:

                    sysout('\n    > Update DENIED: ', dtype, ' [Last: {} / Time remaing: {}]'.format(
                        str(dtt.utcfromtimestamp(int(last_update))), ptime(int(time_check-time_diff))))

                    continue


                else:


                    ## Filter - If last update and current try are BOTH in closed market

                    if not self.chk_market_open() and not self.chk_market_open(last_update):


                        ## Filter (lev 2) measure difference in times for closed-closed market update attempt (curr: 4 hr)

                        if time_diff < (60*60*4):

                            sysout('\n    > Update DENIED: {} [Last: {} / Next: {}]'.format(dtype,
                                str(dtt.utcfromtimestamp(int(last_update))),ptime(int(60 * 60 * 4))))

                            continue



                    ## SURVIVED ALL FILTERS -- UPDATE IS VALID


                    sysout('\n    > Update available: ', dtype, ' [Last: {} / Next: {}]'.format(
                        str(dtt.utcfromtimestamp(int(last_update))), ptime(time_check)))


                    if not dtype == 'keymetrics':   temp_d0 = list(self.data[ac][targ][dtype]['dateu'])[-1]
                    if not isok(self.__robj):       self.__format_req_asset(ac, targ, is_update=True)

                    self.__build_url(dtype, set_d0=temp_d0)


            ## If a request object was created (1 or more updates are available), add the final robj to the list to fetch

            if isok(self.__robj):

                self.robjs.append(self.__robj)
                sysout('\n    > Update data robjs created accordingly\n-----\n')

            else:
                sysout('\n    > No updates are currently available - loading only\n-----\n')



    ### Update check helpers (Public use for checking market open/close as well)

    def is_between(self, x, lb, ub):

        try:

            lb, ub = (lb, ub) if lb < ub else (ub, lb)

            if x < lb or x > ub:

                return False

            else:

                return True

        except Exception:

            return None

    def chk_market_open(self, tchk=None, clock=False):

        cdt =   dtt.utcfromtimestamp(int(tchk)) if isok(tchk) else dtt.today()
        ct =    float('{}{}.{}'.format(cdt.hour, cdt.minute, cdt.second))


        # If weekday (Mon-Friday == 0 : 5) and time <= 9:30 to time <= 4:00 (16:00)

        is_open = cdt.weekday() in range(6) and self.is_between(ct, 930, 1600)

        ## Clock output is full set for timing

        if clock:

            tt_open, tt_close = -1, -1

            if is_open:

                tt_close = np.round(1600 - ct, 2)

            else:

                tt_open = np.round((2359 - ct) + 930, 2) if ct > 930 else np.round(930 - ct, 2)

            return {

                'is_open': is_open,
                'time_string': cdt.ctime(),
                'tt_close': tt_close,
                'tt_open': tt_open
            }

        else:

            return is_open



    ## Handles creating request objects from requested asset input (Initial format (dict), ticker_url adj & build urls)

    def __format_req_asset(self,ac,t,is_update):

        """| Create robj objects - create base dictionary from current requested asset |"""


        self.__robj = {

            'asset_id': ac.lower(),
            'ticker_id': t.lower(),
            'ticker_url': t.lower(),
            'req_id': '{}_{}'.format(ac.lower(), t.lower()),
            'req_dt': dtt.today(),
            'req_urls': {},
            'is_update':is_update
        }

        self.__url_adjust_ticker()

    def __url_adjust_ticker(self):

        """| Updates current robj object's ticker for url parsing according to asset type (if needed) |"""


        # Case 1: FX

        if self.__robj['asset_id'] == 'fx':

            x = self.__robj['ticker_url'].upper()


            ## If already parsed (adjusted) - do nothing

            if not x.find('=X') == -1:
                return

            else:

                x = x.split('/')

                if x[0] == 'USD':
                    self.__robj['ticker_url'] = '{}=x'.format(x[1])

                else:
                    self.__robj['ticker_url'] = '{}{}=x'.format(x[0],x[1])



        # Case 2: Indices (SP500, DJIA, etc...)

        elif self.__robj['asset_id'] == 'indices':


            ## If already parsed (adjusted) -- do nothing

            if self.__robj['ticker_url'][0] == '^':
                return

            else:
                self.__robj['ticker_url'] = '^{}'.format(self.__robj['ticker_url'].lower())



        ## Not needed

        else:
            return

    def __build_url(self,targ,set_d0=None,set_d1=None):

        """| Handles url parsing - @set_d0 & @set_d1 are used by the update checker (if given overwrites defaults) |"""

        self.fxn = self.getfxn()


        try:

            # [0] Setting start day (d0)

            if isok(set_d0):

                self.d0 = set_d0

            else:

                if targ == 'intraday':

                    self.d0 = int(dtt.today().timestamp() - (86400 * 29))


                elif targ in ['mkt_d','mkt_w','mkt_m','div','keymetrics']:

                    self.d0 = int(time.mktime(dtt.strptime('1970-01-01 00:00:00.00','%Y-%m-%d %H:%M:%S.%f').timetuple()))


                else:

                    sysout('\n> {} data type currently WIP - exiting with no effect'.format(targ))
                    return



            # [1] Setting end day (d1) - Should never be used (for current framework - here for expansion purposes)

            if isok(set_d1):

                self.d1 = set_d1


            else:

                self.d1 = int(dtt.today().timestamp())



            # [2] Setting data frequency (used if applicable)

            if targ == 'intraday':

                self.freq = '1m'


            elif targ in ['div','keymetrics']:

                self.freq = '1d'


            elif targ in ['mkt_d','mkt_w','mkt_m']:

                temp_freq = targ.split('_')[-1]

                if temp_freq == 'd': self.freq = '1d'
                elif temp_freq == 'w': self.freq ='1wk'
                else: self.freq = '1mo'


            else:

                sysout('\n> {} data type currently WIP - exiting with no effect'.format(targ))
                return



            # [3] Setting fetch url(s) base --> selection and formatting handled here

            if targ == 'keymetrics':

                self.url = (self.__reqpaths['keymetrics1'].format(self.__robj['ticker_url']),
                              self.__reqpaths['keymetrics2'].format(self.__robj['ticker_url'],
                                                                    self.__robj['ticker_url']))


            elif targ in ['mkt_d','mkt_w','mkt_m','div']:

                self.url = self.__reqpaths['mkt'].format(self.__robj['ticker_url'],
                                                         self.d0,
                                                         self.d1,
                                                         self.freq)


            elif targ == 'intraday':

                """| Intraday data (for 1m intervals) has to be pulled in 5 day chunks, max hist. period set to 30 |"""

                self.url = []

                # time_span_sec = int(dtt.today().timestamp()) - self.d0

                if self.d1 - self.d0 >= 86400*29:

                    ds = dtt.utcfromtimestamp(int(dtt.today().timestamp() - (86400 * 29)))

                    self.d0 = int(dtt(int(ds.year), int(ds.month), int(ds.day), 9, 30, 00, 00).timestamp())


                # [2] Set temp local date iterators

                temp_d0 = self.d0
                temp_d1 = self.d0 + (86400 * 5)


                # [3] While temp end date (5 days from temp start date) is 5 or more days from act. d1, iterate

                while self.d1 - temp_d0 >= (86400 * 5):

                    self.url.append(self.__reqpaths['intraday'].format(self.__robj['ticker_url'],temp_d0,temp_d1))

                    temp_d0 = temp_d1 + 1
                    temp_d1 = temp_d0 + (86400 * 5)



                # [4] If the diff. is less than 5 days, check if at least 5 min of time is left in open range

                if self.d1 - temp_d0 > (60 * 5):

                    self.url.append(self.__reqpaths['intraday'].format(self.__robj['ticker_url'], temp_d0, self.d1))



            else:

                sysout('\n> {} data type currently WIP - exiting with no effect'.format(targ))
                return


        ## Something went wrong - no effect on robj list - check errors

        except Exception:

            self.error('Failed to parse url for {}, data: {}'.format(self.__robj['ticker_url'],targ))
            return


        ## Valid operation - updated current robj being ooperated on

        else:

            if targ == 'keymetrics':

                if len(self.__robj['req_urls'].keys()) == 0:

                    self.__robj['req_urls']['url_keymetrics_1'] = self.url[0]
                    self.__robj['req_urls']['url_keymetrics_2'] = self.url[1]


                else:

                    # TODO self.__url_adjust_ticker()

                    self.__robj['req_urls'].update({

                        'url_keymetrics1': self.url[0],
                        'url_keymetrics2': self.url[1]
                    })


            elif targ == 'intraday':

                for i,u in enumerate(self.url):

                    self.__robj['req_urls']['url_{}_{}'.format(targ,i+1)] = u


            else:

                self.__robj['req_urls']['url_{}'.format(targ)] = self.url



    ## Feed fobjs to spyder - process returned response objects

    def __handle_fetch_objects(self):

        """| Converts all robjs into fobjs --> create fobj object for all urls in robj['req_urls'] per asset |"""


        if not isok(self.robjs) or len(self.robjs) == 0:
            sysout('\n> [!!] No request objects present - spyderweb will not be called')
            return

        else:


            # [1] Create fobjs

            self.fobjs = []

            for x in self.robjs:


                ### If robj is None, an improper type or empty - skip and continue

                if not isok(x) or not isinstance(x,dict) or len(x.keys()) == 0: continue

                else:


                    ### Use try-catch (except) to error check fobj creation (if any errors thrown - skip robj)

                    try:

                        for rk,ru in x['req_urls'].items():


                            temp_dtype = rk.split('url_')[-1] if rk.split('_')[1] == 'mkt' else rk.split('_')[1]

                            self.fobjs.append(

                                {
                                    'asset_id': x['asset_id'],
                                    'ticker_id': x['ticker_id'],
                                    'ticker_url': x['ticker_url'],
                                    'req_id': '{}-{}'.format(x['req_id'],rk.split('url_')[-1]),


                                    ## Even if multiple urls exist (keymetrics, intraday, news, etc..) pos [1] will be core type

                                    'req_dtype': temp_dtype,

                                    'req_dtu': int(dtt.today().timestamp()),
                                    'req_dtc': str(dtt.today()),
                                    'url': ru
                                }
                            )

                    except Exception:

                        continue



            # [2] Feed (pass) fobjs to Spyder request engine for fetching

            sysout('\n-----\n> Feeding Spyder (Standby)...\n-----\n\n')

            self.fobjs = self.spyderweb.spinweb(self.fobjs.copy(), self.maxproc, local=True, show_threads=False)

            self.spyderweb.backup_log()  # Backup log file post-processing



            # [3] Pass responses to the data extraction handler for final processing

            if isok(self.fobjs) and len(self.fobjs) > 0:

                self.__handle_data_extraction()

            else:
                sysout('\n> Resulting fobjs list was invalid or empty - no fetch requests to pass')
                return



        ## Final notice output to UI

        sysout('\n-----\n> [*] -- REQUEST HANDLING FINISHED --\n-----\n')

    def __handle_data_extraction(self):

        sysout('\n-----| Extraction process starting |-----\n')

        self.__op_cache =   {}
        eproctime =         time.time()


        # [1] Sort the fetch object list and extract unique tickers --> (asset_class,ticker)

        self.fobjs =    sorted(self.fobjs, key=lambda x: x['ticker_id'])
        temp_assets =   unique([(x['asset_id'], x['ticker_id']) for x in self.fobjs])


        # [2] Iterate over assets and operate accordingly

        for curr_asset in temp_assets:

            sysout('\n>----Extracting [{}]:  {}...'.format(curr_asset[0].upper(), curr_asset[1].upper()))
            etime = time.time()

            FLAG_NEW_ASSET =    False
            curr_pool =         [x for x in self.fobjs if x['ticker_id'] == curr_asset[1]]


            if self.data == {} or not curr_asset[0] in self.data.keys():
                self.data[curr_asset[0]] = {}


            # [2.1] If current asset is not found in the log file - add entries to log and update files

            if not curr_asset[1] in self.log_file.index:


                ## Add entry to log and update file (-1 is placeholde for NO DATA -- not changed if asset lacks data)

                FLAG_NEW_ASSET =                            True
                self.data[curr_asset[0]][curr_asset[1]] =   {}

                self.log_file =     self.log_file.append(pd.Series([curr_asset[1], curr_asset[0]],
                                                                        index=self.log_file.columns),
                                                                        ignore_index=True)

                self.update_file =  self.update_file.append(pd.Series([curr_asset[1], -1, -1, -1, -1, -1, -1],
                                                                        index=self.update_file.columns),
                                                                        ignore_index=True)

                sysout(' [NEW ASSET ENTRY ADDED]')


            ## Reset Index - at this point, the log/upd files should have min 1+ entries -- index setting should work

            sysout('\n-----')
            self.log_file.index = self.log_file['ticker_id']
            self.update_file.index = self.update_file['ticker_id']


            # [2.2] Iterate over data types and operate on curr_pool

            for dtype in ['keymetrics', 'mkt_d', 'mkt_w', 'mkt_m', 'div', 'intraday']:

                tempval = None
                curr_df = None

                try:
                    resp_to_pass = [x for x in curr_pool if x['req_dtype'] == dtype]
                    if dtype in ['mkt_d', 'mkt_w', 'mkt_m', 'div']: resp_to_pass = resp_to_pass[0]

                except Exception: continue
                else:


                    ## Handle data type

                    if dtype == 'keymetrics' and curr_asset[0] in ['equity','etf']:
                        tempval = self.__handle_keymetrics(resp_to_pass)

                    if dtype in ['mkt_d', 'mkt_w', 'mkt_m']:
                        tempval = self.__handle_mktdata(resp_to_pass)

                    if dtype == 'div' and curr_asset[0] in ['equity','etf']:
                        tempval = self.__handle_dividend(resp_to_pass)

                    if dtype == 'intraday':
                        tempval = self.__handle_intraday(resp_to_pass)



                    ## Append all raw values at extraction run time to the operation cache (for dev ops)

                    self.__op_cache['{}_{}_{}'.format(curr_asset[0], curr_asset[1], dtype)] = tempval



                    ## If valid data from response - operate on New/Updated Asset Acordingly

                    if isok(tempval) and isinstance(tempval, pd.DataFrame):


                        ### HANDLE [1] New Asset or [2] Updating an Asset -- then change entry in update file (today)

                        if FLAG_NEW_ASSET:
                            self.data[curr_asset[0]][curr_asset[1]][dtype] = tempval.copy()
                            self.update_file.at[curr_asset[1], dtype] = int(dtt.today().timestamp())

                        else:

                            if len(tempval.index) > 0:

                                if dtype in ['keymetrics']:
                                    self.data[curr_asset[0]][curr_asset[1]][dtype] = tempval

                                else:
                                    curr_df = self.data[curr_asset[0]][curr_asset[1]][dtype].copy()
                                    curr_df = curr_df.append(tempval)
                                    self.data[curr_asset[0]][curr_asset[1]][dtype] = curr_df.drop_duplicates('dateu')

                                self.update_file.at[curr_asset[1], dtype] = int(dtt.today().timestamp())

                            else:
                                sysout('\n> {}/{} [{}] - [X] ({})'.format(curr_asset[0], curr_asset[1], dtype,
                                                                          type(tempval)))

                    else:
                        sysout('\n> {}/{} [{}] - [X] ({})'.format(curr_asset[0], curr_asset[1], dtype, type(tempval)))


                    if dtype == 'intraday': self.__handle_archive_intraday(curr_asset,tempval)


            ### [***] Write profile (new or updated) to memory - handle file name for FX ( '/' --> '_') + update logs

            temp_ac = curr_asset[0].lower()
            temp_t = curr_asset[1].replace('/','_').lower() if temp_ac == 'fx' else curr_asset[1].lower()

            dict2wb(self.__warehouse_data_fp.format(temp_ac,temp_t),self.data[temp_ac][curr_asset[1]])

            self.__update_log_file()
            self.__update_upd_file()

            sysout('\n    > Extraction process complete - runtime: ', crt(etime), ' sec\n---\n---\n')

        sysout('\n-----\n> Extractions finished - full runtime: ', crt(eproctime), ' sec')

    def __handle_archive_intraday(self, curr_asset, tempval):

        if isok(tempval) and isinstance(tempval, pd.DataFrame) and len(tempval.index) > 0:

            sysout('\n> Archiving Intraday Data...')

            try:

                temp_ac = curr_asset[0]
                temp_t = curr_asset[1].replace('/', '_').lower() if temp_ac == 'fx' else curr_asset[1]

                temp_fp = self.__intraday_archive_fp_base.format(temp_t.lower())
                temp_out = None
                temp_mode = 'a+'

                ## Fetch old data (if possible) then append/remove duplictaes/re-write (else 'a+')

                try: temp_idd = pd.read_csv(self.__intraday_archive_fp_base.format(temp_t.lower()), ',')
                except Exception:

                    sysout(' [!] Unable to find old data - append/create new data...')
                    temp_out = tempval

                else:

                    try:
                        final_idd = temp_idd.append(tempval)
                        final_idd = final_idd.drop_duplicates('dateu')
                    except Exception:

                        sysout(' [!] Unable to merge/clean new/old - append new...')
                        temp_out = tempval

                    else:

                        if isok(final_idd) and isinstance(final_idd, pd.DataFrame) and len(
                            final_idd.index) > 0:

                            sysout(' [*] Merged and Cleaned Successfully - re-write...')
                            temp_out = final_idd
                            temp_mode = 'w+'

                        else:

                            sysout(' [!] Merge/Clean operation was Invalid - restore old data...')
                            temp_out = temp_idd
                            temp_mode = 'w+'


            except Exception:
                sysout(' | [!] - FAILED (No efect)')
            else:

                try: temp_out.to_csv(temp_fp, header=True, sep=',', index=None, mode=temp_mode)
                except Exception:
                    sysout(' | [!] - FAILED (No effect)')
                else:
                    sysout(' | [*] - OK')

        else:
            sysout('\n> [!] Intraday data response ({}) invalid to archive'.format(type(tempval)))


    # endregion



    # region > **Additional functionality (for use in testing / debugging / dev - Use with caution)


    """| Purge memory logs --> re-create files and overwrite old |"""


    def purge_log(self): self.__create_logfile()

    def purge_update(self): self.__create_updfile()

    def get_op_cache(self): return self.__op_cache

    def get_all_urls(self): return self.__all_urls


    # endregion




# req_assets = {
#     'equity':['amzn','gm','aapl','dhi'],
#     'etf':['spy'],
#     'fx':['eur/usd','jpy/eur'],
#     'indices':['gspc','vix','dji']
# }
#
# fsd = FinDataSpyder()
#
# fsd.process_req(req_assets,update=True)

# zzz = fsd.chk_market_open(int(dtt.today().timestamp())-(86400*3)-20000)
#
# # fsd.purge_log()
# # fsd.purge_update()
#
#
# zzz = int(dtt.today().timestamp())-(86400*3)-20000
#
#
# dtt.utcfromtimestamp(zzz)





# from JsonConverter import *

# jj = JsonConverter(b,'__testing_archive/__json_testing/','json_data_save_test')
# jj.pack()





# a = fsd.fobjs.copy()
# b = a.copy()
#
#
# qq = fsd.test_handle_data(a)
#
#
#
# # dd = news_handler('aapl')
#
# # dd.to_csv('warehouse/asset_news/aapl.news',header=True,index=None,sep='|',mode='a+')
#
# qq = fsd.get_op_cache()
#
#
# dd4 = pd.read_csv('warehouse/asset_news/aapl.news',sep='|')
#
# temp_log = fsd.log_file.copy()
# temp_upd = fsd.update_file.copy()
#
# temp_log.index = temp_log['ticker_id']
# temp_upd.index = temp_upd['ticker_id']
#
#
#
#
# b2 = sorted(b,key = lambda x: x['req_id'].split('-')[-1])
# b3 = unique([(x['asset_id'],x['ticker_id']) for x in b2])
# b4 = sorted(b,key=lambda x: x['ticker_id'])
#
# dd2 = [(1,2),(3,6),(4,10),(9,2),(2,2)]
#
# dd3 = sorted(dd2, key = lambda x: x[1])
#
#
#
#
#
#
#
#
#
#
#
# curr_pool = []
#
# for b3_elem in b3:
#
#     curr_pool = [x for x in b2 if x['ticker_id'] == b3_elem[1]]
#
#
#     ## If not in the log file,  add entries to log/upd files (upd --> preload with -1, implies data not held by asset)
#
#     if not b3_elem[1] in temp_log.index:
#
#         temp_log = temp_log.append(pd.Series([b3_elem[1],b3_elem[0],'NA'],index=temp_log.columns),ignore_index=True)
#
#         temp_upd = temp_upd.append(pd.Series([b3_elem[1],-1,-1,-1,-1,-1,-1],index=temp_upd.columns),ignore_index=True)
#
#
#     for dtype in ['keymetrics','mkt_d','mkt_w','mkt_m','div','intraday']:
#
#
#         if dtype == 'keymetrics' and b3_elem[0] in ['equity','etf']:
#
#
#             temp_resp = [(x['req_id'].split('-')[-1],x['response']) for x in curr_pool if x['req_dtype'] == dtype]
#
#
#
#
#
#         temp_div = [x['response'] for x in curr_pool if x['req_dtype'] == 'keymetrics']
#
#
#
#
#
#
# aa = temp_log.append(pd.Series(['ticker_1','ac_1', 'nn'],index=temp_log.columns),ignore_index=True)
# aa = aa.append(pd.Series(['ticker_2','ac_2', 'nn2'],index=temp_log.columns),ignore_index=True)
#
#
# aa.index = aa.ticker_id
# aa.at['ticker_2','name'] = 'ppp'
#
#
#
#


