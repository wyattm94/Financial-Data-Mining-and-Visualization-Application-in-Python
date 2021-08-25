
"""
>> Module: FinAppBaseWorker
>> Author: Wyatt Marciniak

>> USE:

    > Base class to be inherited by children (FinAppWorkers)
    > Imported Modules are also in import chain


                                                                """


from JsonConverter import *
from FinDataSpyder import *
from helper import *


class FinAppBaseWorker(ClassNoticeHandler,JsonConverter):

    cn = 'FinAppBaseWorker'


    # region > Attributes

    PBAR =      ProgBar()   # Progress Bar Object (custom build) for use

    cache =     {}          # Local copy of App Cache
    curr =      []          # Local copy of curr_selected (assets -- rows)
    log =       None        # Local copy of log file
    df =        None        # Current DF being operated on or referenced by worker

    maxproc =   40          # Max number of threads to run concurrently (MT Operations)

    valid_assets = ['equity', 'etf', 'fx', 'indices']       # Current asset types implemented


    # Pre-defined values -- currently used as 'base market data' to fetch at startup

    sector_etfs = mkdict(

        k=['energy', 'financial', 'utilities', 'industrial',
           'technology', 'health_care', 'consumer_discretionary',
           'consumer_staples', 'materials', 'homebuilders'],

        v=['xle', 'xlf', 'xlu', 'xli', 'xlk', 'xlv', 'xly', 'xlp', 'xlb', 'xhb']
    )

    major_indices = mkdict(

        k=['sp500', 'djia', 'vix'],
        v=['gspc', 'dji', 'vix']
    )

    major_currencies = mkdict(

        k=['eur/usd', 'aud/usd', 'jpy/usd', 'cny/usd', 'btc/usd'],
        v=['eur/usd', 'aud/usd', 'jpy/usd', 'cny/usd', 'btc/usd']

    )

    # endregion


    # region > Methods

    def po(self, x):

        if self.PNOTICE: sysout(x)

    def chkpack(self, x):

        if isinstance(x, str):

            try: x = self.unpack(x)
            except Exception: return None
            else: return x

        else: return x

    def usekeys(self, keys, d):

        self.fxn = self.getfxn()

        if not isinstance(keys, (int, float, str, list, tuple)) or not isinstance(d, dict):
            self.error('Bad input type(s) {} & {}'.format(type(keys), type(d)))
            return d

        else:

            try:

                if isinstance(keys, (int, float, str)):
                    keys = [keys]

                temp = mkdict(

                    k=[k for k, v in d.items() if k in keys],
                    v=[v for k, v in d.items() if k in keys]
                )

            except Exception:
                self.error('Bad attempt to create dict')
                return d

            else:
                return temp


    def create_options(self, op, val=None):

        self.fxn = self.getfxn()

        if not isok(op) or not isinstance(op, (list, tuple)) or len(op) == 0:
            self.error('Invalid @op (option(s)) parameter of type ({})'.format(type(op)))
            return None

        else:

            if not isok(val) or not isinstance(val, (list, tuple)) or len(val) == 0:
                val = op.copy()

            return list({'label': op[i], 'value': val[i]} for i in range(len(val)))


    # Handles cache to log with 'selected' values set to last

    def cache_to_log(self, c, log, cs=[],sr=[]):

        self.fxn = self.getfxn()
        self.devout('RUN', '...')

        if not isok(log) or not isok(cs) or not isok(c):

            self.error('Bad input type(s) cache/log/cs - {}/{}/{}'.format(type(c), type(log),type(cs)))
            return None, None

        else:

            c = self.chkpack(c)
            log = self.chkpack(log)
            cs = self.chkpack(cs)


            if not isok(c) or not isok(cs) or not isok(log):
                self.error('Bad unpacked type(s) [NONE] for cache/log/cs - {}/{}/{}'.format(type(c), type(log),
                                                                                            type(cs)))
                return None, None

            else:


                if not isinstance(log,pd.DataFrame):
                    log = pd.DataFrame(log)

                curr_ticks  = [(list(log['ticker_id'])[i], list(log['ac'])[i]) for i in range(len(log.index))]
                ct_t        = [x[0] for x in curr_ticks]

                new_ticks = unlist([[(t, a) for t in tl] for a, tl in c.items()])
                nt_t = [x[0] for x in new_ticks]

                print('\n',curr_ticks,'\n',new_ticks,'\n')


                # _CodeFlow: [1] Handle re-creating log table in order (i.e. new elems to end, keep order on removal)...

                try:

                    if len(new_ticks) >= len(curr_ticks):

                        add_at_end = [nt for nt in new_ticks if not nt[0] in ct_t]  # New Asset rows (tuples) -- at end
                        curr_table = curr_ticks + add_at_end

                    else:

                        curr_table = []

                        for ct in curr_ticks:
                            if ct[0] in nt_t: curr_table.append(ct)

                    new_table = kv2df(

                        k=['ticker_id', 'ac', 'selected'],

                        v=[
                            [x[0] for x in curr_table],
                            [x[1] for x in curr_table],
                            [False for i in range(len(curr_table))]
                        ]
                    )


                except Exception:

                        self.error('Failed in parsing new ticks for log table')
                        return None, None

                else:


                    #_CodeFlow: [2] Handle re-creating curr-selected list and apply to table...

                    try:

                        sel_col = []
                        new_cs = []
                        sel_ticks = [x[1] for x in cs]

                        # new_table.index = list(range(len(new_table.index)))

                        if len(new_ticks) >= len(curr_ticks):   # Use SR directly

                            for r in range(len(new_table.index)):

                                ct = list(new_table['ticker_id'])[r]    # Current Ticker

                                if r in sr:

                                    new_cs.append((r,list(new_table['ticker_id'])[r],list(new_table['ac'])[r]))
                                    sel_col.append(True)

                                else: sel_col.append(False)


                        else:   # Use CS directly (compare by ticker if new log is smaller)

                            for r in range(len(new_table.index)):

                                ct = list(new_table['ticker_id'])[r]  # Current Ticker

                                if ct in sel_ticks:
                                    new_cs.append((r, list(new_table['ticker_id'])[r], list(new_table['ac'])[r]))
                                    sel_col.append(True)

                                else:
                                    sel_col.append(False)

                        new_table['selected'] = sel_col

                    except Exception:
                        self.error('Failed in creating and applying new cache')
                        return None, None

                    else:
                        self.po('\n> [*] Local log and CS update OK')

                        print('\n----\n',new_table,'\n\n',new_cs,'\n')

                        return new_table,new_cs




    # endregion


# fbb = FinAppBaseWorker()
#
# a = fbb.pack(([],0))
# b = fbb.unpack(a)
# b[1]