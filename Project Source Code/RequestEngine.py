
"""
> Module:   RequestEngine
> Author:   Wyatt Marciniak
...

    > Handles N number of request objects --> min req. {'url':'www.path_to/datasource/etc...' }

    > Module designed for general use but originally created for @SpyderWeb deployments


"""


from IPProxy import *
from ProgBars import *


class SpyderWeb(ClassNoticeHandler):

    cn = 'SpyderWeb'


    """

    {SpyderWeb}
    ...



    """


    # region > Attributes


    ipp = None          # IP Proxy and User-Agent tag hander (IPMASK --> /IPProxy.py)

    PBAR = ProgBar()    # Progress bar (custom built class --> /ProgBars.py)

    fobjs = []          # Fetch Objects


    mask_pool = []
    to_drop = []

    min_pool_ipp = 20

    # batch_size = 0.05

    maxproc = 40        # Maximum number of threads to run at once
    speedbump = 1       # Wait time between requests (scales by size of given fetch object list @fobjs at runtime)


    req_timeout = 3
    # mask_droplim = 3
    max_tries = 40

    clean_lim_trigger = 1000
    clean_lim_prct = 0.50

    __IS_UNLOCKED = True

    stop_on_ipmask_down = False
    alert_on_ipmask_down = True



    last_run_trace = []

    trace_conn = None
    last_run_trace_fp = None

    show_threads= False

    __log_fp = 'spyder_assets/backlogs/log.spyderweb'
    __trace_fp_base = 'spyder_assets/backlogs/__threads/optrace_{}.spyderweb'

    __log_backup_fp_base = 'spyder_assets/backlogs/__log_archive/log_backup_{}.spyderweb'


    # endregion


    # Constructor - can pass fobj initially or after creation

    def __init__(self,fetch_objs=None,ipp_overwite=False,ipp_fetch_force=False):


        # Handle creating log file (if needed)

        self.__create_log()



        # If fobjs given, set not and if mask_on - initialize IPProy obj as well

        self.fobjs = fetch_objs if isok(fetch_objs) else []
        self.ipp = IPProxy(ipp_overwite,ipp_fetch_force)

        self.mask_pool = self.ipp.get_mask_pool()


        # Handle cleanup if ipp log size greater than test threshold

        if len(self.ipp.ipp) >= self.clean_lim_trigger:

            self.note('IPP proxy list size ({}) >= limit ({}) to be cleaned ({}%)...'.format(len(self.ipp.ipp),
                                                                                          self.clean_lim_trigger,
                                                                                          self.clean_lim_prct*100))

            self.run_ipp_cleanup(int(self.clean_lim_prct))






    # Create / update (append to) log file

    def __create_log(self):

        if not os.path.isfile(self.__log_fp):

            conn = open(self.__log_fp,'a+')
            conn.writelines([])
            conn.close()

    def update_log(self,running_cleanup=False):

        # Clean data into valid line entries

        to_add = []

        curr_dt = dtt.today()

        for fobj in self.fobjs:

            to_add.append(

                mkstr(

                    str(curr_dt),
                    int(curr_dt.timestamp()),
                    fobj['SUCCESS'],
                    fobj['resp_code'],
                    fobj['rt_full'],
                    fobj['retries'],
                    fobj['rt_success'],
                    fobj['ipp'],
                    fobj['ua'],
                    fobj['url'],sep=', '
                )
            )

        # Write (append) data

        if running_cleanup:
            conn = open('spyder_assets/backlogs/__cleaninglog.spyderweb', 'a+')

        else:
            conn = open(self.__log_fp, 'a+')

        conn.writelines(['{}\n'.format(x) for x in to_add])
        conn.close()

    def backup_log(self):


        try:

            log0_conn = open(self.__log_fp,'r')
            log0 = [x.strip('\n') for x in log0_conn.readlines()]
            log0_conn.close()

        except Exception:

            sysout('\n> [!] Spyderweb log backup operation failure: Retriving current log')
            return

        else:


            try:


                log1_conn = open(self.__log_backup_fp_base.format(int(dtt.today().timestamp())),'w+')
                log1_conn.writelines(['{}\n'.format(x) for x in log0])
                log1_conn.close()


            except Exception:

                sysout('\n> [!] Spyderweb log backup operation failure: Writing Backup')
                return


    # Fetch worker (also updates progress bar & MT tracer) - called by spinweb

    def do_fetch(self, fobj=None):

        if isok(fobj):

            url = fobj['url']
            ipp = None
            ua = None
            time_success = 0
            resp = None

            success = False
            try_counter = 0

            status_code = None


            # curr_speedbump = self.speedbump

            time_full = time.time()


            # [1] Handle thread initiliazed report

            thread_info = '{},{},{},{},{},{},{},{}\n'.format(success,
                                                          status_code,
                                                          fobj['req_id'],
                                                          try_counter,
                                                          crt(time_full),
                                                          ipp,
                                                          ua,
                                                          url)

            self.trace_conn.write(thread_info)

            if self.show_threads:
                sysout('\n', thread_info[:-3])


            # [2] Handle Request

            while not success and try_counter < self.max_tries:


                # curr_speedbump = self.speedbump + (try_counter/10)

                time.sleep(self.speedbump)

                try_counter += 1


                try:

                    ipp = sample(self.mask_pool['ipp'], 1, replace=True)
                    ua = sample(self.mask_pool['ua'], 1, replace=True)

                    time_success = time.time()

                    resp = req.get(url,
                                   proxies={
                                       'http': 'http://{}'.format(ipp),
                                       'https': 'https://{}'.format(ipp)},
                                   headers={'User-Agent': ua},
                                   timeout=self.req_timeout)

                except Exception:
                    self.to_drop.append(ipp)

                else:

                    try:
                        if int(getattr(resp,'status_code')) == 200:
                            status_code = 200
                            success = True

                    except Exception:
                        success = False


                ## If success is false, TRY to get the status code (else default it to None - for thread reporting)

                if not success:

                    try: status_code = int(getattr(resp,'status_code'))
                    except Exception: status_code = None


                ## Update current thread information

                thread_info = '{},{},{},{},{},{},{},{}\n'.format(success,
                                                                    status_code,
                                                                    fobj['req_id'],
                                                                    try_counter,
                                                                    crt(time_full),
                                                                    ipp,
                                                                    ua,
                                                                    url)

                self.trace_conn.write(thread_info)

                if self.show_threads:
                    sysout('\n', thread_info[:-3])



            # Append data to request object and return (adjust for outcomes

            fobj['ipp'] = ipp if success else None
            fobj['ua'] = ua if success else None

            fobj['rt_success'] = crt(time_success) if success else None
            fobj['rt_full'] = crt(time_full)

            fobj['retries'] = try_counter

            fobj['resp'] = resp
            fobj['resp_dtu'] = int(dtt.today().timestamp())
            fobj['resp_dtc'] = str(dtt.today())

            fobj['resp_code'] = status_code
            fobj['SUCCESS'] = success

            if not self.show_threads: self.PBAR.update()

            return fobj


        else:

            if not self.show_threads: self.PBAR.update()
            return None



    # Run fetcher

    def spinweb(self,fobjs=None,maxproc=None,local=False,show_threads=None,running_cleanup=False):
        self.fxn = self.getfxn()


        # Handle param resetting (if needed) and initial filters

        if isok(show_threads): self.show_threads = show_threads
        # if isok(batchsize) and isinstance(batchsize,(int,float)): self.batch_size = int(batchsize)
        if isok(maxproc) and isinstance(maxproc,(int,float)): self.maxproc = int(maxproc)
        if isok(fobjs): self.fobjs = fobjs


        if not isok(self.fobjs) or len(self.fobjs) == 0:

            self.error('No fetch objects found in class or as given param')
            return

        else:


            # If IPProxy object is None (not created) - error and fail (required to run)

            if not isok(self.ipp):

                try: self.ipp = IPProxy()
                except Exception:
                    self.error('IPP masking object was not created - Ending')
                    return


            else:


                # region > Setup for MT


                # [1] Append req_id tags to track worker threads (if NOT already labeled - check all)


                for fi,fe in enumerate(self.fobjs):


                    try:
                        temp_try = fe['req_id'] # If no error thrown when indexing, entry exists - skip (else add seq.)
                    except Exception:
                        fe['req_id'] = 'RID_{}-{}'.format(fi,len(self.fobjs)) # Error (bad index) means not found here
                    else:
                        continue # Valid temp_try value found, next iteration (check all - add tags IFF missing)


                # [2] Create tracer tag for thread recording (loads post MT - saved every run)


                if running_cleanup:

                    temp_dt_tag = 'cleaning_{}'.format(str(dtt.today()).replace(' ', '_').replace(':', '-'))

                else:

                    temp_dt_tag = str(dtt.today()).replace(' ', '_').replace(':', '-')


                self.last_run_trace_fp = self.__trace_fp_base.format(temp_dt_tag)

                self.trace_conn = open(self.last_run_trace_fp,'a+') # Open during all threads




                # [3] Update mask pool & start progress bar


                if len(self.mask_pool['ipp']) < self.min_pool_ipp:

                    temp = self.ipp.get_mask_pool()



                    # [3.1] If pool request is invalid - ID why and return to top level caller (exit current operation)


                    if not isok(temp) or len(self.mask_pool['ipp']) < self.min_pool_ipp:

                        self.fxn = self.getfxn()


                        if self.ipp.check_unlock():

                            self.error('IPProxy object is unlocked and data len under requirements - ERROR')
                            return

                        else:

                            self.note('[!] IPProx object is locked - pending next update - End and retry in 2min')
                            return



                # [4] If setup is finished - run fetching operation (spin the web)


                valid_mt_operation = []

                self.to_drop = []

                if not self.show_threads or running_cleanup:
                    self.PBAR.start(len(self.fobjs), '-', max_length=45)



                # endregion



                ## Run MT

                if len(self.fobjs) == 1:
                    batches = self.fobjs.copy()

                else:
                    batches = sample(self.fobjs.copy(),len(self.fobjs),False,False)

                pool = mtpool(maxproc)
                valid_mt_operation = pool.map(self.do_fetch, batches)
                pool.terminate()
                gc.collect()



                # region > Post MT operations (validate operation, clean ipp cache, etc...


                # [0] Close trace file

                self.trace_conn.close()



                # [1] Check for valid results (pre-check for @self.fobjs)

                if not isok(valid_mt_operation) or len(valid_mt_operation) == 0:
                    self.error('Invalid MT operation (or failure) - check class')

                else:
                    self.fobjs = valid_mt_operation



                # [2] Ipp cleanup - if elems in @self.to_drop (pre-check for @self.mask_pool)

                if len(self.to_drop) > 0:

                    if running_cleanup:
                        self.ipp.ipp_clean(unique(self.to_drop),as_is=True)

                    else:
                        self.ipp.ipp_clean(self.to_drop)

                    temp = self.ipp.get_mask_pool()


                    # [!] CHECK --> If an error occurs and is not handled, this could overwrite and cause MT errors

                    if isok(temp):
                        self.mask_pool = temp

                    else:
                        self.fxn = self.getfxn()
                        self.note('[!] Post drop reload failed - class data unaffected - stop and handle issues')



                # [3] Append to logfile

                self.update_log(running_cleanup)



                # [4] Retrieve last trace file (pre-check for @self.last_run_trace and error catcher for it)

                try:
                    conn = open(self.last_run_trace_fp,'r')
                    self.last_run_trace = [x.strip('\n') for x in conn.readlines()]
                    conn.close()

                except Exception:
                    self.fxn = self.getfxn()
                    self.note(['[!] Failed to retrieve trace file from memory'])


                # endregion



                # Handle local return

                if local: return self.fobjs



    # Use to directly test some % (from 0 to 1) of the IPP pool (backend-cleanup)

    def run_ipp_cleanup(self,test_prct=0,test_url=None):

        self.fxn = self.getfxn()


        if not isok(self.ipp) or not self.ipp.check_unlock():

            self.error('IPProxy object invalid or locked - cannot run cleaner')
            return


        elif not isok(test_prct) or test_prct == 0:

            self.error('Invalid test size percentage (None or 0) - cannot run cleaner')
            return


        else:

            num_test_elems = math.floor( len(self.mask_pool['ipp']) * test_prct)

            test_url = test_url if isok(test_url) else 'http://www.google.com'

            temp_fobjs = [{'url': test_url, 'id': 'url_{}'.format(i)} for i in range(1,(num_test_elems + 1))]


            ## Set testing attributes

            self.speedbump = 0
            self.maxproc = 50
            self.req_timeout = 3
            self.max_tries = 1      ## THIS IS KEY

            self.spinweb(temp_fobjs,running_cleanup=True)

            self.fxn = self.getfxn()
            self.note('[*] Cleaning finished - resetting attributes')

            self.speedbump = 1
            self.maxproc = 40
            self.req_timeout = 3
            self.max_tries = 30

            return



# sw = SpyderWeb()


# a = fsd.results
# b = a[47]
# c = b['response']
# getattr(c,'status_code')

# region > Testing and Examples


# sw = SpyderWeb()
# sw.show_threads = True
# sw.max_tries = 5
# sw.test_sweb(testnum=5)
#
#
# sw.ipp.ipp_clean()


# endregion