
"""
> Module:	IPProxy
> Author: 	Wyatt Marciniak
...

	> This module holds the IPProxy class, which inherits from ClassNoticeHandler /ClassNoticeHandler.py

	> @IPProxy objects handle fetching, storing, retrieving and cleaning memeory for IP Proxy addresses and User-Agent
		system ID tags. This class is designed for general use but was created for multi-threaded spyder web scrapers


"""


from StatsCore import *
from ClassNoticeHandler import *
from datetime import datetime as dtt


class IPProxy(ClassNoticeHandler):

	cn = 'IPP'

	# region > Member Variables

	__IS_UNLOCKED = True

	PRNT = False


	# Core data for masks

	ipp = []
	ua = []

	exus = True


	# Data values, data update freq and user update check freq limiters

	last_ipp_update = 0
	last_check_ipp_update = 0


	last_ua_update = 0
	last_check_ua_update = 0


	__ua_lim_page = 5

	ipp_lim_update_min = 20
	ua_lim_update_days = 30


	# Limits frequency of read calls to memory (allows cleaner integration for scaling)

	check_lim_update_sec = 120

	last_check_unlock_try = 0
	check_lim_unlock_try_sec = 120



	# Limiters for dropping proxies and accessing proxies (will try to refill or lock until valid level reached)

	ipp_lim_drop_freq = 2		# To drop proxy
	ipp_lim_min_pool = 20 		# To return mask (proxy/ua)
	ua_lim_min_pool = 20



	# File paths for memory and data HTML sources

	__ipp_path,__ua_path = ('spyder_assets/cache/{0}'.format(p) for p in ['__cache.ipp','__cache.ua'])

	__ipp_urls = ('https://www.us-proxy.org/','https://free-proxy-list.net/')

	__ua_base = 'http://developers.whatismybrowser.com/useragents/explore/operating_system_name/{0}/'
	__ua_opsys = ['windows','linux','mac-os-x','macos']

	__ipp_graveyard = 'spyder_assets/cache/__graveyard.ipp'
	__ipp_logfile = 'spyder_assets/cache/__log.ipp'

	__update_path = 'spyder_assets/cache/__updates.validator'

	__data_backup_base = 'spyder_assets/cache/backups/'



	# ** Issues with User-Agent tag spyder (use supplied data) - Fix WIP (IPP OK)
	__FLAG_ALLOW_UA_FETCH = False
	__FLAG_ALLOW_IPP_FETCH = True



	# endregion


	# Constructor

	def __init__(self,overwrite=False,force_fetch=False):


		# [1] [Cache Creation Handler] Make cache files in memory (executes on first run or if path changed)

		if not os.path.isfile(self.__ipp_path):

			self.__io('w+', 'ipp')
			sysout('\n> IPP Cache created')


		if not os.path.isfile(self.__ua_path):

			self.__io('w+', 'ua')
			sysout('\n> UA Cache created')



		# [2] [IPP Handler] Run normally if allowed - (Default: ALLOWED)

		sysout('\n> Collecting IP Proxies... ')


		# read / try fetch / check if valid

		self.__io('r', 'ipp')

		self.__fetch_ipp(overwrite, force_fetch)

		if len(self.ipp) < self.ipp_lim_min_pool:

			self.__IS_UNLOCKED = False

			if self.PRNT: sysout(' [!!] IPProxy locked - ipp issues')


		else:

			sysout(' | Complete')



		# [3] [UA Handler] Run UA normally if allowed - (Default: NOT ALLOWED)

		sysout('\n> Collecting ua tags...')


		# read / try fetch / check if valid

		self.__io('r', 'ua')

		# self.__fetch_ua(force_fetch)

		if len(self.ua) < self.ua_lim_min_pool:

			self.__IS_UNLOCKED = False

			if self.PRNT: sysout('\n> [!!] IPProxy locked - ua issues')


		else:

			sysout(' | Complete')


	# Handle access requests and unlocking attempts


	def check_unlock(self):
		return self.__IS_UNLOCKED


	def __try_unlock(self):


		# If time passed from last unlock attempt < required amount, return false (disallow requests until valid)

		if not (time.time() - self.last_check_unlock_try) > self.check_lim_unlock_try_sec:

			return False


		else:


			# Good request - update last request time and try fetching

			self.last_check_unlock_try = time.time()

			valid_fetch = self.__fetch_ipp()



			if len(self.ipp) < self.ipp_lim_min_pool:

				return False


			else:


				# Handle update of lock status here

				self.__IS_UNLOCKED = True
				return True


	def __access(self):

		if self.PRNT: sysout('\n> Acessing... (last fxn = {})'.format(self.fxn))

		# If is unlocked return true - else return result of trying to unlock

		if self.__IS_UNLOCKED:

			if len(self.ipp) < self.ipp_lim_min_pool:

				if self.PRNT: sysout('\n> Access allowed - calling fetcher for refill')

				valid_fetch = self.__fetch_ipp()


				if len(self.ipp) >= self.ipp_lim_min_pool:

					return True

				else:

					if self.PRNT: sysout('\n> [!!] IPProxy was jus locked')

					self.__IS_UNLOCKED = False
					return False

			else:

				return True

		else:
			return self.__try_unlock()



	# region > [Subsection] Handle all read/write and update validation



	## [I] General IO operations (specified for this class)

	def __io(self, io, targ, data=None, path=None):


		# Handle data cleaning --> to list

		if not isok(data): data = []

		if isinstance(data,(str,int,float,bool)):
			data = [data]


		# Handle fp determination

		if isok(path):

			fp = path

		else:

			fp = {

				'ipp': self.__ipp_path,
				'ua': self.__ua_path,
				'graveyard': self.__ipp_graveyard,
				'log': self.__ipp_logfile,
				'update': self.__update_path

			}[targ]


		temp = []


		# Handle open/close read/write

		try: conn = open(fp, io)
		except Exception:
			self.fxn = self.getfxn()
			self.error('Bad connection (local) at: {}'.format(fp))
			return

		else:

			try:

				if io in ['r', 'r+']:
					temp = [x.strip('\n') for x in conn.readlines()]

				else:
					conn.writelines(['{}\n'.format(x) for x in data])

			except Exception:
				self.fxn = self.getfxn()
				self.error('Bad {} attempt at: {}'.format(io,fp))

			conn.close()


		# Handle setting data or returning result (if read operation valid)

		if io in ['r','r+']:

			if targ == 'ipp':
				self.ipp = temp

			elif targ == 'ua':
				self.ua = temp

			else:
				return temp



	## [II] Update log file, graveyard (dead, dropped addresses) & data update tracker

	def __update_tracker(self,tracker,to_add=[]):



		# Log and graveyard - append datetime string

		if tracker in ['log','graveyard']:

			if isinstance(to_add, (str, int, float, bool)):
				to_add = [to_add]

			self.__io('a+',
					  tracker,
					  ['{}| {}| {}|'.format(x,str(dtt.today()),int(dtt.today().timestamp())) for x in to_add])



		# Update - if called by ipp, write current datetime int and @self.last_ua_update (and vice versa)

		elif tracker == 'update':

			if self.fxn == '__fetch_ipp':

				self.last_ipp_update = int(dtt.today().timestamp())
				self.last_check_ipp_update = int(dtt.today().timestamp())

			else:

				self.last_ua_update = int(dtt.today().timestamp())
				self.last_check_ua_update = int(dtt.today().timestamp())


			self.__io('w+', tracker, [self.last_ipp_update, self.last_ua_update])



		else:
			sysout('\n> Update tracker failed for ({})'.format(tracker))



	## [III] Check if request time is valid (beyond limit range from last check)

	def __check_update(self):

		calling_function = self.fxn

		if self.PRNT: sysout('\n> {} trying to update'.format(self.fxn))


		# If update file does not exist - create it and pass True (or if overwrite is forced)

		if not os.path.isfile(self.__update_path):

			self.__update_tracker('update')
			return True


		else:

			temp = self.__io('r', 'update')

			self.fxn = calling_function # Reset - classes use this to determine ops



			# If file does not contain 2 elments, return true (self-correction)

			if not isok(temp) or not len(temp) == 2:

				self.__update_tracker('update')
				return True


			else:

				try:

					if self.fxn == '__fetch_ipp':

						is_valid = (time.time() - int(temp[0])) / 60 >= self.ipp_lim_update_min

					else:
						is_valid = (time.time() - int(temp[1])) / (60 * 60 * 24) >= self.ua_lim_update_days


				# More self-correction

				except Exception:

					self.__update_tracker('update')
					return True


				# If updates exists & @is_valid is ok, reset attributes & if update is valid, reset updates for fetch
				else:

					self.last_ipp_update = temp[0]
					self.last_ua_update = temp[1]

					if is_valid:
						self.__update_tracker('update')

					return is_valid



	## [IV] Handle IPP backups (UA to come with new algo -- for now, you must manually move that file (if path changed)

	def __backup(self,targ,mod_fp=None):


		# Param @mod_fp changes backup file name from default 'cache_backup' (used for { self.get_mask_pool() }

		fp_mod = mod_fp if isok(mod_fp) else 'cache_backup'

		try:

			if targ == 'ipp':

				self.__io('w+',
						  None,
						  self.ipp,
						  '{}{}__{}.ipp'.format(self.__data_backup_base, fp_mod, int(dtt.today().timestamp())))

			else:

				self.__io('w+',
						  None,
						  self.ua,
						  '{}{}__{}.ua'.format(self.__data_backup_base, fp_mod, int(dtt.today().timestamp())))

		except Exception:
			if self.PRNT: sysout('\n> [!] Backup Failed')



	# endregion



	# region > [Subsection] Fetch handlers




	## Fetch request handler for ip proxy data (overwrite or append iff can check for update and check is valid)

	def __fetch_ipp(self,overwrite=False, force_fetch=False):

		self.fxn = self.getfxn()


		if self.PRNT: sysout('\n> Requesting to fetch ip proxies...')


		# If allowed by class:

		if self.__FLAG_ALLOW_IPP_FETCH:


			# [1] Filter ipp check time (limit for freq of checking updates - if too often return false and disallow)

			if not (time.time() - self.last_check_ipp_update) >= self.check_lim_update_sec and not force_fetch:

				if self.PRNT: sysout(' | DENIED - see time limit')
				return False



			# [2] Check request time is beyond limit range - (NOTE: IPP data update may still fail -- time delays)

			if self.__check_update() or force_fetch:

				if self.PRNT: sysout(' | APPROVED...')



				# Iterate over source urls - scrape HTML data and parse for target data

				to_chk = []

				for i, u in enumerate(self.__ipp_urls):

					raw = BeautifulSoup(req.get(u).content, features='lxml')

					tbl = pd.read_html(str(raw.find('table', attrs={'id': 'proxylisttable'})))[0]
					tbl['Port'] = [str(x).split('.')[0] for x in list(tbl['Port'])[:-1]] + ['na']



					# Iterate over rows create list of values to try

					for r in range(len(tbl.index) - 1):


						# Filter (skip to next iteration) or append to @to_chk

						if tbl['Anonymity'][r] not in ['elite proxy', 'anonymous']:
							continue

						elif not self.exus and not tbl['Code'][r] == 'US':
							continue

						else:
							to_chk.append(mkstr([x for i, x in enumerate(list(tbl.loc[r])) if i < 2], sep_list=':'))



				# If overwrite - set data to cleaned proxy list or append proxies not already held - re-write memory

				self.ipp = to_chk if overwrite else self.ipp + [c for c in to_chk if not c in self.ipp]

				self.__io('w', 'ipp', self.ipp.copy())



				# Create backup of current IP proxies

				if self.PRNT: sysout('\n> Backing up ipp data')

				self.__backup('ipp')

				return True



			# [3] Check request timing was valid but update data request timing was not (and not forced)

			else:

				if self.PRNT: sysout('\n> DENIED - check data update req. invalid time')

				self.last_check_ipp_update = int(dtt.today().timestamp())
				return False

		else:

			if self.PRNT: self.error('IPP fetching disallowed --> fix in class')
			return False



	## Fetch request handler for ua tag data

	def __fetch_ua(self, force_fetch=False):

		self.fxn = self.getfxn()


		# Flag for allowing spyder to run (** Defaults to off - LEAVE OFF - target host server is sensitive)

		if self.__FLAG_ALLOW_UA_FETCH:


			# Filter for check request limit

			if not (time.time() - self.last_check_ua_update) >= self.check_lim_update_sec and not force_fetch:
				return False



			# Valid request attempt - try update

			elif self.__check_update() or force_fetch:



				# Valid update request - do update (iterate over operating system categories)

				urls = [self.__ua_base.format(x) for x in self.__ua_opsys]


				for i, u in enumerate(urls):



					# Iterating over pages - limiter active


					for p in range(self.__ua_lim_page):

						tbl = pd.read_html(
							str(BeautifulSoup(
								req.get(mkstr(u, p + 1)).content, features='lxml').find_all('table')))[0]



						# Apply filtering - if valid, check ua tag for duplicate entry - if unique, append


						for r in list(tbl.index):

							chk = list(tbl.loc[r])


							if all([chk[2:][j] == ['Web Browser', 'Computer', 'Very common'][j] for j in range(3)]):


								if not chk[0] in self.ua:
									self.ua.append(chk[0])



						# 'Speed bump' implemented to slow request sequence - per page

						time.sleep(2)



					# 'Speed bump' - per category

					time.sleep(5)



				# Rewrite memory (Always)

				self.__io('w', 'ua', self.ipp.copy())

				self.__backup('ua')

				return True



			# Valid request attempt but invald update attempt

			else:

				self.last_check_ua_update = int(dtt.today().timestamp())

				return False


		# Flag disallows fetching us tags (THIS SHOULD BE THE CASE)

		else:
			return False




	# endregion



	## Change memory paths (will adjust all needed memory paths accordingly - dir and filename required with '\' chars)

	def change_path(self, nfd=None, nfn=None, backup_path=None):
		self.fxn = self.getfxn()

		if isok(nfd) and isok(nfn) and isok(backup_path):
			self.__ipp_path, self.__ua_path = ('{}{}'.format(nfd, p)
											   for p in ['{}_ipp.ipp'.format(nfn), '{}_ua.ua'.format(nfn)])

			self.__ipp_graveyard = '{}{}_graveyard.ipp'.format(nfd, nfn)
			self.__ipp_logfile = '{}{}_log.ipp'.format(nfd, nfn)
			self.__update_path = '{}{}_updates.validator'.format(nfd, nfn)
			self.__data_backup_base = backup_path

		else:
			self.note('[!] Paths unchanged - all params must be valid')




	## Handle turning on/off spyder algo for scraping UA data (DEFAULT: off ** [!] DO NOT CHANGE AT THIS TIME)

	def wake_spyder_ua(self):
		self.__FLAG_ALLOW_UA_FETCH = True

	def sleep_spyder_ua(self):
		self.__FLAG_ALLOW_UA_FETCH = False




	## Clean (drop) proxies from list

	def ipp_clean(self, to_drop=None, as_is=False):

		self.fxn = self.getfxn()

		sysout('\n> Attempting to drop dead proxies from cache...')


		# Indicates self-correction (remove duplicates) --> update

		if not isok(to_drop):

			sysout('\n    > None given - removing any duplicates...')

			self.ipp = unique(self.ipp)
			self.__io('w','ipp',self.ipp.copy())

			sysout(' | OK')

			return


		# Clean cache IFF UNLOCKED - (NOTE: Does not try to unlock object --> call 'mask' family functions)

		if self.__IS_UNLOCKED:

			sysout('\n    > List to drop contains {} proxies'.format(len(to_drop)))


			# Gather unique ip values, count appearence in drop list, create filtered drop list by count filter (*)

			if not as_is:

				try:

					uid = unique(to_drop)

					sysout(' | {} are unique...'.format(len(uid)))

					uidc = [inList(x_uid, to_drop)['count'] for x_uid in uid]

					uidf = [uid[i] for i in range(len(uid)) if uidc[i] >= self.ipp_lim_drop_freq]

				# If an error thown, empty drop list

				except Exception:

					if self.PRNT:
						self.error('Bad values (drop list set to [] --> Nothing dropped)')

					uidf = []

			else:

				uidf = to_drop



			# Run cleaner

			if len(uidf) > 0:

				sysout(' | Dropping {}...'.format(len(uidf)))


				# Create a backup before cleaning

				self.__backup('ipp')



				# Add proxies to be dropped to the graveyard (with datetime tag), adjust ipp list and re-write memory cache

				self.__update_tracker('graveyard', uidf)

				self.ipp = [ip for ip in self.ipp if ip not in uidf]

				self.__io('w', 'ipp',self.ipp.copy())

				sysout(' | OK')


				# If length of post-drop ipp list is less than required (* see attributes) try fetching

				if len(self.ipp) < self.ipp_lim_min_pool:

					valid_fetch = self.__fetch_ipp()


					# If fetch request returned False (failed) or length is still invalid - Lock

					if not valid_fetch or len(self.ipp) < self.ipp_lim_min_pool:

						self.__IS_UNLOCKED = False

			else:

				sysout(' | No proxies failed >= {}, cache unchanged'.format(self.ipp_lim_drop_freq))



		# Exists only for cases where @err_show is True (primarily for testing)

		else:

			if self.PRNT:
				self.error('[X] IPProxy object is LOCKED')



	## Req 1 'mask' (dictionary of ua tag and ip proxie address)

	def mask(self, num_masks=1):

		self.fxn = self.getfxn()


		# [0] Check for access to pass resources

		if self.__access():

			# [1] If access is granted (unlocked or valid attempt to unlock) - try to create mask(s)

			num_masks = max(1, num_masks)

			try:

				iso_ipp = sample(self.ipp, num_masks, True, True)
				iso_ua = sample(self.ua, num_masks, True, True)

				if num_masks == 1:

					ipmask = {

						'head': {'User-Agent': iso_ua},
						'proxi': {'http': 'http://{}'.format(iso_ipp), 'https': 'https://{}'.format(iso_ipp)}
					}


				else:

					ipmask = [

						{
							'head': {'User-Agent': iso_ua[i]},
							'proxi': {'http': 'http://{}'.format(iso_ipp[i]),
									  'https': 'https://{}'.format(iso_ipp[i])}

						} for i in range(num_masks)]


			except Exception:

				if self.PRNT:
					self.fxn = self.getfxn()
					self.error('Bad request (data issue - check attributes)')

				return None


			# [2] If mask operation threw no errors, check if valid - if so, append to log file and return to caller

			else:

				if not isok(ipmask) or not isinstance(ipmask,(dict,list)):

					if self.PRNT:
						self.fxn = self.getfxn()
						self.error('Bad IPP mask creation')

					return None

				else:


					if isinstance(ipmask,dict):

						self.__update_tracker('log',
											  ['{}| {}'.format(
												  ipmask['proxi']['http'].strip('http://'),
												  ipmask['head']['User-Agent'])])


					else:

						self.__update_tracker('log',
											  ['{}| {}'.format(
												  x['proxi']['http'].strip('http://'),
												  x['head']['User-Agent']) for x in ipmask])


					# Return valid mask(s) to caller

					return ipmask




		# [-1] If access denied

		else:

			if self.PRNT:
				self.error('[X] IPProxy object is locked')

			return None



	## Request copy of current ip proxy and ua tag lists (Created for multi-threader usage)

	def get_mask_pool(self):

		self.fxn = self.getfxn()


		temp = None

		if self.__access():

			try:

				temp = {

					'ua': self.ua,
					'ipp': self.ipp
				}

			except Exception:

				self.fxn = self.getfxn()

				if self.PRNT:
					self.error('Failed to export mask pool - IPProxy unlocked - ERROR')

				return None

			else:

				if isok(temp) and isinstance(temp,dict):

					self.__backup('ipp','mask_data_export')
					return temp


				else:

					self.fxn = self.getfxn()

					if self.PRNT:
						self.error('Failed to export mask pool - IPProxy unlocked - ERROR')

					return None

		else:

			self.fxn = self.getfxn()

			if self.PRNT:
				self.error('[X] IPProxy object is locked')

			return None


# region > Testing and examples:


# Example 1

# ipp = IPProxy()
# ipp.PRNT = True
#
# ipp.ipp_clean()
#
# ipp_mask1 = None
# ipp_mask1 = ipp.mask(10000)
#
# ipp_pool1 = ipp.get_mask_pool()

# endregion