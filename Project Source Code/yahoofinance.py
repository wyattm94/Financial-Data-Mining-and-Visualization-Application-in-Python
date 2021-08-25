# -*- coding: utf-8 -*-
"""
Created on Thu Aug  9 00:16:17 2018

@author: Wyatt Marciniak
"""

import time
import os
import sys
import re
import gc
import io
import base64
import openpyxl
import datetime as dt
from datetime import datetime as dtt
import requests as req
import json
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from bs4 import BeautifulSoup
from helper import *
from StatsCore import *
from stockinfo import *

# a = open('file.txt','r')
# b = a.readlines()
# a.close()


# Quick Market Price Collector

import multiprocessing.pool as mpool
from multiprocessing.pool import ThreadPool as mtpool
from multiprocessing import Process, Queue
from itertools import product

def qk_agg_get(ticks,f='d',d0='1970-1-1',d1='2019-5-17',wide=False,
			   divs=False,to_json=False,out_fp='qkfetch_data',local=True):
	if wide:
		out = {}; time0 = time.time()
		for wf in ['d','w','m']:
			sysout('[qkget] Fetching : ',wf)
			out[wf] = mkdict(['price','div'],qk_agg_get(ticks,wf,d0,d1,False,
														divs,to_json,out_fp,local)[1:])
			sysout(' | OK')
			sysout(' - [RT] {t:5.3f} seconds\n'.format(t=round(time.time() - time0, 3)))
		return out

	if not isinstance(ticks,(list,tuple,)): ticks = [ticks]
	k,p,d = [],{},{}
	for t in ticks:
		sysout(' ',t)
		try: temp = yget_stock(t,f,d0,d1)
		except Exception:
			print(' [ERROR in fetching]'); continue
		else:
			k.append(t.upper())
			try:
				p[t.upper()] = pd.DataFrame(temp['price'])
				if divs: d[t.upper()] = pd.DataFrame(temp['dividend'])
			except Exception: continue
	if to_json:
		outconn = open("{0}_{1}.{2}".format(out_fp,mkstr('price_',f,' '),'json'),'w+')
		outconn.write(json.dumps({pn: p[pn].to_dict('list') for pn in p.keys()}))
		outconn.close()
		gc.collect()
		if divs:
			outconn = open("{0}_{1}.{2}".format(out_fp, mkstr('divs_', f, ' '), 'json'),'w+')
			outconn.write(json.dumps({pn: p[pn].to_dict('list') for pn in p.keys()}));
			outconn.close()
			gc.collect()
	else:
		dict2wb(mkstr(out_fp,'_',dt_tag(),'_price_',f,'.xlsx'),p)
		if divs: dict2wb(mkstr(out_fp,'_',dt_tag(),'_divs_',f,'.xlsx'),d)
	if local: return k,p,d

# dt_tag()
# aa=qk_agg_get(['dhi','tsla','coty'],divs=True,out_fp='tests/qkfetch',wide=True)

# Date Handlers
def yahoo_calc_date(d):
	if isinstance(d, (str, )):
		convd = [int(x.split(' ')[0]) for x in d.split('-')]
		if int(convd[0]) < 1970:
			return 0  # Special Case
		current = dtt(convd[0],convd[1],convd[2])
		return [str(x) for x in str(time.mktime(current.timetuple())).split('.')][0]
	else: return [str(x) for x in str(time.mktime(d.timetuple())).split('.')][0]


def yahoo_format_date(d):

	if not isinstance(d,list): d = [int(d)]

	else: d = [int(x) for x in d]


	# print(time.ctime(d[0]))

	return [
		dtt.strptime(
			time.ctime(x),
			'%a %b %d %H:%M:%S %Y').strftime('%Y-%m-%d_%H:%M:%S')
		for x in d]


# sss = inList('aa',['aa','bd','a','csda','cg','x__a','accc'])
# sss = 'http://developers.whatismybrowser.com/useragents/explore/operating_system_name/windows/'
# s1 = req.get(mkstr(sss,0)).content
# s2 = BeautifulSoup(s1,features='lxml')
# s3 = s2.find_all('table')
# tbl = pd.read_html(
# 				str(BeautifulSoup(
# 					, parser='html',features='lmxl')))[0]



# ua_fetch(plim=20)

# ua_fetch(plim=5)
# uatest = ua_load()





# a = {'equity':['aapl','dhi'],'etf':['spy']}
# a2 = json.dumps(a)

# a3 = {
# 	'equity':{
# 		'aapl':{
# 			'mkt_d':{
# 				'datec':[1,2,3,4,5,6,7,8,9,10],
# 				'dateu':[1,2,3,4,5,6,7,8,9,10],
# 				'data1':[3,3,3,4,5,6,6,6,5,5]},
# 			'blah':{
# 				'col1':[1,2,3],
# 				'col2':[1,2,3]}},
# 		'dhi':{
# 			'd1':{
# 				'a1':[1,2,3],
# 				'a2':[1,2,3]}}},
# 	'etf':{
# 		'etf1':{
# 			'etfd1':{
# 				'ee1':[1,2,3,4,5],
# 				'ee2':[1,2,3,4,5]}}}
# }
# a4 = json.dumps(a3)
# a5 = json.loads(a4)
def scramble_list(x): return sample(x,len(x),False,False)

# a = scramble_list([[1,2,3],[4,5,6],[7,8,9],[1],45,33,'aaaaaa',0.099002112])

def mt_asset_req(assets):
	# Create or Load Log File
	if os.path.isfile('warehouse/logfile.xlsx'): log = xb2py('warehouse/logfile.xlsx')
	else: log = create_log()

	# Iterate over asset dictionary
	for ac,ticks in assets.items():
		for t in ticks:

			# Check if ticker exists in log
			if t in list(log['tid']) or t in list(log['name']):
				if t in list(log['tid']): trow_id = inList(t, list(log['tid']))['index'][0]
				else: trow_id = inList(t, list(log['name']))['index'][0]

	start = '1970-1-1'; end = dtt.today()
	t0,t1 = (yahoo_calc_date(x) for x in [start,end])
	out = []
	for t in assets:
		base = 'https://query1.finance.yahoo.com/v8/finance/chart/{0}'.format(t.upper())
		for fk,fv in {'d':'1d','w':'1wk','m;':'1mo'}.items():
			url = ("""{}?&lang=en-US&region=US&period1={}&period2={}&interval={}
					&events=div%7Csplit&corsDomain=finance.yahoo.com""")
			url = url.format(base, t0, t1, fv)
			out.append((t,mkstr('mkt_',fk),url))
	return out

# mta1 = mt_asset_req([])
# mta2 = scramble_list(mta1)
#
# qq = mta2[0][2]
# qqr = req.get(qq)


# qqr1 = (qqr.elapsed.total_seconds(),qqr.cookies._now,str(dtt.utcfromtimestamp(int(qqr.cookies._now))),qqr.url,
# 		qqr.request.headers._store['user-agent'][1])
# qqr1




# ipp_fetch(overwrite=False)

# ipp_fetch()
# ipptest = ipp_load()

# ua_z = ua_load()
# ip_z = ipp_load()
#
# sample(ip_z,1)


def mt_fetch_helper(fobj):
	uacache,ippcache = ua_load(),ipp_load()
	ippheader = ippcache[0]
	res,resp,ipp,ua = None,None,None,None
	time_out = 3; success = False; to_drop=[] #currfetch = 1
	while not success:
		# Select a User-Agent and IP Proxy
		ua,ipp =  sample(uacache,1), sample(ippcache[1:], 1)
		head = {'User-Agent':ua}
		prox = {'http': mkstr('http://', ipp), 'https': mkstr('https://', ipp)}
		try: resp = req.get(fobj[2],proxies=prox,header=head,timeout=time_out)
		except Exception: to_drop.append(ipp); ippcache.remove(ipp)
		else:
			# res = resp.json()
			success = True
	return fobj[0],fobj[1],resp,to_drop


def mt_fetch_batch_handler(objs,mp=10):
	pool = mtpool(mp); out = pool.map(mt_fetch_helper, objs)
	pool.terminate(); gc.collect(); return out


def mt_fetch_main(objs,batch_size=10,mprocesses=10):
	sysout('\n> Starting fetch on ',len(objs),' URLs (Batch Size: ',batch_size,'):')
	t0,data_hold,res_dict,cb = time.time(),[],{},0
	while len(objs) > 0:
		# Iterate over batches of URLs
		tb = time.time()
		curr_batch = objs[:min(batch_size,len(objs))]
		sysout('\n    > Batch ',cb,' (',len(curr_batch),') ...'); cb += 1
		curr_fetch = mt_fetch_batch_handler(curr_batch,mprocesses) # MT Caller per Batch
		sysout(' | Finished in ', crt(tb), ' sec');  time.sleep(1)

		# Adjust IPP Cache file after each batch (Better resource handling - updates cache more frequently)
		ipp_clean(unique(unlist([x[3] for x in curr_fetch]))) # Collect to_drop Proxy Lists

		# Append op data to log


		# Store Data - Continue
		data_hold += [x[:3] for x in curr_fetch]  # Strips to_drop/resp data
		objs = objs[len(curr_batch):]  # Update input objs list for next batch (if applicable)

	sysout('\n> Fetching Completed in ',crt(t0),' sec')
	sysout('\n> Starting Data Extraction...'); t1 = time.time()
	for m in data_hold:
		if not m[0] in res_dict.keys(): res_dict[m[0]] = {}
		if 'mkt_' in m[1]:
			temp = {
				'price': extract_price_data(m[2]),
				'dividend': extract_div_data(m[2])
			}
			res_dict[m[0]][m[1]] = pd.DataFrame(temp['price']).sort_values('dateu')
			if m[1] == 'mkt_d':
				try: res_dict[m[0]]['div'] = pd.DataFrame(temp['dividend']).sort_values('dateu')
				except Exception: res_dict[m[0]]['div'] = False
		else: res_dict[m[0]]['other'] = True
	sysout(' | Finished in ',crt(t1),' sec')
	sysout('\n-- Full Runtime: ',crt(t0),' sec --')
	return res_dict

# Runner
# ftest1 = mt_fetch_main(mta2)

# def req_log()

# [Main] Get Market Data (OHLC + Volume + Adjusted Close, Dividends, Splits)
def yget_stock(t,f='d',start='1970-1-1',end=dtt.today()):

	# Handle dates, fetch data, extract data sets/return result

	d0 = start.split('-'); d1 = (str(end).split(' '))[0].split('-')

	d0 = d0[1]+'/'+d0[2]+'/'+d0[0]; d1 = d1[1]+'/'+d1[2]+'/'+d1[0]

	fetch = yahoo_get_data(t=t.upper(),f=f,start=start,end=end)


	return {
		'price': extract_price_data(fetch),
		'dividend': extract_div_data(fetch),
		'ticker': t,'start': d0,'end': d1,'freq': f
	}


# Parse Url - Fetch Raw Data
def yahoo_get_data(t,d='all',f='d',start='1970-1-1',end=dtt.today()):
	base = 'https://query1.finance.yahoo.com/v8/finance/chart/%s' % (t)

	# Error Handling on data/freq parameters + Calculations (dates)
	if not d in ['all','price','div','split']: d='price'
	if not f in ['d','w','m']: freq = '1d'
	else: freq = {'d':'1d','w':'1wk','m':'1mo'}[f]
	t0,t1 = (yahoo_calc_date(x) for x in [start,end])

	# Parse fetching URL and GET data response (return as .json)
	url = ("""{}?&lang=en-US&region=US&period1={}&period2={}&interval={}
		&events=div%7Csplit&corsDomain=finance.yahoo.com""")
	url = url.format(base,t0,t1,freq)

	res = req.get(url).json() # Returns JSON (Call from Server)
	return res


# Extract Data - Market Data + Dividend
def extract_price_data(r):
	dateu = r['chart']['result'][0]['timestamp']
	datec = yahoo_format_date(dateu)

	raw = r['chart']['result'][0]['indicators']
	adj = raw['adjclose'][0]['adjclose']
	prc = raw['quote'][0]

	keys = ['dateu','datec','open','high','low','close','volume','adjusted']
	vals = [dateu,datec,np.array(prc['open']),np.array(prc['high']),
		 np.array(prc['low']),np.array(prc['close']),np.array(prc['volume']),
		 np.array(adj)]

	return dict(zip(keys,vals))


def extract_div_data(r):
	try: raw = r['chart']['result'][0]['events']['dividends'] #['splits']
	except Exception: return False
	return {
		'dateu': np.array([raw[x]['date'] for x in sorted(raw)]),
		'datec': yahoo_format_date([raw[x]['date'] for x in sorted(raw)]) ,
		'amount': np.array([raw[x]['amount'] for x in sorted(raw)])
	}


# yyy = yget_stock('aapl','d','2019-07-19','2019-07-18')



# def build_mktdata_paths(ticker_objs):





def get_finmetrics(e,fid=None,sop=False):
	if sop: sysout('\n[*] Fetching Financial Data')
	if not isok(fid): fid = ['Q','Y']
	# if not isok(fid) or not isinstance(fid,list) or len(fid)>2 or len(fid)==0
	z,dq,dy,t_elap = {},None,None,time.time()
	base = 'https://stockrow.com/api/companies' \
		   '/{}/financials.xlsx?dimension=MR{}&section={}&sort=asc'
	for i,d in enumerate(['Metrics','Growth',
						  'Income%20Statement','Balance%20Sheet','Cash%20Flow']):
		for f in fid:
			if '%20' in d:
				d2 = d.split('%20');
				if sop: sysout('\n    > ',d2[0],'_',d2[1],' - ',f)
			else:
				if sop: sysout('\n    > ',d,' - ',f)
			temp = pd.read_excel(base.format(u_(e),f,d))
			temp.columns = ['metrics']+list(temp.columns)[1:]
			if i < 2: z[mkstr(d.lower(),'_',f.lower())]= temp
			elif i==2:
				z[mkstr('is_',f.lower())]= temp
				# Get last dates - indicates next update date
				temp_date = list(temp.columns)[-1]
				if f is 'Q': dq = (int(yahoo_calc_date(temp_date)),str(temp_date))
				else: dy = (int(yahoo_calc_date(temp_date)),str(temp_date))
			elif i == 3: z[mkstr('bs_', f.lower())] = temp
			else: z[mkstr('cf_', f.lower())] = temp
			if sop: sysout(' | OK - [runtime] ', np.round(time.time() - t_elap, 3), ' seconds')
			time.sleep(0.5)
		time.sleep(0.5)
	if sop: sysout('\n')
	return z,dq,dy

# ygstk = yget_stock('aapl',end='2019-02-12')
# yp1 = pd.DataFrame(ygstk['price'])
# ygstk2 = yget_stock('aapl',start='2019-02-11')
# yp2 = pd.DataFrame(ygstk2['price'])

# yf1 = yget_stock(t='amd')

# yf2 = yget_fx('usd/eur')
# yf1 = pd.DataFrame(yget_fx('usd/eur')['price'])
def yget_fx(t, f='d', start='1970-1-1', end=dtt.today()):
	url = 'https://query1.finance.yahoo.com/v8/finance/chart/'
	# Error Handling on data/freq parameters
	if not f in ['d', 'w', 'm']:
		print('Bad freq...')
		return None
	# Calculate url-parsed params
	def f_switch(f):
		switch = {'d': '1d', 'w': '1wk', 'm': '1mo'}
		return switch.get(f)
	freq = f_switch(f)
	t0 = yahoo_calc_date(start); t1 = yahoo_calc_date(end)
	# Adjust ticker to upper case + start date for pandas plotting
	t.upper()
	d0 = start.split('-'); d1 = (str(end).split(' '))[0].split('-')
	d0 = d0[1] + '/' + d0[2] + '/' + d0[0]; d1 = d1[1] + '/' + d1[2] + '/' + d1[0]
	# Parse FX 'ticker' input
	if '/' in t:
		split = t.split('/')
		if split[0] == 'USD':
			url_ticker = split[1] + '=x'
		else:
			url_ticker = split[0] + split[1] + '=x'
		url = url + url_ticker
	else:
		url = url + t; url_ticker = t

	# Parse fetching URL and GET data response (return as .json)
	end = ("""{}?&lang=en-US&region=US&period1={}&period2={}&interval={}
		&events=div%7Csplit&corsDomain=finance.yahoo.com""")
	url = end.format(url, t0, t1, freq)

	res = req.get(url).json()
	price	= extract_price_data(res)
	return {'price': price, 'dividend': None, 'call':t,'ticker':url_ticker,'start':d0,'end':d1,'freq':f}


def yf_estimates(t):
	# [1] Parse URL - Request - Response
	url = 'https://finance.yahoo.com/quote/%(t)s/analysis?p=%(t)s?sortname=marketcapitalizationinmillions&sorttype=1' % {'t':t.upper()}
	src = req.get(url).content
	raw = BeautifulSoup(src,'html')
	tbl = raw.findAll('table')[0:2]
	# [2] Extract Data from HTML page response
	result = list()
	for t in tbl:
		head0 = t.findAll('th')
		head1 = [h.span.string for h in head0]
		# print('Headers: ', head1)
		rows = t.findAll('tr')
		etbl = [[] for ti in range(0, len(head1))]
		for row in rows[1:]:
			elems = row.findAll('td')
			ehold = [e.string for e in elems]
			# print('Row: ', ehold)
			for i in range(0, len(ehold)):
				etbl[i].append(ehold[i])
		result.append({'head':head1,'body':etbl})
	return(result)
def yf_plot(sd,what='adjusted',how='mkt'): #mkt,period,growth
	def h_switch(h):
		switch = {'mkt':what+' market data',
			'period':what+' periodic returns (%)',
			'growth':what+' relative growth (% from time 0)'}
		return switch.get(h)
	def f_switch(f):
		switch = {'d':'daily','w':'weekly','m':'monthly'}
		return switch.get(f)
	def y_alter(s):
		if how == 'mkt':
			return s
		elif how == 'period':
			temp = []
			for i in range(1,len(s),1):
				temp.append((s[i]-s[(i-1)])/s[(i-1)])
			return temp
		else:
			temp = []
			for i in range(0,len(s),1):
				if i == 0:
					temp.append(0)
				else:
					temp.append((s[i]-s[0])/s[0])
			return temp

	tick = sd['ticker']
	freq = f_switch(sd['freq'])
	yraw = sd['price'][what]
	xraw = pd.date_range(start=sd['start'],end=sd['end'],periods=len(yraw)) #,periods=len(yraw));
	# xadj
	#  = 0
	yadj = y_alter(yraw)
	if len(yadj) < len(yraw):
		xadj = xraw[1:]
	else:
		xadj = xraw
	hadj = h_switch(how)

	ts = pd.Series(yadj,index=xadj)
	fig = plt.figure()

	ts.plot()

	plt.title(str(tick+' '+freq+' time series: '+hadj))
	# plt.close(fig)
	return fig

# dtt(time.time())

# dtnow()
# showdt(float(time.time()))
#
# dtt.today()
# dtt.fromtimestamp(time.time())
# from StatsCore import *
#
# gh=111
# isok(gh)
# ifelse(isok(gh),1,2)

# type(isok(gh))
# xdx=None
# ifelse(isok(xdx),1,0)

# Check if current time (UTC) or input time indicates open (US) markets


# aaa=is_between(2,[1,2,3],4)


def dtnow(as_groups=False,tchk=None):
	wd_ops = ['Mon','Tues','Wed','Thurs','Fri','Sat','Sun']
	if isok(tchk): curr = dtt.fromtimestamp(tchk)
	else: curr = dtt.today()
	wd = int(curr.weekday())
	cdy, cdm, cdd = tuple(int(x) for x in str(curr).split(' ')[0].split('-'))
	cth, ctm, cts = tuple(int(x) for x in str(curr).split(' ')[1].split('.')[0].split(':'))
	if as_groups:
		return (
			wd_ops[wd],
			"{0}-{1}-{2}".format(cdy,cdm,cdd),
			"{0}:{1}:{2}".format(cth,ctm,cts)
	)
	else: return (wd,cdy,cdm,cdd,cth,ctm,cts)
def market_open(tchk=None):
	try:
		if isok(tchk): dnow = dtnow(tchk=tchk)
		else: dnow = dtnow()
		w, ch, cm, cs = tuple(dnow[x] for x in (0,4,5,6))
		if not w in rng(-1,6) or not is_between(float(mkstr([ch,cm,'.',cs])),
												930,1600): return False
		else: return True
	except Exception: return False
# dtnow(True)
# xx = {x:{'a':[x,x,x,x],'d':[1,2,3,x]} for x in range(5)}

# dtt.fromtimestamp(int(time.time()))
# f=xb2py('warehouse/logfile.xlsx')
# f.sort_values('cid',inplace=True)

# uat2= uat1.copy()
# uat3 = uat2.to_json()





# clean_logfile(True)
# clean_logfile()

def update_profile(t,ac,data0,d,f,is_div=False):
	if ac in ['equity','etf','indices']:
		# Mutable and Scalable
		data = yget_stock(t,f,str(dtt.fromtimestamp(d)),dtt.today())
	elif ac=='fx': data = yget_fx(t,f,str(dtt.fromtimestamp(d)),dtt.today())
	else: return None

	if is_div: to_add = pd.DataFrame(data['dividend']).sort_values('dateu')
	else: to_add = pd.DataFrame(data['price']).sort_values('dateu')
	for r in list(range(len(to_add.index))):
		data0.loc[int(len(list(data0.index))+1)] = list(to_add.loc[r])

	if ac=='fx': return data0.sort_values('dateu'),data['ticker']
	else: return data0.sort_values('dateu')

# def update_ee_profile(t,d,f,ee0,is_div=False):
# 	temp = yget_stock(t,f,str(dtt.fromtimestamp(d)),dtt.today())
# 	if is_div: to_add = pd.DataFrame(temp['dividend']).sort_values('dateu')
# 	else: to_add = pd.DataFrame(temp['price']).sort_values('dateu')
# 	for r in list(range(len(to_add.index))):
# 		ee0.loc[int(len(list(ee0.index))+1)] = list(to_add.loc[r])
# 	return ee0.sort_values('dateu')

def fetch_ee_profile(ticker,ac,box=None,update=False,force_update=False,
					 testdates=None,updom=1800,updlim=7200,overwrite=False):
	if not isok(box): return None,None
	fp = 'warehouse/{0}/{1}.xlsx'.format(ac, ticker)

	if not update or overwrite:
		# Market Data and Dividends
		sysout('\n    > Mkt/Div Data')
		for f in ['d', 'w', 'm']:
			if isok(testdates): temp = yget_stock(ticker, f, testdates[0], testdates[1])
			else: temp = yget_stock(ticker, f, '1970-1-1', dtt.today())
			box[str('mkt_' + f)] = pd.DataFrame(temp['price']).sort_values('dateu')
			if f == 'd':
				try: box['div'] = pd.DataFrame(temp['dividend']).sort_values('dateu')
				except Exception: box['div'] = False
			time.sleep(0.5)
		sysout(' - OK')

		if ac == 'equity':
			# Key Metrics
			sysout('\n    > Key Metrics')
			box['key_metrics'] = get_keymetrics(ticker)

			# ETF exposure / Comps
			sysout(' | ETF Exp/Comps ')
			try: box['etf_exposure'] = ws_raw2df(get_etf_exposure(ticker),[3],local=True)
			except Exception: box['etf_exposure'] = False
			else:
				box['etf_exposure'].columns = ['tid','cid','perc_total']
			time.sleep(0.25)
			try: box['comps'] = ws_raw2df(get_competitors(ticker),[3],local=True)
			except Exception: box['comps'] = False
			else:
				box['comps'] = box['comps'].drop(list(box['comps'].columns)[4:],axis=1)
				box['comps'].columns = ['cid', 'tid', 'exchid','last','last_chg','volume',
										'today_high','today_low','52w_high','52w_low','pe',
										'mktcap'][:len(box['comps'].columns)]
			time.sleep(0.25)
			sysout(' - OK')

			# Financial Statements
			# dq,dy = None
			# temp,dq,dy = get_finmetrics(ticker)
			# for k,v in temp.items():
			# 	box[k] = v
			return box,fp #,dq,dy
		elif ac == 'etf':
			etfinfo_temp = etf_info_info(ticker)
			etfbd_temp = etf_info_breakdowns(ticker)
			etfinfo_temp.update(etfbd_temp)
			box.update(etfinfo_temp)
			return box,fp
		else: return box, fp
	else:
		sysout(' | Updating'); temp_box = box.copy()
		sysout('\n    > Handline Market/Dividend Data')
		for k,v in box.items():
			if 'mkt_' in k or 'div' in k:
				lastdate = int(v['dateu'][-1:])
				if 'mkt_' in k: f = k.split('_')[1].lower()
				else: f = 'd'
				if (market_open(lastdate) and not market_open(time.time())) or \
					all([market_open(lastdate),market_open(time.time()),time.time()-lastdate>=updom]) or \
					time.time()-lastdate>=updlim or force_update:
					if k=='div': temp_box[k] = update_profile(ticker,ac,v,lastdate,f,True)
					else: temp_box[k] = update_profile(ticker,ac,v,lastdate,f,False)
				else: continue
			else: continue
		sysout(' - OK')
		return temp_box,fp

# testfetch = fetch_ee_profile('aapl','equity',{},testdates=('1970-1-1','2017-1-1'))[0]
# testfetch2 = fetch_ee_profile('aapl','equity',testfetch,update=True,force_update=True)

# def update_fx_profile(t,d,f,fx0,is_div=False):
# 	temp = yget_fx(t,f,str(dtt.fromtimestamp(d)),dtt.today())
# 	to_add = pd.DataFrame(temp['price']).sort_values('dateu')
#
# 	for r in list(range(len(to_add.index))):
# 		fx0.loc[int(len(list(fx0.index))+1)] = list(to_add.loc[r])
# 	return fx0.sort_values('dateu'),temp['ticker']

def fetch_fx_profile(ticker,ac,box=None,update=False,force_update=False,
					 testdates=None,updom=1200,updlim=7200,overwrite=False):
	if not isok(box): return None, None, '-'
	fp = 'warehouse/{0}/{1}.xlsx'.format(ac, ticker.replace('/', '_'))

	if not update or overwrite:
		start, end, fx_ticker = '1970-1-1', dtt.today(),'-'
		for f in ['d', 'w', 'm']:
			try:
				if isok(testdates):
					temp = yget_fx(ticker, f, testdates[0], testdates[1])
				else:
					temp = yget_fx(ticker, f, '1970-1-1', dtt.today())
				box[str('mkt_' + f)] = pd.DataFrame(temp['price'])
			except Exception: box[str('mkt_' + f)] = False
			else:
				if f == 'd': fx_ticker = temp['ticker']
		sysout(' - OK')
		return box, fp,fx_ticker
	else:
		sysout(' | Updating '); temp_box = box.copy()
		for k, v in box.items():
			if 'mkt_' in k:
				lastdate = int(v['dateu'][-1:]); f = k.split('_')[1].lower()

				if (market_open(lastdate) and not market_open(time.time())) or \
					all([market_open(lastdate), market_open(time.time()), time.time() - lastdate >= updom]) or \
					time.time() - lastdate >= updlim or force_update:
					temp_box[k],fx_ticker = update_profile(ticker,ac,v,lastdate,f,False)
				else: continue
			else: continue
		sysout(' - OK ')
		return temp_box, fp, fx_ticker

# tf21 = fetch_fx_profile('usd/eur','fx',{},testdates=('1970-1-1','2017-1-1'))[0]
# tf22 = fetch_fx_profile('usd/eur','fx',tf21,update=True,force_update=True)

# clean_logfile()
# create_log()

def req_assets(assets,update=False,force_update=False,overwrite=False):
	out = {}
	for k,v in assets.items():
		# out[k] = {}
		for t in v:
			f_timer = time.time(); out[t.lower()] = fetch_asset(t,k,update,force_update,overwrite)
			f_timer = round(time.time() - f_timer, 3)
			sysout(' - [TIME] {t:5.3f} seconds'.format(t=f_timer))
	return out
def fetch_asset(ticker,ac,do_update=False,force_update=False,overwrite=False):
	# Pre-processing setup
	trow_id,flag_fetch,flag_update = None,False,False
	ticker,ac = ticker.lower(),ac.lower()
	asset_data = {}

	# Create or Load Log File
	if os.path.isfile('warehouse/logfile.xlsx'): log = xb2py('warehouse/logfile.xlsx')
	else: log = create_log()

	# If ticker logged, check if file exists as well, read into python else fetch/update
	if ticker in list(log['tid']) or ticker in list(log['name']):

		# Check tid or cid (fx) columns for row id
		if ticker in list(log['tid']): trow_id = isin([ticker], list(log['tid']))['id'][0]
		else: trow_id = isin([ticker], list(log['name']))['id'][0]
		fp = list(log['fp'])[trow_id]

		# If file not found (log error) or force overwrite, set to fetch (+ clean log)
		if not os.path.isfile(fp) or overwrite:
			flag_fetch = True
			if not os.path.isfile(fp): clean_logfile() # Clean log if true
		elif do_update or force_update: flag_update = True
		else:
			sysout('\n>> Ticker: ',ticker.upper(),' (',ac,') found - loading... ')
			asset_data = xb2py(fp)
			sysout(' - OK')
	else: flag_fetch = True

	# If flagged for fetching, pull the asset profile
	if flag_fetch:
		sysout('\n>> Ticker: ',ticker.upper(),' (',ac,') fetching... ')

		# Fetching
		fx_ticker,fp,dq,dy = '-','path/to/file.ext','-','-'
		if ac in ['equity','etf','indices']:
			asset_data,fp = fetch_ee_profile(ticker,ac,box=asset_data)
		elif ac == 'fx':
			asset_data,fp,fx_ticker = fetch_fx_profile(ticker,ac,box=asset_data)
		else:
			sysout('\n>> Other asset type')

		# Add or replace entry in logfile (update) + write data to memory
		if not isok(trow_id): trow_id = int(len(log.index))
		update_log(ticker,ac,log,trow_id,fx_ticker,fp,dq,dy)
		dict2wb(fp, asset_data)
		return asset_data
	elif flag_update:
		# Handles updating of datasets (append) - beta implementation
		fx_ticker,dq,dy = '-','-','-'
		if ac in ['equity','etf']:
			asset_data = fetch_ee_profile(ticker,ac,xb2py(fp),
										  update=True,force_update=force_update)
		elif ac == 'fx':
			asset_data,fx_ticker = fetch_fx_profile(ticker,ac,xb2py(fp),
										  update=True,force_update=force_update)

			# Add or replace entry in logfile
			if not isok(trow_id): trow_id = int(len(log.index))
			update_log(ticker, ac, log, trow_id, fx_ticker, fp, dq, dy)
			dict2wb(fp, asset_data)
			return asset_data
			# curr_wd, curr_d, curr_t = dtnow(True)
			# curr_dt = int(yahoo_calc_date(dtt.today()))
			# log.loc[trow_id] = [ifelse(ac == 'fx', fx_ticker, ticker),
			# 					ifelse(ac == 'fx', ticker, '-'),
			# 					ac, curr_wd, curr_d, curr_t, str(curr_dt), fp]
	return asset_data

# pp = 'https://query1.finance.yahoo.com/v8/finance/chart/DHI?symbol=DHI&period1=1561296600&period2=1561728599&interval=1m&includePrePost=false&events=div%7Csplit%7Cearn&lang=en-US&region=US&corsDomain=finance.yahoo.com'
#
# ipc = ipp_load()[1:]
# uac = ua_load()
# raw=[]
# for i in range(5):
# 	head = {'User-Agent': sample(uac,1)}
# 	prox = {'http': mkstr('http://', ipc[i]), 'https': mkstr('https://', ipc[i])}
# 	raw.append(req.get(pp,proxies=prox, headers=head, timeout=3))
#
# ff = pd.DataFrame(([1,2,3],[3,2,1]),columns=['a','b'])

def extract_intraday_data(r):

	r = r.json()
	dateu = r['chart']['result'][0]['timestamp']
	datec = yahoo_format_date(dateu)
	raw = r['chart']['result'][0]['indicators']['quote'][0]


	keys = ['dateu','datec','open','high','low','close','volume']
	vals = [dateu,datec,
			np.array(raw['open']),
			np.array(raw['high']),
			np.array(raw['low']),
			np.array(raw['close']),
			np.array(raw['volume'])]

	return dict(zip(keys,vals))


def fetch_intraday(fobj):
	uacache, ippcache = ua_load(), ipp_load()
	ippcache = ippcache[1:]
	time_out = 3; success = False; resp = None
	while not success:
		# Select a User-Agent and IP Proxy
		ua, ipp = sample(uacache, 1), sample(ippcache, 1)
		head = {'User-Agent': ua}
		prox = {'http': mkstr('http://', ipp), 'https': mkstr('https://', ipp)}
		try:
			resp = req.get(fobj[1], proxies=prox, headers=head, timeout=time_out)
		except Exception: ippcache.remove(ipp)
		else: success = True
	return fobj[0], resp



def build_intraday_datasets(reqpaths,maxbatch=10,maxproc=10):
	ipp_fetch(overwrite=True)
	data_catch = []; curr_batch = 0
	# Run MT
	while len(reqpaths) > 0:
		curr_batch += 1; ilim = min(maxbatch,len(reqpaths)); t0 = time.time()
		sysout('\n  > Batch: ',curr_batch,' (',ilim,' of remaining ',len(reqpaths),')...')

		# MT
		pool = mtpool(maxproc)
		out = pool.map(fetch_intraday, reqpaths[:ilim])
		pool.terminate(); gc.collect()
		data_catch += out

		if len(reqpaths) < maxbatch: reqpaths = []
		else: reqpaths = reqpaths[ilim:]
		sysout(' | Complete in ',np.round(time.time()-t0,3),' sec')
	return data_catch

def build_intraday_paths(ticker,freq='1m',afterhours=False):
	ndays, pull_day_max = 30, 5  # Max is actually 7
	# freq = '1m' if not isok(freq) or freq not in ['1m','2m','5m','10m','30m'] else freq
	url = mkstr(
		'https://query1.finance.yahoo.com/v8/finance/chart/',
		'{0}?symbol={0}'.format(ticker.upper()),
		'&period1={0}&period2={1}&interval=', freq.lower(), '&includePrePost=',
		str(afterhours).lower(),
		'&events=div%7Csplit%7Cearn&lang=en-US&region=US',
		'&corsDomain=finance.yahoo.com', sep=''
	)

	# Set start date
	ds = dtt.utcfromtimestamp(int(dtt.today().timestamp() - (86400 * ndays)))
	ds = int(dtt(int(ds.year), int(ds.month), int(ds.day), 9, 30, 00, 00).timestamp())

	# Parse lists of all  d0 and d1 elems respectively & Generate fetching URLs
	dt_subsets = [
		(
			ds + (i * (86400 * pull_day_max)),
			ds + ((i + 1) * (86400 * pull_day_max)) - 1
		)
		for i in range(int(np.round(ndays / pull_day_max, 0)))
	]

	date_chars = [
		(str(dtt.utcfromtimestamp(dts[0])), str(dtt.utcfromtimestamp(dts[1])))
		for dts in dt_subsets
	]
	return [url.format(dts[0], dts[1]) for dts in dt_subsets]

# dd = xb2df('warehouse/__intraday_mkt_data/logfile_intraday.xlsx')

# region
def req_intraday_data(tickers, freq='1m', afterhours=False, maxbatch=10,maxproc=10):
	logfp, logfile = 'warehouse/__intraday_mkt_data/logfile_intraday.xlsx', None
	outfp = 'warehouse/__intraday_mkt_data/{0}.xlsx'
	try: logfile = xb2df(logfp)['log_main']
	except Exception: logfile = kv2df(['tid', 'last_update', 'fpath'], [[] for i in range(3)])

	# Create all paths - reorder list (of tuples to carry ticker id)
	req_paths = []
	for t in tickers:
		req_paths += [(t,x) for x in build_intraday_paths(t, freq, afterhours)]
	req_paths = sample(req_paths,len(req_paths),False,False)

	sysout('\n> Requesting Intraday Data...'); t0 = time.time()
	bids = build_intraday_datasets(req_paths,maxbatch,maxproc)
	# dict2wb('warehouse/testintradays.xlsx',bids)

	# Compile and clean data sets
	tags = [x[0] for x in bids]
	bids_df = {}
	for tag in tags:
		bids_df[tag] = pd.concat([pd.DataFrame(extract_intraday_data(r[1]))
									for r in bids if r[0] == tag]).sort_values(by=['dateu'],ascending=True)

	# .sort_values(by=['dateu'], inplace=True)
	# return final_out

	for k,v in bids_df.items():
		curr_fp = outfp.format(k.lower())
		if k in logfile['tid']:
			log_id = inList(k,logfile['tid'])['index'][0]
			df = pd.concat(v,xb2df(logfile['fpath'][log_id]),copy=False)
			df.sort_values(['dateu']); df.to_excel(curr_fp,'data',index=False)
		else:
			v.sort_values(['dateu']); v.to_excel(curr_fp,'data')
			log_id = len(logfile.index)

		logfile.loc[log_id] = [k.lower(), str(dtt.today()), curr_fp]
		logfile.to_excel(logfp, 'log_main', index=False)
		time.sleep(0.10); xb2df(logfp)

	sysout('\n> Requests Complete - Runtime: ', np.round(time.time() - t0, 3), ' sec')
	return bids_df
# endregion

# inList(2,[3,4,5])
# ridd = req_intraday_data(['dhi','amd','aapl','luv','fb','amzn','wmt'],maxproc=25)
# ridd['f'].iloc[3]
# ipp_fg = ipp_load()

# bids = build_intraday_dataset('dhi',maxproc=10)
# bids2 = bids.copy()
# bids3 = pd.concat([pd.DataFrame(extract_intraday_data(r)) for r in bids2])
# bids3.sort_values(by=['dateu'],inplace=True)
#
# # bids4.sort_values(by=['dateu'],inplace=True)
# # for b in bids3[1:]: bids4.append(b,ignore_index=True)
# df = pd.DataFrame()
# for i,r in enumerate(bids2):
# 	if i==0: df = pd.DataFrame(extract_intraday_data(r))
# 	else: df.append(pd.DataFrame(extract_intraday_data(r)),ignore_index=True)
# 	print(df.columns)
# 	print(len(df.index))


# aa = '{0} :D - D: {1}'.format(unlist(('a','b')))
# www=pd.DataFrame(extract_intraday_data(bids[0]))

# dtt.utcfromtimestamp(1561383000)
# eee = extract_intraday_data('aapl')

# bids = build_intraday_dataset('dhi')



# sorted([1,2,6,9,23,4,1],reverse=True)
#
# rassets2 = req_assets({'equity':['aapl','f']},overwrite=True)

# rassets = req_assets({'equity':['aapl','f','amd','Luv'],'etf':['spy','xlf'],'fx':['usd/eur','eur/usd']})

# fa1 = fetch_asset('dhi','equity')
# fa2 = req_assets(assets={'equity':['dhi','hpe'],'etf':['spy','xlf']})

# fa1['div'] = fa1['div'].sort_values('dateu')


# zzz = req_assets({'indices':['^vix']})

# def det_asset_class(t):
# 	if t.find('/') != -1: return 'fx'
# 	else:
# 		url = str('https://etfdailynews.com/etf/'+t.upper()+'/')
# 		tag = l_(wbget(url,'select',None,'.sixteen')[0].text)
# 		# raw = BeautifulSoup(req.get(url).content, features='html.parser')
# 		# tag = l_(raw.select('.sixteen')[0].text)
# 		if tag == 'stock': return 'equity'
# 		else: return tag
# def fetch_assets(tickers=[],by='guest',start='1970-1-1',end=dtt.today(),q=False,
# 				 local_cf=None):
# 	# [*] Setup files/dir
# 	t_elap0 = time.time(); dir_flag=True; upd_flag, skip_flag = False, False
#
# 	sysout('\n> Determining Asset Classes...')
# 	asset_req = {'equity':[],'etf':[],'fx':[]}
# 	for t in tickers:
# 		sysout('\n[Fetching Assets]: ',t)
# 		ac = det_asset_class(t)
# 		asset_req[ac].append(t)
# 	sysout(' Complete\n-----\n')
#
# 	if local_cf is not None: cflocal = local_cf
# 	else: globals()['cflocal'] = dict()
#
# 	# [*] Loop through asset types (x keys)
# 	# qso(q,'\nto ac loop 1\n')
# 	for aci,ac in enumerate(list(asset_req)):
# 		t = asset_req[ac] # Current AC tickers
# 		sleep_base = max(1, np.round(len(t) / 10)); sleep_curr = sleep_base; time_last = 0
# 		f_dir = str(pcf(cflocal,'path','warehouse')+l_(ac)+'/')
#
# 		# sysout('-----\n(!) Current Base Sleep Interrupt set to ', sleep_base, '\n-----\n')
# 		sysout('> Asset Class: ', ac, ' - ',(aci+1),'/',len(list(asset_req)),'\n-----\n')
#
# 		# [*] Loop through tickers in asset class
#
# 		if not isinstance(t,(list,)):
# 			temp_t = []; temp_t.append(t); t = temp_t
#
# 		for i, e in enumerate(t):
# 			sysout('  + ', e.upper(), ' (', str(i + 1), '/', len(t), ') ... ')
# 			cfn = str(f_dir+e.lower()+'.xlsx')
#
# 			writer = pd.ExcelWriter(cfn) # Writer - warehouse
# 			logcon = pd.ExcelWriter(pcf(cflocal, 'path', 'logfile'))  # writer - logfile
# 			logdf  = wb2dict(pcf(cflocal, 'path', 'logfile'))  # Copied log file (1 x 1)
# 			aclog  = logdf[ac]  # Current directory entries by AC filter
# 			ft0 = time.time()
#
# 			sysout('\n  + Directory Exists')
#
# 			t_in_dir = e.lower() in str(aclog['ticker'])
# 			sysout('  | ', u_(e), ' Found - ', t_in_dir, '\n')
#
# 			if t_in_dir:
# 				txr = float(np.where(aclog['ticker'] == e.lower())[0])
# 				txd = float(aclog.loc[txr, 'updated_u'])
#
# 				# Check for needed updates
# 				update_threshold = float(pcf(cflocal, 'ctrl',
# 										'update_time')) * 86400  # n days x seconds in 1 day
# 				if (float(yahoo_calc_date(dtt.today())) - txd) >= update_threshold:
# 					upd_flag = True
#
# 				if upd_flag:
# 					# new_t0 = str(yahoo_format_date([txd]))
# 					new_t0 = str(aclog.loc[txr, 'updated_c'])
# 					new_t1 = str(dtt.today())
# 				else:
# 					skip_flag = True  # NO UPDATE
# 					sysout('\n  + Update Possible: ', upd_flag)
#
# 			if skip_flag:  # Cache, increase iterators (i,e,etc...) by 1
# 				# if str(ac+'_'+e.lower()) not in list(cache):
# 				# 	sysout(' - Caching...\n-----\n')
# 				# 	# do_cache(e.lower(),ac,1,cflocal)
# 				continue # Next Iteration
#
# 			if upd_flag:  # Get copy of the old data, append to it then store/cache
# 				sysout('\n  + Copying Old Data\n-----\n')
# 				df0 = wb2dict(cfn)  # wb --> dict
#
# 			# [***] Data Scrape Start
# 			sysout('\t+ Price & Dividend Data:')
# 			for freq in ['d','w','m']:
# 				sysout('\n\t  + frequency: ',freq)
# 				ft0 = time.time()
#
# 				# [*] Update flag affects date range --> append new data to bottom of old DF
# 				# [*] Update flags affects write to excel, or append and write
# 				if upd_flag:
# 					sysout(' | Fetch Missing')
# 					temp = yget_stock(e,f=freq,start=new_t0,end=new_t1)
# 					local2 = True
# 				else:
# 					temp = yget_stock(e,f=freq,start=start,end=end)
# 					local2 = False
#
# 				for d in list(temp.keys()):
# 					if type(temp[d]) is dict:
# 						sn = str(e.lower() + '_' + d + '_' + freq)
# 						temp_dict = {'head':list(temp[d].keys()),'body':temp[d]}
#
# 						# Catch for local (when applicable)
# 						catch_upd = ws_raw2df(temp_dict,
# 											  list([0]+list(range(2,len(temp_dict['head'])))),
# 											  sort='dateu', xc_writer=writer, fp=None,
# 											  sn=sn,
# 											  local=local2)
#
# 						# [*] If updating, append DFs, re-write warehouse
# 						if upd_flag:
# 							sysout(' | Append Local')
# 							df0[sn] = df0[sn].append(catch_upd)
# 							dict2wb(writer,df0)
#
#
# 						time.sleep(2)
# 				sysout('  [*] Completed in ', np.round(time.time() - ft0, 3), ' seconds')
# 			sysout('\n-----\n')
#
#
# 			# Stats
# 			fts = time.time() - ft0
# 			# Equity stats
# 			if not upd_flag and ac not in ['etf','fx']: # Run only for new equity extractions
#
# 				sysout('\n> Rev/EPS Est...')
# 				snl = ['eps_est','rev_est']
# 				ft0 = time.time()
# 				temp = yf_estimates(e)
# 				for j,dtemp in enumerate(temp):
# 					ws_raw2df(dtemp,numcols=list(range(1,len(dtemp['body']))),xc_writer=writer,fp=None,
# 							  sn=str(e.lower() +'_'+ snl[j]))
# 				sysout('\t [*] Completed in ', np.round(time.time() - ft0, 3), ' seconds')
#
#
# 				sysout('\n> Mkt/Fin Metrics...')
# 				ft0 = time.time()
# 				get_finmetrics(e,xcon=writer)
# 				sysout(' [*] Completed in ', np.round(time.time() - ft0, 3), ' seconds')
#
#
# 				sysout('\n> ETF Exp (%)...')
# 				ft0 = time.time()
# 				ws_raw2df(get_etf_exposure(e),[2], xc_writer=writer, fp=None,
# 						  sn=str(e.lower() + '_etf_exp'))
# 				sysout('\t [*] Completed in ',np.round(time.time() - ft0, 3),' seconds')
#
#
# 				sysout('\n> Comp List...')
# 				ft0 = time.time()
# 				ws_raw2df(get_competitors(e),list(range(3,12)), xc_writer=writer, fp=None,
# 						  sn=str(e.lower() + '_comps'))
# 				sysout('\t\t [*] Completed in ', np.round(time.time() - ft0, 3),
# 					   ' seconds\n-----\n')
#
# 			# {*} Saving/Cachine/Logging
# 			writer.save()  # Flush sheets into workbook
# 			writer.close() # Close Connection (Memory Leaks/Perf. Issues)
# 			# do_cache(e.lower(), ac, 1,cflocal)
#
# 			# [*] Update (or add) log entry
# 			log_row = [e.lower(),float(yahoo_calc_date(dtt.today())),str(dtt.today()),
# 								str(by),str(cfn)]
#
#
# 			# if not dir_flag: logdf.loc[i] = log_row
# 			if upd_flag:
# 				# aclog.loc[float(np.where(aclog['ticker'] == e.lower())[0])] = log_row
# 				aclog.loc[aclog['ticker'] == e.lower()] = [log_row]
# 			else:
# 				tempdf = pd.DataFrame(log_row).transpose()
# 				tempdf.columns = list(aclog)
# 				aclog  = aclog.append(tempdf)
# 				logdf[ac] = aclog
# 				# return logdf
# 				dict2wb(logcon,logdf)
#
# 			# logcon.save()
# 			# logcon.close()
#
# 			# [2b] Determine sleep time adjustments (if needed)
# 			if i == 0: time_last = fts
# 			elif (fts-time_last)/time_last >= 0.25:
# 				sleep_curr += 1
# 				time_last = fts
# 				# sysout('(!) Speed Adjustment - Sleep Time Increased to ',sleep_curr,'\n')
# 			elif (fts-time_last)/time_last < 0:
# 				sleep_curr = max((sleep_curr - 1),sleep_base)
# 				time_last = fts
# 				# sysout('(!) Speed Adjustment - Sleep Time Decreased to ', sleep_curr, '\n')
# 			else:
# 				sleep_curr += 0
# 				time_last = fts
# 			time.sleep(sleep_curr)
#
# 			# # Re-writing/storing updated/loaded data & logs
# 			# if not dir_flag:
# 			# 	logdf.to_excel(logcon,sheet_name=ac,index=False)
# 			# else:
#
# 	# End (Full elapsed Time)
# 	sysout('-----\nTotal Operation Time: ',np.round(time.time()-t_elap0,3),' seconds\n-----\n\n')

# showdt()

# Get Financial Key metrics and Q/Y freq report summaries + growth (will later fetch ALL statements)



# frr = get_keymetrics('aapl')



# tt0 = time.time()
# gfin3 = get_finmetrics('dhi',fid=['Q'])
# print(np.round(time.time()-tt0,3))
#
# gfin2 = get_finmetrics('aapl','testfile.xlsx')
#
# gf1_names = [list(v['metric']) for k,v in gfin.items() if k is not 'key_metrics']
# gf2_names = [list(v['metrics']) for k,v in gfin2.items() if k is not 'key_metrics']

# for i in range(len(gf1_names)):
# 	print(len(gf1_names[i]),len(gf2_names[i]))
# 	print(gf1_names[i]==gf2_names[i])
#
# gf1_names[0]
# gf2_names[0]




# jerry_ticks = ['xlk','amzn','msft','aapl', 'csco','v']

# ooo = gfin[2]
# ooo2 = int(yahoo_calc_date(list(ooo)[-1]))
