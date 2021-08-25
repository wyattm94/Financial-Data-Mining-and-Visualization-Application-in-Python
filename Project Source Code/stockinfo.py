"""
Created on Sat Sep 29 17:16:20 2018

@author: Wyatt
"""

from helper import *
import os, sys, io, re, gc, base64
# import time, math, certifi, xlsxwriter, json
# import openpyxl as opx
# import random as rand
# import numpy as np
import pandas as pd
# import datetime as dt
# from datetime import datetime as dtt
import requests as req
from bs4 import BeautifulSoup
import matplotlib.pyplot as plt

# u='https://seekingalpha.com/etfs-and-funds/etf-tables'
# raw = BeautifulSoup(req.get(u).content, features='lxml')

# u = 'https://etfdailynews.com/etf/{0}/'.format('VOO')
# raw = BeautifulSoup(req.get(u).content, features='lxml')
# raw0 = [x.get_text().split(' - ')[1] for x in raw.find_all('h3',attrs={'class':'margin-top-more'})[1:3]]
# raw2 = [pd.read_html(str(x))[0] for i,x in enumerate(raw.find_all('table')) if i in [0,1,3,4]]
# raw2[1].columns = ['tid','name','etf_weight']
#
# rc_c0, rc_c1 = [],[]
# for i in [0,2,3]:
# 	rc_c0 += list(raw2[i][0]); rc_c1 += list(raw2[i][1])
# for i,x in enumerate(rc_c1):
# 	if not x.find('%') == -1: rc_c1[i] = np.round(float(x.strip('%'))/100,3)
# 	elif not x.find('$') == -1: rc_c1[i] = np.round(float(x.strip('$')),2)
# 	else: continue
#
# rcomb = kv2df(['metric','value'],[rc_c0,rc_c1])
# mdd2 = mkdict(['stats','holdings'],[rcomb,raw2[1]])
# raw22 = [raw2[0]]
#
# raw2[3][0]
#
# raw23 = raw.find_all('table')
# raw2
#
# raw21 = pd.read_html(str(raw2))[0]
# rawlist = [pd.read_html(str(x))[0] for x in raw2]




def etf_info_info(e):
	url = 'https://etfdailynews.com/etf/{0}/'.format(e.upper())
	raw = BeautifulSoup(req.get(url).content, features='lxml')
	raw2 = [pd.read_html(str(x))[0] for i, x in enumerate(raw.find_all('table')) if i in [0, 1, 3, 4]]
	raw2[1].columns = ['tid', 'name', 'etf_weight']

	rc_c0, rc_c1 = [], []
	for i in [0, 2, 3]:
		rc_c0 += list(raw2[i][0]); rc_c1 += list(raw2[i][1])
	for i, x in enumerate(rc_c1):
		if not x.find('%') == -1: rc_c1[i] = np.round(float(x.strip('%')) / 100, 3)
		elif not x.find('$') == -1: rc_c1[i] = np.round(float(x.strip('$')), 2)
		else: continue

	stat_comb_df = kv2df(['metric', 'value'], [rc_c0, rc_c1])
	return mkdict(['stats', 'holdings'], [stat_comb_df, raw2[1]])
def etf_info_breakdowns(e):
	# holdlims=(0.01,5)
	base = 'https://etfdb.com/etf/{0}/#etf-holdings&sort_name=weight&sort_order=desc&page={1}'
	url = base.format(e.upper(),1)
	raw = BeautifulSoup(req.get(url).content, features='lxml')
	tags = [x.get_text().lower().replace(' ','_') for x in raw.select('h3.h4')[9:15]]

	# Breakdowns
	data =  [pd.read_html(str(x))[0] for x in raw.select('table.chart.base-table')]
	for i,d in enumerate(data):
		data[i]['Percentage'] = [float(x.strip('%'))/100 for x in d['Percentage']]
	# Holdings
	# sysout('\n>> Fetching ETF Top Holdings')
	# htemp,wtemp,currp = [],[],1
	# htbl = pd.read_html(str( raw.find_all('table')[4]))[0]
	# htemp += list(htbl['Holding'])[:-1]
	# wtemp += [float(x.strip('%'))/100 for x in list(htbl['Weighting'])[:-1]]
	# while not any([x < holdlims[0] for x in wtemp]) and currp <= holdlims[1]:
	# 	time.sleep(0.5); currp += 1; url = base.format(e.upper(),currp)
	# 	raw = BeautifulSoup(req.get(url).content, features='lxml')
	# 	htbl = pd.read_html(str(raw.find_all('table')[4]))[0]
	# 	htemp += list(htbl['Holding'])[:-1]
	# 	wtemp += [float(x.strip('%')) / 100 for x in list(htbl['Weighting'])[:-1]]
	# holdings = kv2df(['Holding','Weighting'],[htemp,wtemp])
	# sysout(' | OK')
	#
	# out_k = tags + ['Top_Holdings']
	# out_v = data + [holdings]
	# sysout('\n>> Returning ETF Breakdown/Holding\n')
	# return mkdict(k = out_k, v = out_v)

# rr = etf_info_breakdowns('SPY')
# r2d2 = rr.find_all('table')[4]
# r2d22 = pd.read_html(str(r2d2))[0]
#
#
# rrtest = etf_info_breakdowns('SPY')
# rr2 = [x.get_text() for x in rr.select('h3.h4')[9:15]]
# rr3 = rr2[0]
# rr4 = pd.read_html(str(rr3))[0]
	return mkdict(tags,data)

# etfinfo_test = etf_info_info('SPY')
# etfbd_test = etf_info_breakdowns('SPY')
#
# etfinfo_test.update(etfbd_test)

# e = 'dhi'
# u = str('https://www.finviz.com/quote.ashx?t=' + l_(e))
# raw = BeautifulSoup(req.get(u).content,features='lxml')
# r2 = raw.select('table.fullview-ratings-outer')
# r4 = [x.get('href') for x in raw.select('a.tab-link-news')]
#
# r4_2 = [x.get_text() for x in r4]
# r2_a = pd.read_html(str(raw.select('table.fullview-ratings-outer')[0]))[0]
#
# r3 = raw.select('table.fullview-news-outer')
# r5 = pd.read_html(str(r3[0]))[0]
#
# raw_date = list(r5[0])
# for i,v in enumerate(raw_date):
# 	if v.find(' ') == -1 and i > 0:
# 		raw_date[i] = str(raw_date[i-1].split(' ')[0]+' '+v)
# r5[0] = raw_date
# r5[2] = r4
#
# ee = list(r5[0])[0]
# ee2=dtt.strptime(ee,'%b-%d-%y %I:%M%p')
# ee3=int(ee2.timestamp())
#
# str(ee2)


def get_assetnews(t,local_html=None):
	if not isok(local_html):
		raw = BeautifulSoup(
			req.get(
				str('https://www.finviz.com/quote.ashx?t=' + t.lower())).content,
			features='lxml')
	else: raw = local_html

	news_table = pd.read_html(str(raw.select('table.fullview-news-outer')[0]))[0]
	raw_date = list(news_table[0])

	for i, v in enumerate(raw_date):
		if (v.find(' ') == -1 or len(v) < 10) and i > 0:
			raw_date[i] = str(raw_date[i - 1].split(' ')[0] + ' ' + v)

	news_table[2] = [x.get('href') for x in raw.select('a.tab-link-news')]
	news_table[0],news_table[3] = clean_dates(news_table[0],'%b-%d-%y %I:%M%p')
	news_table.columns = ['datec','article_title','article_url','dateu']

	return news_table

# asset_news_test1 = get_assetnews('aapl')

def get_keymetrics(e):
	sysout('\n[*] Fetching Key Metrics'); time0 = time.time()
	z = {}
	urls = [str('https://stockrow.com/api/companies/' + l_(e) + '.json?ticker=' + l_(e)),
			str('https://www.finviz.com/quote.ashx?t=' + l_(e))]
	temp, temp2, temp3 = None, None, None
	for i, u in enumerate(urls):
		if i == 0:
			temp = req.get(u).json()
			# return temp
			# temp = pd.read_json(u).transpose()
			temp2 = pd.DataFrame(
				{
					'metrics': [k for k in temp.keys()][:-2],

					# 'values': list(temp[temp.columns[1]])}).drop([9, 10])

					'values': [v for k,v in temp.items()][:-2]
				})
		else:
			raw = BeautifulSoup(req.get(u).content,features='lxml')
			temp3 = temp2.append(pd.DataFrame(
				{'metrics': [x.get_text() for x in raw.select('.snapshot-td2-cp')],
				 'values': conv_s2n([x.get_text() for x in raw.select('.snapshot-td2')], ['AMC',
																						  'AMO',
																						  'BMO',
																						  'BMC'])}), ignore_index=True)
		time.sleep(0.5)
	sysout(' | OK - [Time] ',np.round(time.time()-time0,3),'seconds')
	return temp3

# gkk = get_keymetrics('aapl')

def get_etf_exposure(t):


	# Raw HTML parsing from url

	url = 'https://etfdailynews.com/stock/%s/' % (t.upper())
	src = req.get(url).content
	raw = BeautifulSoup(src, features='html.parser')
	tbl = raw.find("table", {"id": "etfs-that-own"})


	# return(tbl)
	# Headers (Column names)

	head0 = tbl.findAll('th')
	head1 = [h.string for h in head0]


	# print('Headers: ',head1)
	# Values (Column values)

	rows = tbl.findAll('tr')
	etbl = [[] for t in range(0, 3)]

	for row in rows[1:]:

		elems = row.findAll('td')
		ehold = [e.string for e in elems]
		etbl[0].append(ehold[0])
		etbl[1].append(ehold[1])
		etbl[2].append(ehold[2])


	# edict = dict(zip(head1,etbl))

	tdict = dict(zip(['head', 'body', 'iter'], [head1, etbl, [i for i in range(0, len(etbl[0]))]]))

	# return(etbl)

	return (tdict)


def get_competitors(t):
	url = 'https://www.nasdaq.com/symbol/%s/competitors' % (t.upper())
	src = req.get(url).content
	raw = BeautifulSoup(src, features='html.parser')
	tbl = raw.findAll('table')[2]

	# Head (Column names)
	head = [re.sub('[\t\n\r\xa0▲\xa0▼" "":"]+', ' ', e.get_text().strip())
			for e in tbl.findAll('th')]
	# print(head)
	head = head[0].split(' ') + head[1:4] + head[4].split('/') + \
		   head[5].split(' / ') + head[6:]
	head = head[0:7] + ["Todays's" + head[7], head[8], "52 Weeks " + head[9], head[10].strip(' '),head[11]]
	# print(head)
	# print(len(head))

	# Values (Columns values)
	vals = [[] for ti in range(0, len(head))]
	for itr, etr in enumerate(tbl.tbody.findAll('tr')):
		# print('>> (tr): '+str(itr))
		etd = etr.findAll('td')
		loc = 0
		for itd, etd in enumerate(etd):
			# print('>> (td): '+str(itd))
			ess = etd.stripped_strings
			for istr, v in enumerate(ess):
				# print('>> String:'+str(loc)+':'+v)
				if loc == 0:
					vals[loc].append(v)
				else:
					vals[loc].append(re.sub('[\t\n\r\xa0▲\xa0▼" "":"]+', '', v))
				loc += 1

	tdict = dict(zip(['head','body','iter'],[head,vals,[i for i in range(0,len(vals[0]))]]))
	return(tdict)

def get_yc():
	url = 'https://www.treasury.gov/resource-center/data-chart-center/interest-rates/Pages/TextView.aspx?data=yield'
	src = req.get(url).content
	raw = BeautifulSoup(src, 'html')
	tbl = raw.findAll('table')[0]
	# Head (Column Names)
	head = [elem.string.strip() for elem in tbl.findAll('th')]
	head.remove('2 mo')
	# Contents
	vals = [[] for i in range(0, len(head))]
	# print(len(vals))
	for row in tbl.findAll('tr')[2:]:
		loc = 0
		for ei,elem in enumerate(row.findAll('td')):
			if ei == 2 or elem.string is None: # 2 month (2 mo) note yield
				continue
			vals[loc].append(elem.string)
			loc += 1
	# Yield Curve Plot (Most recent date)
	curr_date = vals[0][-1]
	curr_yields = []
	for y in vals[1:]:
		curr_yields.append(float(y[-1]))

	# print(head[1:])
	# print(len(head[1:]))
	# print(curr_yields)
	# print(len(curr_yields))

	ts = pd.Series(curr_yields,index=head[1:])
	fig = plt.figure(); ts.plot()
	plt.title('US Treasury Yield Curve as of: '+curr_date)
	img = io.BytesIO(); plt.savefig(img,format='png'); img.seek(0)
	plt_img = base64.b64encode(img.getvalue()).decode()

	# gc.collect()

	# Fill output disctionary and return it
	tdict = dict(zip(['head','body','plot','iter'],[head,vals,plt_img,[i for i in range(0,
																					len(vals))]]))
	return(tdict)
def get_centralrates():
	url1 = 'https://www.investing.com/central-banks/'
	src = req.get(url1, headers={'User-agent':'Mozilla/5.0'}).content
	raw  = BeautifulSoup(src,'html')
	tbl = raw.findAll('table')[0]
	# Head (Column Names)
	head = []
	for th in tbl.thead.findAll('th'):
		ss = th.stripped_strings
		for elem in ss:
			head.append(elem)
	head.insert(1, 'Symbol')
	# print(head)
	# Values (Column Content)
	vals = [[] for i in range(0, len(head))]
	for tr in tbl.findAll('tr')[1:]:
		loc = 0
		for elem in tr.stripped_strings:
			vals[loc].append(elem)
			loc += 1
	# print(vals)
	tdict = dict(zip(['head', 'body', 'iter'], [head, vals, [i for i in range(0, len(vals[0]))]]))
	return (tdict)

# Youtube (Download URLs from youtube music playlist)
# u = 'https://www.youtube.com/playlist?list=PLncEkcBIz172_2YIWgP5gPvgGprCnKXqD'
# raw = BeautifulSoup(req.get(u).content, features='lxml')
# ff = [mkstr('https://www.youtube.com/watch?',x.get_attribute_list('href')[0].replace('/watch?',''),'\n')
# 	  for x in list(raw.find_all('a',attrs={'class':'yt-uix-sessionlink','aria-hidden':'true'}))]
# ffconn = open('youtube.txt','w+')
# ffconn.writelines(ff)
# ffconn.close()




