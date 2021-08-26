'''
______________________________________________

Script  : finapp.py
Type   : Application  - Dash
Root   : finapp/root_app
Author  : Wyatt Marciniak
Date  : 2019-05-09
______________________________________________
'''

# region >> Dependancies
import sys
import base64
import os
import math
import xlrd
from helper import *
from fred_api import *
from app_utilitiles import *
# from custom_classes import *
from StatsCore import *
import numpy as np
import pandas as pd
# import openpyxl as opxl
import dash
import dash_daq as daq
# daq._components
# daq.Tank()
# dash.__version__
import dash_renderer
import dash_table as dtbl
from dash_table.Format import Format, Scheme, Sign, Symbol
import dash_core_components as dcc
import dash_html_components as h
from dash.dependencies import Output as ddo
from dash.dependencies import Input as ddi
from dash.dependencies import State as dds
import datetime as dt
from datetime import datetime as dtt
import plotly
import plotly.plotly as ppy
from plotly import graph_objs as go
import multiprocessing.pool as mpool
from multiprocessing.pool import ThreadPool as mtpool
from multiprocessing import Process, Queue
from itertools import product
import openpyxl as opx
# import flask
# from app import app as f_app
# from proxy_mask import *
# endregion -----

gc.collect()
sysout('\n>> Application Spinning Up... ')

# region >> Local Toolkit

# Operational Tools
def map_srow(df,targ,sr,index_by=None):
    if isok(index_by): df.index = df[index_by]
    else: df.index = [i for i in range(len(df.index))]
    return df.loc[sr][targ]
def tojson(cn=None,cv=None,df=None):
    if isok(df) and isinstance(df,pd.DataFrame):
        return df.to_json(date_format='iso',orient='split')
    else:
        return pd.DataFrame(dict(zip(cn,cv))).to_json(date_format='iso',orient='split')
def fromjson(jd):
    return pd.read_json(jd, orient='split')
def parse_asset_in(x):
    # or not istype(x, 'str')
    if x in ['',' ','--']:
        # print(x)
        # print(['',' ','--'])
        # print(x in ['',' ','--'])
        # print(x in [' ','--'])
        return []
    else:
        x.strip()
        # print(x.split(','))
        if not x.find(',') == -1: return [a.strip().upper() for a in x.split(',')]
        else: return [x.upper()]

def non_local_cache(cache,to_add,logfp=None,dbfp=None):
    pipe_log = ifelse(not bool(isok(logfp)),)

# Tools to add common elems
def appH(n, cn, id, txt):
    ptags = (mkstr(cn, '_wrap '), mkstr(id, '_wrap '))
    return h.Div(className=ptags[0], id=ptags[1],
                 children=[
                     eval("h.H{0}(className='{1}',id='{2}',children=['{3}'])"
                          .format(n, cn, id, txt))
                 ])
def appIN(cn,id,cval,t='text'):
    toe = "h.Div(className='{0}',id='{1}',children=["
    toe += "dcc.Input(className='{2}',id='{3}',type='{4}',value='{4}')"
    toe += "])"
    return eval(toe.format(mkstr(cn, '_wrap '), mkstr(id, '_wrap '),cn, id,cval,t))
def appDD(cn,id,ops,cval,chold=None,clear=False):
    toe = "h.Div(className='{0}',id='{1}',children=["
    toe += "dcc.Dropdown(className='{2}',id='{3}',options={4},"
    if isok(chold):
        e5 = chold; toe += "placeholder='{5}'"
    else:
        e5 = cval; toe += "value={5}"
    toe += ",clearable={6})])"
    return eval(toe.format(mkstr(cn, '_wrap '),mkstr(id, '_wrap '),
                           cn,id,'ops','e5',clear))
def appDT(id,tc,td,ts_table,ts_cell,srows,s_as_list=True):
    return dtbl.DataTable(
        id=id,
        columns=tc,
        data=td,
        editable=False,
        filtering=False,
        sorting=False,
        sorting_type="multi",
        row_selectable="multi",
        row_deletable=False,
        style_table=ts_table,
        style_cell=ts_cell,
        style_cell_conditional=[],  # tp_scc,
        # n_fixed_rows=1,
        # n_fixed_columns=2,
        selected_rows=srows,
        style_as_list_view=s_as_list)
def appBTN(cn,id,lab,nc=0,nct=0):
    ptags = (mkstr(cn, '_wrap '), mkstr(id, '_wrap '))
    return h.Div(className=ptags[0], id=ptags[1],
                 children=[
                     h.Button(className=cn, id=id, children=[lab],
                              n_clicks=nc, n_clicks_timestamp=nct)
                 ])
def addScatterPlot(df,xcol,ptype,ptag,pfont=12,pcol='black'):
    x_values = list(df[xcol])
    df.drop([xcol],1,inplace=True)
    xnames = list(df.columns)

    data = [go.Scatter(
        x=x_values,y=df[cn],mode=ptype,name=xnames
        ) for cn in xnames]
    layout = go.Layout(
        title={'text': ptag, 'font': {'size': pfont, 'color': pcol}},
    )
    return dict(data=data,layout=layout)
def ifg(x,y=None,defv=''):
    try:
        if isok(x): return x
        elif isok(y): return y
        else: return defv
    except Exception: print(' ')

def dtnow():
    curr = dtt.today(); wd = int(curr.weekday())
    cdy, cdm, cdd = tuple(int(x) for x in str(curr).split(' ')[0].split('-'))
    cth, ctm, cts = tuple(int(x) for x in str(curr).split(' ')[1].split('.')[0].split(':'))
    return (wd,cdy,cdm,cdd,cth,ctm,cts)
def market_open(chk=None):
    try:
        dnow = dtnow()
        dsub = tuple(dnow[x] for x in (0,4,5))
        if isok(chk): w,ch,cm = (dsub[x]-chk[x] for x in (0,4,5))
        else: w,ch,cm = dsub
        if w > 5 or ch < 9 or ch > 16: return False
        elif ch == 9 and cm < 30: return False
        elif ch == 16 and cm >= 0: return False
        else: return True
    except Exception: return False


def to_cache(cache):
    sysout('\n>> Caching... ')
    to_cache = dict()
    if not isok(cache) or not istype(cache,dict):
        sysout('\n[ERROR] Cache input must be dict (or missing - None) ')
        return None
    else:
        # ck = list(cache.keys())
        for i,(k,v) in enumerate(cache.items()):
            if bool(istype(v,list,dict)) is True: to_cache[k] = v
            elif bool(istype(v,np.ndarray)) is True: to_cache[k] = list(v)
            elif bool(istype(v,pd.DataFrame)) is True: to_cache[k] = v.to_dict()
            else:
                sysout('\n[ERROR on ',k,' --> Unknown Type - ',type(v))
                to_cache[k] = [v]

        to_return = json.dumps(to_cache)
        # logconn = open(path_data_out, 'w'); logconn.write(to_return)
        # logconn.close(); gc.collect()
        return to_return
def from_cache(js_cache,as_tables=False): return json.loads(js_cache)

#  endregion

# region >> Initialize App
# Initialize App - block callback (non-essential)
app = dash.Dash('__main__',assets_folder='finapp_assets')
app.config.supress_callback_exceptions = True
app.config.update({ 'routes_pathname_prefix':'','requests_pathname_prefix':''})
dir_db = 'finapp_assets/finapp_warehouse/app_sessions/'
# dir_image = 'finapp_assets/ref_images/'
# dir_archive = mkstr(dir_db,'archive/ ')

# Set Session Backend (ID and paths + files)
sysout(' | Setting up Session... ')
app_session_id = dt_tag()
# Temp and Overflow Sessions > Temps stores
path_log = '{0}{1}_log.json'.format(eval('dir_db'),eval('app_session_id'))
path_data = '{0}{1}_cache.json'.format(eval('dir_db'),eval('app_session_id'))





# a = ['equity','GOO']
#
# mkdfout(a[0],a[1])

# Creare Archive (if None) --> Always Pull
# archive0 = None
# if not os.path.isfile(path_archive):
#     sysout('\n>> Creating Archive ')
#     conn_arch = open(path_archive,'w+')
#     conn_arch.write("{0},{1},{2},{3},{4}".format('asset_class','ticker','upd_c','upd_u','datapath','\n'))
#     conn_arch.close()

# sysout('\n>> Pulling Archive ')
# conn_arch = open(path_archive,'r')
# archive0 = [s.strip('\n') for s in conn_arch.readlines()]
# conn_arch.close()

sysout('\n>> Creating Session logfile ')
temp = open(path_log, mode='w+')
temp.write("{0},{1},{2},{3}".format('id','asset_class','ticker','loaded','fetched','\n'))
temp.close()

del temp
gc.collect()

app_cache = {
    'asset_ticks': unlist(
        [
            ["XLF", "XLK", "XLV", "XLE", "XLI", "XLY", "XLP", "XLU", "XLB", "XBI"],
            ['GSPC', 'DJIA', '^IXIC', '^VIX', 'CJNK', 'ULST', 'STOT', 'TOTL']
        ]),
    'asset_names': unlist(
        [
            ['Financial', 'Technology', 'Health_Care', 'Energy', 'Industrial',
             'Consumer_Discretionary', 'Consumer_Staple', 'Utilities', 'Materials', 'Biotech'],
            [
                'S&P500', 'DowJones', 'Nasdaq', 'VIX', 'ICE BofAML US High Yield Bonds',
                 'BloombergBarclays UST Bellwether 3m Bonds',
                 'BloombergBarclays US Agg 1-3y Bonds',
                 'BloombergBarclays US Agg Bonds'
             ]
        ]),
    'asset_classes': unlist(
        [
            # From Above (Future --> Config File Data (Drop Local)
            ['etf' for e in range(10)],['index' for e in range(4)],['etf' for e in range(4)]
        ]),
    # Dictionaries or False
    'asset_data_price':[],
    'asset_data_div':[],
    # List as-is
    'asset_data_updated':[]
}

ticks_load = app_cache['asset_ticks']
names_load = app_cache['asset_names']
aclass_load = app_cache['asset_classes']

# for i,a in enumerate(ticks_load):
#     if aclass_load[i] in ['equity','etf']



# endregion

# region >> Define Fixed Variables
fetch_delay_in = 0.5
fetch_delay_mult = 2
fetch_delay_start = 10

# endregion

# region >> Define Selection Tool Options
# Data Options
dd_dcat = create_options(['Price Data','Dividends','Financials'],['price','div','financials'])
dd_dfreq = create_options(['Daily','Weekly','Monthly'],['d','w','m'])
dd_dtype = create_options(
    ['Open','High','Low','Close','Adjusted Close','Volume','Dividends'],
    ['open','high','low','close','adjusted','volume','dividends']
)
dd_drep = create_options(['Actual','Returns','Growth'],['raw','chg','growth'])
dd_table_sort = create_options(
    ['Selected','ID','Asset Class','Ticker'],
    ['Selected','ID','Asset_Class','Ticker']
)



# endregion

sysout(' | DONE - Rendering Layout... \n ')

### APP LAYOUT
app.layout = h.Div(className='appcontent',children=[

    # Storage (Sessions)
    h.Div(className='hidden_div', id='temp_dump', style={'display': 'none'}),
    h.Div(className='hidden_div', id='cache', style={'display': 'none'}),
    h.Div(className='hidden_div', id='h_cache_table', style={'display': 'none'}),

    # Header
    h.Header(className='header',id='headerp',children=[
        appH(1,'header_h','apptitle','Financial Analysis Toolkit (v1.5.0)')
    ]),

    # Main Content
    h.Div(className='maincontent',children=[

        # Left
        h.Div(className='col',id='mainleft_p',children=[

            # Available Symbols
            h.Div(className='row', id='cache_table_div',children=[
                appH(1,'table_elem','cache_table_h','Cached Assets'),
                h.Div(className='table_holder', id='cache_table_wrap',
                      children=[dtbl.DataTable(id='the_table')]),
                h.Div(className='table_ops',id='tb_allornoneorupd_p',children=[
                    appBTN('table_btn1','tb_all','All'),
                    appBTN('table_btn1','tb_all','Update'),
                    appBTN('table_btn1','tb_clear','Clear')
                ]),
                h.Div(className='table_ops',id='tb_sort_p',children=[
                    appDD('table_btn2','tb_sort_dd',dd_table_sort,dd_table_sort[0]['value']),
                    appBTN('table_btn2','tb_sort_asc','Asc'),
                    appBTN('table_btn2', 'tb_sort_desc', 'Desc')
                ])
            ])
        ]),

        # Center / Center + Right
        h.Div(className='col', id='mainright_p', children=[

            # (Top) Data Options Bar
            h.Div(className='data_ops_bar',id='header_ops_p',children=[

                     # d_loaders
                     h.Div(className='dopbar_elem_div', id='dbar_loader', children=[
                         appH(2,'dop_elem_h','dop_cp','Data Type'),
                         h.Div(className='dopbar_elem_col',id='div_readwrite',children=[
                             appBTN('dbtn_rw','d_reader','Read'),
                             appBTN('dbtn_rw','d_writer','Write')
                         ])
                     ]),

                     # dd_dtype
                     h.Div(className='dopbar_elem_p', id='dbar_dd_dtype_p', children=[
                         appH(2,'dop_elem_h','dop_dtype_h','Data Type'),
                         appDD('dopbar_elem','dd_dtype',dd_dtype,dd_dtype[4]['value'])
                     ]),

                     # dd_drep
                     h.Div(className='dopbar_elem_p', id='dbar_dd_drep_p', children=[
                         appH(2, 'dop_elem_h', 'dop_drep_h', 'Data Flavor'),
                         appDD('dopbar_elem', 'dd_drep', dd_drep, dd_drep[0]['value'])
                     ]),

                     # dd_freq
                     h.Div(className='dopbar_elem_p', id='dbar_dd_dfreq_p', children=[
                         appH(2, 'dop_elem_h', 'dop_dfreq_h', 'Data Freq.'),
                         appDD('dopbar_elem', 'dd_dfreq', dd_dfreq, dd_dfreq[0]['value']),
                     ]),

                     # dates (D0 : D1)
                     h.Div(className='dopbar_elem_p', id='dbar_dates_p', children=[
                         appH(2, 'dop_elem_h', 'dop_dates_h', '[D0 : D1]'),
                         h.Div(className='date_in_div',id='date_in_p',children=[
                                appIN('date_in','date_0','1970-01-01'),
                                appIN('date_in','date_1',str(dtt.today().today()).split(' ')[0])
                         ]),
                        appBTN('datebtn','max_range','Max Range'),
                        appBTN('datebtn','fit_range','Clear Range')
                        # appBTN('datebtn', '', 'Current Selected Date Range')
                         # appDD('dopbar_elem', 'dd_dfreq', dd_dfreq, dd_dfreq[0]['value']),
                     ])
             ]),

            # Body (Graphs)
            h.Div(className='plot_wrapper',id='main_plot_p',
                  children=[

                      dcc.Graph(className='mainplot',id='mplot1'),
                      dcc.Graph(className='mainplot',id='mplot2'),
                      dcc.Graph(className='mainplot',id='mplot3')
                      # h.Img(className='jrtag',id='jrtag_wrap',
                      #       src='data:image/png;base64,{0}'.format(app_images_loaded['jr_static']))
            ]),

            # Bottom (Above Footer)
            h.Div(className='row', id='the_stuff', children=[

            ]),

        ])

    ]),

    # Footer
    h.Footer(className='footer', id='app_footer',children=[
        # Selectors
        h.Div(className='footer_p_wrap',id='footer_message_wrap',children=[

        # Header (title)
         appH(1,'footer_h','footer_title','Add Assets to your Cached Memory (Table) From Here:'),

         # Stocks
         h.Div(className='symbol_row',id='symbolwrap_stock',children=[
             appIN('symin','stock_in',''),
             appBTN('symbtn','stock_fetch','Fetch Stock(s)')
         ]),

        # ETF
        h.Div(className='symbol_row', id='symbolwrap_etf', children=[
            appIN('symin', 'etf_in', ''),
            appBTN('symbtn', 'etf_fetch', 'Fetch ETF(s)')
        ]),

        # FX
        h.Div(className='symbol_row', id='symbolwrap_fx', children=[
            appIN('symin_fx', 'fx_num_in',''),
            appIN('symin_fx', 'fx_denom_in', ""),
            appBTN('symbtn_fx', 'fx_fetch', 'Fetch FX(s)')
        ])

       ])
    ])

])

# region >> Tool Value Handlers

# region >> Reset Buttons / Dropdowns
@app.callback(
    [
        ddo('stock_fetch','n_clicks'),
        ddo('etf_fetch', 'n_clicks'),
        ddo('fx_fetch', 'n_clicks'),
        ddo('tb_sort_asc', 'n_clicks'),
        ddo('tb_sort_desc', 'n_clicks'),
        ddo('stock_in', 'value'),
        ddo('etf_in', 'value'),
        ddo('fx_num_in', 'value'),
        ddo('fx_denom_in', 'value')
    ],
    [
        ddi('h_cache_table','children')
    ],
    [

    ]
)
def reset_buttons(hc):
    sysout('\n>> Resetting Button Click Values / Asset Input Fields ')
    hc =from_cache(hc)
    return 0,0,0,0,0,'','','',''
# endregion

# endregion

# region >> Update Cache
@app.callback(
    ddo('cache','children'),
    [
        ddi('stock_fetch','n_clicks_timestamp'),
        ddi('etf_fetch', 'n_clicks_timestamp'),
        ddi('fx_fetch', 'n_clicks_timestamp'),
    ],
    [
        dds('stock_fetch','n_clicks'),
        dds('etf_fetch', 'n_clicks'),
        dds('fx_fetch', 'n_clicks'),
        dds('cache','children'),
        dds('stock_in','value'),
        dds('etf_in','value'),
        dds('fx_num_in','value'),
        dds('fx_denom_in', 'value')
    ]
)
def update_cache(fstc,fetc,fxtc,fsnc,fenc,fxnc,c0,ins,ine,infn,infd):
    sysout('\n>> Handle Caching ')
    if not isok(c0):
        sysout('\n>> Making Cache ')
        cache = app_cache
        #mkdict(cache_elem_tags,[[] for i in range(len(cache_elem_tags))])
    else:
        sysout('\n>> Loading Cache ')
        cache = from_cache(c0)

    # Parse Valid Input (, = multiple) - stocks/etfs, for fx (num/denom makes ALL pairs)
    ticks,acs,to_write = [],[],[]
    if fsnc > 0:
        ticks = parse_asset_in(ins); acs = list(np.repeat('equity',len(ticks)))
    elif fenc > 0:
        ticks = parse_asset_in(ine); acs = list(np.repeat('etf',len(ticks)))
    else:
        fx_nums,fx_denoms = parse_asset_in(infn),parse_asset_in(infd)
        if len(fx_denoms) > 0 and len(fx_nums) == 0:
            ticks = ['{0}=X'.format(x.upper()) for x in fx_denoms]
        elif len(fx_denoms) == 0 and len(fx_nums) > 0:
            ticks = ['{0}USD=X'.format(x.upper()) for x in fx_nums]
        else:
            ticks = [['{0}{1}=X'.format(n,d) for d in fx_denoms if not d == n] for n in fx_nums]
        acs = list(np.repeat('fx', len(ticks)))

    sysout('\nCurrent Ticks (>> IN >>): [',ticks,'] ',sep=('',', ',''))
    delay_next = max((len(ticks)/5)*fetch_delay_mult,fetch_delay_in)

    for i,t in enumerate(ticks):
        if not t in cache['k']:

            # Add Info Data
            cache['k'].append(t)
            cache['ac'].append(acs[i])
            new_id,add_time = (len(cache['id'])+1),time.time()
            cache['id'].append(new_id)
            cache['upd'].append(add_time)
            cache['Selected'].append(True) # Selected By Default
            # if len(cache['id']) == 0: cache['id'].append(1)
            # else: cache['id'].append(int(list(cache['id'])[-1]+1))

            # Fetch Data
            for f in ['d', 'w', 'm']:
                sysout('\n>> Fetching: ',t,' (Class: ',acs[i],' / Freq: ',f,') ')
                try:
                    if acs[i] in ['equity','etf']: temp = yget_stock(t,f)
                    else:
                        print('hello')
                        temp = yget_fx(t,f)
                    cache[mkstr('p', f, ' ')].append(pd.DataFrame(temp['price']).to_dict('list'))
                    cache[mkstr('d', f, ' ')].append(pd.DataFrame(temp['dividend']).to_dict('list'))
                except Exception: sysout(' | Failed ')
                else:
                    sysout(' | Added ') # id,ac,t,date
                    time.sleep(fetch_delay_in)  # 0.5

            logconn = open(path_log, 'a+')

            time.sleep(delay_next) #0.5 for 1

    sysout('\n>> Sending updated cache {to_cache} ')
    logconn = open(path_log,'a')
    [logconn.writelines(to_write) for x in range(4)]
    logconn.close()
    return to_cache(cache)
# endregion

# region >> Update Hidden Ticker (Cache) Table (cache/sort --> tojson (DF) )
"""
Returns tojson
"""

@app.callback(
    ddo('h_cache_table','children'),
    [
        ddi('cache','children'),
        ddi('tb_sort_asc','n_clicks_timestamp'),
        ddi('tb_sort_desc', 'n_clicks_timestamp')
        # Add sort/filter buttons here
    ],
    [
        # dds('h_cache_table','children'),
        dds('tb_sort_dd','value'),
        dds('the_table', 'selected_rows'),
        dds('tb_sort_asc', 'n_clicks'),
        dds('tb_sort_desc', 'n_clicks')
    ]
)
def update_h_ctable(c,tsa,tsd,sddv,st,tsac,tsdc):
    sysout('\n>> Updating Hidden Cache Table + Rewrite Memory Cache\n')
    if not isok(c): return kv2df(['ID','Asset_Class','Ticker','Selected'],[[],[],[],[]])
    else:
        if not isok(st): st = [i for i in range(len(c['id']))]
        ct = kv2df(
            k=['ID','Asset_Class','Ticker','Selected'],
            v=[c['id'],c['ac'],c['k'],[True if x in st else False for x in st]]
        )

    # Reset selected rows on new, unsorted (etc..) DF
    curr_t = map_srow(ct,'Ticker',st)
    print('\nCurr. Tickers: ',list(curr_t))
    ct['Selected'] = [True if x in curr_t else False for x in ct['Ticker']]


    # (Re)Apply Sorting
    a = sddv; b = False
    if tsa > tsd: b = True
    to_eval = mkstr('ct.sort_values(["', a, '"], ascending= ', b, ') ')
    sysout('-----\n>> Sorting Cache Table: \n', to_eval, '\n')
    ct = eval(to_eval)

    return tojson(df=ct)

# endregion

# region >> Update Cache Table (UI Level - Main)

# region >> [I] Handle: Update Main Table Elem (Runs first)
@app.callback(
    ddo('cache_table_wrap','children'),
    [
        ddi('h_cache_table','children')
    ],
    [
        dds('cache','children')
    ]
)
def op_cachetable(hc,cc):
    sysout('\n>> Handling Table Reload ')
    if not isok(hc):
        sysout('\n>> [*] Hidden Cache : None ')
        if isok(cc):
            sysout('\n>> [**] Cache File : Used ')
            c = from_cache(cc)
            df = kv2df(
                k=['ID', 'Asset_Class', 'Ticker', 'Selected'],
                v=[c['id'], c['ac'], c['k'], [i for i in range(len(c['id']))]]
            )
        else:
            sysout('\n>> [**] NEW DF made ')
            return kv2df(['ID', 'Asset_Class', 'Ticker', 'Selected'],[[] for i in range(4)])
    else:
        df = fromjson(hc)
        sysout('\n>> [*] Hidden Cache : Loaded ')

    st = [x for x in range(len(df.index)) if bool(list(df['Selected'])[x]) is True]
    df = df.iloc[:,0:3]

    # Create Table data inputs
    columns = [{
                      'name': c,
                      'id': c,
                      'type': ifelse(i == 0, 'numeric', 'str'),
                      'format': ifelse(i < 1,
                                       Format(nully='na', precision=2),
                                       Format(nully='na'))
                      } for i, c in enumerate(list(df.columns))
    ]
    data = df.to_dict('rows'),

    tp_st = {
        # 'overflowX': 'scroll',
        'overflowY': 'scroll',
        'maxHeight': '350px',
        'maxWidth': '500px',
        'border': 'thin black solid'
    }
    tp_sc = {
       'minWidth': '10px',
       'maxWidth': '100px',
       'textOverflow': 'ellipsis',
       'overflow': 'hidden',
       'whiteSpace':'normal'
    }

    return appDT('the_table',columns,data,tp_st,tp_sc,st,True)

# endregion

# region >> [II] Handle: Selected (add other cond here) cell styling
@app.callback(
    ddo('the_table','style_cell_conditional'),
    [
        ddi('cache_table_wrap','children'),
        ddi('the_table','selected_rows')
    ],
    [
        dds('h_cache_table','children')
    ]
)
def style_tickers(st,ss,hc):
    if isok(st) and isok(ss):
        sysout('\n>> Handler: Cache table styles ')
        hc = fromjson(hc); rmax = len(hc.index)
        ns = [x for x in range(0,rmax) if x not in ss]
        print('\n-----\nss: ',ss,'\n-----')
        print('ns: ',ns,'  \n-----\n')
        return unlist([
            [{'if': {'row_index': s}, 'background-color': '#9effae'} for s in ss],
            [{'if': {'row_index': s}, 'background-color': '#ffd1d1'} for s in ns],
            [
                {'if': {'column_id': 'ID'}, 'width': '20%'},
                {'if': {'column_id': 'Asset_Class'}, 'width': '20%'},
                {'if': {'column_id': 'Ticker'}, 'width': '60%'}
            ]
        ])

# endregion

# region >> [III] Handle: Selected Rows (Add/Clear Buttons)
@app.callback(
    ddo('the_table','selected_rows'),
    [
        ddi('tb_all', 'n_clicks_timestamp'),
        ddi('tb_clear', 'n_clicks_timestamp')
    ],
    [
        dds('h_cache_table','children')
    ]
)
def chg_sel_tickers(a,c,hc):
    if isok(c) and isok(a) and cSum([c,a]) > 0:
        sysout('\n>> Handler: Cache table all/clear option ')
        hc = fromjson(hc); rmax = len(hc.index); ss = None
        if max(c,a) == a:
            ss = [x for x in range(0, rmax)]; ns = []
        if max(c, a) == c:
            ss = []; ns = [x for x in range(0, rmax)]
        sysout('\n>> [OK]: Handler {cache_table_selected} finished ')
        return ss
    else: sysout('\n>> [ERROR]: Handler {cache_table_selected} failed ')

# endregion

# endregion


# region >> Update Plots


# endregion <<

if __name__ == '__main__':
    app.run_server(debug=True)



# fxtest1 = yget_fx('USDEUR=X')