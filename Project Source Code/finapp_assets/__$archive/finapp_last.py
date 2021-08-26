

'''

Script  : finapp.py
Type    : Application (Dash)
Root    : __FinDataPlatform
Author  : Wyatt Marciniak
Date    : 2019-08-21
-----

Details :

    > ...
    > ...
    > ...
    > ...

                                            '''

import datetime

print('----------\n> [*] Application session [{}] starting (Standby for setup)...\n----------'.format(
    str(datetime.datetime.today())
))


#_Note: Dependencies

from FinDataSpyder import *
from FinAppWorkerMain import *
from FinAppGraphicsWorker import *

import dash
import dash_html_components as dhtml
import dash_core_components as dcc
import dash_table as dtbl

import dash_daq as daq
import dash_renderer


from dash.dependencies import Input as cbi
from dash.dependencies import Output as cbo
from dash.dependencies import State as cbs

from dash.exceptions import PreventUpdate

import plotly.graph_objs as go
import flask
import os
import base64

sysout('\n> [*] Dependancies Loaded...')



#_Note: Developer Options - for testing / debug / general dev work

FLAG_DEVDEBUG =     True            # If True, debugger is active
FLAG_DEVTESTING =   True            # If True, dev-placed data printing is active
FLAG_DEVNOFETCH =   False           # If True, fetch operation is disallowed

maxproc =           40
do_update =         False

os.path.basename('warehouse/graphics/')



#_Note: Core object / param creation

fworker =           FinAppWorkerMain(devprint=FLAG_DEVTESTING,allow_updates=do_update,maxproc=maxproc)
fplotter =          FinAppWorkerGraphics(devprint=True,htext_lim=2000)
PBAR =              ProgBar()

cache =             fworker.cache.copy()
local_log =         fworker.log.copy()
curr_selected =     fworker.curr.copy()
req_assets =        fworker.req_assets.copy()

graphics_repo_path = 'finapp_assets/graphics/'


fworker.DEVOPS_NOFETCH = FLAG_DEVNOFETCH


## Img testing -- OK
img_name = 'jackrabbit_crest.png'
encode_img = base64.b64encode(open('{}{}'.format(graphics_repo_path,img_name),'rb').read())



sysout('\n> [*] Core objects and parameters created...')



#_Note: Pre-processing (Optional - Default: Base market data -- see /FinAppWorkerMain.py)

if fworker.fetchData_explicit({'etf':['spy'],'fx':['eur/usd'],'indices':['gspc']}):

    cache = fworker.cache.copy()
    local_log = fworker.log.copy()
    curr_selected = fworker.curr.copy()

    sysout('\n> [*] Pre-processing for data fetching finished')

else: sysout('\n> [!] No preprocessing data to load (not run or invalid) [OK]')




#_Note: APP INITIALIZATION

sysout('\n-----\n> Application Initializing...\n-----\n ')

app = dash.Dash('__main__',assets_folder='finapp_assets',suppress_callback_exceptions=True)
app.config.update({ 'routes_pathname_prefix':'','requests_pathname_prefix':''})



#_Note: *** APP LAYOUT

app.layout = dhtml.Div(className='finappContent',children=[

    # region > [SESSION STORAGE] Hidden div elems used for session maintenence


    dhtml.Div(className='session', id='cache', style={'display': 'none'},children=fworker.pack(cache)),

    dhtml.Div(className='session', id='local_log', style={'display': 'none'},children=fworker.pack(local_log)),

    dhtml.Div(className='session', id='curr_selected', style={'display': 'none'},children=fworker.pack(curr_selected)),

    dhtml.Div(className='session', id='lastSR', style={'display': 'none'},children=fworker.pack(([],0))),

    # endregion



    # [HEADER]

    dhtml.Div(className='row',id='appHeader',children=[dhtml.H3(id='headerH3',children=['FinApp Beta - v2.0'])]),


    # region < BODY ...


    dhtml.Div(className='row',id='appBody',children=[


        ### LEFT TABLE / CONTROL COL

        dhtml.Div(className='col',id='bodyLeftCol',children=[


            #_Note: [1] Fetch Controls...

            dhtml.Div(className='row',id='fetchControlsP',children=[


                ## Inputs (text - tickers)

                dhtml.Div(className='col',id='fetchControlsIn',children=[

                    dcc.Input(className='row',id='fetchRowEquity',
                              type='text',placeholder='Equity Symbols...',debounce=True),


                    dcc.Input(className='row',id='fetchRowEtf',
                              type='text', placeholder='ETF Symbols...', debounce=True),


                    dcc.Input(className='row',id='fetchRowFx',
                              type='text', placeholder='F/X Symbols...', debounce=True),


                    dcc.Input(className='row',id='fetchRowIndices',
                              type='text', placeholder='Indices Symbols...', debounce=True)

                ]),



                ## Fetch submit buttons (historical only or add update attempts)

                dhtml.Div(className='col',id='fetchControlsBtns',children=[


                    ## Fetch & Fetch with update checking

                    dhtml.Div(className='row',id='fetchGoAndUpdWrap',children=[

                        dhtml.Button(className='col', id='fetchGO', n_clicks_timestamp=0, n_clicks=0,
                                     children=['Fetch']),

                        dhtml.Button(className='col', id='fetchGOupd', n_clicks_timestamp=0, n_clicks=0,
                                     children=['Fetch + Update'])

                    ]),


                    ## Fetch request from universe files (NOT YET IMPLEMENTED)

                    dhtml.Div(className='row',id='otherFetchControls',children=[

                        dcc.Dropdown(className='col', id='fetchLoadUniverseDD',clearable=False,
                                         options=fworker.create_options(['None'], ['None']),
                                         value=['None']),

                        dhtml.Button(className='col', id='fetchLoadUniverseSubmit', n_clicks_timestamp=0, n_clicks=0,
                                     children=['LOAD'])



                    ])


                ])

            ]),



            #_Note: [2] Local Table / Controls...

            dhtml.Div(className='row',id='controlWrap',children = [


                #_CodeFlow: TABLE

                dhtml.Div(className='col',id='localTableWrap',children=[



                    ## Initialized Table for asset listing / selection

                    dtbl.DataTable(id='localTable',


                                    columns =                   [{'id':cn,'name':cn} for cn in local_log.columns],
                                    data =                      local_log.to_dict('rows'),

                                    hidden_columns =            ['selected'],


                                    # fixed_rows =                {'headers': True, 'data': 0},


                                    row_selectable =            'multi', # 'single'

                                    row_deletable =             False,
                                    editable =                  False,
                                    filter_action =             'none',
                                    sort_action =               'none',

                                    style_as_list_view =        True,

                                    style_table = {

                                        # 'min-width':            '200',
                                        # 'max-width':            '300',

                                        'width':                '300',

                                        'min-height':           '400',
                                        'max-height':           '500',

                                        # 'height':               '500',

                                        # 'margin-left':          '8',
                                        # 'margin-top':           '2',
                                        # 'margin-bottom':        '0',
                                        # 'margin-right':         '2',

                                        'padding':              '0 0 0 0',

                                        'border-style':         'solid',
                                        'border-width':         '2',
                                        'border-color':         'black',

                                        'overflowY':            'scroll',
                                        # 'overflowX':          'scroll',

                                        # 'display':              'block',
                                        # 'flex':                 'none',

                                    },

                                    style_cell = {

                                        'min-width':            '15',
                                        'max-width':            '20',

                                        'text-align':           'center',
                                        'font-size':            '20',

                                        'whiteSpace':           'normal',

                                        # 'textOverflow':         'ellipsis',
                                        'overflow':             'hidden',

                                        # 'flex':                 'none',

                                    },

                                    selected_rows =             [],
                                    selected_cells =            [],

                                    style_data_conditional =    [],
                                    style_cell_conditional =    []

                                   )

                ]),



                #_CodeFlow: CONTROLS

                dhtml.Div(className='col',id='localControls',children=[



                    # > Select all / none (table)

                    dhtml.Div(className='row',id='tableAllOrNone',children=[


                        dhtml.Button(className='col',id='tableALL',n_clicks_timestamp=0,n_clicks=0,
                                     children=['All']),

                        dhtml.Button(className='col',id='tableNONE',n_clicks_timestamp=0,n_clicks=0,
                                     children=['None'])


                    ]),



                    # > Update / Remove Selected

                    dhtml.Div(className='row',id='tableUpdRemWrap',children=[


                        ## Update selected tickers

                        dhtml.Button(className='col',id='updateSelected',n_clicks_timestamp=0,n_clicks=0,
                                 children=['Update']),


                        ## Remove (from local cache) selected tickers

                        dhtml.Button(className='col',id='removeSelected',n_clicks_timestamp=0,n_clicks=0,
                                 children=['Remove'])



                    ]),


                    # > Base option controls for ALL data

                    dhtml.Div(className='row',id='dataOpBaseControlsWrap',children=[



                        # > Select Data Type and Freq

                        dhtml.Div(className='row', id='dataTypeAndFreqWrap', children=[

                            dcc.Dropdown(className='col', id='dcbDataTypeDD', clearable=False,

                                         options=fworker.create_options(['Market', 'Returns', 'Growth']),

                                         value='Market'),

                            dcc.Dropdown(className='col', id='dcbDataFreqDD', clearable=False,

                                         options=fworker.create_options(['Daily', 'Weekly', 'Monthly', 'Intraday']),

                                         value='Daily')
                        ]),



                        # > Select start and end dates

                        dhtml.Div(className='row', id='dateRangeWrap', children=[


                            dhtml.H3(className='row', id='dateRangeH', children=['Date Range:']),

                            dcc.Input(className='col', id='dateRangeFromIN', type='text',
                                      value='1970-01-01'),

                            dcc.Input(className='col', id='dateRangeToIN', type='text',

                                      value=str(dtt.today()).split(' ')[0])

                        ])

                    ]),


                    # TODO: [III] Sorting - Sort by ticker, asset class (then ticker) or selected (then ac, then ticker)

                    dhtml.Div(className='row',id='tableSortWrap',children=[

                        ## Label

                        dhtml.H3(className='row',id='tableSortH',children=['Sort by']),



                        ## Sorting Tools

                        dhtml.Div(className='row',id='tableSortOps',children=[


                            ## [I] Select 'sort by x'

                            dcc.Dropdown(className='col', id='tableSortSelect',clearable=False,
                                         options=fworker.create_options(['ticker', 'ac', 'selected'], ['t', 'a', 's']),
                                         value=['t']),


                            ## [II] Select 'asc' or 'desc' --> clicks are submits

                            dhtml.Button(className='col', id='tableSortAsc', n_clicks_timestamp=0, n_clicks=0,
                                         children=['Asc']),

                            dhtml.Button(className='col', id='tableSortDesc', n_clicks_timestamp=0, n_clicks=0,
                                         children=['Desc'])


                        ])


                    ]),



                    # > RUN OPERATION

                    dhtml.Button(className='col', id='plotRun', n_clicks_timestamp=0, n_clicks=0,
                                     children=['RUN'])



                ]),


            ])


        ]),



        ### MAIN BODY

        dhtml.Div(className='col', id='bodyMainCol', children=[


            dhtml.Div(className='row', id='plotWrap', children=[

                    dcc.Graph(id='plotPlot')

            ]),


            dhtml.Div(className='row',id='tabsWrap',children=[


                dcc.Tabs(className='row', id='op_tabs', value='General', children=[



                    dcc.Tab(id='tabGeneral',label='General',value='General',children=[

                    ]),



                    dcc.Tab(id='tabCorrelations', label='Correlation Testing', value='CorrTest', children=[

                    ]),



                    dcc.Tab(id='tab3DVariable', label='3D Variation', value='3D', children=[


                    ])
                ])

            ])

        ]),


    ]),

    # endregion ... /BODY>



    # [FOOTER]

    dhtml.Div(className='row',id='appFooter',children=[

        dhtml.H5(className='col',id='footerCredTag',
                 children=['Author: Wyatt Marciniak -- Maintainer: Wyatt Marciniak']),

        dhtml.Img(id='footerJRImg',src='data:image/png;base64,{}'.format(encode_img.decode()))
    ])

])



#_Note: APP CALLBACKS

# region > _CodeFlow: [UPDATE CACHE]...

@app.callback(

    [
        cbo('cache','children'),

        cbo('fetchRowEquity', 'value'),
        cbo('fetchRowEtf', 'value'),
        cbo('fetchRowFx', 'value'),
        cbo('fetchRowIndices', 'value'),

        cbo('fetchGO', 'n_clicks_timestamp'),       cbo('fetchGOupd', 'n_clicks_timestamp'),
        cbo('updateSelected','n_clicks_timestamp'), cbo('removeSelected','n_clicks_timestamp')
    ],
    [
        cbi('fetchGO','n_clicks'),                  cbi('fetchGOupd','n_clicks'),
        cbi('updateSelected','n_clicks'),           cbi('removeSelected','n_clicks')
    ],
    [
        cbs('cache','children'),
        cbs('local_log','children'),
        cbs('curr_selected','children'),

        cbs('fetchRowEquity','value'),
        cbs('fetchRowEtf','value'),
        cbs('fetchRowFx','value'),
        cbs('fetchRowIndices','value'),

        cbs('fetchGO', 'n_clicks_timestamp'),       cbs('fetchGOupd', 'n_clicks_timestamp'),
        cbs('updateSelected','n_clicks_timestamp'), cbs('removeSelected','n_clicks_timestamp')
    ]
)

def fetch_assets(fgoNC,fgo_updNC,updselNC,remselNC,
                 c,log,cs,teq,tetf,tfx,tind,
                 fgoNCT,fgo_updNCT,updselNCT,remselNCT):

    # START ----------

    no_update = (c, teq, tetf, tfx, tind, 0, 0, 0, 0)

    if not cSum([fgoNC,fgo_updNC,updselNC,remselNC]) == 0:


        if max([fgoNCT,fgo_updNCT,updselNCT,remselNCT]) == updselNCT:      # Update Selected

            cs =        fworker.unpack(cs)
            temp_a =    unique([x[2] for x in cs])
            temp_req =  {}

            for a in temp_a: temp_req[a] = [x[1] for x in cs if x[2] == a]

            if fworker.fetchData_explicit(req_assets=temp_req, allow_updates=True):

                if fworker.update_cache(fworker.unpack(c)):

                    sysout(' [*] (Update Selected): OK')
                    return fworker.pack(fworker.cache.copy()), '', '', '', '', 0, 0, 0, 0

                else:
                    sysout(' [!] (Update Selected): FAILED')
                    return no_update

            else:
                sysout(' [!] (Update Selected): FAILED')
                return no_update



        elif max([fgoNCT,fgo_updNCT,updselNCT,remselNCT]) == remselNCT:     # Remove Selected

            if fworker.drop_cache_elems(c,cs):

                sysout(' [*] (Remove Selected): OK')
                return fworker.pack(fworker.cache.copy()), '', '', '', '', 0, 0, 0, 0

            else:
                sysout(' [!] (Remove Selected): FAILED')
                return no_update



        elif max([fgoNCT,fgo_updNCT,updselNCT,remselNCT]) in [fgoNCT,fgo_updNCT]:   # Fetch requests

            if fworker.fetchData_inapp(to_try=[teq,tetf,tfx,tind],log=log,allow_updates= fgo_updNCT > fgoNCT):

                if fworker.update_cache(fworker.unpack(c)):

                    sysout(' [*] (Fetch Operation) - OK')

                    return fworker.pack(fworker.cache.copy()), '', '', '', '', 0, 0, 0, 0

                else:
                    sysout(' [!] (Fetch Operation) - FAILED')
                    return no_update

            else:
                sysout(' [!] (Fetch Operation) - FAILED')
                return no_update


        ## BAD PARAMS

        else:

            sysout('\n> [!] Operation to change cache (ANY) - FAILED (Bad request params)')
            return no_update


    ## SKIP

    else:
        sysout('\n [!] fetch_assets() called and skipped')
        return no_update


# endregion


# region > _CodeFlow: [UPDATE SELECTED ROWS] -- { ALL or NONE}...

@app.callback(

    cbo('localTable','selected_rows'),

    [cbi('tableALL','n_clicks'),cbi('tableNONE','n_clicks')],
    [
        cbs('local_log','children'),
        cbs('localTable','selected_rows'),
        cbs('tableALL','n_clicks_timestamp'),
        cbs('tableNONE','n_clicks_timestamp')
    ]
)

def op_all_or_none(btn_all,btn_none,log,sr,btn_all_t,btn_none_t):

    if btn_all > 0 or btn_none > 0:

        sysout('\n> [RUN] op_all_or_none()...')

        try:
            tlog = pd.DataFrame(fworker.unpack(log))
        except Exception:

            sysout(' [!] FAIL - temp log is invalid')
            return sr

        else:

            try:

                if btn_all_t > btn_none_t:
                    return [i for i in range(len(tlog.index))]
                else:
                    return []


            except Exception:

                sysout(' [!] FAIL - Invalid return operation')
                return sr

    else: return sr



# endregion


# region > _CodeFlow: [UPDATE LOCAL LOG AND CURR SELECTED ASSETS -- Session Storage]...

@app.callback(

    [   cbo('local_log','children'),
        cbo('curr_selected', 'children')
    ],

    [   cbi('cache','children'),
        cbi('localTable', 'selected_rows')
    ],

    [   cbs('local_log','children'),
        cbs('curr_selected','children'),
    ]
)

def update_log_curr(c,sr,log,cs):

    sysout('\n> [UPDATING LOCAL LOG AND CURR_SELECTED]')


    ## Update current selected -- then apply to create new log file (changes if needed)

    new_log, new_cs = fworker.cache_to_log(c, log, cs, sr)

    if not isok(new_log) or not isok(new_cs):
        sysout('\n> [!] Bad update attempt -- no effect')
        return log, cs

    else:

        print('\n-----curr_selected----- =\n',
              ["[{}]: {} ({})".format(*x) for x in new_cs],
              '\n----------[X]-----------\n')

        return fworker.pack(new_log), fworker.pack(new_cs)

    # new_cs = fworker.update_curr_selected(c, log, sr)
    #
    # if not isok(new_cs): return log, cs
    # else:
    #
    #     c2log = fworker.cache_to_log(c, log, cs, sr)
    #
    #     if not isok(c2log): return log, new_cs
    #     else:
    #
    #         print('\n-----curr_selected----- =\n',
    #               ["[{}]: {} ({})".format(*x) for x in new_cs],
    #               '\n----------[X]-----------\n')
    #
    #         return fworker.pack(c2log), fworker.pack(new_cs)

# endregion


# region > _CodeFlow: [UPDATE LOCAL TABLE - UI ELEM]...

@app.callback(

    # Ouput
    [   cbo('localTable','data'),
        cbo('localTable','style_data_conditional'),
    ],

    # Input
    [   cbi('local_log','children'),    cbi('curr_selected','children')],

    # State
    [   cbs('localTable','data'),
        cbs('localTable','style_data_conditional')
    ]
)

def update_localTable(log,cs,tbl_data,curr_cond):

    sysout('\n> [RUN] update_localTable()...')

    temp_log = fworker.unpack(log)


    try:
        temp_tbl = pd.DataFrame(temp_log)


    ## INVALID

    except Exception:

        sysout(' [!] FAIL - Error in DF conversion - Restoring...')
        return tbl_data, curr_cond


    ## VALID

    else:


        ## Generate conditional styling list

        try: try_cond = fworker.genStyle_dataCond(log,cs,len(temp_tbl.index))
        except Exception:

            sysout(' [!] FAIL - Error in cond style gen - Restoring...')
            return tbl_data, curr_cond

        else:

            sysout(' [*] OK')

            # temp_tbl['selected'] = []

            # if FLAG_DEVTESTING:
            #     print('\n-----curr_DF-----\n',temp_tbl,'\n----------[X]----------\n')

            return temp_tbl.to_dict('rows'), try_cond

# endregion


@app.callback(

    cbo('plotWrap','children'),

    [
        cbi('plotRun', 'n_clicks')
    ],

    [
        cbs('plotRun', 'n_clicks_timestamp'),

        cbs('dcbDataTypeDD','value'),
        cbs('dcbDataFreqDD','value'),
        cbs('dateRangeFromIN','value'),
        cbs('dateRangeToIN','value'),
        cbs('plotWrap','children')

        # cbs('dateRangeFromIN','value'),
    ]
)

def plot_controller(rNC,rNCT,dtv,dfv,d0,d1,plot0):

    sysout('\n> [***] plot_controller() called - clicks: {}, last: {}'.format(rNC, rNCT))

    if not isok(rNC) or rNC == 0 or not isok(rNCT) or rNCT == 0:
        sysout(' | [!] Called and skipped')

        return plot0

    else:

        d0 = d0 if isok(d0) and isinstance(d0,str) else '1970-01-01'
        d1 = d1 if isok(d1) and isinstance(d1,str) else str(dtt.today()).split(' ')[0]

        sysout('\n> [***] PLotter Running For: - {}/{}/{}/{}'.format(dtv,dfv,d0,d1))
        return dcc.Graph(id='plotPLot')


#_Note: APP MAIN - (Run call)

if __name__ == '__main__':
    app.run_server(debug=FLAG_DEVDEBUG)
