

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
fplotter =          FinAppGraphicsWorker()
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

                                        'min-height':           '200',
                                        'max-height':           '300',

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




                    # TODO: [III] Sorting - Sort by ticker, asset class (then ticker) or selected (then ac, then ticker)

                    dhtml.Div(className='row',id='tableSortWrap',children=[


                        dhtml.H3(className='row',id='tableSortH',children=['Sort by']),

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

                    dhtml.Div(className='row', id='dateRangeWrap', children=[

                                dhtml.H3(className='row', id='dateRangeH', children=['Date Range:']),

                                dcc.Input(className='col', id='dateRangeFromIN', type='text',
                                          value='1970-01-01'),

                                dcc.Input(className='col', id='dateRangeToIN', type='text',

                                          value=str(dtt.today()).split(' ')[0])

                    ])

                ])

            ]),



            # _Note: [3] Plot Controls by tab selection...

            dhtml.Div(className='row', id='tabsWrap', children = [


                # _CodeFlow: Tab Parent...

                dcc.Tabs(className='row', id='op_tabs', value='Tech', children=[



                    #_Note: TECHNICAL...

                    dcc.Tab(id='tabTech', label='Technical', value='Tech', children=[


                        dhtml.Div(className='row', id='tabTech_Body', children=[


                            dhtml.Div(className='row', id='tabTech_row1', children=[


                                dhtml.H3(className='row', id='tabTechH_r1', children=['Data Type/Freq/Column:']),



                                dcc.Dropdown(className='col', id='tabTech-dt-dd', clearable=False,
                                                 options=fworker.create_options(
                                                     ['Market', 'Returns', 'Growth']),
                                                 value='Market'),


                                dcc.Dropdown(className='col', id='tabTech-df-dd', clearable=False,
                                                 options=fworker.create_options(
                                                     ['Daily', 'Weekly', 'Monthly', 'Intraday']),
                                                 value='Daily'),


                                dcc.Dropdown(className='col', id='tabTech-dc-dd', clearable=False,
                                                options=fworker.create_options(
                                                     ['Open', 'High', 'Low', 'Close', 'Volume','AdjClose']),
                                                value='Close')

                            ]),



                            dhtml.Div(className='row', id='tabTech-plotStyleWrap', children=[


                                dhtml.H3(className='col', id='tabTech-plotStyleH', children=['Plot Style:']),

                                dcc.Dropdown(className='col', id='tabTech-plotStyle', clearable=False,
                                                 options=fworker.create_options(

                                                     ['Scatter/TS','Technical','Distribution','Summary Stats'],
                                                     ['scatter','tech','dist','stats']

                                                 ),
                                                 value='scatter'),


                                dhtml.Div(className='row',id='tech_plot_modWrap',children=[


                                    dhtml.H3(className='col', id='tech_plot_modH', children=['Plot Mod:']),

                                    dcc.Dropdown(className='col', id='tech_plot_mod', clearable=False,
                                                 options=fworker.create_options(['Marks','Lines'],['marks','lines']),
                                                 value='marks'),

                                    dcc.Dropdown(className='col', id='tech_plot_mod2', clearable=False,
                                                 options=fworker.create_options(['N/A'],['na']),value='na')


                                ])


                            ]),



                            dhtml.Button(className='col',id='tabTech_GO',n_clicks=0,n_clicks_timestamp=0,
                                         children=['RUN'])
                        ]),
                    ]),



                    #_Note: 3D...

                    dcc.Tab(id='tab3DVariable', label='3D View', value='3D', children=[

                        dhtml.Div(className='row', id='tab3D_body', children=[




                            dhtml.Div(className='col',id='col_x',children=[

                                dhtml.H3(className='row', id='tab3DH_x', children=['X - Value']),

                                dcc.Dropdown(className='row', id='3Dx_type', clearable=False,
                                             options=fworker.create_options(
                                                 ['Market', 'Returns', 'Growth']),
                                             value='Market'),

                                dcc.Dropdown(className='row', id='3Dx_col', clearable=False,
                                             options=fworker.create_options(
                                                 ['Open', 'High', 'Low', 'Close', 'Volume', 'AdjClose']),
                                             value='Close'),

                                dcc.Dropdown(className='row', id='3Dx_targSel', clearable=False,
                                             options=fworker.create_options(['All']),value='All')

                            ]),



                            dhtml.Div(className='col',id='col_y',children=[

                                dhtml.H3(className='row', id='tab3DH_y', children=['Y - Value']),

                                dcc.Dropdown(className='row', id='3Dy_type', clearable=False,
                                             options=fworker.create_options(
                                                 ['Market', 'Returns', 'Growth']),
                                             value='Market'),

                                dcc.Dropdown(className='row', id='3Dy_col', clearable=False,
                                             options=fworker.create_options(
                                                 ['Open', 'High', 'Low', 'Close', 'Volume', 'AdjClose']),
                                             value='Close'),

                                dcc.Dropdown(className='row', id='3Dy_targSel', clearable=False,
                                             options=fworker.create_options(['All']),value='All')

                            ]),



                            dhtml.Div(className='col',id='col_z',children=[

                                dhtml.H3(className='row', id='tab3DH_z', children=['Z - Value']),

                                dcc.Dropdown(className='row', id='3Dz_type', clearable=False,
                                             options=fworker.create_options(
                                                 ['Market', 'Returns', 'Growth']),
                                             value='Market'),

                                dcc.Dropdown(className='row', id='3Dz_col', clearable=False,
                                             options=fworker.create_options(
                                                 ['Open', 'High', 'Low', 'Close', 'Volume', 'AdjClose']),
                                             value='Close'),

                                dcc.Dropdown(className='row', id='3Dz_targSel', clearable=False,
                                             options=fworker.create_options(['All']),value='All')

                            ]),



                            dhtml.Div(className='col',id='col_submit',children=[

                                dhtml.H3(className='row', id='tab3DH_submit', children=['Freq.']),

                                dcc.Dropdown(className='row', id='3D_freq', clearable=False,
                                             options=fworker.create_options(
                                                 ['Daily', 'Weekly', 'Monthly', 'Intraday']),
                                             value='Daily'),

                                dhtml.Button(className='row', id='tab3D_GO', n_clicks=0, n_clicks_timestamp=0,
                                         children=['RUN'])

                            ])

                        ])

                    ]),



                    #_Note: BETA STAT TEST(S)...

                    dcc.Tab(id='tabCorrelations', label='Beta Test', value='BetaView', children=[

                        dhtml.Div(className='row', id='tabBeta_body', children=[


                            dhtml.Div(className='row',id='tabBeta_dataSel',children=[


                                dhtml.Div(className='row',id='Beta_targWrap',children=[


                                    dhtml.H3(className='col', id='tabBetaH_1', children=['Target (X):']),

                                    dcc.Dropdown(className='col', id='Beta_targ', clearable=False,
                                                 options=fworker.create_options(['All']),value='All')

                                ]),


                                dhtml.Div(className='row',id='Beta_tcWrap',children=[


                                    dhtml.H3(className='col', id='tabBetaH_2', children=['Data Freq/Col:']),

                                    dcc.Dropdown(className='col', id='Beta_freq', clearable=False,
                                                 options=fworker.create_options(
                                                     ['Daily', 'Weekly', 'Monthly', 'Intraday']),
                                                 value='Daily'),

                                    dcc.Dropdown(className='col', id='Beta_col', clearable=False,
                                                 options=fworker.create_options(
                                                     ['Open', 'High', 'Low', 'Close', 'Volume', 'AdjClose']),
                                                 value='Close')
                                ])

                            ]),


                            dhtml.Div(className='row',id='tabBeta_plotMods',children=[


                                dhtml.H3(className='col', id='tabBetaH_3', children=['Data to Show:']),

                                dcc.Dropdown(className='col', id='Beta_dataMod', clearable=False,
                                             options=fworker.create_options(['All Data','Beta Only']),
                                             value ='All Data'),

                                dhtml.H3(className='col', id='tabBetaH_4', children=['Plot 2D/3D:']),

                                dcc.Dropdown(className='col', id='Beta_plotMod', clearable=False,
                                             options=fworker.create_options(['2D','3D']),
                                             value='2D'),

                                dhtml.Button(className='col',id='tabBeta_GO',n_clicks=0,n_clicks_timestamp=0,
                                         children=['RUN'])


                            ])

                        ])

                    ])

                ])

            ])

        ]),



        ### MAIN BODY

        dhtml.Div(className='col', id='bodyMainCol', children=[


            #_Note: [1] MAIN PLOT...

            dhtml.Div(className='row', id='plotWrap', children=[

                    dcc.Graph(id='plotPlot',figure=go.Figure(),
                              style = {'height':625,'width':'100%','border-style':'solid', 'border-color':'black'},
                              config = dict(

                                  displaylogo       = False,
                                  scrollZoom        = True,
                                  editable          = False,
                                  displayModeBar    = True,
                                  responsive        = True,
                                  toImageButtonOptions = dict(format='jpeg', filename='BasePLot',
                                                                height=800, width=900, scale=1)
                              )
                    )
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



#_Note: APP CALLBACKS --


#_Note: DD Updater Callbacks --> Used for plotting...


# region > _CodeFlow: Update DDs using current selected assets as options...

@app.callback(

    [
        cbo('3Dx_targSel','options'), cbo('3Dx_targSel','value'),
        cbo('3Dy_targSel','options'), cbo('3Dy_targSel','value'),
        cbo('3Dz_targSel','options'), cbo('3Dz_targSel','value'),
        cbo('Beta_targ','options'),   cbo('Beta_targ','value')
    ],
    [   cbi('curr_selected','children')],
    [
        cbs('3Dx_targSel','options'), cbs('3Dx_targSel','value'),
        cbs('3Dy_targSel','options'), cbs('3Dy_targSel','value'),
        cbs('3Dz_targSel','options'), cbs('3Dz_targSel','value'),
        cbs('Beta_targ','options'),   cbs('Beta_targ','value')
    ]
)

def updateAssetSelecters(cs,xo,xv,yo,yv,zo,zv,bo,bv):

    sysout('\n> [RUN] Updating Asset Selecter DD Options for UI')

    try: cs_local = fworker.unpack(cs)
    except Exception:

        sysout('\n> [!] Asset Select DDs unchanged -- bad attempt to unpack cs')
        return xo,xv,yo,yv,zo,zv,bo,bv

    else:

        if not isinstance(cs_local,list):
            sysout('\n> [!] Asset Select DDs unchanged -- bad type extracted from unpacking cs')
            return xo, xv, yo, yv, zo, zv, bo, bv

        else:

            try: x = ['All'] + sorted([x[1] for x in cs_local])
            except Exception:
                sysout('\n> [!] Post extraction of cs elems to return to DDs was invalid')
                return xo,xv,yo,yv,zo,zv,bo,bv

            else:

                x = fworker.create_options(x)

                sysout('\n> [*] Updating DDS - OK')
                return x.copy(),xv,x.copy(),yv,x.copy(),zv,x.copy(),bv


@app.callback(

    [
        cbo('tech_plot_mod','options'), cbo('tech_plot_mod','value'),
        cbo('tech_plot_mod2','options'), cbo('tech_plot_mod2','value')
    ],
    [   cbi('tabTech-plotStyle','value')],
    [
        cbs('tech_plot_mod','options'), cbs('tech_plot_mod','value'),
        cbs('tech_plot_mod2','options'), cbs('tech_plot_mod2','value')
    ]
)

def updateTechPlotMods(ps0,pmo1,pmv1,pmo2,pmv2):

    sysout('\n> [RUN] Updating Technical Analysis Plot Mod Options... ')

    try:

        if ps0 == 'scatter':
            return (fworker.create_options(['Marks','Lines'],['marks','lines']),'marks',
                    fworker.create_options(['N/A'], ['na']), 'na')

        elif ps0 == 'tech':
            return (fworker.create_options(['OHLC', 'Candles'], ['ohlc', 'candles']), 'ohlc',
                    fworker.create_options(['Green/Red','Black/White','Blue/Gray']), 'Green/Red')

        elif ps0 == 'dist':
            return (fworker.create_options(['As-is', 'Normalized'], ['asis','norm']), 'asis',
                    fworker.create_options(['Stacked','Overlaid'], ['stack','overlay']), 'stack')

        elif ps0 == 'stats':
            return (fworker.create_options(['Box Plot', 'Violin Plot'], ['box', 'violin']), 'box',
                    fworker.create_options(['Show Data','Only Summary'], ['all_data','summary']), 'all_data')

        else:
            sysout(' [!] This should NEVER run -- ERROR')
            return pmo1, pmv1, pmo2, pmv2

    except Exception:
        sysout(' [!] Error - Invalid update (no effect)')
        return pmo1,pmv1,pmo2,pmv2



# endregion


unlist([[1,2,3],[4,5,6]])

# region > _CodeFlow: [UPDATE PLOT]...

@app.callback(

    [cbo('plotPlot','figure'),  cbo('plotPlot','style'),  cbo('plotPlot','config')],
    [
        cbi('tabTech_GO','n_clicks'),
        cbi('tab3D_GO','n_clicks'),
        cbi('tabBeta_GO','n_clicks')
    ],
    [

        # Core required

        cbs('cache', 'children'),
        cbs('curr_selected', 'children'),
        cbs('dateRangeFromIN','value'),
        cbs('dateRangeToIN','value'),



        # Technical Analysis Tab

        cbs('tabTech_GO', 'n_clicks_timestamp'),
        cbs('tabTech-dt-dd', 'value'),
        cbs('tabTech-df-dd', 'value'),
        cbs('tabTech-dc-dd', 'value'),
        cbs('tabTech-plotStyle', 'value'),
        cbs('tech_plot_mod','value'),
        cbs('tech_plot_mod2','value'),



        # 3D Data viewing Tab

        cbs('tab3D_GO','n_clicks_timestamp'),
        cbs('3Dx_type','value'),    cbs('3Dx_col','value'), cbs('3Dx_targSel','value'),
        cbs('3Dy_type','value'),    cbs('3Dy_col','value'), cbs('3Dy_targSel','value'),
        cbs('3Dz_type','value'),    cbs('3Dz_col','value'), cbs('3Dz_targSel','value'),
        cbs('3D_freq','value'),



        # Beta Tab (3D Betas - alter date range and frequency (also target index if in cache))

        cbs('tabBeta_GO','n_clicks_timestamp'),
        cbs('Beta_targ','value'),
        cbs('Beta_freq','value'),
        cbs('Beta_col','value'),
        cbs('Beta_dataMod','value'),
        cbs('Beta_plotMod','value'),

        cbs('plotPlot', 'figure'), cbs('plotPlot', 'style'), cbs('plotPlot', 'config')

    ]

)

def update_plot(ttech,t3d,tbeta,
                c,cs,d0,d1,
                _ttech,ttech_type,ttech_freq,ttech_col,ttech_ps,ttech_pm1,ttech_pm2,
                _t3d,t3d_xt,t3d_xc,t3d_xs,t3d_yt,t3d_yc,t3d_ys,t3d_zt,t3d_zc,t3d_zs,t3d_freq,
                _tbeta,tbeta_targ,tbeta_freq,tbeta_col,tbeta_dm,tbeta_pm,
                _pfig0,_pstyle0,_pconfig0):

    sysout('\n> [!] update_plot() CALLED...')

    reject = (_pfig0,_pstyle0,_pconfig0)


    #_CodeFlow: Initial filters for skipping (misfire) or invalid core params (cache and cs) -- unique filters inside..

    if not any([x > 0 for x in [ttech,t3d,tbeta]]):
        sysout(' | MISFIRE (Skipped)')
        return reject

    elif not all([isok(x) for x in [c,cs]]):
        sysout(' | Bad core param types for cache ({}) and/or cs ({}) -- no effect'.format(type(c),type(cs)))
        return reject

    else:

        bc = [_ttech,_t3d,_tbeta]


        #_CodeFlow: Technical Analysis / General Analysis Tab --

        if max(bc) == _ttech:

            if not all([isok(x) for x in [ttech_type,ttech_freq,ttech_col,ttech_ps]]):
                sysout(' | [ERROR] Bad params')
                return reject

            elif not fplotter.build(ttech_ps,ttech_pm1,ttech_pm2,c,cs,d0,d1,ttech_type,ttech_freq,ttech_col):
                sysout(' | [ERROR] Plot construction')
                return reject

            else:

                sysout('\n> [*] New / Updated plot is OK - passing to UI...')
                return fplotter.FIGURE, fplotter.STYLE, fplotter.CONFIG


        # _CodeFlow: 3D Plotting -- full selection --

        elif max(bc) == _t3d:   # 3D Plotting

            if not all([isok(x) for x in [t3d_xt,t3d_xc,t3d_xs,t3d_yt,t3d_yc,t3d_ys,t3d_zt,t3d_zc,t3d_zs,t3d_freq]]):
                sysout(' [!] Bad input type(s) for 3D Params -- no effect')
                return reject

            elif not fplotter.build3D(c, cs, d0, d1, t3d_xt,t3d_xc,t3d_xs,t3d_yt,t3d_yc,t3d_ys,
                                    t3d_zt,t3d_zc,t3d_zs,t3d_freq):
                sysout(' [!] Bad plot construction opereration for 3D Params -- no effect')
                return reject

            else:

                sysout('\n> [*] New / Updated plot is OK - passing to UI...')
                return fplotter.FIGURE, fplotter.STYLE, fplotter.CONFIG




        #_CodeFlow: Beta correlation testing -- NOT IMPLEMEM

        elif max(bc) == _tbeta: # Beta Analysis

            if not all([isok(x) for x in [tbeta_targ,tbeta_freq,tbeta_col,tbeta_dm,tbeta_pm]]):
                sysout(' [!] Bad input type(s) for Beta Params -- no effect')
                return reject

            else:
                return reject



        else:   # Error in max calc --> Should NEVER happen but placed in case

            sysout(' [!] ERROR -- Bad type -- check code')
            return reject


# endregion


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



#_Note: APP MAIN - (Run call)

if __name__ == '__main__':
    app.run_server(debug=FLAG_DEVDEBUG)
