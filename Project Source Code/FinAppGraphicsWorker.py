
"""
>> Module: FinAppWorkerGraphics
>> Author: Wyatt Marciniak

>> USE:

    > Handles plotting -- attrivutes designed for graphing class
    > [1] Handle specific attribute set/reset by ref attribbutes (Obj.attr)
    > [2] Handle full element creation (parse relevant params and pass)


                                                                            """


from FinAppBaseWorker import *

import plotly

# from plotly import graph_objects as go

import plotly.graph_objects as go
import dash_html_components as dhtml
import dash_core_components as dcc
from plotly.offline import plot as plocal



class FinAppGraphicsWorker(FinAppBaseWorker):

    cn = 'FinAppGraphicsWorker'


    # region > Attributes

    classname =     None
    id =            None

    payloads =      []


    ## Config for chart control options

    CONFIG = dict(

        displaylogo             = False,
        scrollZoom              = True,
        editable                = False,
        displayModeBar          = True,
        responsive              = True,
        toImageButtonOptions    = dict(format='jpeg',filename='BasePLot',height=800,width=900,scale=1)
    )

    ## Graph figure (Data/layout) and CSS style overrides (best done from here -- external is tricky)

    FIGURE  = go.Figure()
    DATA    = []

    STYLE   = {
        'height':625,
        'width':'100%',
        'border-style':'solid',
        'border-color':'black'
    }

    LAYOUT  = go.Layout(    title_text      = 'OHLC Plot',
                            paper_bgcolor   = 'rgb(200, 200, 200)',
                            plot_bgcolor    = 'rgb(221, 221, 221)',

                            xaxis = {'title' : {'text':'DateTimes'}, 'rangeslider': {'visible':False},'nticks' : 10})

    hoverdata   = []
    hovertext   = []
    hoverinfo   = 'text'

    htext_lim   = 1000

    PNOTICE     = True


    ohlc_inc_set = {'line':{'color':'green','width':1,'dash':'solid'}}
    ohlc_dec_set = {'line': {'color': 'red', 'width': 1, 'dash': 'solid'}}
    ohlc_tickwidth = 0.5


    # endregion


    def __init__(self):
        super().__init__()

        self.fxn = self.getfxn()
        self.note('READY...')


    def core_param_validator(self,c,cs):

        self.fxn = self.getfxn()

        if not all([isok(x) for x in [c,cs]]):
            self.error('Bad input type(s): cache({}), cs({})'.format(type(c),type(cs)))
            return False

        else:

            c   = self.chkpack(c)
            cs  = self.chkpack(cs)

            if not all([isok(x) for x in [c,cs]]):
                self.error('Bad unpacked type(s): cache({}), cs({})'.format(type(c),type(cs)))
                return False

            else:
                self.cache = c
                # self.log = log
                self.curr = cs

                self.po('\n> [*] Plot Core Params - OK')
                return True

    def create_payloads(self, d0, d1, dtype, dfreq, dcol):

        self.fxn = self.getfxn()

        if not all([isinstance(x, str) for x in [d0, d1, dtype, dfreq, dcol]]):
            self.fxn = self.getfxn()
            self.error('Bad plot param input type(s)')
            return False

        else:

            try:
                self.payloads = [dict(t=x[1], ac=x[2], d0=d0, d1=d1, dtype=dtype, dfreq=dfreq, dcol=dcol) for x in
                                 self.curr]
            except Exception:
                self.error('Unable to create payloads')
                return False

            else:
                self.po('\n> [*] Payload Creation - OK')
                return True


    def data_scrubber(self,payload=None):   # Handles extracting proper DF, cleaning invalid/duplicates & apply dates

        self.fxn = self.getfxn()

        if not isok(payload):
            self.error('Payload was type: None -- cannot operate')
            return None

        else:

            pd.options.mode.use_inf_as_na = True    # Treat -inf/inf as None/nan/(invalid types...)

            try:

                newdf_name = payload['dfreq'].lower() if payload['dfreq'] == 'Intraday' else 'mkt_{}'.format(
                    str(payload['dfreq'])[0].lower())


                newdf = self.cache[payload['ac']][payload['t']][newdf_name]


                if not isinstance(newdf,pd.DataFrame):
                    newdf = pd.DataFrame(newdf)

                self.po('\n> DF Pre-Clean: ')
                print(newdf.shape)


                #_CodeFlow: Clean --
                newdf.drop_duplicates(subset=['dateu'], inplace=True)
                newdf.dropna(inplace=True)
                newdf.sort_values(by=['dateu'], inplace=True)


                #_CodeFlow: Date Range --

                d0 = time.mktime(time.strptime(payload['d0'],'%Y-%m-%d'))
                d1 = time.mktime(time.strptime(payload['d1'], '%Y-%m-%d'))

                newdf = newdf[newdf['dateu'] > d0]
                newdf = newdf[newdf['dateu'] < d1]

                self.po('\n> DF Post-Clean: ')
                print(newdf.shape)


                #_CodeFlow: Adjust to returns / growth if requested --
                if not payload['dtype'] == 'Market':

                    # mod_data = [list(newdf['dateu'])[1:],list(newdf['datec'])[1:]]
                    mod_data = []

                    for cn in newdf.columns:

                        cc = []

                        if payload['dtype'] == 'Returns':


                            try:
                                cc = list((newdf[cn][1:].values / newdf[cn][:-1]) - 1) if not cn in [
                                    'dateu','datec'] else list(newdf[cn])[1:]

                            except Exception:
                                cc = list(newdf[cn])[1:]

                        else:

                            try:
                                cc = list((newdf[cn][1:].values / newdf[cn][0]) - 1) if not cn in [
                                    'dateu', 'datec'] else list(newdf[cn])[1:]

                            except Exception:
                                cc = list(newdf[cn])[1:]

                        mod_data.append(cc)

                    newdf = kv2df(list(newdf.columns),mod_data)


            except Exception:
                self.error('Bad attempt to clean and set date range')
                return None

            else:
                payload['df'] = newdf.copy()
                return payload

    def MT_controller(self):

        if not isinstance(self.payloads, list) or len(self.payloads) == 0:
            self.error('Payloads are invalid or len 0')
            return False

        else:

            try:

                orders = self.payloads.copy()

                pool = mtpool(self.maxproc)
                valid_mt_operation = pool.map(self.data_scrubber, orders)  # Start MT Chain of method calls
                pool.terminate()
                gc.collect()

            except Exception:
                self.fxn = self.getfxn()
                self.error('MT Pool operation threw an error')
                return False

            else:

                if not isinstance(valid_mt_operation, list) or len(valid_mt_operation) == 0:
                    self.fxn = self.getfxn()
                    self.error('MT Operation returned invalid payload list (None or len == 0)')
                    return False

                else:
                    self.po('\n> MT Operation - OK (len: ({})'.format(len(valid_mt_operation)))
                    self.payloads = valid_mt_operation.copy()
                    return True

    def genColors(self,n):

        rgb_string = "rgb({}, {}, {})"
        rgb_out = []

        for i in range(n):

            r = sample(list(range(255)),1)
            g = sample(list(range(255)),1)
            b = sample(list(range(255)),1)

            rgb_out.append(rgb_string.format(r, g, b))

        return rgb_out




    def build(self,gtype,gmod1,gmod2,cache,cs,d0,d1,dtype,dfreq,dcol):

        self.fxn = self.getfxn()

        if      not self.core_param_validator(cache,cs)           : return False
        elif    not self.create_payloads(d0,d1,dtype,dfreq,dcol)  : return False
        elif    not self.MT_controller()                            : return False
        else:

            ## All initial filters & data ops finished -- create plot object

            if gtype == 'scatter' or gmod1 in ['ohlc','candles']:

                if gtype == 'scatter': self.htext_lim = 5000    # Extend for larger sets use GL frameworks


                if len(self.payloads[0]['df'].index) < self.htext_lim:

                    try:
                        self.hovertext = [['Open: {}<br>High: {}<br>Low: {}<br>Close: {}'.format(
                            list(p['df']['open'])[i],
                            list(p['df']['high'])[i],
                            list(p['df']['low'])[i],
                            list(p['df']['close'])[i]) for i in range(len(p['df'].index))] for p in self.payloads]

                    except Exception:
                        self.hovertext = [[] for i in range(len(self.payloads))]

                else: self.hovertext = [[] for i in range(len(self.payloads))]


                if len(self.hovertext) > 0 and any([len(ht) > 0 for ht in self.hovertext]):
                    self.po('\n> [*] Hover Text Included')
                else:
                    self.po('\n> [!] NO HOVERTEXT')


                #_CodeFlow: Scatter PLot (TS --> lines, Scatter --> points) --

                if gtype == 'scatter':

                    temp_col = self.genColors(len(self.payloads))

                    try:
                        self.DATA = [
                            go.Scattergl(

                                x=[x.split('_')[0] for x in list(p['df']['datec'])],
                                y=[np.round(x, 2) for x in list(p['df'][dcol.lower()])],

                                name='{} ({})'.format(p['t'], p['ac']),

                                mode='markers' if gmod1 == 'marks' else 'lines',
                                marker=dict(line=dict(width=1, color=temp_col[i])),

                                showlegend=True,

                                text=np.array(self.hovertext[i]) if isok(self.hovertext[i]) else np.array([]),
                                hoverinfo='text' if isinstance(self.hovertext[i], list) and
                                                    len(self.hovertext[i]) > 0 else 'none'
                            )
                            for i, p in enumerate(self.payloads) if isok(p)]

                        self.LAYOUT = go.Layout(
                            title_text='Scatter Plot',
                            paper_bgcolor='rgb(200, 200, 200)',
                            plot_bgcolor='rgb(221, 221, 221)',

                            xaxis={'title': {'text': 'DateTimes'}, 'rangeslider': {'visible': False}, 'nticks': 10}
                        )



                    except Exception:
                        self.error('Failed to create figure object for plot - ({})'.format(gtype))
                        return False

                    else:
                        self.FIGURE = go.Figure(data=self.DATA, layout=self.LAYOUT)
                        return True


                #_CodeFlow: OHLC --

                elif gmod1 == 'ohlc':

                    try:
                        self.DATA = [
                            go.Ohlc(
                                x       = [x.split('_')[0] for x in list(p['df']['datec'])],
                                open    = p['df']['open'], high  = p['df']['high'],
                                low     = p['df']['low'],  close = p['df']['close'],

                                increasing_line_color=gmod2.split('/')[0].lower(),
                                decreasing_line_color=gmod2.split('/')[1].lower(),

                                name    = '{} ({})'.format(p['t'],p['ac']),

                                showlegend  = True,
                                line        = {'width':2},

                                text        = np.array(self.hovertext[i]) if isok(self.hovertext[i]) else np.array([]),
                                hoverinfo   = self.hoverinfo if isinstance(
                                    self.hovertext[i],list) and len(self.hovertext[i]) > 0 else 'none'
                            )
                        for i,p in enumerate(self.payloads) if isok(p)]

                    except Exception:
                        self.error('Failed to create figure object for plot - ({})'.format(gtype))
                        return False

                    else:
                        self.FIGURE = go.Figure(data=self.DATA,layout=self.LAYOUT)
                        return True


                #_CodeFlow: CandleStick --

                elif gmod1 == 'candles':

                    try:
                        self.DATA = [
                            go.Candlestick(
                                x       = [x.split('_')[0] for x in list(p['df']['datec'])],
                                open    = p['df']['open'], high  = p['df']['high'],
                                low     = p['df']['low'],  close = p['df']['close'],

                                increasing_line_color=gmod2.split('/')[0].lower(),
                                decreasing_line_color=gmod2.split('/')[1].lower(),

                                name    = '{} ({})'.format(p['t'],p['ac']),

                                showlegend  = True,
                                line        = {'width':1},

                                text        = np.array(self.hovertext[i]) if isok(self.hovertext[i]) else np.array([]),
                                hoverinfo   = self.hoverinfo if isinstance(
                                    self.hovertext[i],list) and len(self.hovertext[i]) > 0 else 'none'
                            )
                        for i,p in enumerate(self.payloads) if isok(p)]

                        self.LAYOUT = go.Layout(
                            title_text='Candle Plot',
                            paper_bgcolor='rgb(200, 200, 200)',
                            plot_bgcolor='rgb(221, 221, 221)',

                            xaxis={'title': {'text': 'DateTimes'}, 'rangeslider': {'visible': False}, 'nticks': 10}
                        )


                    except Exception:
                        self.error('Failed to create figure object for plot - ({})'.format(gtype))
                        return False

                    else:
                        self.FIGURE = go.Figure(data=self.DATA,layout=self.LAYOUT)
                        return True


            # _CodeFlow: Distribution(s) [Histograms] --

            elif gtype == 'dist':

                temp_col = self.genColors(len(self.payloads))

                try:
                    self.DATA = [
                        go.Histogram(

                            x=[np.round(a, 2) for a in list(p['df'][dcol.lower()])],

                            name='{} ({})'.format(p['t'], p['ac']),
                            marker_color = temp_col[i]

                            # mode='markers' if gtype == 'scatter' else 'line',
                            # marker=dict(line=dict(width=1, color='rgb(58, 138, 170)')),

                            # showlegend=True

                            # text=np.array(self.hovertext[i]) if isok(self.hovertext[i]) else np.array([]),
                            # hoverinfo='text' if isinstance(self.hovertext[i], list) and
                            #                     len(self.hovertext[i]) > 0 else 'none'
                        )
                        for i, p in enumerate(self.payloads) if isok(p)]

                    self.LAYOUT = go.Layout(
                        title_text='Dist Plot',
                        paper_bgcolor='rgb(200, 200, 200)',
                        plot_bgcolor='rgb(221, 221, 221)',
                    )



                except Exception:
                    self.error('Failed to create figure object for plot - ({})'.format(gtype))
                    return False

                else:
                    self.FIGURE = go.Figure(data=self.DATA, layout=self.LAYOUT)

                    self.FIGURE.update_layout(barmode=gmod2)

                    if gmod2 == 'overlay':
                        self.FIGURE.update_traces(opacity=0.75)

                    if gmod1 == 'norm':
                        self.FIGURE.update_traces(histnorm='probability')

                    return True


            # _CodeFlow: Boxplots --

            elif gmod1 == 'box':

                temp_col = self.genColors(len(self.payloads))

                try:
                    self.DATA = [
                        go.Box(

                            x=[np.round(x, 2) for x in list(p['df'][dcol.lower()])],
                            # y= '{} ({})'.format(p['t'], p['ac']),

                            name='{} ({})'.format(p['t'], p['ac']),
                            marker = {
                                'color':temp_col[i],
                                'outliercolor':'rgb(255, 0, 0)',
                                'line':{
                                    'outliercolor': 'rgb(255, 0, 0)',
                                    'outlierwidth': 2,
                                }
                            },
                            boxpoints='outliers' if gmod2 == 'all_data' else False,

                            # mode='markers' if gtype == 'scatter' else 'line',
                            # marker=dict(line=dict(width=1, color='rgb(58, 138, 170)')),

                            showlegend=True,

                            # text=np.array(self.hovertext[i]) if isok(self.hovertext[i]) else np.array([]),
                            # hoverinfo='text' if isinstance(self.hovertext[i], list) and
                            #                     len(self.hovertext[i]) > 0 else 'none'
                        )
                        for i, p in enumerate(self.payloads) if isok(p)]


                    self.LAYOUT = go.Layout(
                        title_text='Box Plot',
                        paper_bgcolor='rgb(200,200,200)',
                        plot_bgcolor='rgb(221, 221, 221)',
                    )

                except Exception:
                    self.error('Failed to create figure object for plot - ({})'.format(gtype))
                    return False

                else:
                    self.FIGURE = go.Figure(data=self.DATA, layout=self.LAYOUT)
                    return True


            # _CodeFlow: Violin (enhanced box plots) --

            elif gmod1 == 'violin':

                temp_col = self.genColors(len(self.payloads))

                try:
                    self.DATA = [
                        go.Violin(

                            y=[np.round(x, 2) for x in list(p['df'][dcol.lower()])],
                            # x='{} ({})'.format(p['t'], p['ac']),

                            name='{} ({})'.format(p['t'], p['ac']),

                            # mode='markers' if gtype == 'scatter' else 'line',
                            # marker=dict(line=dict(width=1, color='rgb(58, 138, 170)')),

                            # box = True,
                            points = 'outliers' if gmod2 == 'all_data' else False,
                            box_visible=True,
                            meanline_visible=True,
                            line_color='black',
                            fillcolor = temp_col[i],
                            opacity = 0.65,

                            showlegend=True

                            # text=np.array(self.hovertext[i]) if isok(self.hovertext[i]) else np.array([]),
                            # hoverinfo='text' if isinstance(self.hovertext[i], list) and
                            #                     len(self.hovertext[i]) > 0 else 'none'
                        )
                        for i, p in enumerate(self.payloads) if isok(p)]

                    self.LAYOUT = go.Layout(
                        title_text='Violin Plot',
                        paper_bgcolor='rgb(200, 200, 200)',
                        plot_bgcolor='rgb(221, 221, 221)',
                    )


                except Exception:
                    self.error('Failed to create figure object for plot - ({})'.format(gtype))
                    return False

                else:
                    self.FIGURE = go.Figure(data=self.DATA, layout=self.LAYOUT)
                    return True


            else:
                self.error('This should NEVER be run -- Issue in worker design')
                return False

    def build3D(self,cache,cs,d0,d1,xt,xc,xs,yt,yc,ys,zt,zc,zs,freq3D):

        self.fxn = self.getfxn()

        if not self.core_param_validator(cache, cs): return False
        else:

            try:

                temp_payloads = []

                if self.create_payloads(d0, d1, xt, freq3D, xc): temp_payloads += self.payloads
                if self.create_payloads(d0, d1, yt, freq3D, yc): temp_payloads += self.payloads
                if self.create_payloads(d0, d1, zt, freq3D, zc): temp_payloads += self.payloads

                self.payloads = temp_payloads

                if not self.MT_controller():
                    self.error('Bad MT operations')
                    return False

                else:

                    valid_ticks = [p['t'] for p in self.payloads]
                    used_ticks = []

                    temp_col = self.genColors(len(self.payloads))
                    use_col = []

                    XDATA,YDATA,ZDATA = [],[],[]


                    if xs in valid_ticks:
                        XDATA = [p['df'][xc.lower()] for p in self.payloads if p['t'] == xs][0]
                        used_ticks.append(xs)
                    else:
                        XDATA = unlist([list(p['df'][xc.lower()]) for p in self.payloads])



                    if ys in valid_ticks:
                        YDATA = [p['df'][yc.lower()] for p in self.payloads if p['t'] == ys][0]
                        used_ticks.append(ys)
                    else:
                        YDATA = unlist([list(p['df'][yc.lower()]) for p in self.payloads])



                    if zs in valid_ticks:
                        ZDATA = [p['df'][zc.lower()] for p in self.payloads if p['t'] == zs][0]
                        used_ticks.append(zs)
                    else:
                        ZDATA = unlist([list(p['df'][zc.lower()]) for p in self.payloads])



                    self.DATA = go.Scatter3d(

                            x = XDATA,
                            y = YDATA,
                            z = ZDATA,

                            # name='{} ({})'.format(p['t'], p['ac']),

                            mode='markers',
                            marker=dict(
                                size = 10,
                                colorscale = 'Magma',
                                opacity = 0.75
                            ),

                            # showlegend=True,

                            # text=np.array(self.hovertext[i]) if isok(self.hovertext[i]) else np.array([]),
                            # hoverinfo='text' if isinstance(self.hovertext[i], list) and
                            #                     len(self.hovertext[i]) > 0 else 'none'
                        )

                    self.LAYOUT = go.Layout(
                        title_text='Scatter Plot',
                        paper_bgcolor='rgb(200, 200, 200)',
                        plot_bgcolor='rgb(221, 221, 221)',
                        margin = dict(l=0,r=0,b=0,t=0)

                    )



            except Exception:
                self.error('Bad 3D Construction')
                return False

            else:
                self.po('\n> [*] 3D Plot OK')
                self.FIGURE = go.Figure(data=self.DATA, layout=self.LAYOUT)
                return True







