import os,sys,math,random
from yahoofinance import *
from helper import *
from JsonConverter import jsonConverter as jpipe

"""
> Class: TechnicalAnalysisToolbox (tat,techbox,etc...)
> Author: Wyatt Marciniak
> -
-- Notes --
This class handles technical indicators for market data (Bollinger Bands, RSI, etc...).
Core functionalities include: generating / read-write / mutating / cache ops (+ App) / etc...
 
[WIP]
 
"""
#
# rre_iso = {
#     'tid':'aapl',
#     'did':'mkt_d',
#     'd0':'-','d1':'-',
#     'iid':'bbands',
#     'param':{
#         'time_lag':20,
#         'num_stds':2
#     }
# }
# rre = {
#
# }

# import matplotlib.pyplot as mplot

# mplot.Figure()

# def tech_sma(d,p=20):
#     d = [x for x in d if not str(x).lower() in ['none','na','null','-','--',' ']]
#     return [cSum(d[(0+i):(p+i)])/p for i in range(len(d)-p)]



# testdata = [sample(list(range(100,111)),1) for i in range(100)]
# testindex = [dtt.utcfromtimestamp(int(time.time() - (100+i))) for i in range(100)][50:]
# testsma = tech_sma(df['AAPL.Close'],20)
# testsma
#
# len(testsma)
# len(testindex)
#
# import plotly.graph_objs as go
# import plotly.offline as localplot
# fig = go.Figure([go.Scatter(x=df['Date'][20:], y=testsma)])
# localplot.plot(fig)

# from plotly import graph_objs as go
# testdata['datetime_obj'] = [dtt.utcfromtimestamp(int(x)) for x in testdata['dateu']]
# fig = go.Figure([go.Scatter(x=testdata['datetime_obj'],y=testdata['open'])])
#
# fig2 = go.Scatter(x=testdata['datetime_obj'],y=testdata['open'])
# fig2.show()


# len(test_sma_20)
# len(testdata.index) - 20
# mkstr([1,2,3],[4,5,6],6,7,'1','s',sep='_',sep_list='/',sep_tuple='-')

# df[df.columns[0]]

# df.iloc[0:2]

# rassets=req_assets({'equity':['aapl','dhi']})
#
# tempasset1 = rassets['aapl']['mkt_d']

class TechTools:
    """
    > Data Should be pd.DataFrame

    """
    def __init__(self,data=None): sysout('\n> [TechTools] OK')

    # Return Functionalities (periodic/growth/etc...)
    def returns(self,df,dcol,type='period',col_index=0):
        try:
            d = list(df[dcol])
            if type=='period':
                ret = [(d[i+1]-d[i])/d[i] for i in range(len(df.index)-1)]
            else:
                ret = [(d[i+1]-d[0])/d[0] for i in range(len(df.index)-1)]
        except Exception:
            print('OH SNAP')
            return None
        else:
            df = df.iloc[1:]
            return kv2df(
                k=['date', dcol, 'ret_{0}'.format(type)],
                v=[list(df[df.columns[col_index]]), list(df[dcol]), ret])

    # Simple Moving Average
    def sma(self,df,dcol,p=20,col_index=0):
        try:
            sma = [cSum(df[dcol][(0 + i):(p + i)]) / p for i in range(len(df[dcol]) - p)]
        except Exception: return None
        else:
            df = df.iloc[p:]
            return kv2df(
                k=['date',dcol,'sma_{0}'.format(p)],
                v=[list(df[df.columns[col_index]]),list(df[dcol]),sma])

    # Rolling Standard Deviation (Volatility)
    def rolling_std(self,df,dcol,p=20,col_index=0):
        try:
            rstd = [cStd(df[dcol][(0 + i):(p + i)]) for i in range(len(df[dcol]) - p)]
        except Exception: return None
        else:
            df = df.iloc[p:]
            return kv2df(
                k=['date',dcol,'rollstd_{0}'.format(p)],
                v=[list(x) for x in [df[df.columns[col_index]],df[dcol],rstd]])

    # Private helper functions
    def __typical_price(self,df):
        df.columns = [x.lower() for x in df.columns]
        row_list = [[df[n][i] for n in ['high', 'low', 'close']] for i in range(len(df.index))]
        return [cSum(x)/3 for x in row_list]

    def __iso_sma(self,d,p):
        return [cSum(d[(0 + i):(p + i)])/p for i in range(len(d)-p)]

    def __iso_rolling_std(self,d,p):
        return [cStd(d[(0 + i):(p + i)]) for i in range(len(d)-p)]

    # Bollinger Bands (BBands)
    def bbands(self,df,dcol,p=20,nstds=2,col_index=0):
        try:
            sma = self.sma(df,dcol,p)['sma_{0}'.format(p)] # Middle Band
            rstd = self.rolling_std(df,dcol,p)['rollstd_{0}'.format(p)]
            uband = [float(sma[i])+(float(rstd[i])*nstds) for i in range(len(sma))]
            lband = [float(sma[i])-(float(rstd[i])*nstds) for i in range(len(sma))]
        except Exception: return None
        else:
            df = df.iloc[p:]
            return kv2df(
                k=['date',dcol,
                   'bbands_{0}-{1}_low'.format(p,nstds),
                   'bbands_{0}-{1}_mid'.format(p, nstds),
                   'bbands_{0}-{1}_upper'.format(p, nstds)],
                v=[list(x) for x in [df[df.columns[col_index]],df[dcol],lband,sma,uband]])

    # Relative Strength Index (RSI) [0:100] - low/high (30/70)
    # def rsi(self,df,dcol,):

    # Commodit Channel Index (CCI) - [-100:100]
    def cci(self,df,p=20,col_index=0,lambert_constant=0.015):
        tp = self.__typical_price(df)
        sma = self.__iso_sma(tp,p)
        std = self.__iso_rolling_std(tp,p)
        df = df.iloc[p:]; tp = tp[p:]
        df.columns = [x.lower() for x in df.columns]
        cci = [(tp[i]-sma[i])/(lambert_constant*std[i]) for i in range(len(sma))]
        return kv2df(
            k=['date','open','high','low','close','cci_{}'.format(p)],
            v=[list(x) for x in [df[df.columns[col_index]],
                                 df['open'],df['high'],df['low'],df['close'],cci]]
        )

    # Moving Average Conv/Div Oscillator (MACD)

    # MACD Histogram - Distance of MACD to signal line (9day EMA of MACD)


# tt1.date
#
# tt0 = TechTools()
# tt1 = tt0.bbands(df,'AAPL.High',25,2)
# tt2 = tt0.returns(tempasset1,'close','growth')
#
# import plotly.graph_objs as go
# import plotly.offline as localplot
#
# fig = go.Figure()
# fig.add_trace(go.Scatter(
#     x=tt1.date,y=tt1['AAPL.High'],name='Mkt_High',fillcolor='black'))
# fig.add_trace(go.Scatter(
#     x=tt1.date,y=tt1['bbands_25-2_low'],name='bband_low',fillcolor='yellow'))
# fig.add_trace(go.Scatter(
#     x=tt1.date,y=tt1['bbands_25-2_mid'],name='bband_mid',fillcolor='green'))
# fig.add_trace(go.Scatter(
#     x=tt1.date,y=tt1['bbands_25-2_upper'],name='bband_high',fillcolor='red'))
#
# fig.layout.update(title_text='AAPL (High) - Bbands (25,2)',showlegend=True)
#
# localplot.plot(fig)
# fig = go.Figure([go.Scatter(x=df['Date'][20:], y=testsma)])
# localplot.plot(fig)

# import plotly.plotly as go
# import plotly.offline as localplot
# localplot.plot(fig)
# import pandas as pd
# df = pd.read_csv('https://raw.githubusercontent.com/plotly/datasets/master/finance-charts-apple.csv')
#
# fig = go.Figure([go.Scatter(x=df['Date'], y=df['AAPL.High'])])
# fig