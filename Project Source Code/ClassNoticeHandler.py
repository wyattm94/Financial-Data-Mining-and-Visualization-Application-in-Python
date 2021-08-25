
import inspect
from helper import sysout, isok
from ErrorHandler import *


class ClassNoticeHandler:

    """
    [CLASS] ClassNoticeHandler

    > Handles system outputs/notices within a class framework
    > @err class attribute allows for custom errors to be utilized (from file ~/ErrorHandler.py)

    """

    cn = 'ClassNoticeHandler'
    fxn = '()'

    PNOTICE = True


    # Returns current function scope parent name (name of function being called in)

    def getfxn(self):
        return inspect.currentframe().f_back.f_code.co_name

    def devout(self, type='NOTE', m='...'):

        if self.PNOTICE:

            if self.fxn == '()':
                sysout('\n> [{0}] {1}{2}: {3}'.format(type, self.cn, self.fxn, m))

            else:
                sysout('\n> [{0}] {1}.{2}(): {3}'.format(type, self.cn, self.fxn, m))

    def error(self, m='[No Error Info]',err=None):
        mtemp = m
        try: m = err.err_message
        except Exception: m = mtemp

        self.devout('ERROR',m)

    def note(self, m='[No Note Contents]'):
        self.devout('NOTE', m)