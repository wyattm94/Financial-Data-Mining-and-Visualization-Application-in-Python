
"""
> Module:   mkRange
> Author:   Wyatt Marciniak
...


"""

from helper import isok, sysout
import numpy as np


class mkRange:

    """
    [CLASS] mkRange

    > Creates ranges from any start to end (floats included) with 'by' and 'number of jumps' options as well
    ...

    """

    # region > Attributes

    s = None
    e = None
    b = None
    j = None
    r = None
    next_try = None

    s_open = True
    e_open = False

    cache = []
    last = []


    # endregion

    def __b_range(self):

        try:

            self.last = []

            if isok(self.s) and not isok(self.e):

                self.e = self.s
                self.s = 0


            self.next_try = self.s

            i = 0
            while self.next_try <= self.e:


                if isok(self.j) and i == self.j: break
                else:


                    if (i == 0 and self.s_open) or i > 0:
                        self.last.append(np.round(self.next_try ,self.r) if isok(self.r) else self.next_try)

                    i += 1


                if isok(self.b): self.next_try += self.b
                elif isok(self.j): self.next_try += ((self.e - self.s) / self.j)
                else: self.next_try += 1


            if not self.e_open and self.last[-1] >= self.e and len(self.last) > 1:
                self.last = self.last[:-1]

        except Exception:
            sysout('\n> [!] Invalid Range')

        else:
            self.cache.append(self.last.copy())

    def rng(self ,s ,e=None ,b=None ,j=None ,r=None ,s_open=True ,e_open=False ,local=True):

        try:
            self.s = s
            self.e = e
            self.b = b
            self.j = j
            self.r = r
            self.s_open = s_open
            self.e_open = e_open

        except Exception:
            sysout('\n> [!] Invalid Range')
            return

        else:
            self.__b_range()
            if local: return self.last.copy()


aaa = mkRange().rng(-4,129,1.0056891123 ,e_open=True)