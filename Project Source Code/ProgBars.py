
"""
> Module: ProgBars
> Author: Wyatt Marciniak
...


"""

from time import time as ctime
from helper import *
from StatsCore import *
from ClassNoticeHandler import *



class ProgBar(ClassNoticeHandler):

    """
    [CLASS] ProgBars

    > Allows use of adjustable and scalable progress bar (i.e : progress bar for 100,000 iterations fit (scaled) to
    show in 45 (1/45 scale) characters

    > Included % progress indicators (Default tiers: 25, 50, 75, 90) - 100 is the final tier (always)
    ...


    """

    # region > Attributes

    cn = 'ProgBar'
    fxn = '()'

    e = None
    c = None
    char = None
    ppi = None
    ppi_hit = None


    max_length = 40
    pjumps_orig = None
    pjumps_adjusted = None
    max_length_adjusted = None


    scale_orig = 1/max_length

    scale_adjust = math.ceil(scale_orig)

    e_scaled = max_length * scale_adjust


    __pb_time = None
    __pb_updated = False
    __pb_active = False

    print_fail = None
    hidden = False
    suppress_warning = True

    # endregion


    # region > Backend

    def __validate(self):

        try:
            return all([isok(x) for x in [self.e,self.c,self.char,self.ppi,self.ppi_hit,self.__pb_time]])
        except Exception:
            return False

    def __do_update(self):

        if self.__pb_active:

            self.c += 1

            if self.c < self.e:

                if self.c % self.pjumps_adjusted == 0:

                    self.__pb_updated = True

                    for p in list(reversed(list(sorted(self.ppi)))):

                        temp = p if eval('{} >= {}'.format( (self.c/self.e), (p/100))) else None

                        if isok(temp):

                            self.ppi_hit.append(p)
                            self.ppi.remove(p)
                            if not self.hidden: sysout(p)
                            return

                    if not self.hidden:

                        sysout(self.print_fail)

                        if self.c + 1 == self.e:
                            if not self.hidden: sysout(' :')


                else:
                    self.__pb_updated = False

            elif self.c == self.e:

                if not self.hidden:
                    sysout(' 100]')
                    self.__pb_active = False

            else:
                self.__pb_active = False


    # endregion

    # region > Frontend (UI)

    def start(self,iter,char=None,show_perct=None,max_length=None):
        self.fxn = self.getfxn()

        try:

            self.char = char if isok(char) else '-'

            self.ppi = show_perct if isok(show_perct) else [25,50,75,90]
            self.ppi_hit = []

            self.e = iter
            self.c = 0

            if isok(max_length):
                self.max_length = max_length

            self.max_length = min(self.e,self.max_length)
            self.pjumps_orig = self.e / self.max_length

            self.pjumps_adjusted = math.ceil(self.pjumps_orig)

            self.max_length_adjusted = math.ceil(self.e / self.pjumps_adjusted)

        except Exception:
            self.error('Bad params [check call]')
            return

        else:


            # Set fail print character & adjust #self.c and @self.e for @self.max_length

            self.__pb_active = True
            self.print_fail = ' '

            sysout('\n-----| Progress Bar --> Iter: {}, Scale: 1/{} |-----'.format(self.e,self.max_length_adjusted),
                   '\n[')

            for self.c in range(0, self.e): self.__do_update()


            # Reset progress percentage indicator (ppi) lists & current iter tracker (@self.c)

            self.ppi = list(reversed(list(sorted(self.ppi_hit.copy()))))
            self.ppi_hit = []
            self.c = 0

            self.__pb_active = True
            self.__pb_time = ctime()

            sysout('\n[')

    def update(self,alt=None):
        self.fxn = self.getfxn()

        self.hidden = False

        if self.__pb_active and self.__validate():

            self.print_fail = self.char

            if isok(alt) and alt == 'test': self.hidden = True

            self.__do_update()

            if isok(alt) and alt == 'test':
                sysout('\n-----| ProgBar testing - Iter {} of {} |-----'.format(self.c, self.e),
                       '\n> {} % {} = {}\t | {} - {} | '.format(self.c,self.pjumps_adjusted,
                                                                np.round(self.c % self.pjumps_adjusted,2),
                                                                self.__pb_updated,self.__pb_active))

            elif self.c == self.e:
                if not self.hidden: sysout(' (RT: {})'.format(crt(self.__pb_time)))

    # endregion


# region > Tests & Examples


# Example 1


# pbar = ProgBar()
#
# tt = 1587
#
# do_test = 'test'
#
# pbar.start(tt,'-',show_perct=[10,28,37,45,60,88,95],max_length=50)
# for i in range(tt):
#     pbar.update(do_test)

# endregion


# pb = ProgBar()
#
# a = list(range(1,1000000))
# b = []
#
# pb.start(len(a),'-',max_length=45)
#
# for e in a:
#     b.append((e**2)/(e/5))
#     pb.update()
#
# print(b)
