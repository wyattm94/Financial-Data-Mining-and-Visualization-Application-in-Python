
"""



"""


from BaseOption import *


# Implied Volatility calculation functions (bisection, secant, )

class ImpliedVolatility(BaseOption):

    cn = 'ImpliedVolatility'
    fxn = '()'
    iv_method = 'bisection'

    # region > Attributes

    f_iv_f = None
    lb = 0.0001
    ub = 1.0
    flb = None
    fub = None
    fiv = None

    temp_lb = lb
    temp_ub = ub

    iter = 1000
    curr_iter = 0
    tolr = 10 ** -6

    iv = None
    iv_opprice = None
    implied_vol = None
    hold_iv_ts = []
    iv_flag_BREAK = False

    # endregion


    # region > Private Functionality


    """ 
    > Calculates the target function (to optimize) lower-bound (flb) and upper-bound (fub) prices

    """

    def __c_func(self ,vlb ,vub):
        self.fxn = self.getfxn()

        self.flb = self.f_iv_f(v=vlb, local=True)[self.optype]
        self.fub = self.f_iv_f(v=vub, local=True)[self.optype]


    """ 
    > Method: Bisection
        > [1] Calculates the curr target implied volatility optimized function (fiv)
        > [2] Runs operation (iterations) to convergence or None

    """

    def __c_iv_bisection(self):
        self.fxn = self.getfxn()

        # Calculate new IV and f(IV) to optimize
        self.iv = self.temp_lb + ((self.iv_opprice - self.flb) *
                                  ((self.temp_ub - self.temp_lb) / (self.fub - self.flb)))

        self.fiv = self.f_iv_f(v=self.iv, local=True)[self.optype]

        # Append next (current) iv to tracking list
        self.hold_iv_ts.append(self.iv)

    def __c_iter_bisection(self):
        self.fxn = self.getfxn()

        self.__c_func(self.temp_lb, self.temp_ub)
        self.__c_iv_bisection()

    def __c_run_bisection(self):
        self.fxn = self.getfxn()

        try: self.__c_iter_bisection()
        except Exception:
            self.fxn = self.getfxn()
            self.error('Bad Calculation (pre iteration)'); raise Err_Pass
        else:

            while (self.temp_ub - self.temp_lb) > self.tolr and self.curr_iter < self.iter:

                self.curr_iter += 1

                # Alter range to convergence
                if self.fiv < self.iv_opprice: self.temp_lb = self.iv
                else: self.temp_ub = self.iv

                try: self.__c_iter_bisection()
                except Exception:
                    self.error('Bad Calculation (in iteration)'); raise Err_Pass


    """ 
        > Method: Secant
            > [1] Calculates the curr target implied volatility optimized function (fiv)
            > [2] Runs operation (iterations) to convergence or None

            > [NOTE] x_(n-1) = ub (temp_ub) & x_(n-2) = lb (temp_lb) --> At Start

    """

    def __c_iv_secant(self):
        self.fxn = self.getfxn()

        # Calculate new IV
        self.iv = self.temp_ub + (
            (self.iv_opprice - self.fub) *
            ((self.temp_ub - self.temp_lb) / (self.fub - self.flb))
        )
        # self.iv = self.temp_ub - (self.fub * ((self.temp_ub - self.temp_lb) / (self.fub - self.flb)))

        self.hold_iv_ts.append(self.iv)

    def __c_run_secant(self):
        self.fxn = self.getfxn()

        # Main Operation
        while self.curr_iter < self.iter:

            self.curr_iter += 1

            # [*] Next iteration calculations
            try:
                self.__c_func(self.temp_lb, self.temp_ub)
                self.__c_iv_secant()
            except Exception:
                self.error('Bad Calculation (in iteration)')
                raise Err_Pass

            else:

                if not isok(self.iv) or math.isnan(self.iv) or abs(self.iv - self.temp_ub) < self.tolr:
                    break
                else:
                    self.temp_lb = self.temp_ub
                    self.temp_ub = self.iv

        # Executes if max reached before converging
        # self.iv = self.temp_ub


    # endregion


    """
    > Main execution call --> Ideal to keep shell for extended classes

    """

    def calc_iv(self):
        self.cn = 'ImpliedVolatility'
        self.fxn = self.getfxn()

        # Pre-processing
        time0 = time.time()

        self.hold_iv_ts = []
        self.curr_iter = 0
        self.iv = 0
        self.iv_opprice = 0 # Leaves @self.opprice un-unchanged
        self.implied_vol = {}

        try:

            # Recalculate Price

            self.black_scholes()
            self.iv_opprice = self.opprice if isok(self.opprice) else self.bs_opval[self.optype]

        except Exception:
            self.fxn = self.getfxn()
            self.error('Bad Calculation (iv_opprice)')

        else:
            # Main runner
            if self.iv_method in ['bisection' ,'secant']:

                if not isok(self.f_iv_f): self.f_iv_f = self.black_scholes

                self.temp_lb = self.lb
                self.temp_ub = self.ub

                try:
                    self.__c_run_bisection() if self.iv_method == 'bisection' else self.__c_run_secant()
                except Exception:
                    self.fxn = self.getfxn()  # Reset
                    self.error('Bad Calculation (main run function)')

                else:

                    if not isok(self.iv) or math.isnan(self.iv) or math.isinf(self.iv): self.iv = None
                    self.implied_vol = {'iv': self.iv, 'iter': self.curr_iter, 'runtime': crt(time0)}