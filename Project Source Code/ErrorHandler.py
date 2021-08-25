
"""
> Handles Errors

"""

# Base error class
class Err_General(Exception):

    """
    [CLASS]: Err_General

    > Base error class (parent) for custom errors
    > @em allows overwriting of @err_message

    """

    err_type = 'base_error'
    err_message = 'This is a general error'

    def __init__(self,em=None):
        er = self.err_message
        if not em is None and isinstance(em,str):
            self.err_message = em
            er = self.err_message

        super().__init__(er)

# Custom errors to be raised


"""
[CLASS]: Errors

> Wrapper class for custom errors
> Custom errors extend (inherit from) class @Err_General

"""

class Err_Pass(Err_General):
    err_type = 'pass'
    err_message = 'Error in Subprocess (Climbing Tree)'
    pass

class Err_Func_Operation(Err_General):
    err_type = 'func_operation'
    err_message = 'Function operation threw an exception'
    pass

class Err_Calc(Err_General):
    err_type = 'calc'
    err_message = 'Calculation error'
    pass

class Err_Calc_InIter(Err_General):
    err_type = 'calc_in_iter'
    err_message = 'Calculation error (in iteration)'
    pass

class Err_Calc_PreIter(Err_General):
    err_type = 'calc_pre_iter'
    err_message = 'Calculation error (before iteration)'
    pass

class Err_Calc_PostIter(Err_General):
    err_type = 'calc_post_iter'
    err_message = 'Calculation error (after iteration)'
    pass
