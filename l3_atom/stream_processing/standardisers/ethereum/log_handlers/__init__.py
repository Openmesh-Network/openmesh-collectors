from .uniswap_v3 import *
from .uniswap_v2 import *
from .dodo import *
from .curve import *
from .hashflow import *

log_handlers = [UniswapV3SwapHandler, UniswapV2SwapHandler,
                DodoexSellHandler, DodoexBuyHandler, DodoexSwapHandler, CurveSwapHandler, HashflowTradeHandler]
