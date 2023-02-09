from .uniswap_v3 import *
from .uniswap_v2 import *
from .dodo import *
from .curve import *
from .hashflow import *

log_handlers = [UniswapV3SwapHandler, UniswapV2SwapHandler,
                UniswapV2MintHandler, UniswapV2BurnHandler,
                UniswapV3MintHandler, UniswapV3BurnHandler,
                DodoexSellHandler, DodoexBuyHandler, DodoexSwapHandler, CurveSwapHandler, HashflowTradeHandler]
