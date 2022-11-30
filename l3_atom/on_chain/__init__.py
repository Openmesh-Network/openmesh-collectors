from .ethereum import Ethereum

exch = [
    Ethereum
]

mapping = {
    e.name: e for e in exch
}
