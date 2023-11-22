import logging
from openmesh.stream_processing.standardiser import Standardiser
from openmesh.off_chain import OpenSea
from decimal import Decimal
from dateutil import parser


class OpenSeaStandardiser(Standardiser):
    exchange = OpenSea

    async def _trade(self, message):
        payload = message['payload']['payload']

        payment_token = payload['payment_token']
        pt_decimals = int(payment_token['decimals'])

        msg = dict(
            atomTimestamp = message['atom_timestamp'],
            blockTimestamp = payload['event_timestamp'],
            exchange = self.exchange.name,
            maker = payload.get('maker', {}).get('address', None),
            taker = payload.get('taker', {}).get('address', None),
            itemName = payload['item']['metadata']['name'],
            itemId = payload['asset']['nft_id'],
            itemPermalink = payload['asset']['permalink'],
            amountBought = payload['quantity'],
            salePrice = Decimal()
        )

        await self.send_to_topic("nft_trades", **msg)

    async def handle_message(self, msg):
        if msg['event'] == 'item_sold':
            await self._trade(msg)
        else:
            logging.warning(f"{self.id}: Unhandled message: {msg}")
