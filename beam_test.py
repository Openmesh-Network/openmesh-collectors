import apache_beam as beam

class NormaliseCoinbase(beam.DoFn):
    def process(self, data):
        yield f"{data['product_id']}-{data['side']}-{data['price']}-{data['size']}"

msg = {
    "order_id": "94fccfc9-0b03-4207-a985-932af9d27547",
    "order_type": "limit",
    "size": "0.01138282",
    "price": "21371.15",
    "client_oid": "62667100-0000-0000-0000-a6b16dad1a02",
    "type": "received",
    "side": "sell",
    "product_id": "BTC-USD",
    "time": "2022-08-22T06:38:56.004314Z",
    "sequence": 42947205834
}

with beam.Pipeline() as pipeline:
    test = (
        pipeline
        | "Sample order" >> beam.Create([msg])
        | "Normalise" >> beam.ParDo(NormaliseCoinbase())
        | beam.Map(print)
    )

