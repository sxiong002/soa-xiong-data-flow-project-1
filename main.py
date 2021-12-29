import ast
import json
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

subscription_id = 'projects/york-cdf-start/subscriptions/soa-dataflow-sub'
usd_table = 'york-cdf-start:s_xiong_proj_1.usd_order_payment_history_3'
eur_table = 'york-cdf-start:s_xiong_proj_1.eur_order_payment_history_3'
gbp_table = 'york-cdf-start:s_xiong_proj_1.gbp_order_payment_history_3'
currency = ['USD', 'EUR', 'GBP']
pub_sub_id = 'projects/york-cdf-start/subscriptions/dataflow-project-orders-stock-update-sub'
pub_topic_id = 'projects/york-cdf-start/topics/dataflow-project-orders'

# setting the fields for the table schema
table = {
    'fields': [
        {'name': 'order_id', 'type': 'INTEGER', 'mode': 'NULLABLE'},
        {'name': 'order_currency', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'order_address', 'type': 'RECORD', 'mode': 'REPEATED',
         'fields': [
             {'name': 'order_building_number', 'type': 'STRING', 'mode': 'NULLABLE'},
             {'name': 'order_street_name', 'type': 'STRING', 'mode': 'NULLABLE'},
             {'name': 'order_city', 'type': 'STRING', 'mode': 'NULLABLE'},
             {'name': 'order_state_code', 'type': 'STRING', 'mode': 'NULLABLE'},
             {'name': 'order_zip_code', 'type': 'INTEGER', 'mode': 'NULLABLE'}
         ]
         },
        {'name': 'first_name', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'last_name', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'customer_ip', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'cost_total', 'type': 'FLOAT', 'mode': 'NULLABLE'}
    ]
}


class split(beam.DoFn):
    data = {}

    def process(self, element):
        # set the variable to each element in the dictionary
        order_id = int(element['order_id'])
        order_currency = element['order_currency']
        order_building_number = element['order_address'].split()[0]
        street_name = element['order_address'].split(',')[0].split()[1:]
        order_street_name = ' '.join(street_name)
        order_city = element['order_address'].split(',')[1]
        order_state_code = element['order_address'].split(',')[-1].split()[-2]
        order_zip_code = int(element['order_address'].split()[-1])
        first_name = element["customer_name"].split()[0]
        last_name = element["customer_name"].split()[1]
        customer_ip = element['customer_ip']

        # putting order_items into a loop to grab the sum of price in the dictionary
        order_items = element['order_items']
        price = 0
        for i in range(len(order_items)):
            price = price + float(order_items[i]['price'])

        cost_shipping = float(element['cost_shipping'])
        cost_tax = float(element['cost_tax'])
        cost_total = sum([price, cost_shipping, cost_tax])

        # creating new dictionary to contain the new information
        data = {
            'order_id': order_id,
            'order_currency': order_currency,

            # order_address contains a dictionary within
            'order_address': [{
                'order_building_number': order_building_number,
                'order_street_name': order_street_name,
                'order_city': order_city,
                'order_state_code': order_state_code,
                'order_zip_code': order_zip_code
            }],
            'first_name': first_name,
            'last_name': last_name,
            'customer_ip': customer_ip,
            'cost_total': cost_total
        }
        yield data


def filter_currency(element, type_currency):
    return currency.index(element['order_currency'])


class printVal(beam.DoFn):
    def process(self, element):
        element = element.decode()
        element = ast.literal_eval(element)
        # print(element)
        return [element]


# class create_msg(beam.DoFn):
#     def process(self, element):
#         item_list = []
#         items = element['order_id']
#         for i in range(len(items)):
#             item_list.append(items[i])
#         yield item_list


def pipelineFn():
    with beam.Pipeline(options=PipelineOptions(streaming=True, save_main_session=True)) as pipeline:
        usd, eur, gbp = (
                pipeline | 'read in data' >> beam.io.ReadFromPubSub(subscription=subscription_id)
                | 'print data' >> beam.ParDo(printVal())
                | 'Partition' >> beam.Partition(filter_currency, len(currency))
        )

        # convert pcollection and send message to pub sub
        data = pipeline | beam.io.ReadFromPubSub(subscription=subscription_id)
        get_data = data | 'get data' >> beam.ParDo(printVal())
        msg = get_data | 'create output for pub sub' >> beam.ParDo(split())
        convert = msg | 'convert' >> beam.Map(lambda s: json.dumps(s).encode('utf-8'))
        convert | 'publish to pub sub' >> beam.io.WriteToPubSub(topic=pub_topic_id)
        convert | 'read' >> beam.io.ReadFromPubSub(subscription=pub_sub_id) | beam.Map(print)

        # writes out the 3 currencies data onto big Query

        usd_currency = usd | 'take USD currency' >> beam.ParDo(split())
        usd_currency | 'Write to usd currency table' >> beam.io.WriteToBigQuery(
            usd_table,
            schema=table,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED)
        # usd_currency | beam.Map(print)

        eur_currency = eur | 'take EUR currency' >> beam.ParDo(split())
        eur_currency | 'Write to eur currency table' >> beam.io.WriteToBigQuery(
            eur_table,
            schema=table,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED)
        # eur_currency | beam.Map(print)

        gbp_currency = gbp | 'take GBP currency' >> beam.ParDo(split())
        gbp_currency | 'Write to gbp currency table' >> beam.io.WriteToBigQuery(
            gbp_table,
            schema=table,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED)
        # eur_currency | beam.Map(print)


if __name__ == '__main__':
    # runs pipelineFn
    pipelineFn()
