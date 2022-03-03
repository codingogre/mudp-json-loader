import datetime
import sys
import getopt
import simplejson as json
from elasticsearch import Elasticsearch
from elasticsearch.helpers import streaming_bulk

ES = Elasticsearch(
     cloud_id="COVID-19:dXMtZWFzdC0xLmF3cy5mb3VuZC5pbyQ0MmRkYWE2NTg4Yjc0NDkxYjU4ZjdhZDhkZTRlZjM0YiQ3Mjg4ODZjNTRiNTA0MjIzOTM0N2NiNjNjZDBkM2YyMw==",
     http_auth=("elastic", "YZe4U2q1RxVMQLM6MC7TpyWQ")
)


def usage():
    print('mudp-json-transform.py -f <jsonfile> -i <index>')


def parseargs(argv):
    if (len(argv) == 0):
        usage()
        sys.exit(2)
    try:
        opts, args = getopt.getopt(argv, "hf:i:", ["jsonfile=", "index="])
    except getopt.GetoptError:
        usage()
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-h':
            usage()
            sys.exit()
        elif opt in ("-f", "--jsonfile"):
            jsonfile = arg
        elif opt in ("-i", "--index"):
            index = arg
    return {"jsonfile": jsonfile, "index": index}


# Create an dictionary of dictionaries with a key being timestamp
def xform_json(file, index):
    combined_doc = {}
    with open(file) as f:
        data = json.load(f)
        combined_doc['header'] = data['header']
        combined_doc['artifact'] = data['artifact']
        combined_doc['version_info'] = data['version_info']
        del combined_doc['artifact']['streams']
        for timeseries in data['timeseries_values']:
            for count, timestamp in enumerate(timeseries['timestamp']):
                if timestamp in combined_doc:
                    combined_doc[timestamp]['measurements'].insert(0,
                        {

                            "name": timeseries['name'],
                            "source": timeseries['source']
                        }
                    )
                else:
                    esdoc = {
                        "timestamp": datetime.datetime.utcfromtimestamp(timestamp).isoformat(),
                        "measurements": [
                            {
                                "name": timeseries['name'],
                                "source": timeseries['source'],
                            }
                            ]
                    }
                    combined_doc[timestamp] = esdoc

                datatype = type(timeseries['values'][count])
                if datatype is float:
                    combined_doc[timestamp]['measurements'][0]['floatvalue'] = timeseries['values'][count]
                elif datatype is bool:
                    combined_doc[timestamp]['measurements'][0]['boolvalue'] = timeseries['values'][count]
                elif datatype is list:
                    combined_doc[timestamp]['measurements'][0]['direction'] = timeseries['values'][count][2]
                    combined_doc[timestamp]['measurements'][0]['location'] = str(
                        timeseries['values'][count][1]) + "," + str(timeseries['values'][count][0])

        return json.dumps(combined_doc, ignore_nan=True)


# def generate_actions(esdocs):
#     esdoc = {}
#     esdoc['header'] = esdocs['header']
#     esdoc['artifact'] = esdocs['artifact']
#     esdoc['version_info'] = esdocs['version_info']
#     for measurement in esdocs['']:
#         # key = (row["combined_key"] + os.path.basename(url)).encode()
#         yield {
#                 "_index": index,
#                 "_op_type": "index",
#                 # "_id": hashlib.sha1(key).hexdigest(),
#                 "_source": json.dumps(row, ignore_nan=True)
#               }


if __name__ == '__main__':
    args = parseargs(sys.argv[1:])
    file = args['jsonfile']
    index = args['index']
    print('Input file: ', file)
    print('Index: ', index)
    with open('output.json', 'w') as f:
        print(xform_json(file, index), file=f)
    # for success, info in streaming_bulk(client=ES, actions=generate_actions(combined_doc)):
    #     if not success:
    #         print('A document failed:', info)