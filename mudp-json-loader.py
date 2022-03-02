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
    xtimeseries = {}
    with open(file) as f:
        data = json.load(f)
        xtimeseries['header'] = data['header']
        xtimeseries['artifact'] = data['artifact']
        xtimeseries['version_info'] = data['version_info']
        del xtimeseries['artifact']['streams']
        for timeseries in data['timeseries_values']:
            for count, timestamp in enumerate(timeseries['timestamp']):
                if timestamp in xtimeseries:
                    print(type(timeseries['values'][count]))
                    xtimeseries[timestamp]['measurements'].append(
                        {
                            "name": timeseries['name'],
                            "source": timeseries['source'],
                            "value": timeseries['values'][count]
                        }
                    )
                else:
                    print(type(timeseries['values'][count]))
                    measurement = {
                        "timestamp": datetime.datetime.utcfromtimestamp(timestamp).isoformat(),
                        "measurements": [
                            {
                                "name": timeseries['name'],
                                "source": timeseries['source'],
                                "value": timeseries['values'][count]
                            }
                        ]
                    }
                    xtimeseries[timestamp] = measurement
        return json.dumps(xtimeseries, ignore_nan=True)
        # return {
        #         "_index": index,
        #         "_op_type": "index",
        #         # "_id": hashlib.sha1(key).hexdigest(),
        #         "_source": json.dumps(xtimeseries, ignore_nan=True)
        #       }


if __name__ == '__main__':
    args = parseargs(sys.argv[1:])
    file = args['jsonfile']
    index = args['index']
    print('Input file: ', file)
    print('Index: ', index)
    with open('output.json', 'w') as f:
        print(xform_json(file, index), file=f)
