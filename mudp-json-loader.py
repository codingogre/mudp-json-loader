from datetime import tzinfo, timedelta, datetime
import sys
import getopt
import simplejson as json
from elasticsearch import Elasticsearch
from elasticsearch.helpers import streaming_bulk

ES = Elasticsearch(
     cloud_id="Auto:dXMtZWFzdC0xLmF3cy5mb3VuZC5pbyRiNzdkYzY0NWJiNTA0NjJlYjRlNjVkMjI4Nzk1ZTkwYyQ4MjZiOTdkMmJiM2Y0NjlmYmMzNjNhYzE2ZmE0ZmE5NA==",
     basic_auth=("elastic", "oi1J0gdBWzbHkMO8QcWFyoq2")
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
def xform_json(file):
    with open(file) as f:
        data = json.load(f)
        combined_doc = {"header": data['header']}
        date = (str(data['header']['ISO_Date']) + data['header']['Timestamp'])
        try:
            date = datetime.strptime(date, '%Y%m%d%H%M%S')
        except ValueError:
            sys.exit("Invalid date format: " + date)
        combined_doc['artifact'] = data['artifact']
        combined_doc['version_info'] = data['version_info']
        del combined_doc['artifact']['streams']
        for timeseries in data['timeseries_values']:
            for count, timestamp in enumerate(timeseries['timestamp']):
                if timestamp == 0:
                    break
                if timestamp not in combined_doc:
                    esdoc = {
                        "timestamp":  (date + timedelta(microseconds=timestamp)).isoformat(),
                        "measurements": []
                    }
                    combined_doc[timestamp] = esdoc
                # Use insert() to make sure we pop a new measurement into the list
                combined_doc[timestamp]['measurements'].insert(0, {"name": timeseries['name']})
                combined_doc[timestamp]['measurements'][0]['source'] = timeseries['source']

                datatype = type(timeseries['values'][count])
                if datatype is float:
                    combined_doc[timestamp]['measurements'][0]['floatvalue'] = timeseries['values'][count]
                elif datatype is bool:
                    combined_doc[timestamp]['measurements'][0]['boolvalue'] = timeseries['values'][count]
                elif datatype is list:
                    combined_doc[timestamp]['measurements'][0]['direction'] = timeseries['values'][count][2]
                    combined_doc[timestamp]['measurements'][0]['location'] = str(
                        timeseries['values'][count][1]) + "," + str(timeseries['values'][count][0])
    return combined_doc


def generate_actions(xdoc, index):
    esdoc = {'header': xdoc['header'], 'artifact': xdoc['artifact'], 'version_info': xdoc['version_info']}
    for key in xdoc.keys():
        if type(key) is int:
            esdoc['timestamp'] = xdoc[key]['timestamp']
            esdoc['measurements'] = xdoc[key]['measurements']
            yield {
                    "_index": index,
                    "_op_type": "index",
                    "_source": json.dumps(esdoc, ignore_nan=True)
                  }


if __name__ == '__main__':
    args = parseargs(sys.argv[1:])
    infile = args['jsonfile']
    index_name = args['index']
    print('Input file: ', infile)
    print('Index: ', index_name)
    # with open('output.json', 'w') as f:
    #     print(xform_json(infile), file=f)
    docs = xform_json(infile)
    for success, info in streaming_bulk(client=ES, actions=generate_actions(docs, index_name)):
        if not success:
            print('A document failed:', info)
