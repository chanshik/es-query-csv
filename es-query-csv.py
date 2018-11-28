import sys
import argparse
import json

from elasticsearch import Elasticsearch, helpers, ConnectionError


def setup_arg_parser():
    parser = argparse.ArgumentParser(description="Save query result of ElasticSearch to CSV")
    parser.add_argument(
        "-u", help="ElasticSearch URL", default="http://localhost:9200", dest="es_host")
    parser.add_argument("-j", help="JSON file for Query DSL", dest="json")
    parser.add_argument("-o", help="Output CSV file", default="output.csv", dest="output")
    parser.add_argument("-i", help="Index prefix", default="logstash-*", dest="index")
    parser.add_argument("-f", help="Selected result field to save", default="_all", dest="field")
    parser.add_argument(
        "-m", help="Maximum count of output records", default=100000, dest="max_count", type=int)
    parser.add_argument(
        "-s", help="Get results from script_fields", default=False,
        dest="use_script_fields", type=bool)
    parser.add_argument(
        "-d", help="Delimiter", default=",", dest="delimiter", type=str)

    return parser


def connect_es(host):
    try:
        es = Elasticsearch(hosts=host, timeout=60, max_retries=3, retry_on_timeout=True)
        return True, es

    except ConnectionError as e:
        return False, str(e)


def search(es, query, args):
    index = args.index
    output = args.output
    max_count = args.max_count
    field = args.field
    delimiter = args.delimiter
    use_script_fields = args.use_script_fields

    f = open(output, "w")
    written = 0

    if "_source" not in query and field != "_all":
        query["_source"] = field

    results = helpers.scan(es, query=query, index=index, raise_on_error=False)
    for result in results:
        values = []
        if use_script_fields:
            source = result["fields"]
            for _, v in source.items():
                values.append(",".join(v))
        else:
            source = result["_source"]
            for _, v in source.items():
                values.append(v)

        csv_line = delimiter.join(values)
        f.write(csv_line + "\n")

        written += 1
        if written >= max_count:
            break

        if written > 0 and written % 1000 == 0:
            print("Written: {} lines".format(written))

    f.close()

    return True, written


def main():
    parser = setup_arg_parser()
    args = parser.parse_args()

    if args.json is None:
        print(parser.print_help())
        sys.exit(1)

    success, es = connect_es(args.es_host)
    if not success:
        err = es
        print("Connection failed: {}".format(err))
        sys.exit(254)

    with open(args.json) as f:
        query = json.load(f)

    if query is None:
        print("Invalid query json file")
        sys.exit(253)

    success, lines = search(es, query, args)
    print("Total {} lines to {}".format(lines, args.output))


if __name__ == "__main__":
    main()
