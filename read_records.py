import csv
import sys



if __name__ == '__main__':

    with open(sys.argv[1]) as f:
        records = [r['timestamp'] for r in csv.DictReader(f)][1:]