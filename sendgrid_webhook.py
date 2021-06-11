import csv
import sys
import json
import collections

if __name__ == "__main__":

    with open(sys.argv[1]) as stream:
        events = []
        for record in csv.DictReader(stream):
            for event in json.loads(record["event"]):
                events.append(
                    {
                        "event_id": record["event_id"],
                        "event": event,
                        "timestamp": record["timestamp"],
                    }
                )

    counter = collections.Counter((k for event in events for k in event['event'].keys()))
