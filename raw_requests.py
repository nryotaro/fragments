import csv
import sys
import json

if __name__ == "__main__":
    with open(sys.argv[1]) as f, open(sys.argv[2], "w") as stream, open(
        sys.argv[3], "w"
    ) as id_stream:
        records = [
            {
                "request_id": r["request_id"],
                "request": json.loads(r["request"]),
                "timestamp": r["timestamp"],
            }
            for r in csv.DictReader(f)
        ]

        writer = csv.writer(stream)
        writer.writerow(["request_id", "subject"])
        id_writer = csv.writer(id_stream)
        id_writer.writerow(["request_id", "edge_user_id"])

        for record in records:
            request_id = record["request_id"]
            maybe_subject = [
                v
                for k, v in record.items()
                if k == "request"
                for k, v in v.items()
                if k == "session"
                for k, v in v.items()
                if k == "user"
                for k, v in v.items()
                if k == "sub"
            ]
            if len(maybe_subject) == 1:
                writer.writerow([request_id, maybe_subject[0]])

            maybe_cookie = [
                v
                for k, v, in record.items()
                if k == "request"
                for k, v in v.items()
                if k == "headers"
                for k, v in v.items()
                if k == "cookie"
            ]
            if len(maybe_cookie) == 1:
                print(maybe_cookie[0])
                edge_user_id = dict(
                    s.strip().split("=")
                    for s in maybe_cookie[0].split("; ")
                    if s.startswith("edgeUserId")
                ).get("edgeUserId")
                if edge_user_id:
                    id_writer.writerow([request_id, edge_user_id])
