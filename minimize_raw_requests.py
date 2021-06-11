import csv
import sys
import json


if __name__ == "__main__":
    with open(sys.argv[1]) as f, open(sys.argv[2], "w") as stream:
        records = [
            {
                "request_id": r["request_id"],
                "request": json.loads(r["request"]),
                "timestamp": r["timestamp"],
            }
            for r in csv.DictReader(f)
        ]

        writer = csv.writer(stream)
        writer.writerow(["request_id", "request", "timestamp"])

        for record in records:
            mini_request = dict()

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
                mini_request.update({"session": {"user": {"sub": maybe_subject[0]}}})

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
                edge_user_id = dict(
                    s.strip().split("=")
                    for s in maybe_cookie[0].split("; ")
                    if s.startswith("edgeUserId")
                ).get("edgeUserId")
                drift_aid = dict(
                    s.strip().split("=")
                    for s in maybe_cookie[0].split("; ")
                    if s.startswith("edgeUserId")
                ).get("drift_aid")
                if edge_user_id:
                    mini_request.update(
                        {
                            "headers": {
                                "cookie": f"drift_aid={drift_aid}; edgeUserId={edge_user_id}"
                            }
                        }
                    )

            writer.writerow(
                [record["request_id"], json.dumps(mini_request), record["timestamp"]]
            )
