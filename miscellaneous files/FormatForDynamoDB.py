import json
import sys

for l in sys.stdin:
    j = json.loads(l)
    UserID = j["uid"]
    Timestamp = j["timestamp"]
    first = "UserID\x03{\"n\":\"" + str(UserID) + "\"}"
    second = "\x02Timestamp\x03{\"s\":\"" + str(Timestamp) + "\"}"
    third = "\x02Post\x03{\"s\":\"" + l[0:-1].replace("\"", "\\\"") + "\"}"
    print first + second + third
