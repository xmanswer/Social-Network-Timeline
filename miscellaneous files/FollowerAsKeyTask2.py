#used to find follower
#sorted raw data stored in links3.csv
with open("links3.csv") as f:
    lines = f.readlines()
    prev = None
    value = None
    for l in lines:
        s = l[0:-1].split(',')
        if s[1] == prev:
            value = value + s[0] + '\t'
        else:
            lastvalue = value
            value = s[1] + ',' + s[0] + '\t'
            prev = s[1]
            if l is not lines[0]:              
                print lastvalue
