#used to find follower
#sorted raw data stored in links2.csv (sorted by keys from links.csv)
with open("links2.csv") as f:
    lines = f.readlines()
    prev = None
    value = None
    for l in lines:
        s = l[0:-1].split(',')
        if s[0] == prev:
            value = value + s[1] + '\t'
        else:
            lastvalue = value
            value = s[0] + ',' + s[1] + '\t'
            prev = s[0]
            if l is not lines[0]:              
                print lastvalue

    
            
