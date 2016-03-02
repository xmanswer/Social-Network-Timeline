import sys

with open("sample") as f1:
    lines1 = f1.readlines()
    with open("text.txt") as f2:
        lines2 = f2.readlines()
        for i in lines1:
            for j in lines2:
                if i == j:
                    print "true"
            
