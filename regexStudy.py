import re

# pattern = r"(?i)(?P<fsource>^\s+source)\s+:\s+(?P<op>.+)"
# pattern2 = r"(?i)mozilla"
# #2016/05/28 19:24:30 ERROR
# pattern3 = r"(ERROR)(.+)"
#
# with open("regexfile","r") as mfile:
#     for line in mfile:
#         # s2 = re.findall(pattern2,line)
#         # if s2:
#         #     i += 1
#         mat = re.search(pattern3,line)
#         if mat:
#             print(mat.group())
from collections import Counter

pattern2 = r"GET|PUT"
tot = []
with open("regexfile", "r") as mfile:
    for line in mfile:
        s2 = re.findall(pattern2,line)
        tot += s2
y = dict(Counter(tot))
print(y)


