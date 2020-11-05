import re

f = open("index.html", "r")
file_str = f.read()

urls = re.findall("https?://[a-zA-Z0-9_.+-/#]+.zip", file_str)

with open('urls_list.txt', 'w') as f:
	for item in urls:
		f.write("%s\n" % item)
