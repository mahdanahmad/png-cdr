import csv, codecs

from datetime import datetime, timedelta
from os import listdir, makedirs
from os.path import isfile, join, exists

list_file	= './data/list.txt'
root_folder	= 'data/'
dest_folder	= 'result/'

header		= ['interaction', 'direction', 'correspondent_id', 'call_duration', 'antenna_id', 'datetime']

encoding	= 'utf-8'

def run():
	try:
		with open(list_file, 'r') as lf: list = lf.read().split(',')
	except:
		list	= []

	bydate		= {}
	byuser		= {}
	for filename in list:
		filepath	= root_folder + filename
		if exists(filepath):
			print 'start on ' + filename

			with codecs.open(filepath, 'r', encoding) as stream_in:
				reader	= csv.reader(stream_in)

				for line in reader:
					if line[1] in ["0","1","6","7"] and len(line[4]) > 0 and len(line[14]) > 0:
						row		= []
						date	= datetime.strptime(line[24], '%Y%m%d%H%M%S')

						if line[1] in ["0","1"]:
						   row.append("call")
						else:
						   row.append("text")

						if line[1] in ["0","6"]:
						   row.append("in")
						   row.append(line[6])
						else:
						   row.append("out")
						   row.append(line[5])

						row.append(line[26])
						row.append(line[14])

						row.append(date.strftime("%Y-%m-%d %H:%M:%S"))

						if line[4] not in byuser.keys(): byuser[line[4]] = []
						byuser[line[4]].append(row)

						date_key	= date.strftime("%Y%m%d")
						if date_key not in bydate.keys(): bydate[date_key] = []
						bydate[date_key].append(row)

			print filename + ' added'

	for key, item in byuser.iteritems():
		with codecs.open(dest_folder + 'byuser/' + key + '.csv', 'w') as af:
			writer	= csv.writer(af)
			writer.writerow(header)
			writer.writerows(item)

	for key, item in bydate.iteritems():
		with codecs.open(dest_folder + 'bydate/' + key + '.csv', 'w') as af:
			writer	= csv.writer(af)
			writer.writerow(header)
			writer.writerows(item)

if __name__ == "__main__":
	if not exists(dest_folder + 'bydate'): makedirs(dest_folder + 'bydate')
	if not exists(dest_folder + 'byuser'): makedirs(dest_folder + 'byuser')

	run()
