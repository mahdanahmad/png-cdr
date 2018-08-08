import csv, codecs

from datetime import datetime, timedelta
from os import listdir, makedirs
from os.path import isfile, join, exists

root_folder	= 'data/'
dest_folder	= 'result/'

header		= ['interaction', 'direction', 'correspondent_id', 'call_duration', 'antenna_id', 'datetime']

encoding	= 'utf-8'

def iterate(filename):
	with codecs.open(root_folder + filename, "r", encoding) as steam_in:
		with codecs.open(dest_folder + filename, "w", encoding) as stream_out:
			reader	= csv.reader(steam_in)
			writer	= csv.writer(stream_out)
			writer.writerow(header)

			for line in reader:
				row	= []

				if line[1] in ["0","1","6","7"]:
					if line[1] in ["0","1"]:
					   row.append("call")
					else:
					   row.append("text")

					if line[1] in ["0","6"]:
					   row.append("in")
					else:
					   row.append("out")

					row.append(line[5])
					row.append(line[26])
					row.append(line[14])

					row.append((datetime.strptime(line[24], '%Y%m%d%H%M%S') + timedelta(hours=11)).strftime("%Y-%m-%d %H:%M:%S"))

				if len(row):
					writer.writerow(row)

def run():
	for filename in listdir(root_folder):
		if (isfile(join(root_folder, filename)) and (filename[-4:] == '.csv')): iterate(filename)


if __name__ == "__main__":
	if not exists(dest_folder): makedirs(dest_folder)

	run()
