import csv
import os

def collectAttributeNames(files):

	attributes = []

	for file in files:
	
		with open(file, 'r') as inputfile:
			reader = csv.reader(inputfile)
			
			head = reader.__next__()
			for attr in head:
				if attr not in attributes:
					attributes.append(attr)

	return attributes

def createHeaderMapping(header0, header1):

	mapping = []

	for attr in header0:
		if attr in header1:
			mapping.append(header1.index(attr))
		else:
			mapping.append(-1)
	
	return mapping

def validateCSVFiles(inputFiles, exclude = []):
	outputFiles = []
	
	for file in inputFiles:
		if file[-4:] == '.csv' and file not in exclude:
			outputFiles.append(file)
			
	return outputFiles
	
# Main

outFileName = os.path.basename(os.getcwd()) + '.csv'

files = validateCSVFiles(os.listdir(), exclude=[outFileName])

header = collectAttributeNames(files)

with open(outFileName, 'w', newline='') as outFile:
	writer = csv.writer(outFile)
	writer.writerow(header) # write csv header
	
	for file in files:
		with open(file, 'r') as inputfile:
			reader = csv.reader(inputfile)
			localHeader = reader.__next__() # skip header
			
			mapping = createHeaderMapping(header, localHeader)
			
			for row in reader:
				buffer = []
				for i in mapping:
					if i != -1:
						buffer.append(row[i])
					else:
						buffer.append('')
			
				writer.writerow(buffer)
