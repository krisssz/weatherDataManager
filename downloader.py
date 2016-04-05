import csv
import datetime
import json
import requests
import os
from concurrent.futures import ThreadPoolExecutor

API_key = ''
base_url = 'https://api.forecast.io/forecast/%s/' % (API_key)
params = {'units': 'ca', 'exclude': 'currently,daily,alerts,flags'}
ws_path = '%s\\..\\Data' % (os.getcwd())

start_date = datetime.datetime(2015, 1, 1)

farms = {
'AE008-EM': {'id': 15594, 'sid': 25563, 'lat': 41.386541, 'lon': 1.9334},
'Ballarat Holden': {'id': 36829, 'sid': 33672, 'lat': -37.557406, 'lon': 143.819587},
'fragale total': {'id': 33436, 'sid': 30648, 'lat': 38.099141, 'lon': 14.692466},
'Orpheus_Nord_1': {'id': 4388, 'sid': 3463, 'lat': 45.4189777, 'lon': 10.9292523},
'33.65_-117.05_2006_UPV_200MW': {'lat': 33.65, 'lon': -117.05}, # -
'33.75_-116.65_2006_UPV_200MW': {'lat': 33.75, 'lon': -116.65}, # -
'34.35_-118.25_2006_UPV_200MW': {'lat': 34.35, 'lon': -118.25}, # -
'34.35_-118.35_2006_UPV_200MW': {'lat': 34.35, 'lon': -118.35}, # -
'34.35_-118.65_2006_UPV_200MW': {'lat': 34.35, 'lon': -118.65}, # -
'34.55_-118.55_2006_UPV_200MW': {'lat': 34.55, 'lon': -118.55}, # - 
'34.65_-115.15_2006_UPV_200MW': {'lat': 34.65, 'lon': -115.15}, # -
'34.85_-116.75_2006_UPV_200MW': {'lat': 34.85, 'lon': -116.75}
}

def getUrl(farm, date):
	return '%s%f,%f,%s' % (base_url, farm['lat'], farm['lon'], date.__str__().replace(' ', 'T'))

def writeToCSV(jsonObj, filePath):

	try:
		with open(filePath + '.csv', 'w', newline='') as csvfile:
			writer = csv.writer(csvfile)
			
			head = True
			for row in jsonObj['hourly']['data']:
				buffer = []
				
				if head:
					for attr in row:
						buffer.append(attr)
					head = False
					writer.writerow(buffer)
					buffer.clear()
					
				for attr in row:
					buffer.append(row[attr])
				writer.writerow(buffer)
		
		print(' '.join([filePath, 'COMPLETED..']))
	
	except Exception as e:
		print('>>> file write failed: \'%s\'' % (filePath))
		print(e)

def downloadData(url, filePath):
	try:
		response = json.loads(requests.get(url, params=params).text)
		writeToCSV(response, filePath)
	except Exception as e:
		print('>>> query failed: \'%s\'' % (url))
		print(e)

# Main
		
with ThreadPoolExecutor(max_workers=16) as executor:

	for i in range(365):

		date = start_date + datetime.timedelta(i)
		
		for farm_name in farms:
		
			farm = farms[farm_name]
			date = start_date + datetime.timedelta(i)
			url = getUrl(farm, date)
			path = '\\'.join([ws_path, farm_name])
			
			try:
				if not os.path.exists(path): os.makedirs(path)
				filePath = '\\'.join([path, date.__str__()[:10]])
				
				executor.submit(downloadData, url, filePath)
			except Exception as e:
				print('>>> query failed: \'%s\'' % (url))
				print(e)
