import requests
from bs4 import BeautifulSoup
import pandas as pd

#url to scrape data from
base_url = "https://www.ncei.noaa.gov/data/local-climatological-data/access/2021/"

#fetch the web page
response = requests.get(base_url)
response.raise_for_status() # check for request errors

# parse the webpage with beaitfulsoup

soup = BeautifulSoup(response.text, 'html.parser')

# Extract all links to files in the directory

# links = soup.find_all('a', href=True)
links = soup.find('table')
# identify the file correspong to the max data

target_timestamp = '2024-01-19 10:27'
file_url = None

# iterate
for row in links.find_all('tr')[1:]:
    cells = row.find_all('td')

    #check if column has target value
    if len(cells) > 2 and target_timestamp in cells[1].text:
        file_url = cells[0].find('a')['href']
        break

if not file_url:
    raise Exception(f'File with timestam {target_timestamp} not found!')


# If the file URL is relative, make it absolute by combining with the base URL
base_url = 'https://www.ncei.noaa.gov/data/local-climatological-data/access/2021/'  # Replace with the actual base URL
file_url = base_url + file_url if not file_url.startswith('http') else file_url

# Download file
file_name = file_url.split("/")[-1]
response = requests.get(file_url)
response.raise_for_status()

#save file locally
with open(file_name, 'wb') as f:
    f.write(response.content)

print(f'Donwloaded {file_name}')


#load the file into Pandas

df = pd.read_csv(file_name)

df['HourlyDryBulbTemperature'] = pd.to_numeric(df['HourlyDryBulbTemperature'], errors='coerce')
#find the reocrds with the highest temperature
max_temp_record = df[df['HourlyDryBulbTemperature'] == df['HourlyDryBulbTemperature'].max()]

print('Records with the highest HourlyDryBulbTemperature:')
print(max_temp_record)

def main():
    # your code here
    pass


if __name__ == "__main__":
    main()
