# Import requests and BeautifulSoup
import requests
from bs4 import BeautifulSoup

# Define the URL and the headers
url = "https://infostrefa.com/infostrefa/pl/spolki?market=mainMarket"
headers = {"User-Agent": "Mozilla/5.0"}

# Send a GET request and parse the response
response = requests.get(url, headers=headers)
soup = BeautifulSoup(response.text, "html.parser")

# Find the table with the ticker data
table = soup.find("table",
                  class_="table table-text table-text-left custom-border")

# Loop through the table rows starting from the second row
for row in table.find_all("tr")[1:]:
  cells = row.find_all("td")
  company = cells[0].text.strip()  # The ticker is in the first cell
  ticker = cells[2].text.strip() # The company name is in the second cell
  if ticker:
    print(f"'{ticker}.WA', #{company}")  # Print the result
