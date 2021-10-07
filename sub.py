import requests
from bs4 import BeautifulSoup

url = "https://www.cnlopb.ca/information/statistics/#rm"

response = requests.get(url)

soup = BeautifulSoup(response.text, 'html.parser')


links = soup.find_all('a')
user_links = soup.find('h5', text=pattern).find_next_sibling('ul').find_all('a')
print(links, user_links)

i = 0

for link in links:
    if ('oil' in link.get('href', [])):
        i += 1
        response = requests.get(link.get('href'))
        pdf = open("pdf" + str(i) + ".pdf", 'wb')
        pdf.write(response.content)
        pdf.close()
