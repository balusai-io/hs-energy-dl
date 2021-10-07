import requests
from bs4 import BeautifulSoup

url = "https://www.cnlopb.ca/information/statistics/#rm"

response = requests.get(url)

soup = BeautifulSoup(response.text, 'html.parser')

pattern = input("Enter your value: ")
links = soup.find_all('a')
user_links = soup.find('h5', text=pattern).find_next_sibling('ul').find_all('a')

if pattern == '':
    i = 0
    for link in links:
        if ('oil' in link.get('href', [])):
            i += 1
            print("Downloading file: ", i)

            response = requests.get(link.get('href'))
            name = link.get('href').split('/')[-1]
            pdf = open(name, 'wb')
            pdf.write(response.content)
            pdf.close()
            print("File ", i, " downloaded")

else:
    i = 0
    for link in user_links:
        if ('.pdf' in link.get('href', [])):
            i += 1
            print("Downloading file: ", i)

            response = requests.get(link.get('href'))

            name = link.get('href').split('/')[-1]
            pdf = open(name, 'wb')
            pdf.write(response.content)
            pdf.close()
            print("File ", i, " downloaded")