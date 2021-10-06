import requests
from bs4 import BeautifulSoup

# URL from which pdfs to be downloaded

url = "https://www.cnlopb.ca/information/statistics/#rm"
# Requests URL and get response object
response = requests.get(url)

# Parse text obtained
soup = BeautifulSoup(response.text, 'html.parser')

# Find all hyperlinks present on webpage
pattern = input("Enter your value:")
links = soup.find_all('a')
user_links = soup.find('h5', text=pattern).find_next_sibling('ul').find_all('a')
print(links, user_links)

i = 0

# From all links check for pdf link and
# if present download file
for link in user_links:
    if ('.pdf' in link.get('href', [])):
        i += 1
        print("Downloading file: ", i)

        # Get response object for link
        response = requests.get(link.get('href'))

        # Write content in pdf file
        pdf = open("pdf" + str(i) + ".pdf", 'wb')
        pdf.write(response.content)
        pdf.close()
        print("File ", i, " downloaded")

print("All PDF files downloaded")
