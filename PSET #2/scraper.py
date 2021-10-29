import requests
import re
import time
from bs4 import BeautifulSoup

url = 'http://books.toscrape.com/'


def get_book_title(book):
    description_content = book.find('h3').find('a', href=True)
    return description_content['title']


def get_num_stars(book):
    star_content = book.find('p')

    # Finds the first match and extracts the string matching .*
    # E.g. <p class="star-rating Three"> -----> Three
    return re.search('<p class="star-rating (.*)">|$', str(star_content)).group(1)


def get_price_and_availability(book):
    price_and_availability_content = book.find('div', {'class': 'product_price'})
    price_content = price_and_availability_content.find('p', {'class': 'price_color'})
    price = price_content.get_text(strip=True)
    # print(price)
    availability_content = price_and_availability_content.find('p', {'class': 'instock availability'})
    availability = availability_content.get_text(strip=True)
    is_available = availability == 'In stock'
    # print(availability_content.get_text(strip=True) )
    # print(price_content.encode_contents(formatter='html'))
    return price, is_available

if __name__ == '__main__':
    # Scrape main website
    response = requests.get(url)
    soup = BeautifulSoup(response.text, 'html.parser')

    # Get links to book categories
    book_categories = soup.find('div', {'class': 'side_categories'})

    # Iterate through each book category
    for link in book_categories.find_all('a', href=True):
        file_url = url + link['href']
        print("Scraping data from: ", file_url)

        category_response = requests.get(file_url)
        category_soup = BeautifulSoup(category_response.text, 'html.parser')

        # Iterate through each book
        for book in category_soup.find('body').find('ol').find_all('article', {'class': 'product_pod'}):
            book_title = get_book_title(book)
            num_stars = get_num_stars(book)
            price, is_available = get_price_and_availability(book)
            print(book_title, num_stars, price, is_available)
        
        time.sleep(0.5)
        break
