from typing import DefaultDict
from collections import defaultdict
import requests
import re
import time
from bs4 import BeautifulSoup

url_to_scrape = 'http://books.toscrape.com/'


def get_website_data(url: str) -> BeautifulSoup:
    """Sends an HTTP GET request to the specified URL and returns the response inside a BeautifulSoup object."""
    response = requests.get(url)
    return BeautifulSoup(response.text, 'html.parser')


def get_num_stars(book_soup: BeautifulSoup) -> str:
    # Need to do .next_sibling twice b/c it will grab white text
    star_content = book_soup.find('p', {'class': 'instock availability'}).next_sibling.next_sibling

    # Finds the first match and extracts the string matching .*
    # E.g. <p class="star-rating Three"> -----> Three
    return re.search('<p class="star-rating (.*)">|$', str(star_content)).group(1)


def get_book_data(file_url: str, book_info: BeautifulSoup) -> DefaultDict:
    book_title = book_info['title']

    # Go to book product page
    book_url = file_url + '/../' + book_info['href']
    book_soup = get_website_data(book_url)
    book_soup = book_soup.find('article', {'class': 'product_page'})

    # Need to do .next_sibling twice b/c it will grab white text
    book_description = book_soup.find('div', {'id': 'product_description'}).next_sibling.next_sibling.get_text(strip=True)

    # Store product information as a dictionary
    book_information_dict = defaultdict(str)
    book_information_rows = book_soup.find('table').find_all('tr')
    for row in book_information_rows:
        row_name = row.find('th').get_text(strip=True)
        row_data = row.find('td').get_text(strip=True)
        book_information_dict[row_name] = row_data

    # Store other information
    book_information_dict['Title'] = book_title
    book_information_dict['Stars'] = get_num_stars(book_soup)
    book_information_dict['Description'] = book_description

    # Finds the first match and extracts the string matching .*
    # E.g. In stock (19 available) -----> 19
    book_information_dict['Availability'] = re.search('In stock \((.*) available\)|$', book_information_dict['Availability']).group(1)

    return book_information_dict


if __name__ == '__main__':
    # Scrape main website
    main_soup = get_website_data(url_to_scrape)

    # Get links to book categories
    book_categories = main_soup.find('div', {'class': 'side_categories'})

    # Iterate through each book category
    for link in book_categories.find_all('a', href=True):
        file_url = url_to_scrape + link['href']
        print("Scraping data from: ", file_url)
        category_soup = get_website_data(file_url)

        # Iterate through each book
        for book in category_soup.find('body').find('ol').find_all('article', {'class': 'product_pod'}):
            book_info = book.find('h3').find('a', href=True)
            book_info_dict = get_book_data(file_url, book_info)
            print(book_info_dict)

        time.sleep(0.5)
        break
