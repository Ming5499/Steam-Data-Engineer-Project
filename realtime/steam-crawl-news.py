import requests
from bs4 import BeautifulSoup
import time

def get_steam_news(url="https://store.steampowered.com/news/"):
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }
    
    news_list = []
    
    print(f"Fetching news from {url}...")
    
    try:
        # Use requests to get the HTML content of the page
        response = requests.get(url, headers=headers)
        response.raise_for_status()  # Raise an exception for bad status codes
        
        # Parse the HTML content with BeautifulSoup
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Find all news item containers.
        # The main news items are inside a div with the class 'news_item'.
        news_items = soup.find_all('div', class_='news_item')
        
        if not news_items:
            print("No news items found on the page.")
            return news_list
            
        print(f"Found {len(news_items)} news items. Parsing data...")

        for item in news_items:
            try:
                # Extract the title
                title_element = item.find('a', class_='snr_item_title')
                title = title_element.get_text(strip=True) if title_element else "No title found"
                
                # Extract the link
                link = title_element.get('href') if title_element else "No link found"
                
                # Extract the summary
                summary_element = item.find('div', class_='snr_item_content')
                summary = summary_element.get_text(strip=True) if summary_element else "No summary found"

                # Extract the date
                date_element = item.find('div', class_='snr_item_date')
                date = date_element.get_text(strip=True) if date_element else "No date found"
                
                # Store the extracted data in a dictionary
                news_item = {
                    'title': title,
                    'date': date,
                    'summary': summary,
                    'link': link
                }
                news_list.append(news_item)
                
            except Exception as e:
                print(f"Error parsing a news item: {e}")
                continue
                
        print("Data parsing complete.")

    except requests.exceptions.RequestException as e:
        print(f"An error occurred while fetching the page: {e}")
        return []
    
    return news_list

if __name__ == '__main__':
    # Get the news data
    steam_news_data = get_steam_news()
    
    # Check if we got any data and print it
    if steam_news_data:
        print("\n--- Steam News ---")
        for i, news in enumerate(steam_news_data, 1):
            print(f"\n{i}. Title: {news['title']}")
            print(f"   Date: {news['date']}")
            print(f"   Summary: {news['summary']}")
            print(f"   Link: {news['link']}")
            print("-" * 20)
    else:
        print("\nFailed to retrieve news data.")
