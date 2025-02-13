import os
import requests
from tqdm import tqdm
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
from s3_utils import upload_file_to_s3, delete_local_file

# SEC Financial Statement Data URL
SEC_URL = "https://www.sec.gov/data-research/sec-markets-data/financial-statement-data-sets"
S3_ZIP_FOLDER = "sec_raw_zips/"

def get_zip_links():
    """Uses Selenium to scrape ZIP file links from the SEC website."""
    chrome_options = Options()
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--window-size=1920,1080")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--disable-blink-features=AutomationControlled")  # Avoid bot detection

    # Initialize WebDriver
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=chrome_options)

    print("Accessing SEC website...")
    driver.get(SEC_URL)
    driver.implicitly_wait(5)  

    #zip_links = [link.get_attribute("href") for link in driver.find_elements(By.XPATH, "//a[contains(@href, '.zip')]")]
    zip_links = [link.get_attribute("href") for link in driver.find_elements(By.XPATH, "//a[contains(@href, 'financial-statement-data-sets/') and contains(@href, '.zip')]")]
    driver.quit()

    if zip_links:
        print(f"Found {len(zip_links)} ZIP files.")
    else:
        print("No ZIP files found!")

    return zip_links

def download_and_upload_zip(url):
    """Downloads a ZIP file and uploads it to S3."""
    filename = url.split("/")[-1]
    
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}
    response = requests.get(url, headers=headers, stream=True)

    if response.status_code == 200:
        with open(filename, "wb") as file, tqdm(desc=filename, total=int(response.headers.get("content-length", 0)), unit="B", unit_scale=True) as bar:
            for chunk in response.iter_content(chunk_size=1024):
                file.write(chunk)
                bar.update(len(chunk))
        print(f"Downloaded: {filename}")

        upload_file_to_s3(filename, os.path.join(S3_ZIP_FOLDER, filename))
        delete_local_file(filename)
    else:
        print(f"Failed to download: {url}")

def scrape_and_download():
    """Scrape SEC ZIP links and upload them to S3."""
    zip_links = get_zip_links()

    if not zip_links:
        print("No ZIP files found! Exiting...")
        return

    for zip_url in zip_links:
        download_and_upload_zip(zip_url)

    print("All ZIP files uploaded to S3!")

if __name__ == "__main__":
    scrape_and_download()
