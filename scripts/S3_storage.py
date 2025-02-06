import os
import time
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager

# Code to scrap the links and store the files in local.

# Directory to save ZIP files
download_dir = os.path.abspath("SEC_Financial_Statements_ASS2")
os.makedirs(download_dir, exist_ok=True)

# Configure Chrome options
chrome_options = Options()
chrome_options.add_argument("--disable-blink-features=AutomationControlled")
chrome_options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36")
chrome_options.add_argument("--headless")  # Run in headless mode

# Set Chrome download preferences
prefs = {
    "download.default_directory": download_dir,  
    "download.prompt_for_download": False,
    "download.directory_upgrade": True,
    "safebrowsing.enabled": True
}
chrome_options.add_experimental_option("prefs", prefs)

# Initialize WebDriver
service = Service(ChromeDriverManager().install())
driver = webdriver.Chrome(service=service, options=chrome_options)

# Load the SEC page
url = "https://www.sec.gov/data-research/sec-markets-data/financial-statement-data-sets"
driver.get(url)

# Extract ZIP links
zip_elements = driver.find_elements(By.XPATH, "//a[contains(@href, '.zip')]")
zip_urls = [element.get_attribute("href") for element in zip_elements if element.get_attribute("href")]

# Print extracted ZIP links
print("\nExtracted ZIP Links:")
for link in zip_urls:
    print(link)

# Click on each ZIP link to trigger download
print("\nDownloading ZIP files...")
for zip_element in zip_elements:
    try:
        zip_element.click()
        print(f"Started downloading: {zip_element.get_attribute('href')}")
        time.sleep(80)  # Allow time for the download to start
    except Exception as e:
        print(f"Error clicking link: {e}")

# Function to wait for downloads to complete
def wait_for_downloads(download_path, timeout=300):
    """Wait until all .crdownload files convert to final ZIPs."""
    start_time = time.time()
    while time.time() - start_time < timeout:
        crdownload_files = [f for f in os.listdir(download_path) if f.endswith(".crdownload")]
        if not crdownload_files:  # No more incomplete downloads
            return
        print(f"Waiting for downloads to complete: {crdownload_files}")
        time.sleep(80)  # Check every 5 seconds

# Wait for all downloads to finish
wait_for_downloads(download_dir)

print("\nAll ZIP files downloaded successfully in:", download_dir)

# Close the browser
driver.quit()