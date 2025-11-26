# SI_DEPREC_playwright.py
# Playwright script for SmartImpact CRM Sales By Gateway data fetching

import sys
import io

# Set UTF-8 encoding for Windows console
if sys.platform == 'win32':
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8')

import os
import time
import logging
import zipfile
from datetime import datetime, timedelta, date
from playwright.sync_api import Playwright, sync_playwright
from pathlib import Path
import pytz

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('si_deprec_playwright.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Configuration
DOWNLOAD_DIR = os.getcwd()
os.makedirs(DOWNLOAD_DIR, exist_ok=True)

# Default Credentials (can be overridden by function parameters)
DEFAULT_USERNAME = "DepRec"
DEFAULT_PASSWORD = "Manuel123$$"

import pickle
from pathlib import Path

# Load date range from bank statement processor
data_dir = Path.cwd()
with open(data_dir / 'deprec_date_metadata.pkl', 'rb') as f:
    date_metadata = pickle.load(f)

min_date_str = date_metadata['min_date_str']  # e.g., "2025/07/30"
max_date_str = date_metadata['max_date_str']  # e.g., "2025/10/03"
print(f"📅 {min_date_str} - {max_date_str} is the date range used for the report")

# We wiil use this for the date selection
nyc_tz = pytz.timezone('America/New_York')
today = datetime.now(nyc_tz)
today_month_day = f"/{today.month:02d}/{today.day:02d}"  # Format as "/11/13"
print(f"Using today's month/day pattern for date buttons (NYC time): {today_month_day}")

# Browser configuration
BROWSER_ARGS = [
    '--disable-dev-shm-usage',
    '--no-sandbox',
    '--disable-setuid-sandbox',
    '--disable-gpu',
    '--disable-software-rasterizer',
    '--disable-extensions',
    '--disable-default-apps',
    '--disable-sync',
    '--disable-translate',
    '--hide-scrollbars',
    '--metrics-recording-only',
    '--mute-audio',
    '--no-first-run',
    '--safebrowsing-disable-auto-update'
]

def _parse_and_adjust_date(date_input, lag_days):
    """
    Parse date input (datetime, date, or string) and subtract lag days
    Returns date object with lag applied
    """
    if date_input is None:
        return None
    
    # Convert to date object if needed
    if isinstance(date_input, datetime):
        date_obj = date_input.date()
    elif isinstance(date_input, date):
        date_obj = date_input
    elif isinstance(date_input, str):
        # Try common date formats
        for fmt in ['%Y-%m-%d', '%Y/%m/%d', '%m/%d/%Y', '%d/%m/%Y']:
            try:
                date_obj = datetime.strptime(date_input, fmt).date()
                break
            except ValueError:
                continue
        else:
            raise ValueError(f"Unable to parse date string: {date_input}")
    else:
        raise ValueError(f"Unsupported date type: {type(date_input)}")
    
    # Apply lag
    adjusted_date = date_obj - timedelta(days=lag_days)
    return adjusted_date

def Playwright_SI_Sales_By_Gateway(
    playwright: Playwright,
    username: str = None,
    password: str = None,
    min_date = None,
    max_date = None,
    lag: int = 0
) -> None:
    """
    Fetches Sales By Gateway report from SmartImpact CRM
    Saves file as Sales_By_Gateway.csv in current directory
    
    Args:
        playwright: Playwright instance
        username: Login username (defaults to DEFAULT_USERNAME)
        password: Login password (defaults to DEFAULT_PASSWORD)
        min_date: Minimum date for report (datetime, date, or string in YYYY-MM-DD format)
        max_date: Maximum date for report (datetime, date, or string in YYYY-MM-DD format)
        lag: Number of days to subtract from both min_date and max_date before applying filter
    """
    # Use default credentials if not provided
    username = username or DEFAULT_USERNAME
    password = password or DEFAULT_PASSWORD
    
    # Parse and adjust dates with lag
    adjusted_min_date = None
    adjusted_max_date = None
    if min_date is not None and max_date is not None:
        adjusted_min_date = _parse_and_adjust_date(min_date, lag)
        adjusted_max_date = _parse_and_adjust_date(max_date, lag)
        logger.info(f"Date range: {adjusted_min_date} to {adjusted_max_date} (lag: {lag} days)")
    
    max_retries = 3
    retry_delay = 5  # seconds

    zip_filename = "dashboard_sales_by_gateway.zip"
    csv_filename = "Sales_By_Gateway.csv"
    zip_path = os.path.join(DOWNLOAD_DIR, zip_filename)
    final_csv_path = os.path.join(DOWNLOAD_DIR, csv_filename)

    for attempt in range(1, max_retries + 1):
        browser = None
        context = None
        try:
            logger.info(f"🟢 Attempt {attempt}: Starting SmartImpact Playwright automation...")

            # Launch browser
            browser = playwright.chromium.launch(
                headless=False,
                args=BROWSER_ARGS
            )
            context = browser.new_context(
                viewport={'width': 1920, 'height': 1080},
                accept_downloads=True
            )
            page = context.new_page()

            # Set timeouts
            page.set_default_timeout(60000)
            page.set_default_navigation_timeout(60000)

            # Navigate to login page
            logger.info("Navigating to SmartImpact login page...")
            page.goto(
                "https://smartimpactllc.sticky.io/admin/login.php?url=%2Fadmin%2Freports%2Findex.php%3Fd%3DSales%2BBy%2BGateway",
                wait_until="networkidle",
                timeout=60000
            )

            # Login with retry mechanism
            login_success = False
            for login_attempt in range(3):
                try:
                    logger.info(f"Login attempt {login_attempt + 1}...")

                    # Fill username (simulate typing)
                    page.get_by_placeholder("Username").click()
                    page.wait_for_timeout(500)
                    page.get_by_placeholder("Username").fill(username, timeout=10000)
                    page.wait_for_timeout(300)

                    # Fill password (simulate typing)
                    page.get_by_placeholder("Password").click()
                    page.wait_for_timeout(500)
                    page.get_by_placeholder("Password").fill(password, timeout=10000)
                    page.wait_for_timeout(300)

                    # Click login button
                    page.get_by_role("button", name="Log In →").click()
                    page.wait_for_load_state("networkidle", timeout=30000)

                    login_success = True
                    logger.info("✅ Login successful")
                    break

                except Exception as e:
                    logger.error(f"Login attempt {login_attempt + 1} failed: {e}")
                    if login_attempt < 2:
                        page.reload()
                        time.sleep(2)

            if not login_success:
                raise Exception("Failed to login after multiple attempts")

            # Navigate to Sales By Gateway report
            logger.info("Navigating to Sales By Gateway report...")
            page.get_by_role("link", name=" Sales By Gateway UPDATED").click()
            page.wait_for_load_state("networkidle", timeout=30000)

            # Wait for iframe to load
            logger.info("Waiting for report iframe to load...")
            page.wait_for_selector("#js-looker-iframe", state="visible", timeout=30000)
            time.sleep(5)  # Extra wait for report data to fully load

            # Wait for report content inside iframe to be ready
            iframe = page.frame_locator("#js-looker-iframe")
            logger.info("Waiting for report content to fully load...")
            iframe.get_by_role("button", name="Dashboard actions").wait_for(state="visible", timeout=30000)
            time.sleep(3)  # Additional wait for report data

            # Apply date range filter if dates are provided
            if adjusted_min_date is not None and adjusted_max_date is not None:
                try:
                    logger.info("Setting date range filter...")
                    
                    # Format dates as YYYY/MM/DD for the UI
                    start_date_str = adjusted_min_date.strftime("%Y/%m/%d")
                    end_date_str = adjusted_max_date.strftime("%Y/%m/%d")
                    logger.info(f"Applying date range: {start_date_str} to {end_date_str}")
                    
                    # Click "is today" button to open date filter
                    logger.info("Clicking 'is today' button...")
                    iframe.get_by_role("button", name="is today").click()
                    page.wait_for_timeout(1500)
                    
                    # Click combobox textbox
                    logger.info("Clicking combobox textbox...")
                    iframe.get_by_label("combobox").get_by_role("textbox").click()
                    page.wait_for_timeout(1000)
                    
                    # Select "is in range" option
                    logger.info("Selecting 'is in range' option...")
                    iframe.get_by_text("is in range").click()
                    page.wait_for_timeout(1500)
                    
                    # Click on the date button (first date field)
                    # Note: The button name pattern like "/11/13" may vary based on current date display
                    # We'll try the user's pattern first, then fallback to generic selectors
                    logger.info("Clicking first date field...")
                    date_button_clicked = False
                    try:
                        # Try user's specific pattern first (button name with "/MM/DD" pattern)
                        # This might work if the UI shows a similar pattern
                        iframe.get_by_role("button", name=today_month_day).nth(1).click()
                        date_button_clicked = True
                        logger.info("Clicked date button using specific pattern")
                    except Exception as e1:
                        logger.debug(f"Specific pattern failed: {e1}, trying alternative selectors...")
                        try:
                            # Try to find buttons that might be date pickers by looking for common patterns
                            # Look for buttons containing "/" which might indicate date displays
                            buttons = iframe.locator("button").all()
                            if len(buttons) > 1:
                                buttons[1].click()  # nth(1) as per user's example
                                date_button_clicked = True
                                logger.info("Clicked date button using nth(1) selector")
                        except Exception as e2:
                            logger.warning(f"Alternative selector also failed: {e2}")
                    
                    if not date_button_clicked:
                        logger.warning("Could not click date button with any selector, attempting to proceed...")
                    page.wait_for_timeout(1000)
                    
                    # Fill start date
                    logger.info(f"Filling start date: {start_date_str}")
                    start_date_input = iframe.get_by_test_id("text-input").first
                    start_date_input.click()
                    page.wait_for_timeout(500)
                    start_date_input.fill("")  # Clear existing value
                    page.wait_for_timeout(500)
                    start_date_input.fill(start_date_str)
                    page.wait_for_timeout(500)
                    start_date_input.press("Enter")
                    page.wait_for_timeout(1000)
                    start_date_input.press("Enter")  # Second Enter press
                    page.wait_for_timeout(500)
                    
                    # Click "Open calendar" button ONCE for start date
                    logger.info("Clicking 'Open calendar' button (first time) for start date...")
                    iframe.get_by_role("button", name="Open calendar").click()
                    page.wait_for_timeout(500)
                    
                    # Click "Open calendar" button AGAIN for start date
                    logger.info("Clicking 'Open calendar' button (second time) for start date...")
                    iframe.get_by_role("button", name="Open calendar").click()
                    page.wait_for_timeout(1000)
                    
                    # Get today's date formatted as "/MM/DD" (e.g., "/11/13") using NYC timezone
                    # The button names use today's month and day

                    
                    # Click on the min date button (nth(1)) to confirm start date
                    logger.info("Clicking min date button (nth(1)) to confirm start date...")
                    try:
                        iframe.get_by_role("button", name=today_month_day).nth(1).click()
                        logger.info(f"Clicked min date button using {today_month_day} with nth(1)")
                        page.wait_for_timeout(1000)
                    except Exception as e:
                        logger.warning(f"Could not find min date button with {today_month_day}.nth(1): {e}")
                        # Fallback: try exact match with start date
                        try:
                            iframe.get_by_role("button", name=start_date_str, exact=True).click()
                            page.wait_for_timeout(1000)
                        except:
                            logger.warning("All min date button selectors failed, proceeding...")

                    # Fill end date using the same logic as start date
                    logger.info("Setting end date...")

                    # Click on the max date button (nth(2)) to switch to end date field
                    logger.info("Clicking max date button (nth(2)) to switch to end date field...")
                    try:
                        iframe.get_by_role("button", name=today_month_day).nth(2).click(timeout=10000)
                        logger.info(f"Clicked max date button using {today_month_day} with nth(2)")
                        page.wait_for_timeout(1000)
                    except Exception as e:
                        logger.warning(f"Could not click nth(2) button: {e}")
                        logger.info("Attempting to continue with alternative approach...")

                    # IMMEDIATELY click the text-input field (the SAME field, no nth)
                    logger.info("Clicking text-input field to enter max date...")
                    iframe.get_by_test_id("text-input").click()
                    page.wait_for_timeout(500)

                    # Clear the field by filling with blank
                    logger.info("Clearing text field...")
                    iframe.get_by_test_id("text-input").fill("")
                    page.wait_for_timeout(500)

                    # Press Enter to confirm clear
                    iframe.get_by_test_id("text-input").press("Enter")
                    page.wait_for_timeout(500)

                    # Fill with max date
                    logger.info(f"Filling end date: {end_date_str}")
                    iframe.get_by_test_id("text-input").fill(end_date_str)
                    page.wait_for_timeout(500)
                    iframe.get_by_test_id("text-input").press("Enter")
                    page.wait_for_timeout(1000)

                    # Click "Open calendar" button TWICE for end date
                    logger.info("Clicking 'Open calendar' button (first time) for end date...")
                    iframe.get_by_role("button", name="Open calendar").click()
                    page.wait_for_timeout(500)

                    logger.info("Clicking 'Open calendar' button (second time) for end date...")
                    iframe.get_by_role("button", name="Open calendar").click()
                    page.wait_for_timeout(1000)

                    # After adding the date in, press the calendar button AGAIN
                    logger.info("Clicking 'Open calendar' button (third time) after entering end date...")
                    iframe.get_by_role("button", name="Open calendar").click()
                    page.wait_for_timeout(1000)

                    # Now click Update button to apply the filter
                    logger.info("Clicking Update button to apply date filter...")
                    iframe.get_by_role("button", name="Update").click()
                    
                    # Wait for page to fully load after Update button is clicked
                    logger.info("Waiting for report to fully refresh after applying date filter...")
                    
                    # Wait for network to be idle
                    page.wait_for_load_state("networkidle", timeout=30000)
                    logger.info("Network is idle")
                    
                    # Wait for the Dashboard actions button to be visible again (indicates report has refreshed)
                    logger.info("Waiting for Dashboard actions button to be visible (report refresh indicator)...")
                    iframe.get_by_role("button", name="Dashboard actions").wait_for(state="visible", timeout=30000)
                    logger.info("Dashboard actions button is visible - report has refreshed")
                    
                    # Additional wait to ensure all data is fully loaded
                    page.wait_for_timeout(3000)
                    logger.info("✅ Date range filter applied successfully and page is fully loaded")
                    
                except Exception as date_filter_error:
                    logger.error(f"Failed to apply date range filter: {date_filter_error}")
                    logger.warning("Continuing without date filter...")
                    # Continue execution even if date filter fails

            # Click Dashboard actions - ensure page is fully loaded first
            logger.info("Ensuring Dashboard actions button is ready...")
            iframe.get_by_role("button", name="Dashboard actions").wait_for(state="visible", timeout=30000)
            logger.info("Opening Dashboard actions menu...")
            iframe.get_by_role("button", name="Dashboard actions").click()
            page.wait_for_timeout(2000)

            # Click Download option
            logger.info("Selecting Download option...")
            iframe.get_by_role("menuitem", name="Download alt⇧D").click()
            page.wait_for_timeout(2000)

            # Select CSV format - click dropdown
            logger.info("Opening Format dropdown...")
            iframe.get_by_placeholder("Format").click()
            page.wait_for_timeout(1000)

            # Click CSV option multiple times until it sticks
            logger.info("Selecting CSV format (pressing until it disappears)...")
            for i in range(3):
                try:
                    iframe.get_by_role("option", name="CSV").click(timeout=2000)
                    page.wait_for_timeout(500)
                    logger.info(f"CSV click #{i+1}")
                except:
                    logger.info(f"CSV option disappeared after {i+1} click(s)")
                    break

            page.wait_for_timeout(2000)

            # Click Download button - this will trigger the download
            logger.info("Clicking Download button...")
            with page.expect_download(timeout=60000) as download_info:
                iframe.get_by_role("button", name="Download").click()

            # Save the downloaded file (it's a ZIP)
            download = download_info.value
            download.save_as(zip_path)
            logger.info(f"✅ Downloaded ZIP file: {zip_path}")

            # Extract CSV from ZIP
            logger.info("Extracting CSV from ZIP file...")
            try:
                with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                    # List all files in the ZIP
                    file_list = zip_ref.namelist()
                    logger.info(f"Files in ZIP: {file_list}")

                    # Find the CSV file (usually the first/only file)
                    csv_file_in_zip = None
                    for file in file_list:
                        if file.endswith('.csv'):
                            csv_file_in_zip = file
                            break

                    if not csv_file_in_zip:
                        # If no .csv extension, try the first file
                        csv_file_in_zip = file_list[0]

                    logger.info(f"Extracting: {csv_file_in_zip}")

                    # Extract and save as Sales_By_Gateway.csv
                    with zip_ref.open(csv_file_in_zip) as source:
                        content = source.read()
                        with open(final_csv_path, 'wb') as target:
                            target.write(content)

                logger.info(f"✅ Extracted CSV to: {final_csv_path}")

                # Delete the ZIP file
                os.remove(zip_path)
                logger.info(f"✅ Deleted ZIP file: {zip_path}")

            except Exception as extract_error:
                logger.error(f"Failed to extract ZIP: {extract_error}")
                # Keep the ZIP file for debugging
                logger.warning(f"ZIP file kept for debugging: {zip_path}")
                raise

            # Close browser
            context.close()
            browser.close()

            logger.info("✅ SmartImpact Sales By Gateway fetch completed successfully")
            return  # Success, exit retry loop

        except Exception as e:
            logger.error(f"Attempt {attempt} failed: {str(e)}")

            # Clean up on failure
            if context:
                try:
                    context.close()
                except:
                    pass
            if browser:
                try:
                    browser.close()
                except:
                    pass

            if attempt < max_retries:
                logger.info(f"Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)
            else:
                logger.error("All retry attempts failed")
                raise

def main(
    username: str = None,
    password: str = None,
    min_date = None,
    max_date = None,
    lag: int = 0
):
    """
    Main function to run the Playwright automation
    
    Args:
        username: Login username (defaults to DEFAULT_USERNAME)
        password: Login password (defaults to DEFAULT_PASSWORD)
        min_date: Minimum date for report (datetime, date, or string in YYYY-MM-DD format)
        max_date: Maximum date for report (datetime, date, or string in YYYY-MM-DD format)
        lag: Number of days to subtract from both min_date and max_date before applying filter
    """
    try:
        logger.info("🚀 Starting SmartImpact Sales By Gateway fetch...")
        with sync_playwright() as playwright:
            Playwright_SI_Sales_By_Gateway(
                playwright,
                username=username,
                password=password,
                min_date=min_date,
                max_date=max_date,
                lag=lag
            )
        logger.info("✅ Process completed successfully")
        return True
    except Exception as e:
        logger.error(f"❌ Process failed: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    # Use the dates loaded from pickle file
    # Note: You may need to set lag value here if needed
    main(
        min_date=min_date_str,
        max_date=max_date_str,
        lag=3  # Adjust lag value as needed
    )