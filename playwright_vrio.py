"""
Simple Vrio File Retriever
==========================
Simple Vrio file retriever - logs in, navigates to DEPREC report, inserts date range, then exports.
"""

import os
import sys
import time
import logging
import platform
import asyncio
from playwright.async_api import async_playwright

# Fix for Windows Playwright issue (if using Playwright)
if sys.platform.startswith('win'):
    asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('playwright_vrio.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Configuration
DOWNLOAD_DIR = os.getcwd()
os.makedirs(DOWNLOAD_DIR, exist_ok=True)

# Browser configuration for server environment
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

IS_SERVER = platform.system() == "Linux"

if IS_SERVER:
    # Set virtual display for Playwright headless=False
    os.environ['DISPLAY'] = ':99'
    os.makedirs("logs", exist_ok=True)
    os.makedirs("downloads", exist_ok=True)
    print("✅ Server environment configured")
    print(f"✅ Display set to: {os.environ.get('DISPLAY')}")
    print(f"✅ Current directory: {os.getcwd()}")

    # Add file handler for server logging
    file_handler = logging.FileHandler('logs/playwright_vrio_server.log')
    file_handler.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(formatter)
    logging.getLogger().addHandler(file_handler)
    print("✅ Server logging configured")
else:
    print("🖥️ Running on local machine")


async def Playwright_Vrio_GF_Project_3_async(playwright):
    """
    Simple Vrio file retriever - logs in, navigates to DEPREC report, inserts date range, then exports.
    """
    import os
    import time

    max_retries = 3
    retry_delay = 5  # seconds

    csv_filename = "Sales_By_Gateway.csv"
    final_path = os.path.join(DOWNLOAD_DIR, csv_filename)

    for attempt in range(1, max_retries + 1):
        try:
            logger.info(f"🟢 Attempt {attempt}: Starting Vrio export...")

            browser = await playwright.chromium.launch(
                headless=False,
                args=BROWSER_ARGS
            )
            context = await browser.new_context(
                viewport={'width': 1920, 'height': 1080},
                accept_downloads=True
            )
            page = await context.new_page()

            # Open the page
            logger.info("Navigating to login page...")
            await page.goto("https://campaignx2.vrio.app//auth/login", timeout=30000)
            await page.wait_for_load_state("networkidle", timeout=20000)

            # Login with retries
            login_success = False
            for login_attempt in range(3):
                try:
                    logger.info(f"Login attempt {login_attempt + 1}...")
                    await page.get_by_placeholder("email").click(timeout=10000)
                    await page.get_by_placeholder("email").fill("m45762629@gmail.com")
                    await page.get_by_placeholder("password").click(timeout=10000)
                    await page.get_by_placeholder("password").fill("F&OqbOP3iSx;")
                    await page.get_by_role("button", name="Login").click(timeout=10000)
                    await page.wait_for_load_state("networkidle", timeout=20000)
                    login_success = True
                    break
                except Exception as e:
                    logger.error(f"Login attempt {login_attempt + 1} failed: {e}")
                    if login_attempt < 2:
                        await page.reload()
                        time.sleep(2)
            if not login_success:
                raise Exception("Failed to login after multiple attempts")

            # Navigate to Analytics
            logger.info("Navigating to Analytics...")
            analytics_link = page.get_by_role("link", name=" Analytics")
            await analytics_link.wait_for(state="visible", timeout=10000)
            await analytics_link.click()
            await page.wait_for_load_state("networkidle", timeout=20000)

            # Go to Saved Reports tab
            logger.info("Opening Saved Reports...")
            saved_reports_tab = page.get_by_role("tab", name="Saved Reports")
            await saved_reports_tab.wait_for(state="visible", timeout=10000)
            await saved_reports_tab.click()
            await page.wait_for_load_state("networkidle", timeout=20000)

            # Open DEPREC report
            logger.info("Opening DEPREC...")
            deprec_link = page.get_by_role("link", name="DEPREC")
            await deprec_link.wait_for(state="visible", timeout=10000)
            await deprec_link.click()
            await page.wait_for_load_state("networkidle", timeout=20000)

            # Insert date range
            logger.info("Inserting date range...")
            date_range_field = page.locator("#rb_date_range")
            await date_range_field.wait_for(state="visible", timeout=10000)
            await date_range_field.click()
            # Select all and delete existing content
            await page.keyboard.press("Control+a")
            await page.wait_for_timeout(200)
            await page.keyboard.press("Delete")
            await page.wait_for_timeout(300)
            # Type the date range as if a human is typing (character by character)
            await date_range_field.type("11/01/2025 - 11/21/2025", delay=100)
            await page.wait_for_timeout(500)
            await date_range_field.press("Enter")
            logger.info("Date range entered, waiting for page to load...")
            await page.wait_for_load_state("networkidle", timeout=20000)

            # Add Merchant column
            logger.info("Adding Merchant column...")
            add_dimension = page.get_by_role("link", name="Add Dimension")
            await add_dimension.wait_for(state="visible", timeout=10000)
            await add_dimension.click()
            await page.wait_for_timeout(500)
            
            select_dimension = page.get_by_role("textbox", name="Select Next Dimension")
            await select_dimension.wait_for(state="visible", timeout=10000)
            await select_dimension.click()
            await page.wait_for_timeout(500)
            
            searchbox = page.get_by_role("searchbox")
            await searchbox.wait_for(state="visible", timeout=10000)
            await searchbox.fill("Mercha")
            await page.wait_for_timeout(500)
            
            merchant_option = page.get_by_role("option", name="Merchant", exact=True)
            await merchant_option.wait_for(state="visible", timeout=10000)
            await merchant_option.click()
            await page.wait_for_timeout(500)
            
            filter_option = page.locator("#rb_filter_options div").filter(has_text="OverallConnectionCustomer").nth(1)
            await filter_option.wait_for(state="visible", timeout=10000)
            await filter_option.click()
            await page.wait_for_load_state("networkidle", timeout=20000)
            logger.info("Merchant column added, waiting for page to load...")

            # Click More Options and Export
            logger.info("Initiating download...")
            more_options = page.get_by_role("button", name="More Options ")
            await more_options.wait_for(state="visible", timeout=10000)
            await more_options.click()
            await page.wait_for_timeout(2000)

            async with page.expect_download(timeout=30000) as download_info:
                logger.info("Clicking Export Report...")
                export_link = page.get_by_role("link", name="Export Report")
                await export_link.wait_for(state="visible", timeout=10000)
                await export_link.click()

            download = await download_info.value
            await download.save_as(final_path)
            logger.info(f"✅ File downloaded and saved as: {final_path}")

            await context.close()
            await browser.close()
            return  # Success, exit the retry loop

        except Exception as e:
            logger.error(f"Attempt {attempt} failed: {str(e)}")
            if attempt < max_retries:
                logger.info(f"Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)
            else:
                logger.error("All retry attempts failed")
                raise


async def main():
    """Main function to run the Playwright automation"""
    print("🚀 Starting Vrio file retrieval...")
    print("=" * 50)
    
    try:
        async with async_playwright() as playwright:
            await Playwright_Vrio_GF_Project_3_async(playwright)
        
        print("=" * 50)
        print("✅ Vrio file retrieval completed successfully!")
        print(f"📁 File saved in: {DOWNLOAD_DIR}")
        
    except Exception as e:
        print("=" * 50)
        print(f"❌ Error: {e}")
        logger.error(f"Main execution failed: {e}", exc_info=True)
        raise


if __name__ == "__main__":
    # Run the script
    asyncio.run(main())

