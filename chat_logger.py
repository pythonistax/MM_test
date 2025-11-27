import os
import asyncio
import subprocess
import sys
import time
import logging
from pathlib import Path
from datetime import datetime
from telegram import Update
from telegram.ext import ApplicationBuilder, CommandHandler, ContextTypes, MessageHandler, filters

def cleanup_directory():
    """Delete all files in current directory except the essential ones."""
    import os

    # Files to keep, matched to current requirements (@SI_requirements.txt)
    keep_files = {
        'CRM_report_integrator_with_bank_statements.py',
        'bank_statement_retriever_and_organizer.py',
        'MM_DEPREC_playwright.py',
        'GWID_MM.xls',
        'chat_logger.py',
        'SI_requirements.txt'
    }
    
    
    deleted_count = 0
    
    for item in os.listdir('.'):
        # Skip if it's a directory
        if os.path.isdir(item):
            continue
            
        # Skip if it's in the keep list
        if item in keep_files:
            continue
            
        # Delete the file
        try:
            os.remove(item)
            deleted_count += 1
            print(f"Deleted: {item}")
        except Exception as e:
            print(f"Failed to delete {item}: {e}")
    
    print(f"Cleanup complete: {deleted_count} files deleted")
    return deleted_count

# Set UTF-8 encoding for Windows console (must be done BEFORE logging setup)
if sys.platform == 'win32':
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8')

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('agent_serv_si.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("👋 Logger bot is running. Send something here.")


async def handle_all_messages(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # This function will be called for every non-command text message
    try:
        message = update.message
        text = message.text
        chat_id = message.chat_id
        user = message.from_user

        print(update)
        # Your processing logic here
        print(f"Received message: '{text}' from {user.username if user else 'Unknown'} in chat {chat_id}")
        
        # Check for bot mentions
        bot_username = context.bot.username
        if f"@{bot_username}" in text:
            # Store the mention time in chat_data for persistence
            now = datetime.now()
            if 'recent_mentions' not in context.chat_data:
                context.chat_data['recent_mentions'] = {}
            context.chat_data['recent_mentions'][chat_id] = now
    except Exception as e:
        print(f"Error in handle_all_messages: {e}")
        print(f"Update: {update}")


async def handle_document_logger(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Check if file is a bank statement and download it."""
    try:
        import re

        # Get basic file info
        file = update.message.document
        file_name = file.file_name
        chat_id = update.message.chat_id
        user = update.message.from_user
        
        print(update)
        print(f"Received document: '{file_name}' from {user.username if user else 'Unknown'} in chat {chat_id}")

        # Clean filename: remove special characters, lowercase
        cleaned_name = re.sub(r'[^a-zA-Z0-9]', '', file_name).lower()

        # Check if it's a bank statement (bofa, boa, or chase)
        is_bank_statement = 'bofa' in cleaned_name or 'boa' in cleaned_name or 'chase' in cleaned_name

        if is_bank_statement:
            print(f"✅ Bank Statement verified: {file_name}")
            print(f"   Cleaned name: {cleaned_name}")
            if 'bofa' in cleaned_name or 'boa' in cleaned_name:
                print(f"   Matched: BofA")
            elif 'chase' in cleaned_name:
                print(f"   Matched: Chase")

            # Save directly in the same directory as this script (no subdirectory)
            script_dir = os.path.dirname(os.path.abspath(__file__))

            # Download the file
            new_file = await context.bot.get_file(file.file_id)
            file_path = os.path.join(script_dir, file_name)
            await new_file.download_to_drive(file_path)

            print(f"💾 Saved bank statement: {file_name}")
            print(f"📂 Location: {file_path}")

            # Track bank statements for batched reply
            if 'bank_statements_batch' not in context.chat_data:
                context.chat_data['bank_statements_batch'] = {}

            if chat_id not in context.chat_data['bank_statements_batch']:
                context.chat_data['bank_statements_batch'][chat_id] = {
                    'count': 0,
                    'last_time': None,
                    'task': None,
                    'files': []  # Track filenames
                }

            now = datetime.now()
            batch_state = context.chat_data['bank_statements_batch'][chat_id]

            # If more than 5 seconds since last file, reset count
            if batch_state['last_time'] is None or (now - batch_state['last_time']).total_seconds() > 5:
                batch_state['count'] = 0
                batch_state['files'] = []

            batch_state['count'] += 1
            batch_state['files'].append(file_name)
            batch_state['last_time'] = now

            # Cancel previous task if exists
            if batch_state['task']:
                batch_state['task'].cancel()

            # Schedule message after 2 seconds
            import asyncio

            async def send_batch_message():
                try:
                    await asyncio.sleep(2)
                    count = batch_state['count']
                    files_list = batch_state.get('files', [])
                    
                    # Print verified files to terminal/logs
                    print(f"\n{'='*60}")
                    print(f"✅ VERIFIED BANK STATEMENTS ({count} total):")
                    print(f"{'='*60}")
                    for i, fname in enumerate(files_list, 1):
                        print(f"  {i}. {fname}")
                    print(f"{'='*60}\n")
                    
                    if count == 1:
                        await update.message.reply_text("✅ Bank Statement verified")
                    else:
                        await update.message.reply_text(f"✅ {count} Bank Statements verified")
                    batch_state['count'] = 0
                    batch_state['files'] = []
                    batch_state['task'] = None

                    # Delete the retrieving message, then send new message after CRM report fetched
                    retrieving_msg = await update.message.reply_text("Retrieving the matching CRM report from Vrio...")
                    logger.info("Triggering agent pipeline after bank statements downloaded")
                    
                    # Create event to signal when Playwright completes
                    playwright_done = asyncio.Event()
                    
                    # Create progress updater task
                    async def update_progress():
                        try:
                            # Wait 1.5 seconds, then update to "logging in to Vrio 🔄"
                            await asyncio.sleep(1.5)
                            if playwright_done.is_set():
                                # Playwright finished early, show filtering message
                                await retrieving_msg.edit_text("Filtering for date range of matching deposits 🔄")
                                return
                            await retrieving_msg.edit_text("Logging into Vrio 🔄")
                            
                            # Wait 1 second, then update to "Entering user and password details 🔄"
                            await asyncio.sleep(1.0)
                            if playwright_done.is_set():
                                # Playwright finished early, show filtering message
                                await retrieving_msg.edit_text("Filtering for date range of matching deposits 🔄")
                                return
                            await retrieving_msg.edit_text("Entering user and password details 🔄")
                            
                            # Wait 2 seconds, then update to "Opening sticky dashboard 🔄"
                            await asyncio.sleep(2.0)
                            if playwright_done.is_set():
                                # Playwright finished early, show filtering message
                                await retrieving_msg.edit_text("Filtering for date range of matching deposits 🔄")
                                return
                            await retrieving_msg.edit_text("Opening Vrio dashboard 🔄")
                            
                            # Wait 5 seconds, then update to "Fetching gateway report 🔄"
                            await asyncio.sleep(5.0)
                            if playwright_done.is_set():
                                # Playwright finished early, show filtering message
                                await retrieving_msg.edit_text("Filtering for date range of matching deposits 🔄")
                                return
                            await retrieving_msg.edit_text("Fetching gateway report 🔄")
                            
                            # Wait 5 seconds after "Fetching gateway report", then update to "Filtering for date range of matching deposits 🔄"
                            await asyncio.sleep(5.0)
                            if playwright_done.is_set():
                                # Playwright already finished, but show filtering message anyway
                                await retrieving_msg.edit_text("Filtering for date range of matching deposits 🔄")
                                return
                            await retrieving_msg.edit_text("Filtering for date range of matching deposits 🔄")
                            
                            # Wait for Playwright to complete (keep showing filtering message)
                            await playwright_done.wait()
                            
                        except asyncio.CancelledError:
                            pass
                        except Exception as e:
                            logger.error(f"Error in progress updater: {e}")
                    
                    progress_task = asyncio.create_task(update_progress())
                    
                    # Run pipeline with playwright completion tracking
                    success = await run_agent_pipeline_with_progress(playwright_done)
                    
                    # Cancel progress updater since pipeline is done
                    progress_task.cancel()
                    try:
                        await progress_task
                    except asyncio.CancelledError:
                        pass

                    if success:
                        await retrieving_msg.edit_text("✅ Vrio report retrieved with success")
                    else:
                        await retrieving_msg.edit_text("❌ Agent Pipeline failed. Check logs for details.")

                except asyncio.CancelledError:
                    pass
                except Exception as e:
                    logger.error(f"Error in send_batch_message: {e}")
                    import traceback
                    logger.error(traceback.format_exc())

            batch_state['task'] = asyncio.create_task(send_batch_message())
        else:
            print(f"⚠️ File '{file_name}' is NOT a bank statement")
            print(f"   Cleaned name: {cleaned_name}")
            print(f"   Reason: No 'bofa', 'boa', or 'chase' found in filename")

    except Exception as e:
        import traceback
        error_msg = f"❌ Error processing file:\n{str(e)}\n\n{traceback.format_exc()}"
        print(error_msg)


# ============================================================================
# AGENT SERVICE FUNCTIONS (from agent_serv-SI.py)
# ============================================================================

# Layer 5: Main synchronous functions
def run_bank_statement_sync():
    """Run bank statement processor as subprocess with real-time output"""
    logger.info("="*80)
    logger.info("STARTING: bank_statement_retriever_and_organizer.py")
    logger.info("="*80)

    try:
        process = subprocess.Popen(
            [sys.executable, "bank_statement_retriever_and_organizer.py"],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            encoding='utf-8',
            errors='replace',
            cwd=Path.cwd(),
            bufsize=1,
            universal_newlines=True
        )

        # Store last 50 lines for error reporting
        output_lines = []
        max_lines = 50

        # Stream output in real-time
        for line in process.stdout:
            print(line, end='')
            output_lines.append(line.rstrip())
            # Keep only last N lines
            if len(output_lines) > max_lines:
                output_lines.pop(0)

        process.wait()
        returncode = process.returncode

        if returncode == 0:
            logger.info("✓ COMPLETED: bank_statement_retriever_and_organizer.py")
        else:
            logger.error(f"✗ ERROR in bank_statement_retriever_and_organizer.py")
            logger.error(f"Return code: {returncode}")
            logger.error("="*80)
            logger.error("LAST OUTPUT LINES (for debugging):")
            logger.error("="*80)
            for line in output_lines[-20:]:  # Show last 20 lines
                logger.error(line)
            logger.error("="*80)

        return returncode

    except Exception as e:
        import traceback
        logger.error(f"✗ EXCEPTION in bank_statement_retriever_and_organizer.py: {e}")
        logger.error(traceback.format_exc())
        return -1


def run_playwright_sync():
    """Run Playwright script as subprocess with real-time output"""
    logger.info("="*80)
    logger.info("STARTING: MM_DEPREC_playwright.py")
    logger.info("="*80)

    try:
        process = subprocess.Popen(
            [sys.executable, "MM_DEPREC_playwright.py"],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            encoding='utf-8',
            errors='replace',
            cwd=Path.cwd(),
            bufsize=1,
            universal_newlines=True
        )

        # Store last 50 lines for error reporting
        output_lines = []
        max_lines = 50

        # Stream output in real-time
        for line in process.stdout:
            print(line, end='')
            output_lines.append(line.rstrip())
            # Keep only last N lines
            if len(output_lines) > max_lines:
                output_lines.pop(0)

        process.wait()
        returncode = process.returncode

        if returncode == 0:
            logger.info("✓ COMPLETED: MM_DEPREC_playwright.py")
        else:
            logger.error(f"✗ ERROR in MM_DEPREC_playwright.py")
            logger.error(f"Return code: {returncode}")
            logger.error("="*80)
            logger.error("LAST OUTPUT LINES (for debugging):")
            logger.error("="*80)
            for line in output_lines[-20:]:  # Show last 20 lines
                logger.error(line)
            logger.error("="*80)

        return returncode

    except Exception as e:
        import traceback
        logger.error(f"✗ EXCEPTION in MM_DEPREC_playwright.py: {e}")
        logger.error(traceback.format_exc())
        return -1


def run_crm_integrator_sync():
    """Run CRM report integrator as subprocess with real-time output"""
    logger.info("="*80)
    logger.info("STARTING: CRM_report_integrator_with_bank_statements.py")
    logger.info("="*80)

    try:
        process = subprocess.Popen(
            [sys.executable, "CRM_report_integrator_with_bank_statements.py"],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            encoding='utf-8',
            errors='replace',
            cwd=Path.cwd(),
            bufsize=1,
            universal_newlines=True
        )

        # Store last 50 lines for error reporting
        output_lines = []
        max_lines = 50

        # Stream output in real-time
        for line in process.stdout:
            print(line, end='')
            output_lines.append(line.rstrip())
            # Keep only last N lines
            if len(output_lines) > max_lines:
                output_lines.pop(0)

        process.wait()
        returncode = process.returncode

        if returncode == 0:
            logger.info("✓ COMPLETED: CRM_report_integrator_with_bank_statements.py")
        else:
            logger.error(f"✗ ERROR in CRM_report_integrator_with_bank_statements.py")
            logger.error(f"Return code: {returncode}")
            logger.error("="*80)
            logger.error("LAST OUTPUT LINES (for debugging):")
            logger.error("="*80)
            for line in output_lines[-20:]:  # Show last 20 lines
                logger.error(line)
            logger.error("="*80)

        return returncode

    except Exception as e:
        import traceback
        logger.error(f"✗ EXCEPTION in CRM_report_integrator_with_bank_statements.py: {e}")
        logger.error(traceback.format_exc())
        return -1


# Layer 4: Async wrappers
async def run_bank_statement_async():
    """Async wrapper for bank statement processor"""
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, run_bank_statement_sync)


async def run_playwright_async():
    """Async wrapper with file polling for Playwright script"""
    # Wait for date metadata pickle to appear
    data_dir = Path.cwd()
    pickle_path = data_dir / 'deprec_date_metadata.pkl'

    logger.info("Waiting for deprec_date_metadata.pkl to appear...")
    start_time = time.time()

    while not pickle_path.exists():
        await asyncio.sleep(0.5)  # Check every 500ms
        elapsed = time.time() - start_time
        if elapsed > 300:  # 5 minute timeout
            logger.error("✗ TIMEOUT: deprec_date_metadata.pkl did not appear within 5 minutes")
            return -1

    logger.info(f"✓ Found deprec_date_metadata.pkl after {time.time() - start_time:.1f}s")

    # Run Playwright script
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, run_playwright_sync)


# Layer 3: Parallel orchestrator
async def run_parallel_scripts():
    """Run bank statement and Playwright scripts in parallel"""
    logger.info("\n" + "="*80)
    logger.info("PHASE 1: PARALLEL EXECUTION")
    logger.info("="*80)

    results = await asyncio.gather(
        run_bank_statement_async(),
        run_playwright_async(),
        return_exceptions=True
    )

    logger.info("\n" + "="*80)
    logger.info("PHASE 1 COMPLETE")
    logger.info(f"Bank Statement Result: {results[0]}")
    logger.info(f"Playwright Result: {results[1]}")
    logger.info("="*80 + "\n")

    return results


# Modified version with progress tracking
async def run_playwright_async_with_progress(playwright_done: asyncio.Event):
    """Async wrapper with file polling for Playwright script, signals completion"""
    # Wait for date metadata pickle to appear
    data_dir = Path.cwd()
    pickle_path = data_dir / 'deprec_date_metadata.pkl'

    logger.info("Waiting for deprec_date_metadata.pkl to appear...")
    start_time = time.time()

    while not pickle_path.exists():
        await asyncio.sleep(0.5)  # Check every 500ms
        elapsed = time.time() - start_time
        if elapsed > 300:  # 5 minute timeout
            logger.error("✗ TIMEOUT: deprec_date_metadata.pkl did not appear within 5 minutes")
            playwright_done.set()  # Signal completion even on error
            return -1

    logger.info(f"✓ Found deprec_date_metadata.pkl after {time.time() - start_time:.1f}s")

    # Run Playwright script
    loop = asyncio.get_event_loop()
    result = await loop.run_in_executor(None, run_playwright_sync)
    
    # Signal that Playwright is done
    playwright_done.set()
    
    return result


async def run_parallel_scripts_with_progress(playwright_done: asyncio.Event):
    """Run bank statement and Playwright scripts in parallel with progress tracking"""
    logger.info("\n" + "="*80)
    logger.info("PHASE 1: PARALLEL EXECUTION")
    logger.info("="*80)

    results = await asyncio.gather(
        run_bank_statement_async(),
        run_playwright_async_with_progress(playwright_done),
        return_exceptions=True
    )

    logger.info("\n" + "="*80)
    logger.info("PHASE 1 COMPLETE")
    logger.info(f"Bank Statement Result: {results[0]}")
    logger.info(f"Playwright Result: {results[1]}")
    logger.info("="*80 + "\n")

    return results


# Main agent execution
async def run_agent_pipeline():
    """Main agent pipeline entry point"""
    start_time = datetime.now()
    logger.info("\n" + "="*80)
    logger.info("SMARTIMPACT DEPREC AGENT SERVICE")
    logger.info(f"Started: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("="*80 + "\n")

    try:
        # Phase 1: Run bank statement and Playwright in parallel
        parallel_results = await run_parallel_scripts()

        # Phase 2: Run CRM integrator after both complete
        logger.info("="*80)
        logger.info("PHASE 2: CRM INTEGRATION")
        logger.info("="*80)

        crm_result = run_crm_integrator_sync()

        # Summary
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()

        cleanup_directory()

        logger.info("\n" + "="*80)
        logger.info("EXECUTION SUMMARY")
        logger.info("="*80)
        logger.info(f"Total Duration: {duration:.1f}s")
        logger.info(f"Bank Statement: {'✓ Success' if parallel_results[0] == 0 else '✗ Failed'}")
        logger.info(f"Playwright: {'✓ Success' if parallel_results[1] == 0 else '✗ Failed'}")
        logger.info(f"CRM Integrator: {'✓ Success' if crm_result == 0 else '✗ Failed'}")
        logger.info("="*80 + "\n")

        return parallel_results[0] == 0 and parallel_results[1] == 0 and crm_result == 0

    except Exception as e:
        logger.error(f"✗ CRITICAL ERROR in agent pipeline: {e}")
        return False


async def run_agent_pipeline_with_progress(playwright_done: asyncio.Event):
    """Main agent pipeline entry point with progress tracking"""
    start_time = datetime.now()
    logger.info("\n" + "="*80)
    logger.info("SMARTIMPACT DEPREC AGENT SERVICE")
    logger.info(f"Started: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("="*80 + "\n")

    try:
        # Phase 1: Run bank statement and Playwright in parallel with progress tracking
        parallel_results = await run_parallel_scripts_with_progress(playwright_done)

        # Phase 2: Run CRM integrator after both complete
        logger.info("="*80)
        logger.info("PHASE 2: CRM INTEGRATION")
        logger.info("="*80)

        crm_result = run_crm_integrator_sync()

        # Summary
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()

        cleanup_directory()

        logger.info("\n" + "="*80)
        logger.info("EXECUTION SUMMARY")
        logger.info("="*80)
        logger.info(f"Total Duration: {duration:.1f}s")
        logger.info(f"Bank Statement: {'✓ Success' if parallel_results[0] == 0 else '✗ Failed'}")
        logger.info(f"Playwright: {'✓ Success' if parallel_results[1] == 0 else '✗ Failed'}")
        logger.info(f"CRM Integrator: {'✓ Success' if crm_result == 0 else '✗ Failed'}")
        logger.info("="*80 + "\n")

        return parallel_results[0] == 0 and parallel_results[1] == 0 and crm_result == 0

    except Exception as e:
        logger.error(f"✗ CRITICAL ERROR in agent pipeline: {e}")
        # Signal completion even on error so progress updater doesn't hang
        playwright_done.set()
        return False


def main():
    # Use same token retrieval pattern as bot_nef_serv.py
    BOT_TOKEN = os.getenv("BOT_TOKEN", "8303001059:AAHGoH5xFRrJ1SRPTpixDh93P1vA3JnFi8k")
    if not BOT_TOKEN or BOT_TOKEN == "your_token_here":
        print("⚠️ Warning: Using hardcoded bot token. Set BOT_TOKEN environment variable for security.")
    
    app = ApplicationBuilder().token(BOT_TOKEN).build()
    print(f"🔐 BOT_TOKEN: {BOT_TOKEN[:5]}...{BOT_TOKEN[-5:]}")
    
    # Handlers aligned with bot_nef_serv.py style
    app.add_handler(CommandHandler("start", start))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_all_messages))
    app.add_handler(MessageHandler(filters.Document.ALL, handle_document_logger))
    
    print("🤖 Logger bot is running... Send messages or drop files.")
    app.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()


