
# Converted from general_deprec_code.ipynb

import sys
import io

# Set UTF-8 encoding for Windows console
if sys.platform == 'win32':
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8')

# ============================================================================
# LOAD DATA FROM BANK STATEMENT RETRIEVER
# ============================================================================
# ============================================================================
# IMPORTS AND SETUP
# ============================================================================
import pandas as pd
import numpy as np
import pickle
from pathlib import Path
from datetime import datetime

# Load helper function
def clean_column_names(df):
    df.columns = (
        df.columns.str.strip()
        .str.lower()
        .str.replace(r'\W+', '_', regex=True)
        .str.replace(r'_+', '_', regex=True)
        .str.rstrip('_')
    )
    return df

# ============================================================================
# LOAD DATA FROM BANK STATEMENT RETRIEVER
# ============================================================================
data_dir = Path.cwd()

print("\n" + "="*80)
print("LOADING DATA FROM BANK STATEMENT RETRIEVER")
print("="*80)

try:
    with open(data_dir / 'deprec_gwids_df.pkl', 'rb') as f:
        gwids_df = pickle.load(f)
    print("✓ Loaded: gwids_df")
    
    with open(data_dir / 'deprec_gwid_df.pkl', 'rb') as f:
        gwid_df = pickle.load(f)
    print("✓ Loaded: gwid_df")
    
    with open(data_dir / 'deprec_processor_df.pkl', 'rb') as f:
        processor_df = pickle.load(f)
    print("✓ Loaded: processor_df")
    
    with open(data_dir / 'deprec_corps_df.pkl', 'rb') as f:
        corps_df = pickle.load(f)
    print("✓ Loaded: corps_df")
    
    with open(data_dir / 'deprec_result_df.pkl', 'rb') as f:
        result_df = pickle.load(f)
    print("✓ Loaded: result_df")
    
    with open(data_dir / 'deprec_releases_df.pkl', 'rb') as f:
        releases_df = pickle.load(f)
    print("✓ Loaded: releases_df")
    
    with open(data_dir / 'deprec_central_df.pkl', 'rb') as f:
        central_df = pickle.load(f)
    print("✓ Loaded: central_df")
    
    print("\n✓ All data loaded successfully!")
    print("="*80 + "\n")
    
except FileNotFoundError as e:
    print(f"\n❌ ERROR: Required data file not found: {e}")
    print("⚠️  Please run bank_statement_retriever_and_organizer.py first!")
    raise

# Export central_df to Excel
from datetime import datetime
import os

excel_filename = f"central_df_{datetime.now().strftime('%Y-%m-%d')}.xlsx"
excel_path = os.path.join(os.getcwd(), excel_filename)

print(f"Exporting central_df to Excel: {excel_filename}")
try:
    with pd.ExcelWriter(excel_path, engine='xlsxwriter') as writer:
        workbook = writer.book
        header_format = workbook.add_format({"bold": True})
        
        # Write central_df to Excel
        central_df.to_excel(writer, sheet_name="central_df", index=False)
        ws = writer.sheets["central_df"]
        ws.set_row(0, None, header_format)
        
        # Auto-adjust column widths
        for i, col in enumerate(central_df.columns):
            col_series = central_df.iloc[:, i]
            max_len = max(col_series.astype(str).map(len).max(), len(str(col)))
            width = min(max_len + 2, 60)
            ws.set_column(i, i, width)
        
        ws.freeze_panes(1, 0)
    
    print(f"✅ Successfully exported central_df to: {excel_path}")
    print(f"   Total rows: {len(central_df)}, Total columns: {len(central_df.columns)}")
except Exception as e:
    print(f"❌ Error exporting central_df to Excel: {e}")

# Import the Sales By Gateway file
sales_by_gateway = pd.read_csv("Sales_By_Gateway.csv")
sales_by_gateway = clean_column_names(sales_by_gateway)

# Rename duplicate columns: if there are multiple columns with the same name, suffix them with a count
def rename_duplicates(df):
    cols = df.columns
    seen = {}
    new_cols = []
    for col in cols:
        if col not in seen:
            seen[col] = 1
            new_cols.append(col)
        else:
            seen[col] += 1
            new_cols.append(f"{col}{seen[col]}")
    df.columns = new_cols
    return df

sales_by_gateway = rename_duplicates(sales_by_gateway)

# Auto-convert columns that contain numbers to numeric types
def auto_convert_numeric_columns(df):
    """Automatically detect and convert columns with numeric data to numeric types"""
    for col in df.columns:
        # Skip if already numeric
        if pd.api.types.is_numeric_dtype(df[col]):
            continue
        
        # Try to clean and convert to numeric
        try:
            # Remove common formatting: $, commas, %, and extra spaces
            cleaned = df[col].astype(str).str.replace('$', '', regex=False)
            cleaned = cleaned.str.replace(',', '', regex=False)
            cleaned = cleaned.str.replace(' ', '', regex=False)
            
            # Check if it's a percentage column (has % signs)
            is_percentage = cleaned.str.contains('%', regex=False).any()
            cleaned = cleaned.str.replace('%', '', regex=False)
            
            # Try converting to numeric
            converted = pd.to_numeric(cleaned, errors='coerce')
            
            # If most values converted successfully (>50%), use the numeric version
            non_null_original = df[col].notna().sum()
            non_null_converted = converted.notna().sum()
            
            if non_null_converted / non_null_original > 0.5:
                df[col] = converted
                # If it was a percentage, divide by 100
                if is_percentage:
                    df[col] = df[col] / 100
        except:
            # If conversion fails, keep as string
            pass
    
    return df

sales_by_gateway = auto_convert_numeric_columns(sales_by_gateway)

# Extract gateway_id, processor, processor_name, and corp from the Merchant column
# Format: "{gateway_id} - {processor}_{processor_name}_{corp}"
# Example: "10 - Paysafe_BASICSLINE LLC_10"

# Extract gateway_id: number before the first "-"
sales_by_gateway["gateway_id"] = (
    sales_by_gateway["merchant"]
        .str.split(r"\s*-\s*", n=1, regex=True)  # split on " - " (flexible spaces)
        .str[0]
        .str.strip()
        .astype("Int64")
)

# Extract processor: text between "-" and first "_"
# Extract processor_name: text between first "_" and last "_"
# Extract corp: text after last "_"
def extract_merchant_fields(merchant_str):
    """Extract processor, processor_name, and corp from merchant string"""
    if pd.isna(merchant_str):
        return pd.Series([None, None, None])
    
    merchant_str = str(merchant_str).strip()
    
    # Split on " - " to get the part after gateway_id
    parts = merchant_str.split(" - ", 1)
    if len(parts) < 2:
        return pd.Series([None, None, None])
    
    after_dash = parts[1].strip()
    
    # Check if there are underscores
    if "_" not in after_dash:
        # No underscores, so no processor/processor_name/corp structure
        return pd.Series([None, None, None])
    
    # Split by underscores
    underscore_parts = after_dash.split("_")
    
    if len(underscore_parts) < 2:
        # Only one part after underscore, treat as processor only
        return pd.Series([underscore_parts[0] if underscore_parts else None, None, None])
    
    # processor is the first part (between "-" and first "_")
    processor = underscore_parts[0].strip() if underscore_parts[0] else None
    
    # corp is the last part (after last "_")
    corp = underscore_parts[-1].strip() if underscore_parts[-1] else None
    
    # processor_name is everything between first "_" and last "_"
    if len(underscore_parts) > 2:
        processor_name = "_".join(underscore_parts[1:-1]).strip()
    else:
        # If only 2 parts, there's nothing between first and last underscore
        processor_name = None
    
    return pd.Series([processor, processor_name, corp])

# Apply extraction
merchant_fields = sales_by_gateway["merchant"].apply(extract_merchant_fields)
sales_by_gateway["processor"] = merchant_fields.iloc[:, 0]
sales_by_gateway["processor_name"] = merchant_fields.iloc[:, 1]
sales_by_gateway["corp"] = merchant_fields.iloc[:, 2]

# Find GWIDs that are present in both gwids_df and sales_by_gateway
matching_gwids = set(gwids_df["gwid"].unique()) & set(sales_by_gateway["gateway_id"].unique())
print("GWIDs in BOTH gwids_df and sales_by_gateway:", matching_gwids)

# Filter sales_by_gateway to only include rows with gateway_id in matching_gwids
filtered_sales_by_gateway = sales_by_gateway[sales_by_gateway["gateway_id"].isin(matching_gwids)]

# Inverse: show gateways NOT present in gwids_df for alert purposes
alert_sales_by_gateway = sales_by_gateway[~sales_by_gateway["gateway_id"].isin(matching_gwids)]
if not alert_sales_by_gateway.empty:
    print("⚠️ ALERT: Number of gateways in 'Sales By Gateway' NOT present in the GWID sheet (potential data mismatch):", len(alert_sales_by_gateway))
else:
    print("✅ All gateways in 'Sales By Gateway' are present in the GWID sheet.")

# Use Net Revenue directly - it already includes all adjustments (chargebacks, refunds, voids, etc.)
# After cleaning, "Net Revenue" becomes "net_revenue"
filtered_sales_by_gateway['total_rev'] = filtered_sales_by_gateway['net_revenue']
gwid_df_analysis = gwid_df.iloc[[-1], :-1]
gwid_df_analysis.index.name = None
# Map occurred deposits from gwid_df to filtered_sales_by_gateway

# Step 1: Get the TOTALS row from gwid_df (this has the sum per gateway)
gwid_totals_row = gwid_df.loc['TOTALS'].drop('TOTALS', errors='ignore')

# Step 2: Create mapping dictionary {gateway_id: deposit_amount}
gwid_deposit_map = gwid_totals_row.to_dict()

# Step 3: Map to filtered_sales_by_gateway based on gateway_id
filtered_sales_by_gateway.loc[:, 'occurred_deposits'] = filtered_sales_by_gateway['gateway_id'].astype(str).map(
    lambda x: gwid_deposit_map.get(x, 0)
)

# Add a 'difference' column showing the difference between occurred_deposits and total_rev
filtered_sales_by_gateway['difference'] = filtered_sales_by_gateway['occurred_deposits'] - filtered_sales_by_gateway['total_rev']

# Create a column 'total_rev_res' by multiplying 'net_revenue' by 0.85 to account for reserves
# Net Revenue already includes all adjustments (chargebacks, refunds, voids, etc.)
reserve_rate_used = 0.15
minus_one_reserve_rate = 1- reserve_rate_used 
filtered_sales_by_gateway['total_rev_res'] = filtered_sales_by_gateway['net_revenue'] * minus_one_reserve_rate

# Create a column 'difference:res' as the difference between occurred_deposits and total_rev_res
filtered_sales_by_gateway['difference_res'] = filtered_sales_by_gateway['occurred_deposits'] - filtered_sales_by_gateway['net_revenue']

# Alert if occurred_deposits is empty or has only NaN/0 values
if filtered_sales_by_gateway['occurred_deposits'].isnull().all() or \
   (filtered_sales_by_gateway['occurred_deposits'].eq(0).all() and not filtered_sales_by_gateway['occurred_deposits'].empty):
    print("ALERT: 'occurred_deposits' column is entirely empty, NaN, or zero!")

filtered_sales_by_gateway.head(10)

# Create a pivot table grouped by processor, showing the sum of difference_res, total_rev_res, and occurred_deposits for each
processor_cr = filtered_sales_by_gateway.groupby('processor_name').agg({
    'difference_res': 'sum',
    'total_rev_res': 'sum',
    'occurred_deposits': 'sum'
}).reset_index()

# Create %_missing_vs_total_rev_res column
# (difference_res/total_rev_res) * 100
processor_cr['%_missing_vs_total_rev_res'] = (
    processor_cr['difference_res'] / processor_cr['total_rev_res']
) * 100

# Sort by difference_res descending (highest to lowest)
processor_cr = processor_cr.sort_values('difference_res', ascending=False)

# Fact check 1: Check the sum of difference_res in the original table
original_sum = filtered_sales_by_gateway['difference_res'].sum()
print(f"\nOriginal filtered_sales_by_gateway sum: ${original_sum:,.2f}")

# Fact check 2: Check the sum of difference_res in the pivoted table
pivoted_sum = processor_cr['difference_res'].sum()
print(f"Pivoted processor_cr sum: ${pivoted_sum:,.2f}")

# Fact check 3: Verify they match
if abs(original_sum - pivoted_sum) < 0.01:
    print("✅ SUCCESS: The pivot is correct - sums match!")
else:
    print(f"❌ ERROR: Sums don't match! Difference: ${abs(original_sum - pivoted_sum):,.2f}")

processor_cr

# Group releases_df by processor name (assuming column 'processor' contains names and 'amount' contains totals)
processor_releases = releases_df.groupby('processor')['amount'].sum().reset_index()
processor_releases = processor_releases.rename(columns={'processor': 'Processor', 'amount': 'Total Released'})
processor_releases

min_date, max_date = result_df['posting_date'].min(), result_df['posting_date'].max()

def generate_telegram_report_v2(filtered_sales_by_gateway, min_date, max_date, reserve_rate_used):
    """Professional message with net difference showing both negative (missing) and positive (excess) deposit sums, plus % of total_rev_res, plus net total."""
    
    report_date = datetime.now()
    analysis_start = min_date.strftime('%b %d, %Y')
    analysis_end = max_date.strftime('%b %d, %Y')
    reserve_rate_pct = reserve_rate_used * 100

    # Totals
    difference_col = pd.to_numeric(filtered_sales_by_gateway["difference_res"], errors="coerce").fillna(0)
    total_rev_res = pd.to_numeric(filtered_sales_by_gateway["total_rev_res"], errors="coerce").fillna(0).sum()
    
    # Net breakdown: missing (negative) and excess (positive) sums
    total_missing = difference_col[difference_col < 0].sum()
    total_excess = difference_col[difference_col > 0].sum()
    net_difference = total_missing + total_excess

    pct_missing = 0.0 if total_rev_res == 0 else (total_missing / total_rev_res) * 100
    pct_excess = 0.0 if total_rev_res == 0 else (total_excess / total_rev_res) * 100
    pct_net = 0.0 if total_rev_res == 0 else (net_difference / total_rev_res) * 100

    report = f"""
<b>📊 DEPOSIT RECONCILIATION REPORT</b>
<i>Bank vs CRM Analysis</i>

<b>Date:</b> {report_date.strftime('%B %d, %Y')}
<b>Time:</b> {report_date.strftime('%H:%M:%S')} UTC
<b>Period:</b> {min_date.strftime('%b %d, %Y')} → {max_date.strftime('%b %d, %Y')}

<b>Adjustments Included:</b>
- Reserve rate adjustment ({reserve_rate_pct:.1f}%)
- Refunded transactions
- Chargeback deductions

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
<b>💰 NET DIFFERENCE</b>
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

<code>• Missing (Shortfall):   ${total_missing:,.2f}   ({pct_missing:+.2f}% of CRM w/ reserves)</code>

<code>• Excess Deposited:      ${total_excess:,.2f}   ({pct_excess:+.2f}% of CRM w/ reserves)</code>

<code>• Net Difference (Sum):  ${net_difference:,.2f}   ({pct_net:+.2f}% of CRM w/ reserves)</code>

"""
    return report

# Generate the message header
message = generate_telegram_report_v2(filtered_sales_by_gateway, min_date, max_date, reserve_rate_used)



def generate_complete_report(filtered_sales_by_gateway, processor_cr, processor_releases, result_df, min_date, max_date, reserve_rate_used):
    """Generate complete report with header and processor analysis - using consistent processor-level totals,
    then add detailed reconciliation data boilerplate with the output Excel filename and included sheets.
    """

    def get_emoji(pct):
        """Return emoji based on percentage threshold"""
        if pct >= -2:
            return "🟢"
        elif pct >= -4:
            return "🟡"
        else:
            return "🔴"

    report_date = datetime.now()
    analysis_start = (min_date - pd.Timedelta(days=3)).strftime('%b %d, %Y')
    analysis_end = (max_date - pd.Timedelta(days=3)).strftime('%b %d, %Y')
    reserve_rate_pct = reserve_rate_used * 100

    # Output file name as in file_context_0
    current_date_str = report_date.strftime('%m_%d_%Y')
    excel_file_name = f"Deprec_Report_{current_date_str}.xlsx"

    # Get totals from processor_cr for consistency
    total_rev_res = processor_cr['total_rev_res'].sum()

    # Separate negative and positive processors
    negative_processors = processor_cr[processor_cr['difference_res'] < 0].copy()
    positive_processors = processor_cr[processor_cr['difference_res'] > 0].copy()

    # Calculate totals at processor level
    total_missing = negative_processors['difference_res'].sum()
    total_excess = positive_processors['difference_res'].sum()
    net_difference = total_missing + total_excess

    # Calculate total occurred deposits for the period
    if 'occurred_deposits' in filtered_sales_by_gateway.columns:
        total_occurred_deposits = filtered_sales_by_gateway['occurred_deposits'].sum()
    elif 'Occurred Deposits' in filtered_sales_by_gateway.columns:
        total_occurred_deposits = filtered_sales_by_gateway['Occurred Deposits'].sum()
    else:
        total_occurred_deposits = 0.0

    # Percentages
    pct_missing = 0.0 if total_rev_res == 0 else (total_missing / total_rev_res) * 100
    pct_excess = 0.0 if total_rev_res == 0 else (total_excess / total_rev_res) * 100
    pct_net = 0.0 if total_rev_res == 0 else (net_difference / total_rev_res) * 100

    # Build message parts
    message_parts = []

    # Header section
    message_parts.append("<b>📊 DEPOSIT RECONCILIATION REPORT</b>")
    message_parts.append("<i>Bank vs CRM Analysis</i>")
    message_parts.append("")
    message_parts.append(f"<b>Date:</b> {report_date.strftime('%B %d, %Y')}")
    message_parts.append(f"<b>Time:</b> {report_date.strftime('%H:%M:%S')} UTC")
    message_parts.append(f"<b>Period:</b> {analysis_start} → {analysis_end}")
    message_parts.append("")
    message_parts.append("<b>Adjustments Included:</b>")
    message_parts.append(f"- Reserve rate adjustment ({reserve_rate_pct:.1f}%)")
    message_parts.append("- Refunded transactions")
    message_parts.append("- Chargeback deductions")
    message_parts.append("")
    message_parts.append("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    message_parts.append("<b>NET DIFFERENCE</b>")
    message_parts.append("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    message_parts.append("")
    # New bullet point at the top: total amount of deposits
    message_parts.append(f"<code>• Total Deposits (Bank):    ${total_occurred_deposits:,.2f}</code>")
    message_parts.append("")
    message_parts.append(f"<code>• Missing (shortfall):   ${total_missing:,.2f}   ({pct_missing:+.2f}% of CRM w/ reserves)</code>")
    message_parts.append("")
    message_parts.append(f"<code>• Excess Deposits:      ${total_excess:,.2f}   ({pct_excess:+.2f}% of CRM w/ reserves)</code>")
    message_parts.append("")
    message_parts.append(f"<code>• Net Difference:  ${net_difference:,.2f}   ({pct_net:+.2f}% of CRM w/ reserves)</code>")

    # Processor Analysis section
    message_parts.append("")
    message_parts.append("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    message_parts.append("<b>1) PROCESSOR ANALYSIS</b>")
    message_parts.append("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    message_parts.append("")

    # Processors with Shortfalls
    message_parts.append("<b>⬇️ PROCESSORS WITH SHORTFALLS</b>")
    message_parts.append("<i>Occurred deposits below CRM values (accounting for reserves)</i>")
    message_parts.append("")

    if len(negative_processors) > 0:
        message_parts.append(f"<b>Total Processors:</b> <code>{len(negative_processors)}</code>")
        message_parts.append(f"<b>Total Shortfall:</b> <code>${total_missing:,.2f}</code>")
        message_parts.append("")

        # Sort by absolute value
        negative_processors['abs_difference'] = negative_processors['difference_res'].abs()
        negative_processors = negative_processors.sort_values('abs_difference', ascending=False)

        for idx, row in negative_processors.iterrows():
            pct = row['%_missing_vs_total_rev_res']
            emoji = get_emoji(pct)
            shortfall_pct = abs(row['difference_res'] / total_missing * 100) if total_missing != 0 else 0

            message_parts.append(
                f"{emoji} <b>{row['processor_name']}</b>: <code>${row['difference_res']:,.2f}</code> "
                f"(<code>{pct:+.2f}%</code> vs expected, <code>-{shortfall_pct:.1f}%</code> of total shortfall)"
            )
            message_parts.append(f"  • Expected (w/ reserves): <code>${row['total_rev_res']:,.2f}</code>")
            message_parts.append(f"  • Actual Deposits: <code>${row['occurred_deposits']:,.2f}</code>")

    else:
        message_parts.append("<b>No processors with lower-than-expected deposit amounts</b>")

    # Processors with Overage
    message_parts.append("")
    message_parts.append("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    message_parts.append("")
    message_parts.append("<b>⬆️ PROCESSORS WITH DEPOSIT OVERAGE</b>")
    message_parts.append("<i>Occurred deposits exceed CRM values (accounting for reserves)</i>")
    message_parts.append("")

    if len(positive_processors) > 0:
        message_parts.append(f"<b>{len(positive_processors)}</b> processors exceeding CRM expectations")
        message_parts.append(f"<b>Total Overage:</b> <code>${total_excess:,.2f}</code>")
        message_parts.append("")
        message_parts.append("<b>Notable Performers:</b>")

        # Sort by difference_res (descending)
        positive_processors = positive_processors.sort_values('difference_res', ascending=False)

        # Get top 3
        top_3 = positive_processors.head(3)
        others = positive_processors.iloc[3:]

        # Add bold to processor names for notable performers
        for idx, row in top_3.iterrows():
            overage_pct = (row['difference_res'] / total_excess * 100) if total_excess != 0 else 0
            pct = row['%_missing_vs_total_rev_res']
            message_parts.append(
                f"• <b>{row['processor_name']}</b>: <code>+${row['difference_res']:,.2f}</code> "
                f"(<code>{pct:+.2f}%</code> vs expected, <code>{int(round(overage_pct))}%</code> of overage)"
            )

        if len(others) > 0:
            others_sum = others['difference_res'].sum()
            others_pct = (others_sum / total_excess * 100) if total_excess != 0 else 0
            message_parts.append(
                f"• <b>{len(others)} others:</b> <code>+${others_sum:,.2f}</code> (<code>{int(round(others_pct))}%</code> of overage)"
            )

        message_parts.append("")
        message_parts.append("<i>No action required - processors depositing above expected amounts.</i>")
    else:
        message_parts.append("⚠️ <b>No processors have positive differences – all are at or below expected values.</b>")

    # Return the full report as a string
    return "\n".join(message_parts)

# Generate complete message
message = generate_complete_report(filtered_sales_by_gateway, processor_cr, processor_releases, result_df, min_date, max_date, reserve_rate_used)
print(message)

# Calculate metrics
num_processors_with_releases = len(processor_releases)
total_releases = processor_releases['Total Released'].sum()

# New: total operations deposits is the sum of 'occurred_deposits' from filtered_sales_by_gateway
if 'occurred_deposits' in filtered_sales_by_gateway.columns:
    total_operations_deposits = filtered_sales_by_gateway['occurred_deposits'].sum()
elif 'Occurred Deposits' in filtered_sales_by_gateway.columns:
    total_operations_deposits = filtered_sales_by_gateway['Occurred Deposits'].sum()
else:
    # fallback: use 0 if the expected column is not present
    total_operations_deposits = 0.0

pct_reserves_on_operations = (total_releases / total_operations_deposits * 100) if total_operations_deposits != 0 else 0

reserves_message = []
reserves_message.append("")
reserves_message.append("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
reserves_message.append("<b>2) PROCESSOR RESERVE RELEASE</b>")
reserves_message.append("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
reserves_message.append("")

# Summary metrics
reserves_message.append(f"<b>Total Processors with Releases:</b> <code>{num_processors_with_releases}</code>")
reserves_message.append(f"<b>Total Reserves Released:</b> <code>${total_releases:,.2f}</code>")
reserves_message.append(f"<b>% of Reserves on Operations Deposits:</b> <code>{pct_reserves_on_operations:.2f}%</code>")
reserves_message.append("")

# Individual processor breakdowns
if num_processors_with_releases > 0:
    # Make this title bold for extra emphasis as requested
    reserves_message.append("<b>Reserves by Processor:</b>")
    
    # Sort by Total Released (descending)
    processor_releases_sorted = processor_releases.sort_values('Total Released', ascending=False)
    
    for idx, row in processor_releases_sorted.iterrows():
        pct_of_total = (row['Total Released'] / total_releases * 100) if total_releases != 0 else 0
        # Make sure processor name is in bold
        reserves_message.append(
            f"• <b>{row['Processor']}</b>: <code>${row['Total Released']:,.2f}</code> "
            f"(<code>{pct_of_total:.1f}%</code> of total releases)"
        )
else:
    reserves_message.append("<b>No processor reserves released during this period</b>")

print("\n".join(reserves_message))

corps_no_total = corps_df.drop(index='TOTALS', errors='ignore')
if "TOTALS" in corps_no_total.columns:
    corps_no_total = corps_no_total.drop(columns=["TOTALS"])
corps_no_total

# Generate corp analysis section
def generate_corp_analysis(corps_df):
    """Generate corp analysis section with deposits per corp.
    If 'Total' column exists, ignore it in analysis."""
    
    message_parts = []
    
    # If 'Total' is a column, exclude it from analysis
    cols_to_include = [col for col in corps_df.columns if col != 'Total']
    filtered_corps_df = corps_df[cols_to_include]
    
    # Calculate metrics - sum all numeric columns to get total deposits
    total_deposits = filtered_corps_df.select_dtypes(include=[np.number]).sum().sum()
    num_corps = len(filtered_corps_df.columns)  # Number of corp columns (excluding 'Total' if present)
    
    # ============================================================================
    # MAIN HEADER: CORP ANALYSIS
    # ============================================================================
    message_parts.append("")
    message_parts.append("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    message_parts.append("<b>3) CORP ANALYSIS</b>")
    message_parts.append("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
    message_parts.append("")
    
    # Summary metrics
    message_parts.append(f"<b>Total Corps:</b> <code>{num_corps}</code>")
    message_parts.append(f"<b>Total Deposits:</b> <code>${total_deposits:,.2f}</code>")
    message_parts.append("")
    
    # Individual corp breakdowns
    if num_corps > 0:
        message_parts.append("<b>Deposits by Corp:</b>")
        
        # Sum each column (corp) and create a sorted list
        corp_totals = []
        for col in filtered_corps_df.columns:
            corp_total = filtered_corps_df[col].sum()
            corp_totals.append((col, corp_total))
        
        # Sort by total (descending)
        corp_totals_sorted = sorted(corp_totals, key=lambda x: x[1], reverse=True)
        
        for corp_name, corp_total in corp_totals_sorted:
            pct_of_total = (corp_total / total_deposits * 100) if total_deposits != 0 else 0
            message_parts.append(
                f"• <b>{corp_name}:</b> <code>${corp_total:,.2f}</code> "
                f"(<code>{pct_of_total:.1f}%</code> of total deposits)"
            )
    else:
        message_parts.append("<b>No corp deposits found during this period</b>")
    
    message_parts.append("")
    
    return "\n".join(message_parts)

# Generate and print corp message
corp_message = generate_corp_analysis(corps_no_total)
print(corp_message)

# Get the output file name
report_date = datetime.now()
current_date_str = report_date.strftime('%m_%d_%Y')
excel_file_name = f"Deprec_Report_{current_date_str}.xlsx"

detailed_message = []
detailed_message.append("")
detailed_message.append("<b>━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━</b>")
detailed_message.append("<b>DETAILED RECONCILIATION DATA</b>")
detailed_message.append("<b>━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━</b>")
detailed_message.append("")
detailed_message.append(f"<b>📎 Attached: {excel_file_name}</b>")
detailed_message.append("")
detailed_message.append("<b>Sheets Included:</b>")
detailed_message.append(f"   1. Reconciliation Summary (All {len(filtered_sales_by_gateway)} MIDs)")
detailed_message.append("   2. Gateway ID Breakdown (Transaction-level detail)")
detailed_message.append("   3. Processor Aggregation (Performance metrics)")
detailed_message.append("   4. Corp Breakdown")
detailed_message.append("")

# Print the detailed_message list as a block of text with newlines (not as a Python list)
print("\n".join(detailed_message))

# Reverse the column name cleaning in filtered_sales_by_gateway
# Convert snake_case back to Title Case with spaces

def reverse_clean_column_names(df):
    """
    Reverses the cleaning: snake_case → Title Case
    Example: gateway_id → Gateway ID
             processor_name → Processor Name
    """
    new_columns = []
    for col in df.columns:
        # Replace underscores with spaces and title case each word
        cleaned = col.replace('_', ' ').title()
        new_columns.append(cleaned)
    
    df.columns = new_columns
    return df

# Apply to filtered_sales_by_gateway
filtered_sales_by_gateway = reverse_clean_column_names(filtered_sales_by_gateway)

# Fix the column names for Total Rev Res and Difference Res
filtered_sales_by_gateway = filtered_sales_by_gateway.rename(columns={
    'Total Rev': 'Total Revenue w/o Reserves',
    'Total Rev Res': 'Total Revenue w/ Reserves',
    'Difference Res': 'Difference w/ Reserves'
})
filtered_sales_by_gateway = filtered_sales_by_gateway.rename(columns={
    'Difference': 'Difference w/o Reserves'
})

# Reorder so that the last columns are in this specific order:
desired_last = [
    "Occurred Deposits",  # Occurred Deposits
    "Total Revenue w/o Reserves",
    "Total Revenue w/ Reserves",
    "Difference w/o Reserves",
    "Difference w/ Reserves"
]

actual_last = [col for col in desired_last if col in filtered_sales_by_gateway.columns]
other_cols = [col for col in filtered_sales_by_gateway.columns if col not in actual_last]
filtered_sales_by_gateway = filtered_sales_by_gateway[other_cols + actual_last]
filtered_sales_by_gateway = filtered_sales_by_gateway[other_cols + actual_last]
filtered_sales_by_gateway.columns
# Currency columns that should be formatted as USD in Excel (after reverse_clean_column_names)
usd_cols = [
    'Gross Product Revenue', 'Net Product Revenue', 'Total Revenue',
    'Refunded Revenue', 'Voided Revenue', 'Chargeback Revenue', 'Alert Revenue',
    'Net Revenue', 'Cycle 1 Revenue', 'Renewal Revenue',
    'Processor Name', 'Corp', 'Occurred Deposits',
    'Total Revenue w/o Reserves', 'Total Revenue w/ Reserves',
    'Difference w/o Reserves', 'Difference w/ Reserves'
]


from datetime import datetime
import pandas as pd

# Generate filename with current date
current_date = datetime.now().strftime('%Y-%m-%d')
file_path = f"DEPREC_Output_{current_date}.xlsx"

# Write to Excel with multiple sheets, including Release Report at the end
with pd.ExcelWriter(file_path, engine="xlsxwriter") as writer:
    workbook = writer.book
    usd_format = workbook.add_format({"num_format": "$#,##0.00"})
    header_format = workbook.add_format({"bold": True})

    # Sheet 1: Filtered Sales by Gateway
    filtered_sales_by_gateway.to_excel(writer, sheet_name="Sales_By_Gateway", index=False)
    ws1 = writer.sheets["Sales_By_Gateway"]
    ws1.set_row(0, None, header_format)
    for i, col in enumerate(filtered_sales_by_gateway.columns):
        col_series = filtered_sales_by_gateway.iloc[:, i]
        max_len = max(col_series.astype(str).map(len).max(), len(str(col)))
        width = min(max_len + 2, 60)
        # Format only if column name is found in usd_cols, regardless of detected dtype
        if col in usd_cols:
            ws1.set_column(i, i, width, usd_format)
        else:
            ws1.set_column(i, i, width)
    ws1.freeze_panes(1, 0)

    # Sheet 2: GWID Breakdown
    gwid_df.to_excel(writer, sheet_name="GWIDS", index=True)
    ws2 = writer.sheets["GWIDS"]
    ws2.set_row(0, None, header_format)
    for i in range(len(gwid_df.columns) + 1):
        ws2.set_column(i, i, 15, usd_format if i > 0 else None)
    ws2.freeze_panes(1, 1)

    # Sheet 3: Processor Breakdown
    processor_df.to_excel(writer, sheet_name="PROCESSORS", index=True)
    ws3 = writer.sheets["PROCESSORS"]
    ws3.set_row(0, None, header_format)
    for i in range(len(processor_df.columns) + 1):
        ws3.set_column(i, i, 20, usd_format if i > 0 else None)
    ws3.freeze_panes(1, 1)

    # Sheet 4: Corp Breakdown
    corps_df.to_excel(writer, sheet_name="CORPS", index=True)
    ws4 = writer.sheets["CORPS"]
    ws4.set_row(0, None, header_format)
    for i in range(len(corps_df.columns) + 1):
        ws4.set_column(i, i, 25, usd_format if i > 0 else None)
    ws4.freeze_panes(1, 1)

    # Sheet 5: Release Report (processor_releases)
    # Make sure to retain original column names as in processor_releases
    # --- Bold Processor names in Export ---
    processor_releases_to_export = processor_releases.copy()
    processor_col_name = None
    # Try to find processor column
    for col_candidate in processor_releases_to_export.columns:
        if col_candidate.strip().lower().replace(' ', '') == "processor":
            processor_col_name = col_candidate
            break
    if processor_col_name:
        # Wrap all processor names in Excel bold
        processor_releases_to_export[processor_col_name] = processor_releases_to_export[processor_col_name].astype(str)
    processor_releases_to_export.to_excel(writer, sheet_name="Release Report", index=False)
    ws5 = writer.sheets["Release Report"]
    ws5.set_row(0, None, header_format)

    # Apply bold formatting to the Processor column's data cells
    bold_cell_format = workbook.add_format({"bold": True})
    # Find the processor col index in the export
    if processor_col_name:
        processor_idx = list(processor_releases_to_export.columns).index(processor_col_name)
        for rownum in range(1, 1 + len(processor_releases_to_export)):
            ws5.write(rownum, processor_idx, processor_releases_to_export.iloc[rownum - 1, processor_idx], bold_cell_format)
    # Set column widths and formatting
    for i, col in enumerate(processor_releases_to_export.columns):
        col_series = processor_releases_to_export.iloc[:, i]
        max_len = max(col_series.astype(str).map(len).max(), len(str(col)))
        width = min(max_len + 2, 60)
        # Apply currency format if the column seems to be currency; here it's usually numeric except the processor name
        if pd.api.types.is_numeric_dtype(col_series) and (not processor_col_name or i != processor_idx):
            ws5.set_column(i, i, width, usd_format)
        else:
            ws5.set_column(i, i, width)
    ws5.freeze_panes(1, 0)

print(f"✅ Exported to: {file_path}")

from telegram import Bot
from telegram.constants import ParseMode
from telegram.error import TelegramError
TELEGRAM_BOT_API = "8303001059:AAHGoH5xFRrJ1SRPTpixDh93P1vA3JnFi8k"
bot = Bot(token=TELEGRAM_BOT_API)

async def main():
    chat_id = -5064077272
    try:
        # Send first message (main report)
        await bot.send_message(
            chat_id=chat_id,
            text=message,
            parse_mode=ParseMode.HTML
        )
        print("✅ Sent main report to Telegram")
        
        # Send second message (processor reserves)
        await bot.send_message(
            chat_id=chat_id,
            text="\n".join(reserves_message),
            parse_mode=ParseMode.HTML
        )
        print("✅ Sent processor reserves to Telegram")

        # Send third message (corp-level data) - USE IT DIRECTLY, IT'S ALREADY A STRING
        await bot.send_message(
            chat_id=chat_id,
            text=corp_message,  # Don't join it, it's already a string!
            parse_mode=ParseMode.HTML
        )
        print("✅ Sent corp analysis to Telegram")
        
        # Send fourth message (detailed reconciliation data)
        await bot.send_message(
            chat_id=chat_id,
            text="\n".join(detailed_message),
            parse_mode=ParseMode.HTML
        )
        print("✅ Sent detailed reconciliation data to Telegram")
        
        # Send the Excel file
        with open(file_path, 'rb') as f:
            await bot.send_document(
                chat_id=chat_id,
                document=f,
                filename=f"DEPREC_Report_{current_date}.xlsx"
            )
        print("✅ Sent Excel file to Telegram")
        
    except TelegramError as e:
        print(f"❌ Telegram Error: {e}")
        print(f"⚠️  Could not send messages to chat_id: {chat_id}")
        print("   Please verify:")
        print("   1. The chat_id is correct")
        print("   2. The bot is added to the chat/group")
        print("   3. The bot has permission to send messages")
        print(f"\n✅ Excel file was successfully exported to: {file_path}")
    except Exception as e:
        print(f"❌ Unexpected error sending to Telegram: {e}")
        print(f"\n✅ Excel file was successfully exported to: {file_path}")

import asyncio
asyncio.run(main())
