import streamlit as st
import pandas as pd
from io import BytesIO
import re
from rapidfuzz import fuzz, process
from xlsxwriter.utility import xl_col_to_name
import math
from datetime import datetime
import openpyxl

st.title("📊 Enhanced Campaign + Shopify Data Processor with Date Columns")
st.markdown("**Now supports multiple file uploads and date-based column grouping for each product with Excel formulas!**")

# ---- MULTIPLE FILE UPLOADS ----
st.subheader("📁 Upload Campaign Data Files")
campaign_files = st.file_uploader(
    "Upload Campaign Data Files (Excel/CSV)", 
    type=["xlsx", "csv"], 
    accept_multiple_files=True,
    key="campaign_files",
    help="Upload one or more Facebook Ads campaign files. Files with matching products and campaign names will be merged."
)

st.subheader("🛒 Upload Shopify Data Files")
shopify_files = st.file_uploader(
    "Upload Shopify Data Files (Excel/CSV)", 
    type=["xlsx", "csv"], 
    accept_multiple_files=True,
    key="shopify_files",
    help="Upload one or more Shopify sales files. Files with matching products and variants will be merged."
)

st.subheader("📋 Upload Reference Data Files (Optional)")
old_merged_files = st.file_uploader(
    "Upload Reference Data Files (Excel/CSV) - to import delivery rates and product costs",
    type=["xlsx", "csv"],
    accept_multiple_files=True,
    key="reference_files",
    help="Upload one or more previous merged data files to automatically import delivery rates and product costs for matching products"
)

# ---- HELPERS ----
def safe_write(worksheet, row, col, value, cell_format=None):
    """Wrapper around worksheet.write to handle NaN/inf safely"""
    if isinstance(value, (int, float)):
        if value is None or (isinstance(value, float) and (math.isnan(value) or math.isinf(value))):
            value = 0
    else:
        if pd.isna(value):
            value = ""
    worksheet.write(row, col, value, cell_format)

def read_file(file):
    """Helper function to read uploaded file"""
    try:
        if file.name.endswith(".csv"):
            return pd.read_csv(file)
        else:
            return pd.read_excel(file)
    except Exception as e:
        st.error(f"❌ Error reading file {file.name}: {str(e)}")
        return None

def find_date_column(df):
    """Find date column in dataframe"""
    date_columns = []
    for col in df.columns:
        if any(keyword in col.lower() for keyword in ['day', 'date', 'time']):
            date_columns.append(col)
    return date_columns[0] if date_columns else None

def standardize_campaign_columns(df):
    """Standardize campaign column names and handle currency conversion"""
    df = df.copy()
    
    # Find and preserve original date column
    date_col = find_date_column(df)
    if date_col:
        # Keep the original date column as is, just rename it to a standard name
        df['Date'] = df[date_col]
        if date_col != 'Date':
            df = df.drop(columns=[date_col])
        st.info(f"📅 Found date column: {date_col}")
    
    # Find purchases/results column
    purchases_col = None
    for col in df.columns:
        if col.lower() in ['purchases', 'results']:
            purchases_col = col
            break
    
    if purchases_col and purchases_col != 'Purchases':
        df = df.rename(columns={purchases_col: 'Purchases'})
        st.info(f"📝 Renamed '{purchases_col}' to 'Purchases'")
    
    # Find amount spent column and handle currency
    amount_col = None
    is_inr = False
    
    # Check for USD first
    for col in df.columns:
        if 'amount spent' in col.lower() and 'usd' in col.lower():
            amount_col = col
            is_inr = False
            break
    
    # If no USD found, check for INR
    if not amount_col:
        for col in df.columns:
            if 'amount spent' in col.lower() and 'inr' in col.lower():
                amount_col = col
                is_inr = True
                break
    
    # If neither USD nor INR specified, assume it's INR and convert
    if not amount_col:
        for col in df.columns:
            if 'amount spent' in col.lower():
                amount_col = col
                is_inr = True  # Assume INR if currency not specified
                break
    
    if amount_col:
        if is_inr:
            # Convert INR to USD by dividing by 100
            df['Amount spent (USD)'] = df[amount_col] / 100
            st.info(f"💱 Converted '{amount_col}' from INR to USD (divided by 100)")
        else:
            df['Amount spent (USD)'] = df[amount_col]
            if amount_col != 'Amount spent (USD)':
                st.info(f"📝 Renamed '{amount_col}' to 'Amount spent (USD)'")
        
        # Remove original column if it's different
        if amount_col != 'Amount spent (USD)':
            df = df.drop(columns=[amount_col])
    
    return df

def merge_campaign_files(files):
    """Merge multiple campaign files"""
    if not files:
        return None
    
    all_campaigns = []
    file_info = []
    
    for file in files:
        df = read_file(file)
        if df is not None:
            # Standardize columns and handle currency conversion
            df = standardize_campaign_columns(df)
            all_campaigns.append(df)
            file_info.append(f"{file.name} ({len(df)} rows)")
    
    if not all_campaigns:
        return None
    
    # Combine all campaign files
    merged_df = pd.concat(all_campaigns, ignore_index=True)
    
    # Group by Campaign name and Date (if available) and sum amounts
    group_cols = ["Campaign name"]
    if 'Date' in merged_df.columns:
        group_cols.append('Date')
    
    required_cols = group_cols + ["Amount spent (USD)"]
    if all(col in merged_df.columns for col in required_cols):
        # Check if Purchases column exists
        has_purchases = "Purchases" in merged_df.columns
        
        agg_dict = {"Amount spent (USD)": "sum"}
        if has_purchases:
            agg_dict["Purchases"] = "sum"
        
        merged_df = merged_df.groupby(group_cols, as_index=False).agg(agg_dict)
    
    st.success(f"✅ Successfully merged {len(files)} campaign files:")
    for info in file_info:
        st.write(f"  • {info}")
    st.write(f"**Total campaigns after merging: {len(merged_df)}**")
    
    return merged_df

def merge_shopify_files(files):
    """Merge multiple Shopify files"""
    if not files:
        return None
    
    all_shopify = []
    file_info = []
    
    for file in files:
        df = read_file(file)
        if df is not None:
            # Find and preserve original date column
            date_col = find_date_column(df)
            if date_col:
                # Keep the original date column as is, just rename it to a standard name
                df['Date'] = df[date_col]
                if date_col != 'Date':
                    df = df.drop(columns=[date_col])
                st.info(f"📅 Found Shopify date column: {date_col}")
            
            all_shopify.append(df)
            file_info.append(f"{file.name} ({len(df)} rows)")
    
    if not all_shopify:
        return None
    
    # Combine all Shopify files
    merged_df = pd.concat(all_shopify, ignore_index=True)
    
    # Group by Product title + Product variant title + Date (if available)
    group_cols = ["Product title", "Product variant title"]
    if 'Date' in merged_df.columns:
        group_cols.append('Date')
    
    required_cols = group_cols + ["Net items sold"]
    if all(col in merged_df.columns for col in required_cols):
        # Group and sum net items sold, keep first price
        agg_dict = {"Net items sold": "sum"}
        if "Product variant price" in merged_df.columns:
            agg_dict["Product variant price"] = "first"  # Keep first price found
        
        merged_df = merged_df.groupby(group_cols, as_index=False).agg(agg_dict)
    
    st.success(f"✅ Successfully merged {len(files)} Shopify files:")
    for info in file_info:
        st.write(f"  • {info}")
    st.write(f"**Total product variants after merging: {len(merged_df)}**")
    
    return merged_df

def merge_reference_files(files):
    """Merge multiple reference files for delivery rates and product costs"""
    if not files:
        return None
    
    all_references = []
    file_info = []
    
    for file in files:
        df = read_file(file)
        if df is not None:
            required_old_cols = ["Product title", "Product variant title", "Delivery Rate"]
            if all(col in df.columns for col in required_old_cols):
                # Process the reference file similar to original logic
                current_product = None
                for idx, row in df.iterrows():
                    if pd.notna(row["Product title"]) and row["Product title"].strip() != "":
                        if row["Product variant title"] == "ALL VARIANTS (TOTAL)":
                            current_product = row["Product title"]
                        else:
                            current_product = row["Product title"]
                    else:
                        if current_product:
                            df.loc[idx, "Product title"] = current_product

                # Filter out total rows
                df_filtered = df[
                    (df["Product variant title"] != "ALL VARIANTS (TOTAL)") &
                    (df["Product variant title"] != "ALL PRODUCTS") &
                    (df["Delivery Rate"].notna()) & (df["Delivery Rate"] != "")
                ]
                
                if not df_filtered.empty:
                    df_filtered["Product title_norm"] = df_filtered["Product title"].astype(str).str.strip().str.lower()
                    df_filtered["Product variant title_norm"] = df_filtered["Product variant title"].astype(str).str.strip().str.lower()
                    all_references.append(df_filtered)
                    file_info.append(f"{file.name} ({len(df_filtered)} valid records)")
            else:
                st.warning(f"⚠️ Reference file {file.name} doesn't contain required columns")
    
    if not all_references:
        return None
    
    # Combine all reference files
    merged_df = pd.concat(all_references, ignore_index=True)
    
    # For duplicates, keep the last occurrence (latest file takes priority)
    merged_df = merged_df.drop_duplicates(
        subset=["Product title_norm", "Product variant title_norm"], 
        keep="last"
    )
    
    has_product_cost = "Product Cost (Input)" in merged_df.columns
    st.success(f"✅ Successfully merged {len(files)} reference files:")
    for info in file_info:
        st.write(f"  • {info}")
    st.write(f"**Total unique delivery rate records: {len(merged_df)}**")
    
    if has_product_cost:
        product_cost_count = merged_df["Product Cost (Input)"].notna().sum()
        st.write(f"**Product cost records found: {product_cost_count}**")
    
    return merged_df

# ---- STATE ----
df_campaign, df_shopify, df_old_merged = None, None, None
grouped_campaign = None

# ---- USER INPUT ----
shipping_rate = st.number_input("Shipping Rate per Item", min_value=0, value=77, step=1)
operational_rate = st.number_input("Operational Cost per Item", min_value=0, value=65, step=1)

# ---- PROCESS MULTIPLE REFERENCE FILES ----
if old_merged_files:
    df_old_merged = merge_reference_files(old_merged_files)
    
    if df_old_merged is not None:
        has_product_cost = "Product Cost (Input)" in df_old_merged.columns
        
        # Show preview
        preview_cols = ["Product title", "Product variant title", "Delivery Rate"]
        if has_product_cost:
            preview_cols.append("Product Cost (Input)")
        st.write("**Preview of merged reference data:**")
        st.write(df_old_merged[preview_cols].head(10))

# ---- PROCESS MULTIPLE CAMPAIGN FILES ----
if campaign_files:
    df_campaign = merge_campaign_files(campaign_files)
    
    if df_campaign is not None:
        st.subheader("📂 Merged Campaign Data")
        st.write(df_campaign)

        # ---- CLEAN PRODUCT NAME ----
        def clean_product_name(name: str) -> str:
            text = str(name).strip()
            match = re.split(r"[-/|]|\s[xX]\s", text, maxsplit=1)
            base = match[0] if match else text
            base = base.lower()
            base = re.sub(r'[^a-z0-9 ]', '', base)
            base = re.sub(r'\s+', ' ', base)
            return base.strip().title()

        df_campaign["Product Name"] = df_campaign["Campaign name"].astype(str).apply(clean_product_name)

        # ---- FUZZY DEDUP ----
        unique_names = df_campaign["Product Name"].unique().tolist()
        mapping = {}
        for name in unique_names:
            if name in mapping:
                continue
            result = process.extractOne(name, mapping.keys(), scorer=fuzz.token_sort_ratio, score_cutoff=85)
            if result:
                mapping[name] = mapping[result[0]]
            else:
                mapping[name] = name
        df_campaign["Canonical Product"] = df_campaign["Product Name"].map(mapping)

        # ---- GROUP BY CANONICAL PRODUCT (without date for summary) ----
        grouped_campaign = (
            df_campaign.groupby("Canonical Product", as_index=False)
            .agg({"Amount spent (USD)": "sum"})
        )
        grouped_campaign["Amount spent (INR)"] = grouped_campaign["Amount spent (USD)"] * 100
        grouped_campaign = grouped_campaign.rename(columns={
            "Canonical Product": "Product",
            "Amount spent (USD)": "Total Amount Spent (USD)",
            "Amount spent (INR)": "Total Amount Spent (INR)"
        })

        st.subheader("✅ Processed Campaign Data")
        st.write(grouped_campaign)

        # ---- FINAL CAMPAIGN DATA STRUCTURE WITH DATE GROUPING ----
        final_campaign_data = []
        has_purchases = "Purchases" in df_campaign.columns
        has_dates = 'Date' in df_campaign.columns

        for product, product_campaigns in df_campaign.groupby("Canonical Product"):
            for _, campaign in product_campaigns.iterrows():
                row = {
                    "Product Name": "",
                    "Campaign Name": campaign["Campaign name"],
                    "Amount Spent (USD)": campaign["Amount spent (USD)"],
                    "Amount Spent (INR)": campaign["Amount spent (USD)"] * 100,
                    "Product": product
                }
                if has_purchases:
                    row["Purchases"] = campaign.get("Purchases", 0)
                if has_dates:
                    row["Date"] = campaign.get("Date", "")
                final_campaign_data.append(row)

        df_final_campaign = pd.DataFrame(final_campaign_data)

        if not df_final_campaign.empty:
            # Sort by product spending and then by date
            order = (
                df_final_campaign.groupby("Product")["Amount Spent (INR)"].sum().sort_values(ascending=False).index
            )
            df_final_campaign["Product"] = pd.Categorical(df_final_campaign["Product"], categories=order, ordered=True)
            
            sort_cols = ["Product"]
            if has_dates:
                sort_cols.append("Date")
            
            df_final_campaign = df_final_campaign.sort_values(sort_cols).reset_index(drop=True)
            df_final_campaign["Delivered Orders"] = ""
            df_final_campaign["Delivery Rate"] = ""

        st.subheader("🎯 Final Campaign Data Structure with Date Grouping")
        display_cols = [col for col in df_final_campaign.columns if col != "Product"]
        st.write(df_final_campaign[display_cols])

# ---- PROCESS MULTIPLE SHOPIFY FILES ----
if shopify_files:
    df_shopify = merge_shopify_files(shopify_files)
    
    if df_shopify is not None:
        required_cols = ["Product title", "Product variant title", "Product variant price", "Net items sold"]
        available_cols = [col for col in required_cols if col in df_shopify.columns]
        
        # Keep date columns if they exist
        if 'Date' in df_shopify.columns:
            available_cols.append('Date')
            
        df_shopify = df_shopify[available_cols]

        # Add extra columns
        df_shopify["In Order"] = ""
        df_shopify["Product Cost (Input)"] = ""
        df_shopify["Delivery Rate"] = ""
        df_shopify["Delivered Orders"] = ""
        df_shopify["Net Revenue"] = ""
        df_shopify["Ad Spend (USD)"] = 0.0
        df_shopify["Shipping Cost"] = ""
        df_shopify["Operational Cost"] = ""
        df_shopify["Product Cost (Output)"] = ""
        df_shopify["Net Profit"] = ""
        df_shopify["Net Profit (%)"] = ""

        # ---- IMPORT DELIVERY RATES AND PRODUCT COSTS FROM MERGED REFERENCE DATA ----
        if df_old_merged is not None:
            # Create normalized versions for matching (case insensitive)
            df_shopify["Product title_norm"] = df_shopify["Product title"].astype(str).str.strip().str.lower()
            df_shopify["Product variant title_norm"] = df_shopify["Product variant title"].astype(str).str.strip().str.lower()
            
            # Create lookup dictionaries from old data
            delivery_rate_lookup = {}
            product_cost_lookup = {}
            has_product_cost = "Product Cost (Input)" in df_old_merged.columns
            
            for _, row in df_old_merged.iterrows():
                key = (row["Product title_norm"], row["Product variant title_norm"])
                
                # Store delivery rate
                delivery_rate_lookup[key] = row["Delivery Rate"]
                
                # Store product cost if column exists and has value
                if has_product_cost and pd.notna(row["Product Cost (Input)"]) and row["Product Cost (Input)"] != "":
                    product_cost_lookup[key] = row["Product Cost (Input)"]
            
            # Match and update delivery rates and product costs
            delivery_matched_count = 0
            product_cost_matched_count = 0
            
            for idx, row in df_shopify.iterrows():
                key = (row["Product title_norm"], row["Product variant title_norm"])
                
                # Update delivery rate
                if key in delivery_rate_lookup:
                    df_shopify.loc[idx, "Delivery Rate"] = delivery_rate_lookup[key]
                    delivery_matched_count += 1
                
                # Update product cost
                if key in product_cost_lookup:
                    df_shopify.loc[idx, "Product Cost (Input)"] = product_cost_lookup[key]
                    product_cost_matched_count += 1
            
            # Clean up temporary normalized columns
            df_shopify = df_shopify.drop(columns=["Product title_norm", "Product variant title_norm"])
            
            st.success(f"✅ Successfully imported delivery rates for {delivery_matched_count} product variants from reference data")
            if has_product_cost and product_cost_matched_count > 0:
                st.success(f"✅ Successfully imported product costs for {product_cost_matched_count} product variants from reference data")
            elif has_product_cost:
                st.info("ℹ️ No product cost matches found in reference data")

        # ---- CLEAN SHOPIFY PRODUCT TITLES TO MATCH CAMPAIGN ----
        def clean_product_name(name: str) -> str:
            text = str(name).strip()
            match = re.split(r"[-/|]|\s[xX]\s", text, maxsplit=1)
            base = match[0] if match else text
            base = base.lower()
            base = re.sub(r'[^a-z0-9 ]', '', base)
            base = re.sub(r'\s+', ' ', base)
            return base.strip().title()

        df_shopify["Product Name"] = df_shopify["Product title"].astype(str).apply(clean_product_name)

        # Build candidate set from campaign canonical names
        campaign_products = grouped_campaign["Product"].unique().tolist() if grouped_campaign is not None else []

        def fuzzy_match_to_campaign(name, choices, cutoff=85):
            if not choices:
                return name
            result = process.extractOne(name, choices, scorer=fuzz.token_sort_ratio, score_cutoff=cutoff)
            return result[0] if result else name

        # Apply fuzzy matching for Shopify → Campaign
        df_shopify["Canonical Product"] = df_shopify["Product Name"].apply(
            lambda x: fuzzy_match_to_campaign(x, campaign_products)
        )

        # ---- CORRECTED AD SPEND ALLOCATION (DAY-WISE DISTRIBUTION) ----
        if grouped_campaign is not None and df_campaign is not None:
            # Initialize Ad Spend column to 0 for all rows
            df_shopify["Ad Spend (USD)"] = 0.0
            
            # Create campaign spend lookup by product and date
            campaign_spend_by_product_date = {}
            
            # First, build the campaign spend lookup from df_campaign (which has dates)
            if 'Date' in df_campaign.columns:
                for _, row in df_campaign.iterrows():
                    product = row['Canonical Product']
                    date = str(row['Date'])
                    amount_usd = row['Amount spent (USD)']
                    
                    if product not in campaign_spend_by_product_date:
                        campaign_spend_by_product_date[product] = {}
                    
                    if date not in campaign_spend_by_product_date[product]:
                        campaign_spend_by_product_date[product][date] = 0
                    
                    campaign_spend_by_product_date[product][date] += amount_usd
            
            # Track which products have received date-specific allocation
            products_with_date_allocation = set()
            
            # Now allocate ad spend to Shopify variants based on their share of items sold per product per date
            for product, product_df in df_shopify.groupby("Canonical Product"):
                if product in campaign_spend_by_product_date:
                    has_any_date_allocation = False
                    
                    # For each date, calculate total items sold by this product on that date
                    for date in campaign_spend_by_product_date[product].keys():
                        date_campaign_spend_usd = campaign_spend_by_product_date[product][date]
                        
                        # Get all variants of this product sold on this date
                        product_date_variants = product_df[product_df['Date'].astype(str) == date]
                        
                        if not product_date_variants.empty:
                            total_items_on_date = product_date_variants['Net items sold'].sum()
                            
                            if total_items_on_date > 0:
                                # Distribute the campaign spend for this date proportionally
                                for idx, variant_row in product_date_variants.iterrows():
                                    variant_items = variant_row['Net items sold']
                                    variant_share = variant_items / total_items_on_date
                                    variant_ad_spend_usd = date_campaign_spend_usd * variant_share
                                    
                                    # Update the ad spend for this specific variant on this date
                                    df_shopify.loc[idx, "Ad Spend (USD)"] = variant_ad_spend_usd
                                    has_any_date_allocation = True
                    
                    # Mark this product as having received date-specific allocation
                    if has_any_date_allocation:
                        products_with_date_allocation.add(product)
            
            # For products WITHOUT any date-specific campaign data, fall back to total allocation
            ad_spend_map = dict(zip(grouped_campaign["Product"], grouped_campaign["Total Amount Spent (USD)"]))
            
            for product, product_df in df_shopify.groupby("Canonical Product"):
                # FIXED: Only allocate total spend if this product did NOT get date-specific allocation
                if product not in products_with_date_allocation and product in ad_spend_map:
                    total_items = product_df["Net items sold"].sum()
                    if total_items > 0:
                        total_spend_usd = ad_spend_map[product]
                        
                        # Allocate spend based on items sold for this product
                        for idx, variant_row in product_df.iterrows():
                            variant_items = variant_row['Net items sold']
                            variant_share = variant_items / total_items
                            variant_ad_spend_usd = total_spend_usd * variant_share
                            df_shopify.loc[idx, "Ad Spend (USD)"] = variant_ad_spend_usd

        # ---- SORT PRODUCTS BY NET ITEMS SOLD (DESC) ----
        product_order = (
            df_shopify.groupby("Product title")["Net items sold"]
            .sum()
            .sort_values(ascending=False)
            .index
        )

        df_shopify["Product title"] = pd.Categorical(df_shopify["Product title"], categories=product_order, ordered=True)
        
        # Sort by product, then by date if available
        sort_cols = ["Product title"]
        if 'Date' in df_shopify.columns:
            sort_cols.append("Date")
            
        df_shopify = df_shopify.sort_values(by=sort_cols).reset_index(drop=True)

        st.subheader("🛒 Merged Shopify Data with CORRECTED Ad Spend (USD) & Date Grouping")
        
        # Show delivery rate and product cost import summary
        if df_old_merged is not None:
            delivery_rate_filled = df_shopify["Delivery Rate"].astype(str).str.strip()
            delivery_rate_filled = delivery_rate_filled[delivery_rate_filled != ""]
            
            product_cost_filled = df_shopify["Product Cost (Input)"].astype(str).str.strip()
            product_cost_filled = product_cost_filled[product_cost_filled != ""]
            
            st.info(f"📊 Delivery rates imported: {len(delivery_rate_filled)} out of {len(df_shopify)} variants")
            if len(product_cost_filled) > 0:
                st.info(f"📊 Product costs imported: {len(product_cost_filled)} out of {len(df_shopify)} variants")
        
        # Show date information
        has_shopify_dates = 'Date' in df_shopify.columns
        if has_shopify_dates:
            unique_dates = df_shopify['Date'].unique()
            unique_dates = [str(d) for d in unique_dates if pd.notna(d) and str(d).strip() != '']
            st.info(f"📅 Found {len(unique_dates)} unique dates in Shopify data: {', '.join(sorted(unique_dates)[:5])}{'...' if len(unique_dates) > 5 else ''}")
        
        # Show ad spend verification
        total_shopify_ad_spend = df_shopify["Ad Spend (USD)"].sum()
        total_campaign_spend = grouped_campaign["Total Amount Spent (USD)"].sum() if grouped_campaign is not None else 0
        st.info(f"💰 Ad Spend Verification: Shopify Total = ${total_shopify_ad_spend:.2f}, Campaign Total = ${total_campaign_spend:.2f}")
        
        # Display without internal columns
        display_cols = [col for col in df_shopify.columns if col not in ["Product Name", "Canonical Product"]]
        st.write(df_shopify[display_cols])

# ---- CREATE DAY-WISE LOOKUPS FROM SHOPIFY DATA ----
# This is the key addition - creating lookups organized by product and date
product_date_avg_prices = {}
product_date_delivery_rates = {}
product_date_cost_inputs = {}

if df_shopify is not None and not df_shopify.empty and 'Date' in df_shopify.columns:
    st.subheader("🔍 Creating Day-wise Lookups from Shopify Data")
    
    # Get unique dates
    unique_dates = sorted([str(d) for d in df_shopify['Date'].unique() if pd.notna(d) and str(d).strip() != ''])
    
    # Initialize lookups for all products and dates
    for product in df_shopify['Canonical Product'].unique():
        product_date_avg_prices[product] = {}
        product_date_delivery_rates[product] = {}
        product_date_cost_inputs[product] = {}
        
        for date in unique_dates:
            product_date_avg_prices[product][date] = 0
            product_date_delivery_rates[product][date] = 0
            product_date_cost_inputs[product][date] = 0
    
    # Build lookups from Shopify data
    for product, product_df in df_shopify.groupby('Canonical Product'):
        for date in unique_dates:
            # Filter data for this product and date
            date_data = product_df[product_df['Date'].astype(str) == date]
            
            if not date_data.empty:
                # Calculate weighted averages for this product-date combination
                total_net_items = date_data['Net items sold'].sum()
                
                if total_net_items > 0:
                    # Weighted average price
                    total_revenue = (date_data['Product variant price'] * date_data['Net items sold']).sum()
                    avg_price = total_revenue / total_net_items
                    product_date_avg_prices[product][date] = avg_price
                    
                    # Weighted average delivery rate
                    delivery_rates = []
                    cost_inputs = []
                    
                    for _, row in date_data.iterrows():
                        net_items = row['Net items sold']
                        delivery_rate = row.get('Delivery Rate', 0)
                        cost_input = row.get('Product Cost (Input)', 0)
                        
                        # Convert delivery rate if it's a string percentage
                        if isinstance(delivery_rate, str):
                            delivery_rate = delivery_rate.strip().replace('%', '')
                        delivery_rate = pd.to_numeric(delivery_rate, errors='coerce') or 0
                        if delivery_rate > 1:  # assume it's given as percentage
                            delivery_rate = delivery_rate / 100.0
                        
                        cost_input = pd.to_numeric(cost_input, errors='coerce') or 0
                        
                        if net_items > 0:
                            delivery_rates.extend([delivery_rate] * int(net_items))
                            cost_inputs.extend([cost_input] * int(net_items))
                    
                    # Calculate weighted averages
                    if delivery_rates:
                        product_date_delivery_rates[product][date] = sum(delivery_rates) / len(delivery_rates)
                    
                    if cost_inputs:
                        product_date_cost_inputs[product][date] = sum(cost_inputs) / len(cost_inputs)
    
    # Display lookup summary
    st.success("✅ Day-wise lookups created successfully!")
    
    # Show sample of lookups
    sample_products = list(product_date_avg_prices.keys())[:3]  # Show first 3 products
    for product in sample_products:
        st.write(f"**{product}:**")
        for date in unique_dates[:3]:  # Show first 3 dates
            avg_price = product_date_avg_prices[product].get(date, 0)
            delivery_rate = product_date_delivery_rates[product].get(date, 0)
            cost_input = product_date_cost_inputs[product].get(date, 0)
            
            if avg_price > 0 or delivery_rate > 0 or cost_input > 0:
                st.write(f"  • {date}: Price=${avg_price:.2f}, Rate={delivery_rate:.2%}, Cost=${cost_input:.2f}")

# ---- BUILD SHOPIFY TOTALS LOOKUP (like in first code) ----
shopify_totals = {}

if df_shopify is not None and not df_shopify.empty:
    for product, product_df in df_shopify.groupby("Canonical Product"):
        delivered_orders = 0
        total_sold = 0

        for _, row in product_df.iterrows():
            rate = row.get("Delivery Rate", "")
            sold = pd.to_numeric(row.get("Net items sold", 0), errors="coerce") or 0

            # Clean rate (it might be "70%" or 0.7 or 70)
            if isinstance(rate, str):
                rate = rate.strip().replace("%", "")
            rate = pd.to_numeric(rate, errors="coerce")
            if pd.isna(rate):
                rate = 0
            if rate > 1:  # assume it's given as percentage
                rate = rate / 100.0

            delivered_orders += sold * rate
            total_sold += sold

        delivery_rate = delivered_orders / total_sold if total_sold > 0 else 0

        shopify_totals[product] = {
            "Delivered Orders": round(delivered_orders, 1),
            "Delivery Rate": delivery_rate
        }

# ---- BUILD WEIGHTED AVERAGE LOOKUPS (like in first code) ----
avg_price_lookup = {}
if df_shopify is not None and not df_shopify.empty:
    for product, product_df in df_shopify.groupby("Canonical Product"):
        total_sold = product_df["Net items sold"].sum()
        if total_sold > 0:
            weighted_avg_price = (
                (product_df["Product variant price"] * product_df["Net items sold"]).sum()
                / total_sold
            )
            avg_price_lookup[product] = weighted_avg_price

avg_product_cost_lookup = {}
if df_shopify is not None and not df_shopify.empty:
    for product, product_df in df_shopify.groupby("Canonical Product"):
        total_sold = product_df["Net items sold"].sum()
        valid_df = product_df[pd.to_numeric(product_df["Product Cost (Input)"], errors="coerce").notna()]
        if total_sold > 0 and not valid_df.empty:
            weighted_avg_cost = (
                (pd.to_numeric(valid_df["Product Cost (Input)"], errors="coerce") * valid_df["Net items sold"]).sum()
                / valid_df["Net items sold"].sum()
            )
            avg_product_cost_lookup[product] = weighted_avg_cost


unique_campaign_dates = []
if campaign_files and df_campaign is not None and 'Date' in df_campaign.columns:
    unique_campaign_dates = sorted([str(d) for d in df_campaign['Date'].unique() if pd.notna(d) and str(d).strip() != ''])

# Calculate default value based on number of unique dates
if len(unique_campaign_dates) > 0:
    n_days = len(unique_campaign_dates)
    if n_days % 2 == 0:
        default_days = n_days // 2  # Even: n/2
    else:
        default_days = (n_days + 1) // 2  # Odd: (n+1)/2
    
    st.info(f"📅 Found {n_days} unique dates in campaign data")
    
    # Input slider for selecting number of days
    selected_days = st.slider(
        "Select number of days to check for negative scores (random, not consecutive)",
        min_value=1,
        max_value=n_days,
        value=default_days,
        help=f"Default is {default_days} days (n/2 for even or (n+1)/2 for odd number of total days). "
             f"The analysis will check if campaigns have negative scores in this many days randomly distributed across all dates."
    )
    
    st.write(f"**Analysis will check:** {selected_days} out of {n_days} total days for negative scores (random distribution)")
else:
    selected_days = 1  # Default fallback
    st.warning("⚠️ No campaign dates found. Using default value of 1 day.")



def convert_shopify_to_excel(df):
    """Original Shopify Excel conversion function (fallback)"""
    output = BytesIO()
    with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
        workbook = writer.book
        worksheet = workbook.add_worksheet("Shopify Data")
        writer.sheets["Shopify Data"] = worksheet

        # Formats
        header_format = workbook.add_format({
            "bold": True, "align": "center", "valign": "vcenter",
            "fg_color": "#DDD9C4", "font_name": "Calibri", "font_size": 11
        })
        grand_total_format = workbook.add_format({
            "bold": True, "align": "left", "valign": "vcenter",
            "fg_color": "#FFC000", "font_name": "Calibri", "font_size": 11
        })
        product_total_format = workbook.add_format({
            "bold": True, "align": "left", "valign": "vcenter",
            "fg_color": "#FFD966", "font_name": "Calibri", "font_size": 11
        })
        variant_format = workbook.add_format({
            "align": "left", "valign": "vcenter",
            "fg_color": "#D9E1F2", "font_name": "Calibri", "font_size": 11
        })

        # Header
        for col_num, value in enumerate(df.columns.values):
            worksheet.write(0, col_num, value, header_format)

        # Column indexes
        delivered_col = df.columns.get_loc("Delivered Orders")
        sold_col = df.columns.get_loc("Net items sold")
        rate_col = df.columns.get_loc("Delivery Rate")
        revenue_col = df.columns.get_loc("Net Revenue")
        price_col = df.columns.get_loc("Product variant price")
        shipping_col = df.columns.get_loc("Shipping Cost")
        operation_col = df.columns.get_loc("Operational Cost")
        product_cost_col = df.columns.get_loc("Product Cost (Output)")
        product_cost_input_col = df.columns.get_loc("Product Cost (Input)")
        net_profit_col = df.columns.get_loc("Net Profit")
        ad_spend_col = df.columns.get_loc("Ad Spend (USD)")
        net_profit_percent_col = df.columns.get_loc("Net Profit (%)")
        product_title_col = df.columns.get_loc("Product title")
        variant_title_col = df.columns.get_loc("Product variant title")

        cols_to_sum = [
            "Net items sold", "Delivered Orders", "Net Revenue", "Ad Spend (USD)",
            "Shipping Cost", "Operational Cost", "Product Cost (Output)", "Net Profit"
        ]
        cols_to_sum_idx = [df.columns.get_loc(c) for c in cols_to_sum]

        # Grand total row
        grand_total_row_idx = 1
        worksheet.write(grand_total_row_idx, 0, "GRAND TOTAL", grand_total_format)
        worksheet.write(grand_total_row_idx, 1, "ALL PRODUCTS", grand_total_format)

        row = grand_total_row_idx + 1
        product_total_rows = []

        # Products
        for product, product_df in df.groupby("Product title"):
            product_total_row_idx = row
            product_total_rows.append(product_total_row_idx)

            worksheet.write(product_total_row_idx, 0, product, product_total_format)
            worksheet.write(product_total_row_idx, 1, "ALL VARIANTS (TOTAL)", product_total_format)

            n_variants = len(product_df)
            first_variant_row_idx = product_total_row_idx + 1
            last_variant_row_idx = product_total_row_idx + n_variants

            # Product SUMs
            for col_idx in cols_to_sum_idx:
                col_letter = xl_col_to_name(col_idx)
                excel_first = first_variant_row_idx + 1
                excel_last = last_variant_row_idx + 1
                worksheet.write_formula(
                    product_total_row_idx, col_idx,
                    f"=SUM({col_letter}{excel_first}:{col_letter}{excel_last})",
                    product_total_format
                )

            # Product weighted avg Delivery Rate
            sold_col_letter = xl_col_to_name(sold_col)
            rate_col_letter = xl_col_to_name(rate_col)
            excel_first = first_variant_row_idx + 1
            excel_last = last_variant_row_idx + 1
            worksheet.write_formula(
                product_total_row_idx, rate_col,
                f"=IF(SUM({sold_col_letter}{excel_first}:{sold_col_letter}{excel_last})=0,0,"
                f"SUMPRODUCT({rate_col_letter}{excel_first}:{rate_col_letter}{excel_last},"
                f"{sold_col_letter}{excel_first}:{sold_col_letter}{excel_last})/"
                f"SUM({sold_col_letter}{excel_first}:{sold_col_letter}{excel_last}))",
                product_total_format
            )

            # Product weighted avg Product variant price
            price_col_letter = xl_col_to_name(price_col)
            worksheet.write_formula(
                product_total_row_idx, price_col,
                f"=IF(SUM({sold_col_letter}{excel_first}:{sold_col_letter}{excel_last})=0,0,"
                f"SUMPRODUCT({price_col_letter}{excel_first}:{price_col_letter}{excel_last},"
                f"{sold_col_letter}{excel_first}:{sold_col_letter}{excel_last})/"
                f"SUM({sold_col_letter}{excel_first}:{sold_col_letter}{excel_last}))",
                product_total_format
            )

            # Product weighted avg Product Cost (Input)
            pc_input_col_letter = xl_col_to_name(product_cost_input_col)
            worksheet.write_formula(
                product_total_row_idx, product_cost_input_col,
                f"=IF(SUM({sold_col_letter}{excel_first}:{sold_col_letter}{excel_last})=0,0,"
                f"SUMPRODUCT({pc_input_col_letter}{excel_first}:{pc_input_col_letter}{excel_last},"
                f"{sold_col_letter}{excel_first}:{sold_col_letter}{excel_last})/"
                f"SUM({sold_col_letter}{excel_first}:{sold_col_letter}{excel_last}))",
                product_total_format
            )

            # Product Net Profit %
            rev_col_letter = xl_col_to_name(revenue_col)
            np_col_letter = xl_col_to_name(net_profit_col)
            excel_row = product_total_row_idx + 1
            worksheet.write_formula(
                product_total_row_idx, net_profit_percent_col,
                f"=IF(N({rev_col_letter}{excel_row})=0,0,"
                f"N({np_col_letter}{excel_row})/N({rev_col_letter}{excel_row})*100)",
                product_total_format
            )

            # Variants
            row += 1
            for _, variant in product_df.iterrows():
                variant_row_idx = row
                excel_row = variant_row_idx + 1

                sold_ref = f"{xl_col_to_name(sold_col)}{excel_row}"
                rate_ref = f"{xl_col_to_name(rate_col)}{excel_row}"
                delivered_ref = f"{xl_col_to_name(delivered_col)}{excel_row}"
                price_ref = f"{xl_col_to_name(price_col)}{excel_row}"
                pc_input_ref = f"{xl_col_to_name(product_cost_input_col)}{excel_row}"
                ad_spend_ref = f"{xl_col_to_name(ad_spend_col)}{excel_row}"
                shipping_ref = f"{xl_col_to_name(shipping_col)}{excel_row}"
                op_ref = f"{xl_col_to_name(operation_col)}{excel_row}"
                pc_output_ref = f"{xl_col_to_name(product_cost_col)}{excel_row}"
                net_profit_ref = f"{xl_col_to_name(net_profit_col)}{excel_row}"
                revenue_ref = f"{xl_col_to_name(revenue_col)}{excel_row}"

                for col_idx, col_name in enumerate(df.columns):
                    if col_idx == product_title_col:
                        worksheet.write(variant_row_idx, col_idx, "", variant_format)
                    elif col_idx == variant_title_col:
                        worksheet.write(variant_row_idx, col_idx, variant.get("Product variant title", ""), variant_format)
                    elif col_name == "Net items sold":
                        worksheet.write(variant_row_idx, col_idx, variant.get("Net items sold", 0), variant_format)
                    elif col_name == "Product variant price":
                        worksheet.write(variant_row_idx, col_idx, variant.get("Product variant price", 0), variant_format)
                    elif col_name == "Ad Spend (USD)":
                        worksheet.write(variant_row_idx, col_idx, variant.get("Ad Spend (USD)", 0.0), variant_format)
                    elif col_name == "Delivery Rate":
                        worksheet.write(variant_row_idx, col_idx, variant.get("Delivery Rate", ""), variant_format)
                    elif col_name == "Product Cost (Input)":
                        worksheet.write(variant_row_idx, col_idx, variant.get("Product Cost (Input)", ""), variant_format)
                    elif col_name == "Date":
                        worksheet.write(variant_row_idx, col_idx, variant.get("Date", ""), variant_format)
                    elif col_name == "Delivered Orders":
                        rate_term = f"IF(ISNUMBER({rate_ref}),IF({rate_ref}>1,{rate_ref}/100,{rate_ref}),0)"
                        worksheet.write_formula(
                            variant_row_idx, col_idx,
                            f"=ROUND(N({sold_ref})*{rate_term},1)",
                            variant_format
                        )
                    elif col_name == "Net Revenue":
                        worksheet.write_formula(
                            variant_row_idx, col_idx,
                            f"=N({price_ref})*N({delivered_ref})",
                            variant_format
                        )
                    elif col_name == "Shipping Cost":
                        worksheet.write_formula(
                            variant_row_idx, col_idx,
                            f"={shipping_rate}*N({sold_ref})",
                            variant_format
                        )
                    elif col_name == "Operational Cost":
                        worksheet.write_formula(
                            variant_row_idx, col_idx,
                            f"={operational_rate}*N({sold_ref})",
                            variant_format
                        )
                    elif col_name == "Product Cost (Output)":
                        pc_term = f"IF(ISNUMBER({pc_input_ref}),{pc_input_ref},0)"
                        worksheet.write_formula(
                            variant_row_idx, col_idx,
                            f"={pc_term}*N({delivered_ref})",
                            variant_format
                        )
                    elif col_name == "Net Profit":
                        worksheet.write_formula(
                            variant_row_idx, col_idx,
                            f"=N({revenue_ref})-N({ad_spend_ref})*100-N({shipping_ref})-N({pc_output_ref})-N({op_ref})",
                            variant_format
                        )
                    elif col_name == "Net Profit (%)":
                        worksheet.write_formula(
                            variant_row_idx, col_idx,
                            f"=IF(N({revenue_ref})=0,0,N({net_profit_ref})/N({revenue_ref})*100)",
                            variant_format
                        )
                    else:
                        worksheet.write(variant_row_idx, col_idx, variant.get(col_name, ""), variant_format)
                row += 1

        # Grand total = sum of product totals
        if product_total_rows:
            for col_idx in cols_to_sum_idx:
                col_letter = xl_col_to_name(col_idx)
                total_refs = [f"{col_letter}{r+1}" for r in product_total_rows]
                worksheet.write_formula(
                    grand_total_row_idx, col_idx,
                    f"=SUM({','.join(total_refs)})",
                    grand_total_format
                )

            # Grand total weighted averages
            sold_col_letter = xl_col_to_name(sold_col)
            rate_col_letter = xl_col_to_name(rate_col)
            product_refs_sold = [f"{sold_col_letter}{r+1}" for r in product_total_rows]
            product_refs_rate = [f"{rate_col_letter}{r+1}" for r in product_total_rows]
            
            # Grand total weighted avg Delivery Rate
            worksheet.write_formula(
                grand_total_row_idx, rate_col,
                f"=IF(SUM({','.join(product_refs_sold)})=0,0,"
                f"SUMPRODUCT({','.join(product_refs_rate)},{','.join(product_refs_sold)})/"
                f"SUM({','.join(product_refs_sold)}))",
                grand_total_format
            )

            # Grand total weighted avg Product variant price
            price_col_letter = xl_col_to_name(price_col)
            product_refs_price = [f"{price_col_letter}{r+1}" for r in product_total_rows]
            worksheet.write_formula(
                grand_total_row_idx, price_col,
                f"=IF(SUM({','.join(product_refs_sold)})=0,0,"
                f"SUMPRODUCT({','.join(product_refs_price)},{','.join(product_refs_sold)})/"
                f"SUM({','.join(product_refs_sold)}))",
                grand_total_format
            )

            # Grand total weighted avg Product Cost (Input)
            pc_input_col_letter = xl_col_to_name(product_cost_input_col)
            product_refs_pc_input = [f"{pc_input_col_letter}{r+1}" for r in product_total_rows]
            worksheet.write_formula(
                grand_total_row_idx, product_cost_input_col,
                f"=IF(SUM({','.join(product_refs_sold)})=0,0,"
                f"SUMPRODUCT({','.join(product_refs_pc_input)},{','.join(product_refs_sold)})/"
                f"SUM({','.join(product_refs_sold)}))",
                grand_total_format
            )

            rev_col_letter = xl_col_to_name(revenue_col)
            np_col_letter = xl_col_to_name(net_profit_col)
            excel_row = grand_total_row_idx + 1
            worksheet.write_formula(
                grand_total_row_idx, net_profit_percent_col,
                f"=IF(N({rev_col_letter}{excel_row})=0,0,N({np_col_letter}{excel_row})/N({rev_col_letter}{excel_row})*100)",
                grand_total_format
            )

        worksheet.freeze_panes(2, 0)
        for i, col in enumerate(df.columns):
            if col in ("Product title", "Product variant title"):
                worksheet.set_column(i, i, 35)
            elif col in ("Product variant price", "Net Revenue", "Ad Spend (USD)", "Shipping Cost", "Operational Cost", "Net Profit"):
                worksheet.set_column(i, i, 15)
            else:
                worksheet.set_column(i, i, 12)

    return output.getvalue()


def convert_shopify_to_excel_with_date_columns_fixed(df):
    """Convert Shopify data to Excel with collapsible column groups every 12 columns after base columns"""
    if df is None or df.empty:
        return None
        
    output = BytesIO()
    with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
        workbook = writer.book
        worksheet = workbook.add_worksheet("Shopify Data")
        writer.sheets["Shopify Data"] = worksheet

        # Check if we have dates
        has_dates = 'Date' in df.columns
        if not has_dates:
            # Fall back to original structure if no dates
            return convert_shopify_to_excel(df)
        
        # Get unique dates and sort them
        unique_dates = sorted([str(d) for d in df['Date'].unique() if pd.notna(d) and str(d).strip() != ''])
        num_days = len(unique_dates)
        
        # Calculate dynamic threshold
        dynamic_threshold = num_days * 5

        # Formats with conditional formatting based on net items sold
        header_format = workbook.add_format({
            "bold": True, "align": "center", "valign": "vcenter",
            "fg_color": "#DDD9C4", "font_name": "Calibri", "font_size": 11,
            "num_format": "#,##0.00"
        })
        date_header_format = workbook.add_format({
            "bold": True, "align": "center", "valign": "vcenter",
            "fg_color": "#B4C6E7", "font_name": "Calibri", "font_size": 11,
            "num_format": "#,##0.00"
        })
        total_header_format = workbook.add_format({
            "bold": True, "align": "center", "valign": "vcenter",
            "fg_color": "#FFD966", "font_name": "Calibri", "font_size": 11,
            "num_format": "#,##0.00"
        })
        grand_total_format = workbook.add_format({
            "bold": True, "align": "left", "valign": "vcenter",
            "fg_color": "#FFC000", "font_name": "Calibri", "font_size": 11,
            "num_format": "#,##0.00"
        })
        
        # Dynamic conditional formats based on calculated threshold (simplified to 2 categories)
        # Format for products with < dynamic_threshold net items sold (Red theme)
        product_total_format_low = workbook.add_format({
            "bold": True, "align": "left", "valign": "vcenter",
            "fg_color": "#DC4E23", "font_name": "Calibri", "font_size": 11,  # Red
            "num_format": "#,##0.00"
        })
        variant_format_low = workbook.add_format({
            "align": "left", "valign": "vcenter",
            "fg_color": "#FFCCCB", "font_name": "Calibri", "font_size": 11,  # Light red
            "num_format": "#,##0.00"
        })
        
        # Format for products with >= dynamic_threshold net items sold (Default theme)
        product_total_format_high = workbook.add_format({
            "bold": True, "align": "left", "valign": "vcenter",
            "fg_color": "#FFD966", "font_name": "Calibri", "font_size": 11,
            "num_format": "#,##0.00"
        })
        variant_format_high = workbook.add_format({
            "align": "left", "valign": "vcenter",
            "fg_color": "#D9E1F2", "font_name": "Calibri", "font_size": 11,
            "num_format": "#,##0.00"
        })
        
        # Define base columns - CHANGED: Cost Per Item to CPI, added BE as 8th column
        base_columns = ["Product title", "Product variant title", "Delivery Rate", "Product Cost (Input)", "Net items sold", "Total Ad Spent", "CPI", "BE"]
        
        # Define metrics that will be repeated for each date (12 metrics = 12 columns per date)
        date_metrics = ["Net items sold", "Avg Price", "Delivery Rate", "Product Cost (Input)", 
                       "Delivered Orders", "Net Revenue", "Ad Spend (USD)", 
                       "Shipping Cost", "Operational Cost", "Product Cost (Output)", 
                       "Net Profit", "Net Profit (%)"]
        
        # Build column structure WITH SEPARATOR COLUMNS
        all_columns = base_columns.copy()
        
        # Add separator column after base columns
        all_columns.append("SEPARATOR_AFTER_BASE")
        
        # Add date-specific columns with separators
        for date in unique_dates:
            for metric in date_metrics:
                all_columns.append(f"{date}_{metric}")
            # Add separator column after each date's columns
            all_columns.append(f"SEPARATOR_AFTER_{date}")
        
        # Add total columns
        for metric in date_metrics:
            all_columns.append(f"Total_{metric}")

        # Write headers (skip separator columns)
        for col_num, col_name in enumerate(all_columns):
            if col_name.startswith("SEPARATOR_"):
                # Leave separator columns empty - don't write any header
                continue
            elif col_name.startswith("Total_"):
                safe_write(worksheet, 0, col_num, col_name.replace("_", " "), total_header_format)
            elif "_" in col_name and col_name.split("_")[0] in unique_dates:
                safe_write(worksheet, 0, col_num, col_name.replace("_", " "), date_header_format)
            else:
                safe_write(worksheet, 0, col_num, col_name, header_format)

        # SET UP COLUMN GROUPING - ACCOUNT FOR SEPARATOR COLUMNS
        # Base columns are 0, 1, 2, 3, 4, 5, 6, 7 (A, B, C, D, E, F, G, H) - NO GROUPING
        # Separator column after base is column 8 - NO GROUPING
        
        # Start grouping from column 9 (column J) onwards - after base + separator
        start_col = 9  # Column J (after base columns A-H + separator I)
        total_columns = len(all_columns)
        
        # Group every 12 columns + 1 separator = 13 positions starting from column 9
        group_level = 1
        while start_col < total_columns:
            # Skip if this is a separator column
            if start_col < len(all_columns) and all_columns[start_col].startswith("SEPARATOR_"):
                start_col += 1
                continue
                
            # Find end of this group (12 data columns)
            data_cols_found = 0
            end_col = start_col
            while end_col < total_columns and data_cols_found < 12:
                if not all_columns[end_col].startswith("SEPARATOR_"):
                    data_cols_found += 1
                if data_cols_found < 12:
                    end_col += 1
            
            # Set column grouping only for data columns (skip separators)
            if end_col < total_columns:
                worksheet.set_column(
                    start_col, 
                    end_col - 1, 
                    12, 
                    None, 
                    {'level': group_level, 'collapsed': True, 'hidden':True}  # Start collapsed
                )
            
            # Move to next group - skip the separator column
            start_col = end_col + 1  # +1 to skip separator after this group
        
        # Set base column widths (always visible, NO GROUPING)
        worksheet.set_column(0, 1, 25)  # Product title and variant title
        worksheet.set_column(2, 4, 15)  # Base delivery rate, product cost, net items sold
        worksheet.set_column(5, 5, 18)  # Total Ad Spent
        worksheet.set_column(6, 6, 15)  # CPI
        worksheet.set_column(7, 7, 15)  # BE
        worksheet.set_column(8, 8, 3)   # Separator column after base - narrow width

        # Configure outline settings for better user experience
        worksheet.outline_settings(
            symbols_below=True,    # Show outline symbols below groups
            symbols_right=True,    # Show outline symbols to the right
            auto_style=False       # Don't use automatic styling
        )

        # Grand total row
        grand_total_row_idx = 1
        safe_write(worksheet, grand_total_row_idx, 0, "GRAND TOTAL", grand_total_format)
        safe_write(worksheet, grand_total_row_idx, 1, "ALL PRODUCTS", grand_total_format)
        
        row = grand_total_row_idx + 1
        product_total_rows = []

        # Group by product and restructure data
        for product, product_df in df.groupby("Product title"):
            product_total_row_idx = row
            product_total_rows.append(product_total_row_idx)

            # Calculate total net items sold for this product to determine formatting
            total_net_items_for_product = 0
            for _, variant_group in product_df.groupby("Product variant title"):
                for _, row_data in variant_group.iterrows():
                    net_items = row_data.get("Net items sold", 0) or 0
                    total_net_items_for_product += net_items
            
            # Choose formatting based on dynamic threshold (simplified to 2 categories)
            if total_net_items_for_product < dynamic_threshold:
                product_total_format = product_total_format_low
                variant_format = variant_format_low
            else:
                product_total_format = product_total_format_high
                variant_format = variant_format_high

            # Product total row
            safe_write(worksheet, product_total_row_idx, 0, product, product_total_format)
            safe_write(worksheet, product_total_row_idx, 1, "ALL VARIANTS (TOTAL)", product_total_format)

            # Group variants within product
            variant_rows = []
            row += 1
            
            for (variant_title), variant_group in product_df.groupby("Product variant title"):
                variant_row_idx = row
                variant_rows.append(variant_row_idx)
                
                # Fill base columns for variant
                safe_write(worksheet, variant_row_idx, 0, "", variant_format)  # Empty product title for variant rows
                safe_write(worksheet, variant_row_idx, 1, variant_title, variant_format)
                
                # Calculate simple averages for base delivery rate and product cost
                delivery_rates = []
                product_costs = []
                
                for _, row_data in variant_group.iterrows():
                    delivery_rate = row_data.get("Delivery Rate", 0) or 0
                    product_cost = row_data.get("Product Cost (Input)", 0) or 0
                    
                    if delivery_rate > 0:
                        delivery_rates.append(delivery_rate)
                    if product_cost > 0:
                        product_costs.append(product_cost)
                
                # Use simple averages for base columns
                avg_delivery_rate = sum(delivery_rates) / len(delivery_rates) if delivery_rates else 0
                avg_product_cost = sum(product_costs) / len(product_costs) if product_costs else 0
                
                safe_write(worksheet, variant_row_idx, 2, round(avg_delivery_rate, 2), variant_format)
                safe_write(worksheet, variant_row_idx, 3, round(avg_product_cost, 2), variant_format)
                
                # Leave Net items sold, Total Ad Spent, CPI, and BE columns empty for variants (will be calculated via formulas)
                safe_write(worksheet, variant_row_idx, 4, "", variant_format)
                safe_write(worksheet, variant_row_idx, 5, "", variant_format)
                safe_write(worksheet, variant_row_idx, 6, "", variant_format)
                safe_write(worksheet, variant_row_idx, 7, "", variant_format)  # BE will reference product total
                
                # Cell references for Excel formulas
                excel_row = variant_row_idx + 1
                base_delivery_rate_ref = f"{xl_col_to_name(2)}{excel_row}"
                base_product_cost_ref = f"{xl_col_to_name(3)}{excel_row}"
                
                # Fill date-specific data and formulas
                for date in unique_dates:
                    date_data = variant_group[variant_group['Date'].astype(str) == date]
                    
                    # Get column indices for this date
                    net_items_col_idx = all_columns.index(f"{date}_Net items sold")
                    avg_price_col_idx = all_columns.index(f"{date}_Avg Price")
                    delivery_rate_col_idx = all_columns.index(f"{date}_Delivery Rate")
                    product_cost_input_col_idx = all_columns.index(f"{date}_Product Cost (Input)")
                    delivered_orders_col_idx = all_columns.index(f"{date}_Delivered Orders")
                    net_revenue_col_idx = all_columns.index(f"{date}_Net Revenue")
                    ad_spend_col_idx = all_columns.index(f"{date}_Ad Spend (USD)")
                    shipping_cost_col_idx = all_columns.index(f"{date}_Shipping Cost")
                    operational_cost_col_idx = all_columns.index(f"{date}_Operational Cost")
                    product_cost_output_col_idx = all_columns.index(f"{date}_Product Cost (Output)")
                    net_profit_col_idx = all_columns.index(f"{date}_Net Profit")
                    net_profit_percent_col_idx = all_columns.index(f"{date}_Net Profit (%)")
                    
                    # Cell references for this date
                    net_items_ref = f"{xl_col_to_name(net_items_col_idx)}{excel_row}"
                    avg_price_ref = f"{xl_col_to_name(avg_price_col_idx)}{excel_row}"
                    delivery_rate_ref = f"{xl_col_to_name(delivery_rate_col_idx)}{excel_row}"
                    product_cost_input_ref = f"{xl_col_to_name(product_cost_input_col_idx)}{excel_row}"
                    delivered_orders_ref = f"{xl_col_to_name(delivered_orders_col_idx)}{excel_row}"
                    net_revenue_ref = f"{xl_col_to_name(net_revenue_col_idx)}{excel_row}"
                    ad_spend_ref = f"{xl_col_to_name(ad_spend_col_idx)}{excel_row}"
                    shipping_cost_ref = f"{xl_col_to_name(shipping_cost_col_idx)}{excel_row}"
                    operational_cost_ref = f"{xl_col_to_name(operational_cost_col_idx)}{excel_row}"
                    product_cost_output_ref = f"{xl_col_to_name(product_cost_output_col_idx)}{excel_row}"
                    net_profit_ref = f"{xl_col_to_name(net_profit_col_idx)}{excel_row}"
                    
                    if not date_data.empty:
                        row_data = date_data.iloc[0]
                        
                        # Actual data for this date
                        net_items = row_data.get("Net items sold", 0) or 0
                        avg_price = row_data.get("Product variant price", 0) or 0
                        delivery_rate = row_data.get("Delivery Rate", 0) or 0
                        product_cost_input = row_data.get("Product Cost (Input)", 0) or 0
                        ad_spend_usd = row_data.get("Ad Spend (USD)", 0) or 0
                        
                        safe_write(worksheet, variant_row_idx, net_items_col_idx, int(net_items), variant_format)
                        safe_write(worksheet, variant_row_idx, avg_price_col_idx, round(avg_price, 2), variant_format)
                        safe_write(worksheet, variant_row_idx, ad_spend_col_idx, round(ad_spend_usd, 2), variant_format)
                        
                        # Date-specific Delivery Rate and Product Cost link to base columns
                        if delivery_rate > 0:
                            safe_write(worksheet, variant_row_idx, delivery_rate_col_idx, round(delivery_rate, 2), variant_format)
                        else:
                            worksheet.write_formula(
                                variant_row_idx, delivery_rate_col_idx,
                                f"=ROUND({base_delivery_rate_ref},2)",
                                variant_format
                            )
                        
                        if product_cost_input > 0:
                            safe_write(worksheet, variant_row_idx, product_cost_input_col_idx, round(product_cost_input, 2), variant_format)
                        else:
                            worksheet.write_formula(
                                variant_row_idx, product_cost_input_col_idx,
                                f"=ROUND({base_product_cost_ref},2)",
                                variant_format
                            )
                        
                    else:
                        # No data for this date - link to base columns and fill other fields with zeros
                        safe_write(worksheet, variant_row_idx, net_items_col_idx, 0, variant_format)
                        safe_write(worksheet, variant_row_idx, avg_price_col_idx, 0.00, variant_format)
                        safe_write(worksheet, variant_row_idx, ad_spend_col_idx, 0.00, variant_format)
                        
                        worksheet.write_formula(
                            variant_row_idx, delivery_rate_col_idx,
                            f"=ROUND({base_delivery_rate_ref},2)",
                            variant_format
                        )
                        worksheet.write_formula(
                            variant_row_idx, product_cost_input_col_idx,
                            f"=ROUND({base_product_cost_ref},2)",
                            variant_format
                        )
                    
                    # FORMULAS for calculated fields (with ROUND for 2 decimal places)
                    
                    # Delivered Orders = Net items sold * Delivery Rate
                    rate_term = f"IF(ISNUMBER({delivery_rate_ref}),IF({delivery_rate_ref}>1,{delivery_rate_ref}/100,{delivery_rate_ref}),0)"
                    worksheet.write_formula(
                        variant_row_idx, delivered_orders_col_idx,
                        f"=ROUND({net_items_ref}*{rate_term},2)",
                        variant_format
                    )
                    
                    # Net Revenue = Delivered Orders * Average Price
                    worksheet.write_formula(
                        variant_row_idx, net_revenue_col_idx,
                        f"=ROUND({delivered_orders_ref}*{avg_price_ref},2)",
                        variant_format
                    )
                    
                    # Shipping Cost = Net items sold * shipping_rate
                    worksheet.write_formula(
                        variant_row_idx, shipping_cost_col_idx,
                        f"=ROUND({shipping_rate}*{net_items_ref},2)",
                        variant_format
                    )
                    
                    # Operational Cost = Net items sold * operational_rate
                    worksheet.write_formula(
                        variant_row_idx, operational_cost_col_idx,
                        f"=ROUND({operational_rate}*{net_items_ref},2)",
                        variant_format
                    )
                    
                    # Product Cost (Output) = Delivered Orders * Product Cost (Input)
                    pc_term = f"IF(ISNUMBER({product_cost_input_ref}),{product_cost_input_ref},0)"
                    worksheet.write_formula(
                        variant_row_idx, product_cost_output_col_idx,
                        f"=ROUND({pc_term}*{delivered_orders_ref},2)",
                        variant_format
                    )
                    
                    # Net Profit = Net Revenue - Ad Spend (USD)*100 - Shipping Cost - Operational Cost - Product Cost (Output)
                    worksheet.write_formula(
                        variant_row_idx, net_profit_col_idx,
                        f"=ROUND({net_revenue_ref}-{ad_spend_ref}*100-{shipping_cost_ref}-{operational_cost_ref}-{product_cost_output_ref},2)",
                        variant_format
                    )
                    
                    # Net Profit (%) = Net Profit / Net Revenue * 100
                    worksheet.write_formula(
                        variant_row_idx, net_profit_percent_col_idx,
                        f"=ROUND(IF({net_revenue_ref}=0,0,{net_profit_ref}/{net_revenue_ref}*100),2)",
                        variant_format
                    )
                
                # TOTAL COLUMNS CALCULATIONS FOR VARIANT (with ROUND for 2 decimal places)
                for metric in date_metrics:
                    total_col_idx = all_columns.index(f"Total_{metric}")
                    
                    if metric == "Net items sold":
                        # SUM: Add all date-specific net items sold (non-contiguous columns)
                        if len(unique_dates) > 1:
                            # Build individual cell references since columns are not contiguous
                            date_refs = []
                            for date in unique_dates:
                                date_col_idx = all_columns.index(f"{date}_{metric}")
                                date_refs.append(f"{xl_col_to_name(date_col_idx)}{excel_row}")
                            
                            sum_formula = "+".join(date_refs)
                            worksheet.write_formula(
                                variant_row_idx, total_col_idx,
                                f"={sum_formula}",
                                variant_format
                            )
                        else:
                            single_date_col = all_columns.index(f"{unique_dates[0]}_{metric}")
                            worksheet.write_formula(
                                variant_row_idx, total_col_idx,
                                f"={xl_col_to_name(single_date_col)}{excel_row}",
                                variant_format
                            )
                    
                    elif metric == "Avg Price":
                        # WEIGHTED AVERAGE: (Price1*NetItems1 + Price2*NetItems2 + ...) / TotalNetItems
                        total_net_items_col_idx = all_columns.index("Total_Net items sold")
                        total_net_items_ref = f"{xl_col_to_name(total_net_items_col_idx)}{excel_row}"
                        
                        if len(unique_dates) > 1:
                            # Build SUMPRODUCT formula for weighted average
                            price_terms = []
                            for date in unique_dates:
                                price_col_idx = all_columns.index(f"{date}_Avg Price")
                                net_items_col_idx = all_columns.index(f"{date}_Net items sold")
                                price_terms.append(f"{xl_col_to_name(price_col_idx)}{excel_row}*{xl_col_to_name(net_items_col_idx)}{excel_row}")
                            
                            sumproduct_formula = "+".join(price_terms)
                            worksheet.write_formula(
                                variant_row_idx, total_col_idx,
                                f"=ROUND(IF({total_net_items_ref}=0,0,({sumproduct_formula})/{total_net_items_ref}),2)",
                                variant_format
                            )
                        else:
                            single_date_col = all_columns.index(f"{unique_dates[0]}_{metric}")
                            worksheet.write_formula(
                                variant_row_idx, total_col_idx,
                                f"=ROUND({xl_col_to_name(single_date_col)}{excel_row},2)",
                                variant_format
                            )
                    
                    elif metric == "Delivery Rate":
                        # WEIGHTED AVERAGE: Same as Avg Price
                        total_net_items_col_idx = all_columns.index("Total_Net items sold")
                        total_net_items_ref = f"{xl_col_to_name(total_net_items_col_idx)}{excel_row}"
                        
                        if len(unique_dates) > 1:
                            rate_terms = []
                            for date in unique_dates:
                                rate_col_idx = all_columns.index(f"{date}_Delivery Rate")
                                net_items_col_idx = all_columns.index(f"{date}_Net items sold")
                                rate_terms.append(f"{xl_col_to_name(rate_col_idx)}{excel_row}*{xl_col_to_name(net_items_col_idx)}{excel_row}")
                            
                            sumproduct_formula = "+".join(rate_terms)
                            worksheet.write_formula(
                                variant_row_idx, total_col_idx,
                                f"=ROUND(IF({total_net_items_ref}=0,0,({sumproduct_formula})/{total_net_items_ref}),2)",
                                variant_format
                            )
                        else:
                            single_date_col = all_columns.index(f"{unique_dates[0]}_{metric}")
                            worksheet.write_formula(
                                variant_row_idx, total_col_idx,
                                f"=ROUND({xl_col_to_name(single_date_col)}{excel_row},2)",
                                variant_format
                            )
                    
                    elif metric == "Product Cost (Input)":
                        # WEIGHTED AVERAGE: Same as Avg Price
                        total_net_items_col_idx = all_columns.index("Total_Net items sold")
                        total_net_items_ref = f"{xl_col_to_name(total_net_items_col_idx)}{excel_row}"
                        
                        if len(unique_dates) > 1:
                            cost_terms = []
                            for date in unique_dates:
                                cost_col_idx = all_columns.index(f"{date}_Product Cost (Input)")
                                net_items_col_idx = all_columns.index(f"{date}_Net items sold")
                                cost_terms.append(f"{xl_col_to_name(cost_col_idx)}{excel_row}*{xl_col_to_name(net_items_col_idx)}{excel_row}")
                            
                            sumproduct_formula = "+".join(cost_terms)
                            worksheet.write_formula(
                                variant_row_idx, total_col_idx,
                                f"=ROUND(IF({total_net_items_ref}=0,0,({sumproduct_formula})/{total_net_items_ref}),2)",
                                variant_format
                            )
                        else:
                            single_date_col = all_columns.index(f"{unique_dates[0]}_{metric}")
                            worksheet.write_formula(
                                variant_row_idx, total_col_idx,
                                f"=ROUND({xl_col_to_name(single_date_col)}{excel_row},2)",
                                variant_format
                            )
                    
                    elif metric == "Net Profit (%)":
                        # CALCULATED: Total Net Profit / Total Net Revenue * 100
                        total_net_profit_col_idx = all_columns.index("Total_Net Profit")
                        total_net_revenue_col_idx = all_columns.index("Total_Net Revenue")
                        total_net_profit_ref = f"{xl_col_to_name(total_net_profit_col_idx)}{excel_row}"
                        total_net_revenue_ref = f"{xl_col_to_name(total_net_revenue_col_idx)}{excel_row}"
                        
                        worksheet.write_formula(
                            variant_row_idx, total_col_idx,
                            f"=ROUND(IF({total_net_revenue_ref}=0,0,{total_net_profit_ref}/{total_net_revenue_ref}*100),2)",
                            variant_format
                        )
                    
                    else:
                        # SUM: All other metrics (Delivered Orders, Net Revenue, Ad Spend, etc.)
                        if len(unique_dates) > 1:
                            # Build individual cell references since columns are not contiguous
                            date_refs = []
                            for date in unique_dates:
                                date_col_idx = all_columns.index(f"{date}_{metric}")
                                date_refs.append(f"{xl_col_to_name(date_col_idx)}{excel_row}")
                            
                            sum_formula = "+".join(date_refs)
                            if metric == "Net items sold":  # Don't round net items sold
                                worksheet.write_formula(
                                    variant_row_idx, total_col_idx,
                                    f"={sum_formula}",
                                    variant_format
                                )
                            else:
                                worksheet.write_formula(
                                    variant_row_idx, total_col_idx,
                                    f"=ROUND({sum_formula},2)",
                                    variant_format
                                )
                        else:
                            single_date_col = all_columns.index(f"{unique_dates[0]}_{metric}")
                            if metric == "Net items sold":  # Don't round net items sold
                                worksheet.write_formula(
                                    variant_row_idx, total_col_idx,
                                    f"={xl_col_to_name(single_date_col)}{excel_row}",
                                    variant_format
                                )
                            else:
                                worksheet.write_formula(
                                    variant_row_idx, total_col_idx,
                                    f"=ROUND({xl_col_to_name(single_date_col)}{excel_row},2)",
                                    variant_format
                                )
                
                # Calculate base columns for variant (link to total columns)
                total_net_items_col_idx = all_columns.index("Total_Net items sold")
                total_ad_spend_col_idx = all_columns.index("Total_Ad Spend (USD)")
                
                worksheet.write_formula(
                    variant_row_idx, 4,
                    f"={xl_col_to_name(total_net_items_col_idx)}{excel_row}",
                    variant_format
                )
                
                worksheet.write_formula(
                    variant_row_idx, 5,
                    f"=ROUND({xl_col_to_name(total_ad_spend_col_idx)}{excel_row},2)",
                    variant_format
                )
                
                # CPI (Cost Per Item) = Total Ad Spent / Net items sold (in USD)
                worksheet.write_formula(
                    variant_row_idx, 6,
                    f"=ROUND(IF({xl_col_to_name(total_net_items_col_idx)}{excel_row}=0,0,{xl_col_to_name(total_ad_spend_col_idx)}{excel_row}/{xl_col_to_name(total_net_items_col_idx)}{excel_row}),2)",
                    variant_format
                )
                
                # BE - REMOVED: Don't calculate BE for individual variants, will reference product total later
                
                row += 1
            
            # Calculate product totals by aggregating variant rows using RANGES (with ROUND for 2 decimal places)
            if variant_rows:
                # Build ranges for product totals
                first_variant_row = min(variant_rows) + 1  # Excel row numbering
                last_variant_row = max(variant_rows) + 1
                
                # Fill Net items sold, Total Ad Spent, CPI, and BE in base columns for product total
                total_net_items_col_idx = all_columns.index("Total_Net items sold")
                total_ad_spend_col_idx = all_columns.index("Total_Ad Spend (USD)")
                
                worksheet.write_formula(
                    product_total_row_idx, 4,
                    f"={xl_col_to_name(total_net_items_col_idx)}{product_total_row_idx+1}",
                    product_total_format
                )
                
                worksheet.write_formula(
                    product_total_row_idx, 5,
                    f"=ROUND({xl_col_to_name(total_ad_spend_col_idx)}{product_total_row_idx+1},2)",
                    product_total_format
                )
                
                # CPI for product total (in USD)
                worksheet.write_formula(
                    product_total_row_idx, 6,
                    f"=ROUND(IF({xl_col_to_name(total_net_items_col_idx)}{product_total_row_idx+1}=0,0,{xl_col_to_name(total_ad_spend_col_idx)}{product_total_row_idx+1}/{xl_col_to_name(total_net_items_col_idx)}{product_total_row_idx+1}),2)",
                    product_total_format
                )
                
                # BE for product total (per item) - CHANGED: Use Delivered Orders instead of Net items sold for consistency with Campaign
                total_net_revenue_col_idx = all_columns.index("Total_Net Revenue")
                total_shipping_cost_col_idx = all_columns.index("Total_Shipping Cost")
                total_operational_cost_col_idx = all_columns.index("Total_Operational Cost")
                total_product_cost_col_idx = all_columns.index("Total_Product Cost (Output)")
                total_delivered_orders_col_idx = all_columns.index("Total_Net items sold")  # CHANGED: Use Delivered Orders
                
                total_net_revenue_ref = f"{xl_col_to_name(total_net_revenue_col_idx)}{product_total_row_idx+1}"
                total_shipping_cost_ref = f"{xl_col_to_name(total_shipping_cost_col_idx)}{product_total_row_idx+1}"
                total_operational_cost_ref = f"{xl_col_to_name(total_operational_cost_col_idx)}{product_total_row_idx+1}"
                total_product_cost_ref = f"{xl_col_to_name(total_product_cost_col_idx)}{product_total_row_idx+1}"
                total_delivered_orders_ref = f"{xl_col_to_name(total_delivered_orders_col_idx)}{product_total_row_idx+1}"
                
                worksheet.write_formula(
                    product_total_row_idx, 7,
                    f"=ROUND(IF(AND({total_net_revenue_ref}>0,{total_delivered_orders_ref}>0),({total_net_revenue_ref}-{total_shipping_cost_ref}-{total_operational_cost_ref}-{total_product_cost_ref})/100/{total_delivered_orders_ref},0),2)",
                    product_total_format
                )
                
                # AFTER calculating product BE, copy this value to ALL variant rows under this product
                product_be_ref = f"H{product_total_row_idx+1}"  # H is column 7 (BE column)
                for variant_row_idx in variant_rows:
                    worksheet.write_formula(
                        variant_row_idx, 7,
                        f"={product_be_ref}",
                        variant_format
                    )
                
                # PRODUCT TOTAL CALCULATIONS (with ROUND for 2 decimal places)
                for date in unique_dates:
                    for metric in date_metrics:
                        col_idx = all_columns.index(f"{date}_{metric}")
                        
                        if metric in ["Avg Price", "Delivery Rate", "Product Cost (Input)"]:
                            # Weighted average based on net items sold for this date using RANGES
                            date_net_items_col_idx = all_columns.index(f"{date}_Net items sold")
                            
                            metric_range = f"{xl_col_to_name(col_idx)}{first_variant_row}:{xl_col_to_name(col_idx)}{last_variant_row}"
                            net_items_range = f"{xl_col_to_name(date_net_items_col_idx)}{first_variant_row}:{xl_col_to_name(date_net_items_col_idx)}{last_variant_row}"
                            
                            worksheet.write_formula(
                                product_total_row_idx, col_idx,
                                f"=ROUND(IF(SUM({net_items_range})=0,0,SUMPRODUCT({metric_range},{net_items_range})/SUM({net_items_range})),2)",
                                product_total_format
                            )
                        elif metric == "Net Profit (%)":
                            # Calculate based on net profit and net revenue for this date
                            net_profit_idx = all_columns.index(f"{date}_Net Profit")
                            net_revenue_idx = all_columns.index(f"{date}_Net Revenue")
                            
                            worksheet.write_formula(
                                product_total_row_idx, col_idx,
                                f"=ROUND(IF({xl_col_to_name(net_revenue_idx)}{product_total_row_idx+1}=0,0,{xl_col_to_name(net_profit_idx)}{product_total_row_idx+1}/{xl_col_to_name(net_revenue_idx)}{product_total_row_idx+1}*100),2)",
                                product_total_format
                            )
                        else:
                            # Sum for other metrics using ranges
                            col_range = f"{xl_col_to_name(col_idx)}{first_variant_row}:{xl_col_to_name(col_idx)}{last_variant_row}"
                            if metric == "Net items sold":  # Don't round net items sold
                                worksheet.write_formula(
                                    product_total_row_idx, col_idx,
                                    f"=SUM({col_range})",
                                    product_total_format
                                )
                            else:
                                worksheet.write_formula(
                                    product_total_row_idx, col_idx,
                                    f"=ROUND(SUM({col_range}),2)",
                                    product_total_format
                                )
                
                # Calculate product totals for Total columns using RANGES (with ROUND for 2 decimal places)
                for metric in date_metrics:
                    col_idx = all_columns.index(f"Total_{metric}")
                    
                    if metric in ["Avg Price", "Delivery Rate", "Product Cost (Input)"]:
                        # Weighted average based on total net items sold using RANGES
                        total_net_items_col_idx = all_columns.index("Total_Net items sold")
                        
                        metric_range = f"{xl_col_to_name(col_idx)}{first_variant_row}:{xl_col_to_name(col_idx)}{last_variant_row}"
                        net_items_range = f"{xl_col_to_name(total_net_items_col_idx)}{first_variant_row}:{xl_col_to_name(total_net_items_col_idx)}{last_variant_row}"
                        
                        worksheet.write_formula(
                            product_total_row_idx, col_idx,
                            f"=ROUND(IF(SUM({net_items_range})=0,0,SUMPRODUCT({metric_range},{net_items_range})/SUM({net_items_range})),2)",
                            product_total_format
                        )
                    elif metric == "Net Profit (%)":
                        # Calculate based on total net profit and total net revenue
                        total_net_profit_idx = all_columns.index("Total_Net Profit")
                        total_net_revenue_idx = all_columns.index("Total_Net Revenue")
                        
                        worksheet.write_formula(
                            product_total_row_idx, col_idx,
                            f"=ROUND(IF({xl_col_to_name(total_net_revenue_idx)}{product_total_row_idx+1}=0,0,{xl_col_to_name(total_net_profit_idx)}{product_total_row_idx+1}/{xl_col_to_name(total_net_revenue_idx)}{product_total_row_idx+1}*100),2)",
                            product_total_format
                        )
                    else:
                        # Sum for other metrics using ranges
                        col_range = f"{xl_col_to_name(col_idx)}{first_variant_row}:{xl_col_to_name(col_idx)}{last_variant_row}"
                        if metric == "Net items sold":  # Don't round net items sold
                            worksheet.write_formula(
                                product_total_row_idx, col_idx,
                                f"=SUM({col_range})",
                                product_total_format
                            )
                        else:
                            worksheet.write_formula(
                                product_total_row_idx, col_idx,
                                f"=ROUND(SUM({col_range}),2)",
                                product_total_format
                            )
                
                # Base columns for product totals - reference the Total weighted averages
                base_delivery_rate_col = 2
                base_product_cost_col = 3
                total_delivery_rate_col_idx = all_columns.index("Total_Delivery Rate")
                total_product_cost_col_idx = all_columns.index("Total_Product Cost (Input)")
                
                worksheet.write_formula(
                    product_total_row_idx, base_delivery_rate_col,
                    f"=ROUND({xl_col_to_name(total_delivery_rate_col_idx)}{product_total_row_idx+1},2)",
                    product_total_format
                )
                
                worksheet.write_formula(
                    product_total_row_idx, base_product_cost_col,
                    f"=ROUND({xl_col_to_name(total_product_cost_col_idx)}{product_total_row_idx+1},2)",
                    product_total_format
                )

        # Calculate grand totals using INDIVIDUAL PRODUCT TOTAL ROWS ONLY (with ROUND for 2 decimal places)
        if product_total_rows:
            # Base columns for grand total
            base_delivery_rate_col = 2
            base_product_cost_col = 3
            base_net_items_col = 4
            base_total_ad_spent_col = 5
            base_cpi_col = 6
            base_be_col = 7
            total_delivery_rate_col_idx = all_columns.index("Total_Delivery Rate")
            total_product_cost_col_idx = all_columns.index("Total_Product Cost (Input)")
            total_net_items_col_idx = all_columns.index("Total_Net items sold")
            total_ad_spend_col_idx = all_columns.index("Total_Ad Spend (USD)")
            
            worksheet.write_formula(
                grand_total_row_idx, base_delivery_rate_col,
                f"=ROUND({xl_col_to_name(total_delivery_rate_col_idx)}{grand_total_row_idx+1},2)",
                grand_total_format
            )
            
            worksheet.write_formula(
                grand_total_row_idx, base_product_cost_col,
                f"=ROUND({xl_col_to_name(total_product_cost_col_idx)}{grand_total_row_idx+1},2)",
                grand_total_format
            )
            
            worksheet.write_formula(
                grand_total_row_idx, base_net_items_col,
                f"={xl_col_to_name(total_net_items_col_idx)}{grand_total_row_idx+1}",
                grand_total_format
            )
            
            worksheet.write_formula(
                grand_total_row_idx, base_total_ad_spent_col,
                f"=ROUND({xl_col_to_name(total_ad_spend_col_idx)}{grand_total_row_idx+1},2)",
                grand_total_format
            )
            
            # CPI for grand total
            worksheet.write_formula(
                grand_total_row_idx, base_cpi_col,
                f"=ROUND(IF({xl_col_to_name(total_net_items_col_idx)}{grand_total_row_idx+1}=0,0,{xl_col_to_name(total_ad_spend_col_idx)}{grand_total_row_idx+1}*100/{xl_col_to_name(total_net_items_col_idx)}{grand_total_row_idx+1})/100,2)",
                grand_total_format
            )
            
            # BE for grand total (per item) - CHANGED: Use Delivered Orders for consistency with Campaign
            total_net_revenue_col_idx = all_columns.index("Total_Net Revenue")
            total_shipping_cost_col_idx = all_columns.index("Total_Shipping Cost")
            total_operational_cost_col_idx = all_columns.index("Total_Operational Cost")
            total_product_cost_col_idx = all_columns.index("Total_Product Cost (Output)")
            total_delivered_orders_col_idx = all_columns.index("Total_Net items sold")  # CHANGED: Use Delivered Orders
            
            total_net_revenue_ref = f"{xl_col_to_name(total_net_revenue_col_idx)}{grand_total_row_idx+1}"
            total_shipping_cost_ref = f"{xl_col_to_name(total_shipping_cost_col_idx)}{grand_total_row_idx+1}"
            total_operational_cost_ref = f"{xl_col_to_name(total_operational_cost_col_idx)}{grand_total_row_idx+1}"
            total_product_cost_ref = f"{xl_col_to_name(total_product_cost_col_idx)}{grand_total_row_idx+1}"
            total_delivered_orders_ref = f"{xl_col_to_name(total_delivered_orders_col_idx)}{grand_total_row_idx+1}"
            
            worksheet.write_formula(
                grand_total_row_idx, base_be_col,
                f"=ROUND(IF(AND({total_net_revenue_ref}>0,{total_delivered_orders_ref}>0),({total_net_revenue_ref}-{total_shipping_cost_ref}-{total_operational_cost_ref}-{total_product_cost_ref})/100/{total_delivered_orders_ref},0),2)",
                grand_total_format
            )
            
            # Date-specific and total columns for grand total using INDIVIDUAL PRODUCT ROWS
            for date in unique_dates:
                for metric in date_metrics:
                    col_idx = all_columns.index(f"{date}_{metric}")
                    
                    if metric in ["Avg Price", "Delivery Rate", "Product Cost (Input)"]:
                        # Weighted average using individual product total rows
                        date_net_items_col_idx = all_columns.index(f"{date}_Net items sold")
                        
                        # Build individual cell references for PRODUCT TOTAL rows only
                        metric_refs = []
                        net_items_refs = []
                        for product_row_idx in product_total_rows:
                            product_excel_row = product_row_idx + 1
                            metric_refs.append(f"{xl_col_to_name(col_idx)}{product_excel_row}")
                            net_items_refs.append(f"{xl_col_to_name(date_net_items_col_idx)}{product_excel_row}")
                        
                        # Build SUMPRODUCT formula for weighted average
                        sumproduct_terms = []
                        for i in range(len(metric_refs)):
                            sumproduct_terms.append(f"{metric_refs[i]}*{net_items_refs[i]}")
                        
                        sumproduct_formula = "+".join(sumproduct_terms)
                        sum_net_items_formula = "+".join(net_items_refs)
                        
                        worksheet.write_formula(
                            grand_total_row_idx, col_idx,
                            f"=ROUND(IF(({sum_net_items_formula})=0,0,({sumproduct_formula})/({sum_net_items_formula})),2)",
                            grand_total_format
                        )
                    elif metric == "Net Profit (%)":
                        # Calculate based on net profit and net revenue for this date
                        net_profit_idx = all_columns.index(f"{date}_Net Profit")
                        net_revenue_idx = all_columns.index(f"{date}_Net Revenue")
                        
                        worksheet.write_formula(
                            grand_total_row_idx, col_idx,
                            f"=ROUND(IF({xl_col_to_name(net_revenue_idx)}{grand_total_row_idx+1}=0,0,{xl_col_to_name(net_profit_idx)}{grand_total_row_idx+1}/{xl_col_to_name(net_revenue_idx)}{grand_total_row_idx+1}*100),2)",
                            grand_total_format
                        )
                    else:
                        # Sum using individual product total rows only
                        sum_refs = []
                        for product_row_idx in product_total_rows:
                            product_excel_row = product_row_idx + 1
                            sum_refs.append(f"{xl_col_to_name(col_idx)}{product_excel_row}")
                        
                        sum_formula = "+".join(sum_refs)
                        if metric == "Net items sold":  # Don't round net items sold
                            worksheet.write_formula(
                                grand_total_row_idx, col_idx,
                                f"={sum_formula}",
                                grand_total_format
                            )
                        else:
                            worksheet.write_formula(
                                grand_total_row_idx, col_idx,
                                f"=ROUND({sum_formula},2)",
                                grand_total_format
                            )
            
            # Total columns for grand total using INDIVIDUAL PRODUCT TOTAL ROWS (with ROUND for 2 decimal places)
            total_net_items_col_idx = all_columns.index("Total_Net items sold")
            
            for metric in date_metrics:
                col_idx = all_columns.index(f"Total_{metric}")
                
                if metric in ["Avg Price", "Delivery Rate", "Product Cost (Input)"]:
                    # Weighted average using individual product total rows
                    
                    # Build individual cell references for PRODUCT TOTAL rows only
                    metric_refs = []
                    net_items_refs = []
                    for product_row_idx in product_total_rows:
                        product_excel_row = product_row_idx + 1
                        metric_refs.append(f"{xl_col_to_name(col_idx)}{product_excel_row}")
                        net_items_refs.append(f"{xl_col_to_name(total_net_items_col_idx)}{product_excel_row}")
                    
                    # Build SUMPRODUCT formula for weighted average
                    sumproduct_terms = []
                    for i in range(len(metric_refs)):
                        sumproduct_terms.append(f"{metric_refs[i]}*{net_items_refs[i]}")
                    
                    sumproduct_formula = "+".join(sumproduct_terms)
                    sum_net_items_formula = "+".join(net_items_refs)
                    
                    worksheet.write_formula(
                        grand_total_row_idx, col_idx,
                        f"=ROUND(IF(({sum_net_items_formula})=0,0,({sumproduct_formula})/({sum_net_items_formula})),2)",
                        grand_total_format
                    )
                elif metric == "Net Profit (%)":
                    # Calculate based on total net profit and total net revenue
                    total_net_profit_idx = all_columns.index("Total_Net Profit")
                    total_net_revenue_idx = all_columns.index("Total_Net Revenue")
                    
                    worksheet.write_formula(
                        grand_total_row_idx, col_idx,
                        f"=ROUND(IF({xl_col_to_name(total_net_revenue_idx)}{grand_total_row_idx+1}=0,0,{xl_col_to_name(total_net_profit_idx)}{grand_total_row_idx+1}/{xl_col_to_name(total_net_revenue_idx)}{grand_total_row_idx+1}*100),2)",
                        grand_total_format
                    )
                else:
                    # Sum using individual product total rows only
                    sum_refs = []
                    for product_row_idx in product_total_rows:
                        product_excel_row = product_row_idx + 1
                        sum_refs.append(f"{xl_col_to_name(col_idx)}{product_excel_row}")
                    
                    sum_formula = "+".join(sum_refs)
                    if metric == "Net items sold":  # Don't round net items sold
                        worksheet.write_formula(
                            grand_total_row_idx, col_idx,
                            f"={sum_formula}",
                            grand_total_format
                        )
                    else:
                        worksheet.write_formula(
                            grand_total_row_idx, col_idx,
                            f"=ROUND({sum_formula},2)",
                            grand_total_format
                        )

        # Freeze panes to keep base columns visible when scrolling
        worksheet.freeze_panes(2, len(base_columns))  # Freeze header and base columns
    
    return output.getvalue()




def convert_final_campaign_to_excel(df, original_campaign_df=None):
    """Original Campaign Excel conversion function (fallback)"""
    if df.empty:
        return None
    
    output = BytesIO()
    with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
        workbook = writer.book
        worksheet = workbook.add_worksheet("Campaign Data")
        writer.sheets["Campaign Data"] = worksheet

        # Formats
        header_format = workbook.add_format({
            "bold": True, "align": "center", "valign": "vcenter",
            "fg_color": "#DDD9C4", "font_name": "Calibri", "font_size": 11
        })
        grand_total_format = workbook.add_format({
            "bold": True, "align": "left", "valign": "vcenter",
            "fg_color": "#FFC000", "font_name": "Calibri", "font_size": 11,
            "num_format": "#,##0.00"
        })
        product_total_format = workbook.add_format({
            "bold": True, "align": "left", "valign": "vcenter",
            "fg_color": "#FFD966", "font_name": "Calibri", "font_size": 11,
            "num_format": "#,##0.00"
        })
        campaign_format = workbook.add_format({
            "align": "left", "valign": "vcenter",
            "fg_color": "#D9E1F2", "font_name": "Calibri", "font_size": 11,
            "num_format": "#,##0.00"
        })

        # Build Columns
        columns = [col for col in df.columns if col != "Product"]
        
        # Add new columns if they don't exist
        new_columns = ["Cost Per Purchase (USD)", "Average Price", "Net Revenue", "Product Cost (Input)", "Total Product Cost", 
                      "Shipping Cost Per Item", "Total Shipping Cost", "Operational Cost Per Item", 
                      "Total Operational Cost", "Net Profit", "Net Profit (%)"]
        
        for new_col in new_columns:
            if new_col not in columns:
                columns.append(new_col)

        # Remove old columns we don't want
        columns_to_remove = ["Cost Per Item", "Cost Per Purchase (INR)", "Amount Spent (INR)"]
        for col_to_remove in columns_to_remove:
            if col_to_remove in columns:
                columns.remove(col_to_remove)

        # Reorder columns to place cost per purchase column right after "Purchases"
        if "Purchases" in columns:
            purchases_index = columns.index("Purchases")
            
            # Remove cost per purchase column from its current position
            if "Cost Per Purchase (USD)" in columns:
                columns.remove("Cost Per Purchase (USD)")
            
            # Insert cost per purchase column after Purchases
            columns.insert(purchases_index + 1, "Cost Per Purchase (USD)")

        for col_num, value in enumerate(columns):
            safe_write(worksheet, 0, col_num, value, header_format)

        # Column Indexes
        product_name_col = 0
        campaign_name_col = columns.index("Campaign Name") if "Campaign Name" in columns else None
        amount_usd_col = columns.index("Amount Spent (USD)") if "Amount Spent (USD)" in columns else None
        purchases_col = columns.index("Purchases") if "Purchases" in columns else None
        cost_per_purchase_usd_col = columns.index("Cost Per Purchase (USD)") if "Cost Per Purchase (USD)" in columns else None
        delivered_col = columns.index("Delivered Orders") if "Delivered Orders" in columns else None
        rate_col = columns.index("Delivery Rate") if "Delivery Rate" in columns else None
        avg_price_col = columns.index("Average Price") if "Average Price" in columns else None
        net_rev_col = columns.index("Net Revenue") if "Net Revenue" in columns else None
        prod_cost_input_col = columns.index("Product Cost (Input)") if "Product Cost (Input)" in columns else None
        total_prod_cost_col = columns.index("Total Product Cost") if "Total Product Cost" in columns else None
        date_col = columns.index("Date") if "Date" in columns else None
        
        # Existing column indexes
        shipping_per_item_col = columns.index("Shipping Cost Per Item") if "Shipping Cost Per Item" in columns else None
        total_shipping_col = columns.index("Total Shipping Cost") if "Total Shipping Cost" in columns else None
        operational_per_item_col = columns.index("Operational Cost Per Item") if "Operational Cost Per Item" in columns else None
        total_operational_col = columns.index("Total Operational Cost") if "Total Operational Cost" in columns else None
        
        # New profit column indexes
        net_profit_col = columns.index("Net Profit") if "Net Profit" in columns else None
        net_profit_pct_col = columns.index("Net Profit (%)") if "Net Profit (%)" in columns else None

        # Columns to sum (including Net Profit but NOT Net Profit % or Cost Per Purchase columns)
        cols_to_sum = []
        for c in ["Amount Spent (USD)", "Purchases", "Total Shipping Cost", "Total Operational Cost", "Net Profit", "Delivered Orders", "Net Revenue"]:
            if c in columns:
                cols_to_sum.append(columns.index(c))

        # GRAND TOTAL ROW
        grand_total_row_idx = 1
        safe_write(worksheet, grand_total_row_idx, 0, "GRAND TOTAL", grand_total_format)
        if campaign_name_col is not None:
            safe_write(worksheet, grand_total_row_idx, campaign_name_col, "ALL PRODUCTS", grand_total_format)

        row = grand_total_row_idx + 1
        product_total_rows = []

        # Group by product
        for product, product_df in df.groupby("Product"):
            # Calculate Cost Per Purchase (USD) and sort by it instead of Amount Spent
            product_df = product_df.copy()
            
            # Calculate Cost Per Purchase (USD) for sorting
            if "Amount Spent (USD)" in product_df.columns and "Purchases" in product_df.columns:
                # Handle division by zero - campaigns with 0 purchases get infinite cost per purchase (sorted last)
                product_df['_temp_cost_per_purchase'] = product_df.apply(
                    lambda row: float('inf') if row["Purchases"] == 0 else row["Amount Spent (USD)"] / row["Purchases"], 
                    axis=1
                )
                # Sort by Cost Per Purchase (USD) in increasing order
                product_df = product_df.sort_values("_temp_cost_per_purchase", ascending=True)
                # Remove temporary column
                product_df = product_df.drop(columns=['_temp_cost_per_purchase'])
            else:
                # Fallback to original sorting if required columns don't exist
                if "Amount Spent (USD)" in product_df.columns:
                    product_df = product_df.sort_values("Amount Spent (USD)", ascending=True)
            
            product_total_row_idx = row
            product_total_rows.append(product_total_row_idx)

            # Product total row
            safe_write(worksheet, product_total_row_idx, 0, product, product_total_format)
            if campaign_name_col is not None:
                safe_write(worksheet, product_total_row_idx, campaign_name_col, "ALL CAMPAIGNS (TOTAL)", product_total_format)

            n_campaigns = len(product_df)
            first_campaign_row_idx = product_total_row_idx + 1
            last_campaign_row_idx = product_total_row_idx + n_campaigns

            # Totals for numeric columns
            for col_idx in cols_to_sum:
                col_letter = xl_col_to_name(col_idx)
                excel_first = first_campaign_row_idx + 1
                excel_last = last_campaign_row_idx + 1
                worksheet.write_formula(
                    product_total_row_idx, col_idx,
                    f"=ROUND(SUM({col_letter}{excel_first}:{col_letter}{excel_last}),2)",
                    product_total_format
                )

            # Cost Per Purchase calculations for product total
            if cost_per_purchase_usd_col is not None and amount_usd_col is not None and purchases_col is not None:
                amount_usd_ref = f"{xl_col_to_name(amount_usd_col)}{product_total_row_idx+1}"
                purchases_ref = f"{xl_col_to_name(purchases_col)}{product_total_row_idx+1}"
                worksheet.write_formula(
                    product_total_row_idx, cost_per_purchase_usd_col,
                    f"=IF(N({purchases_ref})=0,0,ROUND(N({amount_usd_ref})/N({purchases_ref}),2))",
                    product_total_format
                )

            # Add constant values for shipping and operational costs (per item)
            if shipping_per_item_col is not None:
                safe_write(worksheet, product_total_row_idx, shipping_per_item_col, round(shipping_rate, 2), product_total_format)
            
            if operational_per_item_col is not None:
                safe_write(worksheet, product_total_row_idx, operational_per_item_col, round(operational_rate, 2), product_total_format)

            # Campaign rows
            row += 1
            for _, campaign in product_df.iterrows():
                safe_write(worksheet, row, product_name_col, "", campaign_format)

                if campaign_name_col is not None:
                    safe_write(worksheet, row, campaign_name_col, campaign.get("Campaign Name", ""), campaign_format)
                if amount_usd_col is not None:
                    safe_write(worksheet, row, amount_usd_col, round(campaign.get("Amount Spent (USD)", 0), 2), campaign_format)

                if purchases_col is not None:
                    safe_write(worksheet, row, purchases_col, campaign.get("Purchases", 0), campaign_format)
                    
                    # Cost Per Purchase calculations for campaign row
                    if cost_per_purchase_usd_col is not None and amount_usd_col is not None:
                        amount_usd_ref = f"{xl_col_to_name(amount_usd_col)}{row+1}"
                        purchases_ref = f"{xl_col_to_name(purchases_col)}{row+1}"
                        worksheet.write_formula(
                            row, cost_per_purchase_usd_col,
                            f"=IF(N({purchases_ref})=0,0,ROUND(N({amount_usd_ref})/N({purchases_ref}),2))",
                            campaign_format
                        )

                if rate_col is not None:
                    safe_write(worksheet, row, rate_col, "", campaign_format)

                # Date column
                if date_col is not None:
                    safe_write(worksheet, row, date_col, campaign.get("Date", ""), campaign_format)

                # Shipping and operational costs
                
                # Shipping Cost Per Item (constant)
                if shipping_per_item_col is not None:
                    safe_write(worksheet, row, shipping_per_item_col, round(shipping_rate, 2), campaign_format)
                
                # Total Shipping Cost = Shipping Cost Per Item × Purchases
                if total_shipping_col is not None and shipping_per_item_col is not None and purchases_col is not None:
                    shipping_per_ref = f"{xl_col_to_name(shipping_per_item_col)}{row+1}"
                    purchases_ref = f"{xl_col_to_name(purchases_col)}{row+1}"
                    worksheet.write_formula(
                        row, total_shipping_col,
                        f"=ROUND(N({shipping_per_ref})*N({purchases_ref}),2)",
                        campaign_format
                    )
                
                # Operational Cost Per Item (constant)
                if operational_per_item_col is not None:
                    safe_write(worksheet, row, operational_per_item_col, round(operational_rate, 2), campaign_format)
                
                # Total Operational Cost = Operational Cost Per Item × Purchases
                if total_operational_col is not None and operational_per_item_col is not None and purchases_col is not None:
                    operational_per_ref = f"{xl_col_to_name(operational_per_item_col)}{row+1}"
                    purchases_ref = f"{xl_col_to_name(purchases_col)}{row+1}"
                    worksheet.write_formula(
                        row, total_operational_col,
                        f"=ROUND(N({operational_per_ref})*N({purchases_ref}),2)",
                        campaign_format
                    )

                row += 1

        worksheet.freeze_panes(2, 0)
        for i, col in enumerate(columns):
            if col == "Campaign Name":
                worksheet.set_column(i, i, 35)
            elif col in ["Total Shipping Cost", "Total Operational Cost", "Shipping Cost Per Item", "Operational Cost Per Item"]:
                worksheet.set_column(i, i, 18)
            elif col in ["Net Profit", "Net Profit (%)", "Cost Per Purchase (USD)"]:
                worksheet.set_column(i, i, 20)
            else:
                worksheet.set_column(i, i, 15)

    return output.getvalue()


def convert_final_campaign_to_excel_with_date_columns_fixed(df, shopify_df=None, selected_days=None):
    """Convert Campaign data to Excel with day-wise lookups integrated and unmatched campaigns sheet"""
    if df.empty:
        return None
    
    output = BytesIO()
    with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
        workbook = writer.book
        
        # ==== MAIN SHEET: Campaign Data ====
        worksheet = workbook.add_worksheet("Campaign Data")
        writer.sheets["Campaign Data"] = worksheet

        # Formats
        header_format = workbook.add_format({
            "bold": True, "align": "center", "valign": "vcenter",
            "fg_color": "#DDD9C4", "font_name": "Calibri", "font_size": 11
        })
        date_header_format = workbook.add_format({
            "bold": True, "align": "center", "valign": "vcenter",
            "fg_color": "#B4C6E7", "font_name": "Calibri", "font_size": 11
        })
        total_header_format = workbook.add_format({
            "bold": True, "align": "center", "valign": "vcenter",
            "fg_color": "#FFD966", "font_name": "Calibri", "font_size": 11
        })
        grand_total_format = workbook.add_format({
            "bold": True, "align": "left", "valign": "vcenter",
            "fg_color": "#FFC000", "font_name": "Calibri", "font_size": 11
        })
        product_total_format = workbook.add_format({
            "bold": True, "align": "left", "valign": "vcenter",
            "fg_color": "#FFD966", "font_name": "Calibri", "font_size": 11
        })
        campaign_format = workbook.add_format({
            "align": "left", "valign": "vcenter",
            "fg_color": "#D9E1F2", "font_name": "Calibri", "font_size": 11
        })

        # Check if we have dates
        has_dates = 'Date' in df.columns
        if not has_dates:
            # Fall back to original structure if no dates
            return convert_final_campaign_to_excel(df, shopify_df)
        
        # Get unique dates and sort them
        unique_dates = sorted([str(d) for d in df['Date'].unique() if pd.notna(d) and str(d).strip() != ''])
        if selected_days is None:
            if len(unique_dates) > 0:
                n_days = len(unique_dates)
                selected_days = n_days // 2 if n_days % 2 == 0 else (n_days + 1) // 2
            else:
                selected_days = 1
        
        # Define base columns - CHANGED: Cost Per Purchase to CPP, Amount Spent (Zero Net Profit %) to BE
        base_columns = ["Product Name", "Campaign Name", "Total Amount Spent (USD)", "Total Purchases", "CPP", "BE"]
        
        # Define metrics that will be repeated for each date (13 metrics = 13 columns per date)
        date_metrics = ["Avg Price", "Delivery Rate", "Product Cost Input", "Amount Spent (USD)", "Purchases", "Cost Per Purchase (USD)", 
                       "Delivered Orders", "Net Revenue", "Total Product Cost", "Total Shipping Cost", 
                       "Total Operational Cost", "Net Profit", "Net Profit (%)"]
        
        # Build column structure WITH SEPARATOR COLUMNS
        all_columns = base_columns.copy()
        all_columns.append("SEPARATOR_AFTER_BASE")
        
        # Add date-specific columns with separators
        for date in unique_dates:
            for metric in date_metrics:
                all_columns.append(f"{date}_{metric}")
            all_columns.append(f"SEPARATOR_AFTER_{date}")
        
        # Add total columns
        for metric in date_metrics:
            all_columns.append(f"Total_{metric}")
        
        # Add remark column at the end
        all_columns.append("Remark")

        # FIRST: Identify matched and unmatched campaigns BEFORE processing main sheet
        matched_campaigns = []
        unmatched_campaigns = []
        
        # Check each campaign for Shopify data availability
        for product, product_df in df.groupby("Product"):
            # Check if this product has Shopify data (day-wise lookups)
            has_shopify_data = (product in product_date_avg_prices and 
                              any(date in product_date_avg_prices[product] for date in unique_dates) or
                              product in product_date_delivery_rates and 
                              any(date in product_date_delivery_rates[product] for date in unique_dates) or
                              product in product_date_cost_inputs and 
                              any(date in product_date_cost_inputs[product] for date in unique_dates))
            
            for campaign_name, campaign_group in product_df.groupby("Campaign Name"):
                total_amount_spent_usd = campaign_group.get("Amount Spent (USD)", 0).sum() if "Amount Spent (USD)" in campaign_group.columns else 0
                total_amount_spent_inr = campaign_group.get("Amount Spent (INR)", 0).sum() if "Amount Spent (INR)" in campaign_group.columns else 0
                total_purchases = campaign_group.get("Purchases", 0).sum() if "Purchases" in campaign_group.columns else 0
                
                campaign_info = {
                    'Product': str(product) if pd.notna(product) else '',
                    'Campaign Name': str(campaign_name) if pd.notna(campaign_name) else '',
                    'Amount Spent (USD)': round(float(total_amount_spent_usd), 2) if pd.notna(total_amount_spent_usd) else 0.0,
                    'Amount Spent (INR)': round(float(total_amount_spent_inr), 2) if pd.notna(total_amount_spent_inr) else 0.0,
                    'Purchases': int(total_purchases) if pd.notna(total_purchases) else 0,
                    'Has Shopify Data': has_shopify_data,
                    'Dates': sorted([str(d) for d in campaign_group['Date'].unique() if pd.notna(d)])
                }
                
                if has_shopify_data:
                    matched_campaigns.append(campaign_info)
                else:
                    unmatched_campaigns.append(campaign_info)
        
        # FILTER: Create a filtered DataFrame that ONLY contains matched campaigns
        unmatched_campaign_keys = set()
        for campaign in unmatched_campaigns:
            unmatched_campaign_keys.add((campaign['Product'], campaign['Campaign Name']))
        
        # Filter the main DataFrame to exclude unmatched campaigns
        filtered_df_rows = []
        for _, row in df.iterrows():
            campaign_key = (str(row['Product']) if pd.notna(row['Product']) else '', 
                           str(row['Campaign Name']) if pd.notna(row['Campaign Name']) else '')
            if campaign_key not in unmatched_campaign_keys:
                filtered_df_rows.append(row)
        
        # Create filtered DataFrame with only matched campaigns
        if filtered_df_rows:
            filtered_df = pd.DataFrame(filtered_df_rows)
        else:
            # If no matched campaigns, create empty DataFrame with same structure
            filtered_df = df.iloc[0:0].copy()
        
        # Use filtered_df for all main sheet calculations
        df_main = filtered_df

        # Write headers (skip separator columns)
        for col_num, col_name in enumerate(all_columns):
            if col_name.startswith("SEPARATOR_"):
                continue
            elif col_name.startswith("Total_"):
                safe_write(worksheet, 0, col_num, col_name.replace("_", " "), total_header_format)
            elif "_" in col_name and col_name.split("_")[0] in unique_dates:
                safe_write(worksheet, 0, col_num, col_name.replace("_", " "), date_header_format)
            else:
                safe_write(worksheet, 0, col_num, col_name, header_format)

        # SET UP COLUMN GROUPING
        start_col = 7  # Column H (after base columns A, B, C, D, E, F + separator G)
        total_columns = len(all_columns)
        
        group_level = 1
        while start_col < total_columns:
            if start_col < len(all_columns) and all_columns[start_col].startswith("SEPARATOR_"):
                start_col += 1
                continue
                
            data_cols_found = 0
            end_col = start_col
            while end_col < total_columns and data_cols_found < 13:
                if not all_columns[end_col].startswith("SEPARATOR_"):
                    data_cols_found += 1
                if data_cols_found < 13:
                    end_col += 1
            
            if end_col < total_columns:
                worksheet.set_column(
                    start_col, 
                    end_col - 1, 
                    12, 
                    None, 
                    {'level': group_level, 'collapsed': True, 'hidden':True}
                )
            
            start_col = end_col + 1
        
        # Set base column widths
        worksheet.set_column(0, 0, 25)  # Product Name
        worksheet.set_column(1, 1, 30)  # Campaign Name
        worksheet.set_column(2, 2, 20)  # Total Amount Spent (USD)
        worksheet.set_column(3, 3, 15)  # Total Purchases
        worksheet.set_column(4, 4, 18)  # CPP
        worksheet.set_column(5, 5, 25)  # BE
        worksheet.set_column(6, 6, 3)   # Separator column
        # Set width for remark column
        remark_col_idx = all_columns.index("Remark")
        worksheet.set_column(remark_col_idx, remark_col_idx, 30)  # Remark column

        # Configure outline settings
        worksheet.outline_settings(
            symbols_below=True,
            symbols_right=True,
            auto_style=False
        )

        # Grand total row
        grand_total_row_idx = 1
        safe_write(worksheet, grand_total_row_idx, 0, "ALL PRODUCTS", grand_total_format)
        safe_write(worksheet, grand_total_row_idx, 1, "GRAND TOTAL", grand_total_format)

        row = grand_total_row_idx + 1
        product_total_rows = []

        # NEW: Pre-calculate product-level delivery rates AND average prices for Total columns
        product_total_delivery_rates = {}
        product_total_avg_prices = {}
        
        # STORE PRODUCT BE VALUES - This will be populated after main sheet calculation
        product_be_values = {}
        
        # STORE PRODUCT NET PROFIT VALUES - for Profit and Loss Products sheet
        product_net_profit_values = {}
        
        # NEW: STORE PRODUCT COST INPUT VALUES - for Profit and Loss Products sheet
        product_cost_input_values = {}
        
        # Use filtered_df_main for calculations
        for product, product_df in df_main.groupby("Product"):
            # Calculate weighted average delivery rate for this product across all dates
            total_purchases_delivery = 0
            weighted_delivery_rate_sum = 0
            
            # Calculate weighted average price for this product across all dates
            total_purchases_price = 0
            weighted_avg_price_sum = 0
            
            for date in unique_dates:
                date_delivery_rate = product_date_delivery_rates.get(product, {}).get(date, 0)
                date_avg_price = product_date_avg_prices.get(product, {}).get(date, 0)
                date_purchases = product_df[product_df['Date'].astype(str) == date]['Purchases'].sum() if 'Purchases' in product_df.columns else 0
                
                # For delivery rate calculation
                total_purchases_delivery += date_purchases
                weighted_delivery_rate_sum += date_delivery_rate * date_purchases
                
                # For average price calculation
                total_purchases_price += date_purchases
                weighted_avg_price_sum += date_avg_price * date_purchases
            
            # Calculate weighted average delivery rate for this product
            if total_purchases_delivery > 0:
                product_total_delivery_rates[product] = weighted_delivery_rate_sum / total_purchases_delivery
            else:
                product_total_delivery_rates[product] = 0
            
            # Calculate weighted average price for this product
            if total_purchases_price > 0:
                product_total_avg_prices[product] = weighted_avg_price_sum / total_purchases_price
            else:
                product_total_avg_prices[product] = 0

        # Group by product and restructure data - SORT BY CPP WITHIN EACH PRODUCT (using filtered data)
        for product, product_df in df_main.groupby("Product"):
            product_total_row_idx = row
            product_total_rows.append(product_total_row_idx)

            # Product total row
            safe_write(worksheet, product_total_row_idx, 0, product, product_total_format)
            safe_write(worksheet, product_total_row_idx, 1, "ALL CAMPAIGNS (TOTAL)", product_total_format)
            
            # Leave base columns empty for product total (will be calculated via formulas)
            safe_write(worksheet, product_total_row_idx, 2, "", product_total_format)
            safe_write(worksheet, product_total_row_idx, 3, "", product_total_format)
            safe_write(worksheet, product_total_row_idx, 4, "", product_total_format)
            safe_write(worksheet, product_total_row_idx, 5, "", product_total_format)
            
            # Check if product has total amount spent USD = 0 for remark
            product_total_amount_spent = product_df.get("Amount Spent (USD)", 0).sum() if "Amount Spent (USD)" in product_df.columns else 0
            if product_total_amount_spent == 0:
                safe_write(worksheet, product_total_row_idx, all_columns.index("Remark"), "Total Amount Spent USD = 0", product_total_format)
            else:
                safe_write(worksheet, product_total_row_idx, all_columns.index("Remark"), "", product_total_format)

            # Group campaigns within product and calculate CPP for sorting
            campaign_groups = []
            for campaign_name, campaign_group in product_df.groupby("Campaign Name"):
                total_amount_spent_usd = campaign_group.get("Amount Spent (USD)", 0).sum() if "Amount Spent (USD)" in campaign_group.columns else 0
                total_purchases = campaign_group.get("Purchases", 0).sum() if "Purchases" in campaign_group.columns else 0
                
                # MODIFIED CPP CALCULATION: Use 1 for purchases if amount > 0 and purchases = 0
                cpp = 0
                if total_amount_spent_usd > 0 and total_purchases == 0:
                    cpp = total_amount_spent_usd / 1  # Use 1 for formula purposes
                elif total_purchases > 0:
                    cpp = total_amount_spent_usd / total_purchases
                
                campaign_groups.append((cpp, campaign_name, campaign_group))
            
            # Sort campaigns by CPP in ascending order
            campaign_groups.sort(key=lambda x: x[0])
            
            campaign_rows = []
            row += 1
            
            for cpp, campaign_name, campaign_group in campaign_groups:
                campaign_row_idx = row
                campaign_rows.append(campaign_row_idx)
                
                # Fill base columns for campaign
                safe_write(worksheet, campaign_row_idx, 0, product, campaign_format)
                safe_write(worksheet, campaign_row_idx, 1, campaign_name, campaign_format)
                # Leave base columns empty for campaigns (will be calculated via formulas)
                safe_write(worksheet, campaign_row_idx, 2, "", campaign_format)
                safe_write(worksheet, campaign_row_idx, 3, "", campaign_format)
                safe_write(worksheet, campaign_row_idx, 4, "", campaign_format)
                safe_write(worksheet, campaign_row_idx, 5, "", campaign_format)  # BE will reference product total
                
                # Add remark for campaigns with total amount spent USD = 0
                total_amount_spent_usd = campaign_group.get("Amount Spent (USD)", 0).sum() if "Amount Spent (USD)" in campaign_group.columns else 0
                if total_amount_spent_usd == 0:
                    safe_write(worksheet, campaign_row_idx, all_columns.index("Remark"), "Total Amount Spent USD = 0", campaign_format)
                else:
                    safe_write(worksheet, campaign_row_idx, all_columns.index("Remark"), "", campaign_format)
                
                # Cell references for Excel formulas
                excel_row = campaign_row_idx + 1
                
                # Fill date-specific data and formulas
                for date in unique_dates:
                    date_data = campaign_group[campaign_group['Date'].astype(str) == date]
                    
                    # Get column indices for this date
                    avg_price_col_idx = all_columns.index(f"{date}_Avg Price")
                    delivery_rate_col_idx = all_columns.index(f"{date}_Delivery Rate")
                    product_cost_input_col_idx = all_columns.index(f"{date}_Product Cost Input")
                    amount_spent_col_idx = all_columns.index(f"{date}_Amount Spent (USD)")
                    purchases_col_idx = all_columns.index(f"{date}_Purchases")
                    cost_per_purchase_col_idx = all_columns.index(f"{date}_Cost Per Purchase (USD)")
                    delivered_orders_col_idx = all_columns.index(f"{date}_Delivered Orders")
                    net_revenue_col_idx = all_columns.index(f"{date}_Net Revenue")
                    total_product_cost_col_idx = all_columns.index(f"{date}_Total Product Cost")
                    total_shipping_cost_col_idx = all_columns.index(f"{date}_Total Shipping Cost")
                    total_operational_cost_col_idx = all_columns.index(f"{date}_Total Operational Cost")
                    net_profit_col_idx = all_columns.index(f"{date}_Net Profit")
                    net_profit_percent_col_idx = all_columns.index(f"{date}_Net Profit (%)")
                    
                    # Cell references for this date
                    avg_price_ref = f"{xl_col_to_name(avg_price_col_idx)}{excel_row}"
                    delivery_rate_ref = f"{xl_col_to_name(delivery_rate_col_idx)}{excel_row}"
                    product_cost_input_ref = f"{xl_col_to_name(product_cost_input_col_idx)}{excel_row}"
                    amount_spent_ref = f"{xl_col_to_name(amount_spent_col_idx)}{excel_row}"
                    purchases_ref = f"{xl_col_to_name(purchases_col_idx)}{excel_row}"
                    delivered_orders_ref = f"{xl_col_to_name(delivered_orders_col_idx)}{excel_row}"
                    net_revenue_ref = f"{xl_col_to_name(net_revenue_col_idx)}{excel_row}"
                    total_product_cost_ref = f"{xl_col_to_name(total_product_cost_col_idx)}{excel_row}"
                    total_shipping_cost_ref = f"{xl_col_to_name(total_shipping_cost_col_idx)}{excel_row}"
                    total_operational_cost_ref = f"{xl_col_to_name(total_operational_cost_col_idx)}{excel_row}"
                    net_profit_ref = f"{xl_col_to_name(net_profit_col_idx)}{excel_row}"
                    
                    # VALUES FROM DAY-WISE LOOKUPS - Apply to ALL campaigns of this product for this date
                    
                    # Average Price - from day-wise lookup for this product and date
                    date_avg_price = product_date_avg_prices.get(product, {}).get(date, 0)
                    safe_write(worksheet, campaign_row_idx, avg_price_col_idx, round(float(date_avg_price), 2), campaign_format)
                    
                    # Delivery Rate - from day-wise lookup for this product and date
                    date_delivery_rate = product_date_delivery_rates.get(product, {}).get(date, 0)
                    safe_write(worksheet, campaign_row_idx, delivery_rate_col_idx, round(float(date_delivery_rate), 2), campaign_format)
                    
                    # Product Cost Input - from day-wise lookup for this product and date
                    date_cost_input = product_date_cost_inputs.get(product, {}).get(date, 0)
                    safe_write(worksheet, campaign_row_idx, product_cost_input_col_idx, round(float(date_cost_input), 2), campaign_format)
                    
                    if not date_data.empty:
                        row_data = date_data.iloc[0]
                        
                        # Amount Spent (USD) - from campaign data
                        amount_spent = row_data.get("Amount Spent (USD)", 0) or 0
                        safe_write(worksheet, campaign_row_idx, amount_spent_col_idx, round(float(amount_spent), 2), campaign_format)
                        
                        # Purchases - from campaign data  
                        purchases = row_data.get("Purchases", 0) or 0
                        safe_write(worksheet, campaign_row_idx, purchases_col_idx, purchases, campaign_format)
                        
                    else:
                        # No data for this date
                        safe_write(worksheet, campaign_row_idx, amount_spent_col_idx, 0, campaign_format)
                        safe_write(worksheet, campaign_row_idx, purchases_col_idx, 0, campaign_format)
                    
                    # FORMULAS for calculated fields
                    
                    # MODIFIED Cost Per Purchase (USD) formula: Use MAX(Purchases, 1) when Amount > 0
                    worksheet.write_formula(
                        campaign_row_idx, cost_per_purchase_col_idx,
                        f"=ROUND(IF({amount_spent_ref}>0,{amount_spent_ref}/MAX({purchases_ref},1),IF({purchases_ref}=0,0,{amount_spent_ref}/{purchases_ref})),2)",
                        campaign_format
                    )
                    
                    # Delivered Orders = Purchases * Delivery Rate
                    rate_term = f"IF(ISNUMBER({delivery_rate_ref}),IF({delivery_rate_ref}>1,{delivery_rate_ref}/100,{delivery_rate_ref}),0)"
                    worksheet.write_formula(
                        campaign_row_idx, delivered_orders_col_idx,
                        f"=ROUND({purchases_ref}*{rate_term},2)",
                        campaign_format
                    )
                    
                    # Net Revenue = Delivered Orders * Average Price
                    worksheet.write_formula(
                        campaign_row_idx, net_revenue_col_idx,
                        f"=ROUND({delivered_orders_ref}*{avg_price_ref},2)",
                        campaign_format
                    )
                    
                    # Total Product Cost = Delivered Orders * Product Cost Input
                    worksheet.write_formula(
                        campaign_row_idx, total_product_cost_col_idx,
                        f"=ROUND({delivered_orders_ref}*{product_cost_input_ref},2)",
                        campaign_format
                    )
                    
                    # Total Shipping Cost = Purchases * shipping_rate
                    worksheet.write_formula(
                        campaign_row_idx, total_shipping_cost_col_idx,
                        f"=ROUND({purchases_ref}*{shipping_rate},2)",
                        campaign_format
                    )
                    
                    # Total Operational Cost = Purchases * operational_rate
                    worksheet.write_formula(
                        campaign_row_idx, total_operational_cost_col_idx,
                        f"=ROUND({purchases_ref}*{operational_rate},2)",
                        campaign_format
                    )
                    
                    # Net Profit = Net Revenue - Amount Spent (USD)*100 - Total Shipping Cost - Total Operational Cost - Total Product Cost
                    worksheet.write_formula(
                        campaign_row_idx, net_profit_col_idx,
                        f"=ROUND({net_revenue_ref}-{amount_spent_ref}*100-{total_shipping_cost_ref}-{total_operational_cost_ref}-{total_product_cost_ref},2)",
                        campaign_format
                    )
                    
                    # MODIFIED Net Profit (%) = Net Profit / (Avg Price * Delivery Rate * Purchases) * 100
                    # Use MAX(Purchases, 1) when Amount Spent > 0
                    rate_term_for_profit = f"IF(ISNUMBER({delivery_rate_ref}),IF({delivery_rate_ref}>1,{delivery_rate_ref}/100,{delivery_rate_ref}),0)"
                    denominator_formula = f"({avg_price_ref}*{rate_term_for_profit}*IF({amount_spent_ref}>0,MAX({purchases_ref},1),{purchases_ref}))"
                    worksheet.write_formula(
                        campaign_row_idx, net_profit_percent_col_idx,
                        f"=ROUND(IF({denominator_formula}=0,0,{net_profit_ref}/{denominator_formula}*100),2)",
                        campaign_format
                    )
                
                # TOTAL COLUMNS CALCULATIONS FOR CAMPAIGN (FIXED: Use product-level delivery rate AND average price)
                for metric in date_metrics:
                    total_col_idx = all_columns.index(f"Total_{metric}")
                    
                    if metric == "Avg Price":
                        # FIXED: Use the pre-calculated product-level average price for ALL campaigns of this product
                        product_avg_price = product_total_avg_prices.get(product, 0)
                        safe_write(worksheet, campaign_row_idx, total_col_idx, round(float(product_avg_price), 2), campaign_format)
                    
                    elif metric == "Delivery Rate":
                        # FIXED: Use the pre-calculated product-level delivery rate for ALL campaigns of this product
                        product_delivery_rate = product_total_delivery_rates.get(product, 0)
                        safe_write(worksheet, campaign_row_idx, total_col_idx, round(float(product_delivery_rate), 2), campaign_format)
                    
                    elif metric == "Product Cost Input":
                        # WEIGHTED AVERAGE
                        total_purchases_col_idx = all_columns.index("Total_Purchases")
                        total_purchases_ref = f"{xl_col_to_name(total_purchases_col_idx)}{excel_row}"
                        
                        if len(unique_dates) > 1:
                            metric_terms = []
                            for date in unique_dates:
                                metric_col_idx = all_columns.index(f"{date}_{metric}")
                                purchases_col_idx = all_columns.index(f"{date}_Purchases")
                                metric_terms.append(f"{xl_col_to_name(metric_col_idx)}{excel_row}*{xl_col_to_name(purchases_col_idx)}{excel_row}")
                            
                            sumproduct_formula = "+".join(metric_terms)
                            worksheet.write_formula(
                                campaign_row_idx, total_col_idx,
                                f"=ROUND(IF({total_purchases_ref}=0,0,({sumproduct_formula})/{total_purchases_ref}),2)",
                                campaign_format
                            )
                        else:
                            single_date_col = all_columns.index(f"{unique_dates[0]}_{metric}")
                            worksheet.write_formula(
                                campaign_row_idx, total_col_idx,
                                f"=ROUND({xl_col_to_name(single_date_col)}{excel_row},2)",
                                campaign_format
                            )
                    
                    elif metric == "Cost Per Purchase (USD)":
                        # MODIFIED CALCULATED: Total Amount Spent / MAX(Total Purchases, 1) when Amount > 0
                        total_amount_spent_col_idx = all_columns.index("Total_Amount Spent (USD)")
                        total_purchases_col_idx = all_columns.index("Total_Purchases")
                        total_amount_spent_ref = f"{xl_col_to_name(total_amount_spent_col_idx)}{excel_row}"
                        total_purchases_ref = f"{xl_col_to_name(total_purchases_col_idx)}{excel_row}"
                        
                        worksheet.write_formula(
                            campaign_row_idx, total_col_idx,
                            f"=ROUND(IF({total_amount_spent_ref}>0,{total_amount_spent_ref}/MAX({total_purchases_ref},1),IF({total_purchases_ref}=0,0,{total_amount_spent_ref}/{total_purchases_ref})),2)",
                            campaign_format
                        )
                    
                    elif metric == "Net Profit (%)":
                        # MODIFIED CALCULATED: Net Profit / (Avg Price * Delivery Rate * Purchases) * 100
                        # Use MAX(Purchases, 1) when Amount Spent > 0
                        total_net_profit_col_idx = all_columns.index("Total_Net Profit")
                        total_avg_price_col_idx = all_columns.index("Total_Avg Price")
                        total_delivery_rate_col_idx = all_columns.index("Total_Delivery Rate")
                        total_amount_spent_col_idx = all_columns.index("Total_Amount Spent (USD)")
                        total_net_profit_ref = f"{xl_col_to_name(total_net_profit_col_idx)}{excel_row}"
                        total_avg_price_ref = f"{xl_col_to_name(total_avg_price_col_idx)}{excel_row}"
                        total_delivery_rate_ref = f"{xl_col_to_name(total_delivery_rate_col_idx)}{excel_row}"
                        total_amount_spent_ref = f"{xl_col_to_name(total_amount_spent_col_idx)}{excel_row}"
                        total_purchases_ref = f"{xl_col_to_name(total_purchases_col_idx)}{excel_row}"
                        
                        rate_term_total = f"IF(ISNUMBER({total_delivery_rate_ref}),IF({total_delivery_rate_ref}>1,{total_delivery_rate_ref}/100,{total_delivery_rate_ref}),0)"
                        denominator_formula_total = f"({total_avg_price_ref}*{rate_term_total}*IF({total_amount_spent_ref}>0,MAX({total_purchases_ref},1),{total_purchases_ref}))"
                        
                        worksheet.write_formula(
                            campaign_row_idx, total_col_idx,
                            f"=ROUND(IF({denominator_formula_total}=0,0,{total_net_profit_ref}/{denominator_formula_total}*100),2)",
                            campaign_format
                        )
                    
                    else:
                        # SUM: All other metrics
                        if len(unique_dates) > 1:
                            date_refs = []
                            for date in unique_dates:
                                date_col_idx = all_columns.index(f"{date}_{metric}")
                                date_refs.append(f"{xl_col_to_name(date_col_idx)}{excel_row}")
                            
                            sum_formula = "+".join(date_refs)
                            worksheet.write_formula(
                                campaign_row_idx, total_col_idx,
                                f"=ROUND({sum_formula},2)",
                                campaign_format
                            )
                        else:
                            single_date_col = all_columns.index(f"{unique_dates[0]}_{metric}")
                            worksheet.write_formula(
                                campaign_row_idx, total_col_idx,
                                f"=ROUND({xl_col_to_name(single_date_col)}{excel_row},2)",
                                campaign_format
                            )
                
                # Calculate base columns for campaign (link to total columns)
                total_amount_spent_col_idx = all_columns.index("Total_Amount Spent (USD)")
                total_purchases_col_idx = all_columns.index("Total_Purchases")
                total_cost_per_purchase_col_idx = all_columns.index("Total_Cost Per Purchase (USD)")
                
                worksheet.write_formula(
                    campaign_row_idx, 2,
                    f"={xl_col_to_name(total_amount_spent_col_idx)}{excel_row}",
                    campaign_format
                )
                
                worksheet.write_formula(
                    campaign_row_idx, 3,
                    f"={xl_col_to_name(total_purchases_col_idx)}{excel_row}",
                    campaign_format
                )
                
                # CPP (Cost Per Purchase) - link to total cost per purchase column
                worksheet.write_formula(
                    campaign_row_idx, 4,
                    f"={xl_col_to_name(total_cost_per_purchase_col_idx)}{excel_row}",
                    campaign_format
                )
                
                # BE - CHANGED: Reference the product total BE value instead of calculating individually
                # This will be filled after product total BE is calculated
                
                row += 1
            
            # Calculate product totals by aggregating campaign rows using RANGES
            if campaign_rows:
                first_campaign_row = min(campaign_rows) + 1
                last_campaign_row = max(campaign_rows) + 1
                
                # Calculate base columns for product total (link to total columns)
                total_amount_spent_col_idx = all_columns.index("Total_Amount Spent (USD)")
                total_purchases_col_idx = all_columns.index("Total_Purchases")
                total_cost_per_purchase_col_idx = all_columns.index("Total_Cost Per Purchase (USD)")
                
                worksheet.write_formula(
                    product_total_row_idx, 2,
                    f"={xl_col_to_name(total_amount_spent_col_idx)}{product_total_row_idx+1}",
                    product_total_format
                )
                
                worksheet.write_formula(
                    product_total_row_idx, 3,
                    f"={xl_col_to_name(total_purchases_col_idx)}{product_total_row_idx+1}",
                    product_total_format
                )
                
                # CPP for product total
                worksheet.write_formula(
                    product_total_row_idx, 4,
                    f"={xl_col_to_name(total_cost_per_purchase_col_idx)}{product_total_row_idx+1}",
                    product_total_format
                )
                
                # BE (Amount Spent Zero Net Profit % per purchases) for product total - FIXED: Use Total_Purchases (correct)
                total_net_revenue_col_idx = all_columns.index("Total_Net Revenue")
                total_shipping_cost_col_idx = all_columns.index("Total_Total Shipping Cost")
                total_operational_cost_col_idx = all_columns.index("Total_Total Operational Cost")
                total_product_cost_col_idx = all_columns.index("Total_Total Product Cost")
                total_purchases_col_idx = all_columns.index("Total_Purchases")  # FIXED: Use purchases (correct)
                
                total_net_revenue_ref = f"{xl_col_to_name(total_net_revenue_col_idx)}{product_total_row_idx+1}"
                total_shipping_cost_ref = f"{xl_col_to_name(total_shipping_cost_col_idx)}{product_total_row_idx+1}"
                total_operational_cost_ref = f"{xl_col_to_name(total_operational_cost_col_idx)}{product_total_row_idx+1}"
                total_product_cost_ref = f"{xl_col_to_name(total_product_cost_col_idx)}{product_total_row_idx+1}"
                total_purchases_ref = f"{xl_col_to_name(total_purchases_col_idx)}{product_total_row_idx+1}"  # FIXED: Use purchases
                
                zero_net_profit_formula = f'''=ROUND(IF(AND({total_net_revenue_ref}>0,{total_purchases_ref}>0),
                    ({total_net_revenue_ref}-{total_shipping_cost_ref}-{total_operational_cost_ref}-{total_product_cost_ref})/100/{total_purchases_ref},0),2)'''
                
                worksheet.write_formula(
                    product_total_row_idx, 5,
                    zero_net_profit_formula,
                    product_total_format
                )
                
                # STORE THE BE VALUE FOR LOOKUP - Calculate using PURCHASES (FIXED)
                total_net_revenue = 0
                total_shipping_cost = 0
                total_operational_cost = 0
                total_product_cost = 0
                total_purchases = 0  # FIXED: Use purchases instead of delivered orders
                
                for date in unique_dates:
                    date_data = product_df[product_df['Date'].astype(str) == date]
                    if not date_data.empty:
                        date_purchases = date_data['Purchases'].sum() if 'Purchases' in date_data.columns else 0
                        
                        # Get day-wise lookup data
                        date_avg_price = product_date_avg_prices.get(product, {}).get(date, 0)
                        date_delivery_rate = product_date_delivery_rates.get(product, {}).get(date, 0)
                        date_product_cost = product_date_cost_inputs.get(product, {}).get(date, 0)
                        
                        # Calculate components
                        delivery_rate = date_delivery_rate / 100 if date_delivery_rate > 1 else date_delivery_rate
                        delivered_orders = date_purchases * delivery_rate
                        net_revenue = delivered_orders * date_avg_price
                        product_cost = delivered_orders * date_product_cost
                        shipping_cost = date_purchases * shipping_rate
                        operational_cost = date_purchases * operational_rate
                        
                        total_purchases += date_purchases  # FIXED: Sum purchases, not delivered orders
                        total_net_revenue += net_revenue
                        total_shipping_cost += shipping_cost
                        total_operational_cost += operational_cost
                        total_product_cost += product_cost
                
                # Calculate BE for this product using PURCHASES (FIXED)
                be = 0
                if total_net_revenue > 0 and total_purchases > 0:
                    be = (total_net_revenue - total_shipping_cost - total_operational_cost - total_product_cost) / 100 / total_purchases
                
                product_be_values[product] = round(be, 2)
                
                # NEW: Calculate and store Net Profit for this product (for Profit and Loss Products sheet)
                total_amount_spent_usd = product_df.get("Amount Spent (USD)", 0).sum() if "Amount Spent (USD)" in product_df.columns else 0
                net_profit = total_net_revenue - (total_amount_spent_usd * 100) - total_shipping_cost - total_operational_cost - total_product_cost
                
                product_net_profit_values[product] = round(net_profit, 2)
                
                # NEW: Calculate and store Total Product Cost Input for this product (for Profit and Loss Products sheet)
                # This should be the weighted average of product cost input across all dates for this product
                weighted_cost_input_sum = 0
                total_purchases_for_cost = 0
                
                for date in unique_dates:
                    date_data = product_df[product_df['Date'].astype(str) == date]
                    if not date_data.empty:
                        date_purchases = date_data['Purchases'].sum() if 'Purchases' in date_data.columns else 0
                        date_cost_input = product_date_cost_inputs.get(product, {}).get(date, 0)
                        
                        weighted_cost_input_sum += date_cost_input * date_purchases
                        total_purchases_for_cost += date_purchases
                
                # Calculate weighted average product cost input
                if total_purchases_for_cost > 0:
                    product_cost_input_avg = weighted_cost_input_sum / total_purchases_for_cost
                else:
                    product_cost_input_avg = 0
                
                product_cost_input_values[product] = round(product_cost_input_avg, 2)
                
                # AFTER calculating product BE, copy this value to ALL campaign rows under this product
                product_be_ref = f"F{product_total_row_idx+1}"  # F is column 5 (BE column)
                for campaign_row_idx in campaign_rows:
                    worksheet.write_formula(
                        campaign_row_idx, 5,
                        f"={product_be_ref}",
                        campaign_format
                    )
                
                # PRODUCT TOTAL CALCULATIONS (similar to existing logic but with day-wise data)
                for date in unique_dates:
                    for metric in date_metrics:
                        col_idx = all_columns.index(f"{date}_{metric}")
                        
                        if metric == "Avg Price":
                            # FIXED: Use the same product-level average price for product total row
                            date_avg_price = product_date_avg_prices.get(product, {}).get(date, 0)
                            safe_write(worksheet, product_total_row_idx, col_idx, round(float(date_avg_price), 2), product_total_format)
                        elif metric == "Delivery Rate":
                            # FIXED: Use the same product-level delivery rate for product total row
                            date_delivery_rate = product_date_delivery_rates.get(product, {}).get(date, 0)
                            safe_write(worksheet, product_total_row_idx, col_idx, round(float(date_delivery_rate), 2), product_total_format)
                        elif metric == "Product Cost Input":
                            # Weighted average based on purchases for this date using RANGES
                            date_purchases_col_idx = all_columns.index(f"{date}_Purchases")
                            
                            metric_range = f"{xl_col_to_name(col_idx)}{first_campaign_row}:{xl_col_to_name(col_idx)}{last_campaign_row}"
                            purchases_range = f"{xl_col_to_name(date_purchases_col_idx)}{first_campaign_row}:{xl_col_to_name(date_purchases_col_idx)}{last_campaign_row}"
                            
                            worksheet.write_formula(
                                product_total_row_idx, col_idx,
                                f"=ROUND(IF(SUM({purchases_range})=0,0,SUMPRODUCT({metric_range},{purchases_range})/SUM({purchases_range})),2)",
                                product_total_format
                            )
                        elif metric in ["Cost Per Purchase (USD)", "Net Profit (%)"]:
                            # Calculate based on totals for this date
                            if metric == "Cost Per Purchase (USD)":
                                amount_spent_idx = all_columns.index(f"{date}_Amount Spent (USD)")
                                purchases_idx = all_columns.index(f"{date}_Purchases")
                                # MODIFIED CPP formula for product totals
                                worksheet.write_formula(
                                    product_total_row_idx, col_idx,
                                    f"=ROUND(IF({xl_col_to_name(amount_spent_idx)}{product_total_row_idx+1}>0,{xl_col_to_name(amount_spent_idx)}{product_total_row_idx+1}/MAX({xl_col_to_name(purchases_idx)}{product_total_row_idx+1},1),IF({xl_col_to_name(purchases_idx)}{product_total_row_idx+1}=0,0,{xl_col_to_name(amount_spent_idx)}{product_total_row_idx+1}/{xl_col_to_name(purchases_idx)}{product_total_row_idx+1})),2)",
                                    product_total_format
                                )
                            else: # Net Profit (%)
                                net_profit_idx = all_columns.index(f"{date}_Net Profit")
                                avg_price_idx = all_columns.index(f"{date}_Avg Price")
                                delivery_rate_idx = all_columns.index(f"{date}_Delivery Rate")
                                amount_spent_idx = all_columns.index(f"{date}_Amount Spent (USD)")
                                # MODIFIED Net Profit (%) formula for product totals
                                rate_term_product = f"IF(ISNUMBER({xl_col_to_name(delivery_rate_idx)}{product_total_row_idx+1}),IF({xl_col_to_name(delivery_rate_idx)}{product_total_row_idx+1}>1,{xl_col_to_name(delivery_rate_idx)}{product_total_row_idx+1}/100,{xl_col_to_name(delivery_rate_idx)}{product_total_row_idx+1}),0)"
                                denominator_formula_product = f"({xl_col_to_name(avg_price_idx)}{product_total_row_idx+1}*{rate_term_product}*IF({xl_col_to_name(amount_spent_idx)}{product_total_row_idx+1}>0,MAX({xl_col_to_name(purchases_idx)}{product_total_row_idx+1},1),{xl_col_to_name(purchases_idx)}{product_total_row_idx+1}))"
                                worksheet.write_formula(
                                    product_total_row_idx, col_idx,
                                    f"=ROUND(IF({denominator_formula_product}=0,0,{xl_col_to_name(net_profit_idx)}{product_total_row_idx+1}/{denominator_formula_product}*100),2)",
                                    product_total_format
                                )
                        else:
                            # Sum for other metrics using ranges
                            col_range = f"{xl_col_to_name(col_idx)}{first_campaign_row}:{xl_col_to_name(col_idx)}{last_campaign_row}"
                            worksheet.write_formula(
                                product_total_row_idx, col_idx,
                                f"=SUM({col_range})",
                                product_total_format
                            )
                
                # Calculate product totals for Total columns using RANGES (FIXED: Use product-level delivery rate AND average price)
                for metric in date_metrics:
                    col_idx = all_columns.index(f"Total_{metric}")
                    
                    if metric == "Avg Price":
                        # FIXED: Use the pre-calculated product-level average price for product total
                        product_avg_price = product_total_avg_prices.get(product, 0)
                        safe_write(worksheet, product_total_row_idx, col_idx, round(float(product_avg_price), 2), product_total_format)
                    elif metric == "Delivery Rate":
                        # FIXED: Use the pre-calculated product-level delivery rate for product total
                        product_delivery_rate = product_total_delivery_rates.get(product, 0)
                        safe_write(worksheet, product_total_row_idx, col_idx, round(float(product_delivery_rate), 2), product_total_format)
                    elif metric == "Product Cost Input":
                        # Weighted average based on total purchases using RANGES
                        total_purchases_col_idx = all_columns.index("Total_Purchases")
                        
                        metric_range = f"{xl_col_to_name(col_idx)}{first_campaign_row}:{xl_col_to_name(col_idx)}{last_campaign_row}"
                        purchases_range = f"{xl_col_to_name(total_purchases_col_idx)}{first_campaign_row}:{xl_col_to_name(total_purchases_col_idx)}{last_campaign_row}"
                        
                        worksheet.write_formula(
                            product_total_row_idx, col_idx,
                            f"=ROUND(IF(SUM({purchases_range})=0,0,SUMPRODUCT({metric_range},{purchases_range})/SUM({purchases_range})),2)",
                            product_total_format
                        )
                    elif metric in ["Cost Per Purchase (USD)", "Net Profit (%)"]:
                        # Calculate based on totals
                        if metric == "Cost Per Purchase (USD)":
                            total_amount_spent_idx = all_columns.index("Total_Amount Spent (USD)")
                            total_purchases_idx = all_columns.index("Total_Purchases")
                            # MODIFIED CPP formula for product total in Total columns
                            worksheet.write_formula(
                                product_total_row_idx, col_idx,
                                f"=ROUND(IF({xl_col_to_name(total_amount_spent_idx)}{product_total_row_idx+1}>0,{xl_col_to_name(total_amount_spent_idx)}{product_total_row_idx+1}/MAX({xl_col_to_name(total_purchases_idx)}{product_total_row_idx+1},1),IF({xl_col_to_name(total_purchases_idx)}{product_total_row_idx+1}=0,0,{xl_col_to_name(total_amount_spent_idx)}{product_total_row_idx+1}/{xl_col_to_name(total_purchases_idx)}{product_total_row_idx+1})),2)",
                                product_total_format
                            )
                        else: # Net Profit (%)
                            total_net_profit_idx = all_columns.index("Total_Net Profit")
                            total_avg_price_idx = all_columns.index("Total_Avg Price")
                            total_delivery_rate_idx = all_columns.index("Total_Delivery Rate")
                            total_amount_spent_idx = all_columns.index("Total_Amount Spent (USD)")
                            # MODIFIED Net Profit (%) formula for product total in Total columns
                            rate_term_product_total = f"IF(ISNUMBER({xl_col_to_name(total_delivery_rate_idx)}{product_total_row_idx+1}),IF({xl_col_to_name(total_delivery_rate_idx)}{product_total_row_idx+1}>1,{xl_col_to_name(total_delivery_rate_idx)}{product_total_row_idx+1}/100,{xl_col_to_name(total_delivery_rate_idx)}{product_total_row_idx+1}),0)"
                            denominator_formula_product_total = f"({xl_col_to_name(total_avg_price_idx)}{product_total_row_idx+1}*{rate_term_product_total}*IF({xl_col_to_name(total_amount_spent_idx)}{product_total_row_idx+1}>0,MAX({xl_col_to_name(total_purchases_idx)}{product_total_row_idx+1},1),{xl_col_to_name(total_purchases_idx)}{product_total_row_idx+1}))"
                            worksheet.write_formula(
                                product_total_row_idx, col_idx,
                                f"=ROUND(IF({denominator_formula_product_total}=0,0,{xl_col_to_name(total_net_profit_idx)}{product_total_row_idx+1}/{denominator_formula_product_total}*100),2)",
                                product_total_format
                            )
                    else:
                        # Sum for other metrics using ranges
                        col_range = f"{xl_col_to_name(col_idx)}{first_campaign_row}:{xl_col_to_name(col_idx)}{last_campaign_row}"
                        worksheet.write_formula(
                            product_total_row_idx, col_idx,
                            f"=ROUND(SUM({col_range}),2)",
                            product_total_format
                        )

        # Calculate grand totals using INDIVIDUAL PRODUCT TOTAL ROWS ONLY (FIXED: Use weighted average for delivery rate AND average price)
        if product_total_rows:
            # Base columns for grand total
            total_amount_spent_col_idx = all_columns.index("Total_Amount Spent (USD)")
            total_purchases_col_idx = all_columns.index("Total_Purchases")
            total_cost_per_purchase_col_idx = all_columns.index("Total_Cost Per Purchase (USD)")
            
            worksheet.write_formula(
                grand_total_row_idx, 2,
                f"={xl_col_to_name(total_amount_spent_col_idx)}{grand_total_row_idx+1}",
                grand_total_format
            )
            
            worksheet.write_formula(
                grand_total_row_idx, 3,
                f"={xl_col_to_name(total_purchases_col_idx)}{grand_total_row_idx+1}",
                grand_total_format
            )
            
            # CPP for grand total
            worksheet.write_formula(
                grand_total_row_idx, 4,
                f"={xl_col_to_name(total_cost_per_purchase_col_idx)}{grand_total_row_idx+1}",
                grand_total_format
            )
            
            # BE (Amount Spent Zero Net Profit % per purchases) for grand total - FIXED: Use Purchases
            total_net_revenue_col_idx = all_columns.index("Total_Net Revenue")
            total_shipping_cost_col_idx = all_columns.index("Total_Total Shipping Cost")
            total_operational_cost_col_idx = all_columns.index("Total_Total Operational Cost")
            total_product_cost_col_idx = all_columns.index("Total_Total Product Cost")
            total_purchases_col_idx = all_columns.index("Total_Purchases")  # FIXED: Use purchases
            
            total_net_revenue_ref = f"{xl_col_to_name(total_net_revenue_col_idx)}{grand_total_row_idx+1}"
            total_shipping_cost_ref = f"{xl_col_to_name(total_shipping_cost_col_idx)}{grand_total_row_idx+1}"
            total_operational_cost_ref = f"{xl_col_to_name(total_operational_cost_col_idx)}{grand_total_row_idx+1}"
            total_product_cost_ref = f"{xl_col_to_name(total_product_cost_col_idx)}{grand_total_row_idx+1}"
            total_purchases_ref = f"{xl_col_to_name(total_purchases_col_idx)}{grand_total_row_idx+1}"  # FIXED: Use purchases
            
            zero_net_profit_formula = f'''=ROUND(IF(AND({total_net_revenue_ref}>0,{total_purchases_ref}>0),
                ({total_net_revenue_ref}-{total_shipping_cost_ref}-{total_operational_cost_ref}-{total_product_cost_ref})/100/{total_purchases_ref},0),2)'''
            
            worksheet.write_formula(
                grand_total_row_idx, 5,
                zero_net_profit_formula,
                grand_total_format
            )
            
            # Add remark for grand total if total amount spent USD = 0 (using filtered data)
            grand_total_amount_spent = df_main.get("Amount Spent (USD)", 0).sum() if "Amount Spent (USD)" in df_main.columns else 0
            if grand_total_amount_spent == 0:
                safe_write(worksheet, grand_total_row_idx, all_columns.index("Remark"), "Total Amount Spent USD = 0", grand_total_format)
            else:
                safe_write(worksheet, grand_total_row_idx, all_columns.index("Remark"), "", grand_total_format)
            
            # Date-specific and total columns for grand total using INDIVIDUAL PRODUCT ROWS (FIXED: Weighted average for delivery rate AND average price)
            for date in unique_dates:
                for metric in date_metrics:
                    col_idx = all_columns.index(f"{date}_{metric}")
                    
                    if metric in ["Avg Price", "Delivery Rate", "Product Cost Input"]:
                        # Weighted average using individual product total rows
                        date_purchases_col_idx = all_columns.index(f"{date}_Purchases")
                        
                        metric_refs = []
                        purchases_refs = []
                        for product_row_idx in product_total_rows:
                            product_excel_row = product_row_idx + 1
                            metric_refs.append(f"{xl_col_to_name(col_idx)}{product_excel_row}")
                            purchases_refs.append(f"{xl_col_to_name(date_purchases_col_idx)}{product_excel_row}")
                        
                        # Build SUMPRODUCT formula for weighted average
                        sumproduct_terms = []
                        for i in range(len(metric_refs)):
                            sumproduct_terms.append(f"{metric_refs[i]}*{purchases_refs[i]}")
                        
                        sumproduct_formula = "+".join(sumproduct_terms)
                        sum_purchases_formula = "+".join(purchases_refs)
                        
                        worksheet.write_formula(
                            grand_total_row_idx, col_idx,
                            f"=ROUND(IF(({sum_purchases_formula})=0,0,({sumproduct_formula})/({sum_purchases_formula})),2)",
                            grand_total_format
                        )
                    elif metric in ["Cost Per Purchase (USD)", "Net Profit (%)"]:
                        # Calculate based on totals for this date
                        if metric == "Cost Per Purchase (USD)":
                            amount_spent_idx = all_columns.index(f"{date}_Amount Spent (USD)")
                            purchases_idx = all_columns.index(f"{date}_Purchases")
                            # MODIFIED CPP formula for grand total
                            worksheet.write_formula(
                                grand_total_row_idx, col_idx,
                                f"=ROUND(IF({xl_col_to_name(amount_spent_idx)}{grand_total_row_idx+1}>0,{xl_col_to_name(amount_spent_idx)}{grand_total_row_idx+1}/MAX({xl_col_to_name(purchases_idx)}{grand_total_row_idx+1},1),IF({xl_col_to_name(purchases_idx)}{grand_total_row_idx+1}=0,0,{xl_col_to_name(amount_spent_idx)}{grand_total_row_idx+1}/{xl_col_to_name(purchases_idx)}{grand_total_row_idx+1})),2)",
                                grand_total_format
                            )
                        else: # Net Profit (%)
                            net_profit_idx = all_columns.index(f"{date}_Net Profit")
                            avg_price_idx = all_columns.index(f"{date}_Avg Price")
                            delivery_rate_idx = all_columns.index(f"{date}_Delivery Rate")
                            amount_spent_idx = all_columns.index(f"{date}_Amount Spent (USD)")
                            # MODIFIED CPP formula for grand total
                            rate_term_grand = f"IF(ISNUMBER({xl_col_to_name(delivery_rate_idx)}{grand_total_row_idx+1}),IF({xl_col_to_name(delivery_rate_idx)}{grand_total_row_idx+1}>1,{xl_col_to_name(delivery_rate_idx)}{grand_total_row_idx+1}/100,{xl_col_to_name(delivery_rate_idx)}{grand_total_row_idx+1}),0)"
                            denominator_formula_grand = f"({xl_col_to_name(avg_price_idx)}{grand_total_row_idx+1}*{rate_term_grand}*IF({xl_col_to_name(amount_spent_idx)}{grand_total_row_idx+1}>0,MAX({xl_col_to_name(purchases_idx)}{grand_total_row_idx+1},1),{xl_col_to_name(purchases_idx)}{grand_total_row_idx+1}))"
                            worksheet.write_formula(
                                grand_total_row_idx, col_idx,
                                f"=ROUND(IF({denominator_formula_grand}=0,0,{xl_col_to_name(net_profit_idx)}{grand_total_row_idx+1}/{denominator_formula_grand}*100),2)",
                                grand_total_format
                            )
                    else:
                        # Sum using individual product total rows only
                        sum_refs = []
                        for product_row_idx in product_total_rows:
                            product_excel_row = product_row_idx + 1
                            sum_refs.append(f"{xl_col_to_name(col_idx)}{product_excel_row}")
                        
                        sum_formula = "+".join(sum_refs)
                        worksheet.write_formula(
                            grand_total_row_idx, col_idx,
                            f"={sum_formula}",
                            grand_total_format
                        )
            
            # Total columns for grand total using INDIVIDUAL PRODUCT TOTAL ROWS (FIXED: Weighted average for delivery rate AND average price)
            total_purchases_col_idx = all_columns.index("Total_Purchases")
            
            for metric in date_metrics:
                col_idx = all_columns.index(f"Total_{metric}")
                
                if metric in ["Avg Price", "Delivery Rate", "Product Cost Input"]:
                    # Weighted average using individual product total rows
                    metric_refs = []
                    purchases_refs = []
                    for product_row_idx in product_total_rows:
                        product_excel_row = product_row_idx + 1
                        metric_refs.append(f"{xl_col_to_name(col_idx)}{product_excel_row}")
                        purchases_refs.append(f"{xl_col_to_name(total_purchases_col_idx)}{product_excel_row}")
                    
                    # Build SUMPRODUCT formula for weighted average
                    sumproduct_terms = []
                    for i in range(len(metric_refs)):
                        sumproduct_terms.append(f"{metric_refs[i]}*{purchases_refs[i]}")
                    
                    sumproduct_formula = "+".join(sumproduct_terms)
                    sum_purchases_formula = "+".join(purchases_refs)
                    
                    worksheet.write_formula(
                        grand_total_row_idx, col_idx,
                        f"=ROUND(IF(({sum_purchases_formula})=0,0,({sumproduct_formula})/({sum_purchases_formula})),2)",
                        grand_total_format
                    )
                elif metric in ["Cost Per Purchase (USD)", "Net Profit (%)"]:
                    # Calculate based on totals
                    if metric == "Cost Per Purchase (USD)":
                        total_amount_spent_idx = all_columns.index("Total_Amount Spent (USD)")
                        total_purchases_idx = all_columns.index("Total_Purchases")
                        # MODIFIED CPP formula for grand total in Total columns
                        worksheet.write_formula(
                            grand_total_row_idx, col_idx,
                            f"=ROUND(IF({xl_col_to_name(total_amount_spent_idx)}{grand_total_row_idx+1}>0,{xl_col_to_name(total_amount_spent_idx)}{grand_total_row_idx+1}/MAX({xl_col_to_name(total_purchases_idx)}{grand_total_row_idx+1},1),IF({xl_col_to_name(total_purchases_idx)}{grand_total_row_idx+1}=0,0,{xl_col_to_name(total_amount_spent_idx)}{grand_total_row_idx+1}/{xl_col_to_name(total_purchases_idx)}{grand_total_row_idx+1})),2)",
                            grand_total_format
                        )
                    else: # Net Profit (%)
                        total_net_profit_idx = all_columns.index("Total_Net Profit")
                        total_avg_price_idx = all_columns.index("Total_Avg Price")
                        total_delivery_rate_idx = all_columns.index("Total_Delivery Rate")
                        total_amount_spent_idx = all_columns.index("Total_Amount Spent (USD)")
                        total_purchases_idx = all_columns.index("Total_Purchases")
                        # MODIFIED Net Profit (%) formula for grand total in Total columns
                        rate_term_grand_total = f"IF(ISNUMBER({xl_col_to_name(total_delivery_rate_idx)}{grand_total_row_idx+1}),IF({xl_col_to_name(total_delivery_rate_idx)}{grand_total_row_idx+1}>1,{xl_col_to_name(total_delivery_rate_idx)}{grand_total_row_idx+1}/100,{xl_col_to_name(total_delivery_rate_idx)}{grand_total_row_idx+1}),0)"
                        denominator_formula_grand_total = f"({xl_col_to_name(total_avg_price_idx)}{grand_total_row_idx+1}*{rate_term_grand_total}*IF({xl_col_to_name(total_amount_spent_idx)}{grand_total_row_idx+1}>0,MAX({xl_col_to_name(total_purchases_idx)}{grand_total_row_idx+1},1),{xl_col_to_name(total_purchases_idx)}{grand_total_row_idx+1}))"
                        worksheet.write_formula(
                            grand_total_row_idx, col_idx,
                            f"=ROUND(IF({denominator_formula_grand_total}=0,0,{xl_col_to_name(total_net_profit_idx)}{grand_total_row_idx+1}/{denominator_formula_grand_total}*100),2)",
                            grand_total_format
                        )
                else:
                    # Sum using individual product total rows only
                    sum_refs = []
                    for product_row_idx in product_total_rows:
                        product_excel_row = product_row_idx + 1
                        sum_refs.append(f"{xl_col_to_name(col_idx)}{product_excel_row}")
                    
                    sum_formula = "+".join(sum_refs)
                    worksheet.write_formula(
                        grand_total_row_idx, col_idx,
                        f"={sum_formula}",
                        grand_total_format
                    )

        # Freeze panes to keep base columns visible when scrolling
        worksheet.freeze_panes(2, len(base_columns))  # Freeze header and base columns
        
        # ==== NEW SHEET: Unmatched Campaigns ====
        unmatched_sheet = workbook.add_worksheet("Unmatched Campaigns")
        
        # Formats for unmatched sheet
        unmatched_header_format = workbook.add_format({
            "bold": True, "align": "center", "valign": "vcenter",
            "fg_color": "#FF9999", "font_name": "Calibri", "font_size": 11
        })
        unmatched_data_format = workbook.add_format({
            "align": "left", "valign": "vcenter",
            "fg_color": "#FFE6E6", "font_name": "Calibri", "font_size": 11,
            "num_format": "#,##0.00"  # 2 decimal places
        })
        matched_summary_format = workbook.add_format({
            "align": "left", "valign": "vcenter",
            "fg_color": "#E6FFE6", "font_name": "Calibri", "font_size": 11,
            "num_format": "#,##0.00"  # 2 decimal places
        })
        
        # Headers for unmatched sheet
        unmatched_headers = ["Status", "Product", "Campaign Name", "Amount Spent (USD)", 
                           "Amount Spent (INR)", "Purchases", "Cost Per Purchase (USD)", "Dates Covered", "Reason"]
        
        for col_num, header in enumerate(unmatched_headers):
            safe_write(unmatched_sheet, 0, col_num, header, unmatched_header_format)
        
        # Write summary first
        summary_row = 1
        safe_write(unmatched_sheet, summary_row, 0, "SUMMARY", unmatched_header_format)
        safe_write(unmatched_sheet, summary_row + 1, 0, f"Total Campaigns: {len(matched_campaigns) + len(unmatched_campaigns)}", matched_summary_format)
        safe_write(unmatched_sheet, summary_row + 2, 0, f"Matched with Shopify: {len(matched_campaigns)}", matched_summary_format)
        safe_write(unmatched_sheet, summary_row + 3, 0, f"Unmatched with Shopify: {len(unmatched_campaigns)}", unmatched_data_format)
        safe_write(unmatched_sheet, summary_row + 4, 0, f"Date Range: {min(unique_dates)} to {max(unique_dates)}" if unique_dates else "No dates found", matched_summary_format)
        
        # Write unmatched campaigns
        current_row = summary_row + 6
        
        if unmatched_campaigns:
            safe_write(unmatched_sheet, current_row, 0, "CAMPAIGNS WITHOUT SHOPIFY DATA", unmatched_header_format)
            current_row += 1
            
            for campaign in unmatched_campaigns:
                # MODIFIED CPP calculation for unmatched campaigns sheet
                cost_per_purchase_usd = 0
                if campaign['Amount Spent (USD)'] > 0 and campaign['Purchases'] == 0:
                    cost_per_purchase_usd = round(campaign['Amount Spent (USD)'] / 1, 2)  # Use 1 when no purchases but has spending
                elif campaign['Purchases'] > 0:
                    cost_per_purchase_usd = round(campaign['Amount Spent (USD)'] / campaign['Purchases'], 2)
                
                dates_str = ", ".join(campaign['Dates']) if campaign['Dates'] else "No dates"
                
                safe_write(unmatched_sheet, current_row, 0, "UNMATCHED", unmatched_data_format)
                safe_write(unmatched_sheet, current_row, 1, campaign['Product'], unmatched_data_format)
                safe_write(unmatched_sheet, current_row, 2, campaign['Campaign Name'], unmatched_data_format)
                safe_write(unmatched_sheet, current_row, 3, campaign['Amount Spent (USD)'], unmatched_data_format)
                safe_write(unmatched_sheet, current_row, 4, campaign['Amount Spent (INR)'], unmatched_data_format)
                safe_write(unmatched_sheet, current_row, 5, campaign['Purchases'], unmatched_data_format)
                safe_write(unmatched_sheet, current_row, 6, cost_per_purchase_usd, unmatched_data_format)
                safe_write(unmatched_sheet, current_row, 7, dates_str, unmatched_data_format)
                safe_write(unmatched_sheet, current_row, 8, "No matching Shopify day-wise data found", unmatched_data_format)
                current_row += 1
        
        # Write matched campaigns summary
        if matched_campaigns:
            current_row += 2
            safe_write(unmatched_sheet, current_row, 0, "CAMPAIGNS WITH SHOPIFY DATA (FOR REFERENCE)", unmatched_header_format)
            current_row += 1
            
            for campaign in matched_campaigns[:10]:  # Show only first 10 to save space
                # MODIFIED CPP calculation for matched campaigns sheet
                cost_per_purchase_usd = 0
                if campaign['Amount Spent (USD)'] > 0 and campaign['Purchases'] == 0:
                    cost_per_purchase_usd = round(campaign['Amount Spent (USD)'] / 1, 2)  # Use 1 when no purchases but has spending
                elif campaign['Purchases'] > 0:
                    cost_per_purchase_usd = round(campaign['Amount Spent (USD)'] / campaign['Purchases'], 2)
                
                dates_str = ", ".join(campaign['Dates']) if campaign['Dates'] else "No dates"
                
                safe_write(unmatched_sheet, current_row, 0, "MATCHED", matched_summary_format)
                safe_write(unmatched_sheet, current_row, 1, campaign['Product'], matched_summary_format)
                safe_write(unmatched_sheet, current_row, 2, campaign['Campaign Name'], matched_summary_format)
                safe_write(unmatched_sheet, current_row, 3, campaign['Amount Spent (USD)'], matched_summary_format)
                safe_write(unmatched_sheet, current_row, 4, campaign['Amount Spent (INR)'], matched_summary_format)
                safe_write(unmatched_sheet, current_row, 5, campaign['Purchases'], matched_summary_format)
                safe_write(unmatched_sheet, current_row, 6, cost_per_purchase_usd, matched_summary_format)
                safe_write(unmatched_sheet, current_row, 7, dates_str, matched_summary_format)
                safe_write(unmatched_sheet, current_row, 8, "Successfully matched with Shopify day-wise data", matched_summary_format)
                current_row += 1
            
            if len(matched_campaigns) > 10:
                safe_write(unmatched_sheet, current_row, 0, f"... and {len(matched_campaigns) - 10} more matched campaigns", matched_summary_format)
        
        # Set column widths for unmatched sheet
        unmatched_sheet.set_column(0, 0, 12)  # Status
        unmatched_sheet.set_column(1, 1, 25)  # Product
        unmatched_sheet.set_column(2, 2, 35)  # Campaign Name
        unmatched_sheet.set_column(3, 3, 18)  # Amount USD
        unmatched_sheet.set_column(4, 4, 18)  # Amount INR
        unmatched_sheet.set_column(5, 5, 12)  # Purchases
        unmatched_sheet.set_column(6, 6, 20)  # Cost Per Purchase USD
        unmatched_sheet.set_column(7, 7, 25)  # Dates Covered
        unmatched_sheet.set_column(8, 8, 40)  # Reason
        
        # ==== RESTRUCTURED SHEET: Negative Net Profit Campaigns ====
        # Use the filtered df_main for negative profit analysis to ensure consistency
        negative_profit_sheet = workbook.add_worksheet("Negative Net Profit Campaigns")

        # Formats for negative net profit sheet
        negative_profit_header_format = workbook.add_format({
            "bold": True, "align": "center", "valign": "vcenter",
            "fg_color": "#FF6B6B", "font_name": "Calibri", "font_size": 11
        })
        negative_profit_data_format = workbook.add_format({
            "align": "left", "valign": "vcenter",
            "fg_color": "#FFE6E6", "font_name": "Calibri", "font_size": 11,
            "num_format": "#,##0.00"
        })
        # Format for moderate negative campaigns (-1% to 0%)
        moderate_negative_header_format = workbook.add_format({
            "bold": True, "align": "center", "valign": "vcenter",
            "fg_color": "#FFA500", "font_name": "Calibri", "font_size": 11
        })
        moderate_negative_data_format = workbook.add_format({
            "align": "left", "valign": "vcenter",
            "fg_color": "#FFE4B5", "font_name": "Calibri", "font_size": 11,
            "num_format": "#,##0.00"
        })
        # NEW: Format for positive campaigns (0% and above)
        positive_header_format = workbook.add_format({
            "bold": True, "align": "center", "valign": "vcenter",
            "fg_color": "#ABEA53", "font_name": "Calibri", "font_size": 11
        })
        positive_data_format = workbook.add_format({
            "align": "left", "valign": "vcenter",
            "fg_color": "#F0FFF0", "font_name": "Calibri", "font_size": 11,
            "num_format": "#,##0.00"
        })
        # Format for last date negative campaigns
        last_date_header_format = workbook.add_format({
            "bold": True, "align": "center", "valign": "vcenter",
            "fg_color": "#9932CC", "font_name": "Calibri", "font_size": 11
        })
        last_date_data_format = workbook.add_format({
            "align": "left", "valign": "vcenter",
            "fg_color": "#E6E6FA", "font_name": "Calibri", "font_size": 11,
            "num_format": "#,##0.00"
        })

        # FIXED: Helper function to format dates in a more readable way
        def format_date_readable(date_str):
            """Convert date string to more readable format like '9th September 2025'"""
            try:
                from datetime import datetime
                # Try different date formats
                for fmt in ['%Y-%m-%d', '%d/%m/%Y', '%m/%d/%Y', '%d-%m-%Y']:
                    try:
                        date_obj = datetime.strptime(date_str, fmt)
                        # Format as "9th September 2025"
                        day = date_obj.day
                        # Add ordinal suffix
                        if 4 <= day <= 20 or 24 <= day <= 30:
                            suffix = "th"
                        else:
                            suffix = ["st", "nd", "rd"][day % 10 - 1]
                        
                        return f"{day}{suffix} {date_obj.strftime('%B %Y')}"
                    except ValueError:
                        continue
                
                # If no format works, return original
                return date_str
            except:
                return date_str

        # Calculate selected_days threshold (default: len(unique_dates) // 2, minimum 1)
        if selected_days is None:
            selected_days = max(1, len(unique_dates) // 2)

        # Analyze campaigns for negative net profit percentage (using filtered data)
        all_negative_campaigns = []
        current_row = 0

        # Group by product and campaign to analyze net profit percentages (using filtered data)
        for product, product_df in df_main.groupby("Product"):
            for campaign_name, campaign_group in product_df.groupby("Campaign Name"):
                # Get dates for this campaign and sort them
                campaign_dates = sorted([str(d) for d in campaign_group['Date'].unique() if pd.notna(d) and str(d).strip() != ''])
                
                if len(campaign_dates) < selected_days:  # Skip campaigns with fewer dates than selected
                    continue
                
                # Calculate CPP and Amount Spent for this campaign
                total_amount_spent_usd = campaign_group.get("Amount Spent (USD)", 0).sum() if "Amount Spent (USD)" in campaign_group.columns else 0
                total_purchases = campaign_group.get("Purchases", 0).sum() if "Purchases" in campaign_group.columns else 0
                
                # Calculate CPP (Cost Per Purchase)
                cpp = 0
                if total_amount_spent_usd > 0 and total_purchases == 0:
                    cpp = total_amount_spent_usd / 1  # Use 1 for formula purposes when no purchases but has spending
                elif total_purchases > 0:
                    cpp = total_amount_spent_usd / total_purchases
                
                # GET BE VALUE FROM THE PRODUCT-LEVEL LOOKUP (FIXED: Use the correct BE value from main sheet calculation)
                be = product_be_values.get(product, 0)
                
                # Calculate net profit percentages for ALL dates
                date_net_profit_percentages = []
                for date in campaign_dates:
                    date_data = campaign_group[campaign_group['Date'].astype(str) == date]
                    if not date_data.empty:
                        row_data = date_data.iloc[0]
                        
                        # Get data for net profit percentage calculation
                        amount_spent = row_data.get("Amount Spent (USD)", 0) or 0
                        purchases = row_data.get("Purchases", 0) or 0
                        
                        # Get day-wise lookup data
                        date_avg_price = product_date_avg_prices.get(product, {}).get(date, 0)
                        date_delivery_rate = product_date_delivery_rates.get(product, {}).get(date, 0)
                        date_product_cost = product_date_cost_inputs.get(product, {}).get(date, 0)
                        
                        # Calculate net profit percentage using the same logic as in the main sheet
                        if date_avg_price > 0 and (purchases > 0 or (purchases == 0 and amount_spent > 0)):
                            # Handle zero purchases case - use 1 when amount spent > 0 but purchases = 0
                            calc_purchases = 1 if (purchases == 0 and amount_spent > 0) else purchases
                            
                            # Delivery rate conversion
                            delivery_rate = date_delivery_rate / 100 if date_delivery_rate > 1 else date_delivery_rate
                            
                            # Calculate components for net profit percentage
                            delivered_orders = calc_purchases * delivery_rate
                            net_revenue = delivered_orders * date_avg_price
                            total_product_cost = delivered_orders * date_product_cost
                            total_shipping_cost = calc_purchases * shipping_rate
                            total_operational_cost = calc_purchases * operational_rate
                            net_profit = net_revenue - (amount_spent * 100) - total_shipping_cost - total_operational_cost - total_product_cost
                            
                            # Net Profit Percentage = Net Profit / (Avg Price * Delivery Rate * Purchases) * 100
                            denominator = date_avg_price * delivery_rate * calc_purchases
                            net_profit_percentage = (net_profit / denominator * 100) if denominator > 0 else 0
                            
                            date_net_profit_percentages.append({
                                'date': date,
                                'net_profit_percentage': net_profit_percentage,
                                'amount_spent': amount_spent,
                                'purchases': purchases
                            })
                
                # Count negative net profit percentages and check if we have at least 'selected_days' negative values
                negative_profit_data = [data for data in date_net_profit_percentages if data['net_profit_percentage'] < 0]
                
                if len(negative_profit_data) >= selected_days:
                    # This campaign has the required number of negative net profit percentages
                    negative_dates = [data['date'] for data in negative_profit_data]
                    
                    # Format dates in a more readable way
                    formatted_negative_dates = [format_date_readable(date) for date in negative_dates[:10]]
                    formatted_dates_str = ", ".join(formatted_negative_dates)
                    if len(negative_dates) > 10:
                        formatted_dates_str += "..."
                    
                    # Calculate overall Net Profit % for this campaign (using Total columns logic)
                    # Get the campaign's total net profit percentage from the main sheet calculation
                    campaign_net_profit_percentage = 0
                    if total_amount_spent_usd > 0 or total_purchases > 0:
                        # Calculate using the same logic as Total_Net Profit (%) in main sheet
                        product_avg_price = product_total_avg_prices.get(product, 0)
                        product_delivery_rate = product_total_delivery_rates.get(product, 0)
                        
                        if product_avg_price > 0:
                            calc_purchases_total = 1 if (total_purchases == 0 and total_amount_spent_usd > 0) else total_purchases
                            delivery_rate_total = product_delivery_rate / 100 if product_delivery_rate > 1 else product_delivery_rate
                            
                            # Calculate total components
                            total_delivered_orders = calc_purchases_total * delivery_rate_total
                            total_net_revenue = total_delivered_orders * product_avg_price
                            total_product_cost_calc = total_delivered_orders * sum(product_date_cost_inputs.get(product, {}).values()) / len(product_date_cost_inputs.get(product, {})) if product_date_cost_inputs.get(product) else 0
                            total_shipping_cost_calc = calc_purchases_total * shipping_rate
                            total_operational_cost_calc = calc_purchases_total * operational_rate
                            total_net_profit = total_net_revenue - (total_amount_spent_usd * 100) - total_shipping_cost_calc - total_operational_cost_calc - total_product_cost_calc
                            
                            denominator_total = product_avg_price * delivery_rate_total * calc_purchases_total
                            campaign_net_profit_percentage = (total_net_profit / denominator_total * 100) if denominator_total > 0 else 0
                    
                    negative_campaign = {
                        'Product': str(product),
                        'Campaign Name': str(campaign_name),
                        'Total Dates': len(campaign_dates),
                        'Days Checked': selected_days,
                        'Days with Negative Net Profit %': len(negative_profit_data),
                        'CPP': round(cpp, 2),
                        'BE': be,  # FIXED: Use the correct BE value from main sheet
                        'Amount Spent (USD)': round(total_amount_spent_usd, 2),  # NEW: Added amount spent
                        'Net Profit %': round(campaign_net_profit_percentage, 2),  # NEW: Added Net Profit %
                        'Negative Net Profit Dates': formatted_dates_str,
                        'Reason': f"Has {len(negative_profit_data)} negative net profit % days out of {len(campaign_dates)} total days (threshold: {selected_days})"
                    }
                    
                    all_negative_campaigns.append(negative_campaign)

        # SPLIT CAMPAIGNS INTO THREE GROUPS BASED ON NET PROFIT %
        severe_negative_campaigns = []  # Net Profit % <= -1% (between -100% to -1%)
        moderate_negative_campaigns = []  # Net Profit % > -1% and < 0% (between -1% to 0%, excluding 0%)
        positive_campaigns = []  # Net Profit % >= 0% (0% and above)
        
        for campaign in all_negative_campaigns:
            if campaign['Net Profit %'] <= -1:
                severe_negative_campaigns.append(campaign)
            elif campaign['Net Profit %'] < 0:  # > -1% and < 0%
                moderate_negative_campaigns.append(campaign)
            else:  # >= 0%
                positive_campaigns.append(campaign)
        
        # Sort all three groups
        severe_negative_campaigns.sort(key=lambda x: x['Net Profit %'])  # Worst first (most negative)
        moderate_negative_campaigns.sort(key=lambda x: x['Net Profit %'])  # Worst first (most negative)
        positive_campaigns.sort(key=lambda x: x['Net Profit %'], reverse=True)  # Best first (highest positive)

        # ==== PART 1: SEVERE NEGATIVE CAMPAIGNS (-100% to -1%) ====
        current_row = 0
        
        # Headers for severe negative campaigns (with Comment column)
        severe_headers = ["Product", "Campaign Name", "CPP", "BE", "Amount Spent (USD)", "Net Profit %", "Total Dates", "Days Checked", 
                         "Days with Negative Net Profit %", "Negative Net Profit Dates", "Comment", "Reason"]

        safe_write(negative_profit_sheet, current_row, 0, "CAMPAIGNS WITH SEVERE NEGATIVE NET PROFIT % (-100% TO -1%)", negative_profit_header_format)
        current_row += 1

        for col_num, header in enumerate(severe_headers):
            safe_write(negative_profit_sheet, current_row, col_num, header, negative_profit_header_format)
        current_row += 1

        # Write severe negative campaigns
        if severe_negative_campaigns:
            for campaign in severe_negative_campaigns:
                # Calculate daily spend for comment
                daily_spend = campaign['Amount Spent (USD)'] / campaign['Total Dates'] if campaign['Total Dates'] > 0 else 0
                
                # Determine comment based on daily spend
                if daily_spend < 20:
                    comment = "Turn it off"
                else:
                    comment = "Change the bid and keep BE value as add cost"
                
                safe_write(negative_profit_sheet, current_row, 0, campaign['Product'], negative_profit_data_format)
                safe_write(negative_profit_sheet, current_row, 1, campaign['Campaign Name'], negative_profit_data_format)
                safe_write(negative_profit_sheet, current_row, 2, campaign['CPP'], negative_profit_data_format)
                safe_write(negative_profit_sheet, current_row, 3, campaign['BE'], negative_profit_data_format)
                safe_write(negative_profit_sheet, current_row, 4, campaign['Amount Spent (USD)'], negative_profit_data_format)
                safe_write(negative_profit_sheet, current_row, 5, campaign['Net Profit %'], negative_profit_data_format)
                safe_write(negative_profit_sheet, current_row, 6, campaign['Total Dates'], negative_profit_data_format)
                safe_write(negative_profit_sheet, current_row, 7, campaign['Days Checked'], negative_profit_data_format)
                safe_write(negative_profit_sheet, current_row, 8, campaign['Days with Negative Net Profit %'], negative_profit_data_format)
                safe_write(negative_profit_sheet, current_row, 9, campaign['Negative Net Profit Dates'], negative_profit_data_format)
                safe_write(negative_profit_sheet, current_row, 10, comment, negative_profit_data_format)  # NEW: Comment column
                safe_write(negative_profit_sheet, current_row, 11, campaign['Reason'], negative_profit_data_format)
                current_row += 1
        else:
            safe_write(negative_profit_sheet, current_row, 0, f"No campaigns found with severe negative net profit % (-100% to -1%)", negative_profit_data_format)
            current_row += 1

        # Add summary for severe negative campaigns
        current_row += 2
        safe_write(negative_profit_sheet, current_row, 0, "SUMMARY - SEVERE NEGATIVE CAMPAIGNS", negative_profit_header_format)
        safe_write(negative_profit_sheet, current_row + 1, 0, f"Campaigns with severe negative net profit % (-100% to -1%): {len(severe_negative_campaigns)}", negative_profit_data_format)
        current_row += 3

        # ==== PART 2: MODERATE NEGATIVE CAMPAIGNS (-1% to 0%, excluding 0%) ====
        current_row += 2  # Add gap between tables
        
        # Headers for moderate negative campaigns (without Comment column)
        moderate_headers = ["Product", "Campaign Name", "CPP", "BE", "Amount Spent (USD)", "Net Profit %", "Total Dates", "Days Checked", 
                           "Days with Negative Net Profit %", "Negative Net Profit Dates", "Reason"]

        safe_write(negative_profit_sheet, current_row, 0, "CAMPAIGNS WITH MODERATE NEGATIVE NET PROFIT % (-1% TO 0%, EXCLUDING 0%)", moderate_negative_header_format)
        current_row += 1

        for col_num, header in enumerate(moderate_headers):
            safe_write(negative_profit_sheet, current_row, col_num, header, moderate_negative_header_format)
        current_row += 1

        # Write moderate negative campaigns
        if moderate_negative_campaigns:
            for campaign in moderate_negative_campaigns:
                safe_write(negative_profit_sheet, current_row, 0, campaign['Product'], moderate_negative_data_format)
                safe_write(negative_profit_sheet, current_row, 1, campaign['Campaign Name'], moderate_negative_data_format)
                safe_write(negative_profit_sheet, current_row, 2, campaign['CPP'], moderate_negative_data_format)
                safe_write(negative_profit_sheet, current_row, 3, campaign['BE'], moderate_negative_data_format)
                safe_write(negative_profit_sheet, current_row, 4, campaign['Amount Spent (USD)'], moderate_negative_data_format)
                safe_write(negative_profit_sheet, current_row, 5, campaign['Net Profit %'], moderate_negative_data_format)
                safe_write(negative_profit_sheet, current_row, 6, campaign['Total Dates'], moderate_negative_data_format)
                safe_write(negative_profit_sheet, current_row, 7, campaign['Days Checked'], moderate_negative_data_format)
                safe_write(negative_profit_sheet, current_row, 8, campaign['Days with Negative Net Profit %'], moderate_negative_data_format)
                safe_write(negative_profit_sheet, current_row, 9, campaign['Negative Net Profit Dates'], moderate_negative_data_format)
                safe_write(negative_profit_sheet, current_row, 10, campaign['Reason'], moderate_negative_data_format)
                current_row += 1
        else:
            safe_write(negative_profit_sheet, current_row, 0, f"No campaigns found with moderate negative net profit % (-1% to 0%, excluding 0%)", moderate_negative_data_format)
            current_row += 1

        # Add summary for moderate negative campaigns
        current_row += 2
        safe_write(negative_profit_sheet, current_row, 0, "SUMMARY - MODERATE NEGATIVE CAMPAIGNS", moderate_negative_header_format)
        safe_write(negative_profit_sheet, current_row + 1, 0, f"Campaigns with moderate negative net profit % (-1% to 0%, excluding 0%): {len(moderate_negative_campaigns)}", moderate_negative_data_format)
        
        # ==== PART 3: POSITIVE CAMPAIGNS (0% AND ABOVE) ====
        current_row += 5  # Add gap between tables
        
        # Headers for positive campaigns table
        positive_headers = ["Product", "Campaign Name", "CPP", "BE", "Amount Spent (USD)", "Net Profit %", "Total Dates", "Days Checked", 
                           "Days with Negative Net Profit %", "Negative Net Profit Dates", "Reason"]

        safe_write(negative_profit_sheet, current_row, 0, "CAMPAIGNS WITH POSITIVE NET PROFIT % (0% AND ABOVE)", positive_header_format)
        current_row += 1

        for col_num, header in enumerate(positive_headers):
            safe_write(negative_profit_sheet, current_row, col_num, header, positive_header_format)
        current_row += 1

        # Write positive campaigns
        if positive_campaigns:
            for campaign in positive_campaigns:
                safe_write(negative_profit_sheet, current_row, 0, campaign['Product'], positive_data_format)
                safe_write(negative_profit_sheet, current_row, 1, campaign['Campaign Name'], positive_data_format)
                safe_write(negative_profit_sheet, current_row, 2, campaign['CPP'], positive_data_format)
                safe_write(negative_profit_sheet, current_row, 3, campaign['BE'], positive_data_format)
                safe_write(negative_profit_sheet, current_row, 4, campaign['Amount Spent (USD)'], positive_data_format)
                safe_write(negative_profit_sheet, current_row, 5, campaign['Net Profit %'], positive_data_format)
                safe_write(negative_profit_sheet, current_row, 6, campaign['Total Dates'], positive_data_format)
                safe_write(negative_profit_sheet, current_row, 7, campaign['Days Checked'], positive_data_format)
                safe_write(negative_profit_sheet, current_row, 8, campaign['Days with Negative Net Profit %'], positive_data_format)
                safe_write(negative_profit_sheet, current_row, 9, campaign['Negative Net Profit Dates'], positive_data_format)
                safe_write(negative_profit_sheet, current_row, 10, campaign['Reason'], positive_data_format)
                current_row += 1
        else:
            safe_write(negative_profit_sheet, current_row, 0, f"No campaigns found with positive net profit % (0% and above)", positive_data_format)
            current_row += 1

        # Add summary for positive campaigns
        current_row += 2
        safe_write(negative_profit_sheet, current_row, 0, "SUMMARY - POSITIVE CAMPAIGNS", positive_header_format)
        safe_write(negative_profit_sheet, current_row + 1, 0, f"Campaigns with positive net profit % (0% and above): {len(positive_campaigns)}", positive_data_format)
        
        # ==== PART 4: LAST DATE NEGATIVE NET PROFIT CAMPAIGNS ====
        current_row += 5  # Add gap between tables
        
        # Headers for last date table
        safe_write(negative_profit_sheet, current_row, 0, "CAMPAIGNS WITH NEGATIVE NET PROFIT % ON LAST DATE", last_date_header_format)
        current_row += 1
        
        # Headers for last date table
        last_date_headers = ["Product", "Campaign Name", "CPP", "BE", "Amount Spent (USD)", "Net Profit %", "Last Date", "Last Date Net Profit %", 
                           "Last Date Amount Spent (USD)", "Last Date Purchases", "Reason"]

        for col_num, header in enumerate(last_date_headers):
            safe_write(negative_profit_sheet, current_row, col_num, header, last_date_header_format)
        current_row += 1

        # Get the last date
        last_date = unique_dates[-1] if unique_dates else None
        
        # Create set of campaigns already in first three tables to avoid duplicates
        already_processed_campaigns = set()
        for campaign in all_negative_campaigns:
            already_processed_campaigns.add((campaign['Product'], campaign['Campaign Name']))

        # Analyze campaigns for last date negative net profit (using filtered data)
        last_date_negative_campaigns = []
        
        if last_date:
            for product, product_df in df_main.groupby("Product"):
                for campaign_name, campaign_group in product_df.groupby("Campaign Name"):
                    # Skip if this campaign is already in the first three tables
                    if (str(product), str(campaign_name)) in already_processed_campaigns:
                        continue
                    
                    # Check if this campaign has data for the last date
                    last_date_data = campaign_group[campaign_group['Date'].astype(str) == last_date]
                    if last_date_data.empty:
                        continue
                    
                    row_data = last_date_data.iloc[0]
                    
                    # Calculate CPP and Amount Spent for this campaign
                    total_amount_spent_usd = campaign_group.get("Amount Spent (USD)", 0).sum() if "Amount Spent (USD)" in campaign_group.columns else 0
                    total_purchases = campaign_group.get("Purchases", 0).sum() if "Purchases" in campaign_group.columns else 0
                    
                    # Calculate CPP (Cost Per Purchase)
                    cpp = 0
                    if total_amount_spent_usd > 0 and total_purchases == 0:
                        cpp = total_amount_spent_usd / 1  # Use 1 for formula purposes when no purchases but has spending
                    elif total_purchases > 0:
                        cpp = total_amount_spent_usd / total_purchases
                    
                    # GET BE VALUE FROM THE PRODUCT-LEVEL LOOKUP
                    be = product_be_values.get(product, 0)
                    
                    # Calculate overall Net Profit % for this campaign (same as first tables)
                    campaign_net_profit_percentage = 0
                    if total_amount_spent_usd > 0 or total_purchases > 0:
                        product_avg_price = product_total_avg_prices.get(product, 0)
                        product_delivery_rate = product_total_delivery_rates.get(product, 0)
                        
                        if product_avg_price > 0:
                            calc_purchases_total = 1 if (total_purchases == 0 and total_amount_spent_usd > 0) else total_purchases
                            delivery_rate_total = product_delivery_rate / 100 if product_delivery_rate > 1 else product_delivery_rate
                            
                            total_delivered_orders = calc_purchases_total * delivery_rate_total
                            total_net_revenue = total_delivered_orders * product_avg_price
                            total_product_cost_calc = total_delivered_orders * sum(product_date_cost_inputs.get(product, {}).values()) / len(product_date_cost_inputs.get(product, {})) if product_date_cost_inputs.get(product) else 0
                            total_shipping_cost_calc = calc_purchases_total * shipping_rate
                            total_operational_cost_calc = calc_purchases_total * operational_rate
                            total_net_profit = total_net_revenue - (total_amount_spent_usd * 100) - total_shipping_cost_calc - total_operational_cost_calc - total_product_cost_calc
                            
                            denominator_total = product_avg_price * delivery_rate_total * calc_purchases_total
                            campaign_net_profit_percentage = (total_net_profit / denominator_total * 100) if denominator_total > 0 else 0
                    
                    # Get data for last date net profit percentage calculation
                    amount_spent = row_data.get("Amount Spent (USD)", 0) or 0
                    purchases = row_data.get("Purchases", 0) or 0
                    
                    # Get day-wise lookup data for last date
                    date_avg_price = product_date_avg_prices.get(product, {}).get(last_date, 0)
                    date_delivery_rate = product_date_delivery_rates.get(product, {}).get(last_date, 0)
                    date_product_cost = product_date_cost_inputs.get(product, {}).get(last_date, 0)
                    
                    # Calculate net profit percentage for last date
                    if date_avg_price > 0 and (purchases > 0 or (purchases == 0 and amount_spent > 0)):
                        # Handle zero purchases case - use 1 when amount spent > 0 but purchases = 0
                        calc_purchases = 1 if (purchases == 0 and amount_spent > 0) else purchases
                        
                        # Delivery rate conversion
                        delivery_rate = date_delivery_rate / 100 if date_delivery_rate > 1 else date_delivery_rate
                        
                        # Calculate components for net profit percentage
                        delivered_orders = calc_purchases * delivery_rate
                        net_revenue = delivered_orders * date_avg_price
                        total_product_cost = delivered_orders * date_product_cost
                        total_shipping_cost = calc_purchases * shipping_rate
                        total_operational_cost = calc_purchases * operational_rate
                        net_profit = net_revenue - (amount_spent * 100) - total_shipping_cost - total_operational_cost - total_product_cost
                        
                        # Net Profit Percentage = Net Profit / (Avg Price * Delivery Rate * Purchases) * 100
                        denominator = date_avg_price * delivery_rate * calc_purchases
                        net_profit_percentage = (net_profit / denominator * 100) if denominator > 0 else 0
                        
                        # Check if net profit percentage is negative
                        if net_profit_percentage < 0:
                            last_date_campaign = {
                                'Product': str(product),
                                'Campaign Name': str(campaign_name),
                                'CPP': round(cpp, 2),
                                'BE': be,
                                'Amount Spent (USD)': round(total_amount_spent_usd, 2),
                                'Net Profit %': round(campaign_net_profit_percentage, 2),
                                'Last Date': format_date_readable(last_date),
                                'Last Date Net Profit %': round(net_profit_percentage, 2),
                                'Last Date Amount Spent (USD)': round(amount_spent, 2),
                                'Last Date Purchases': int(purchases),
                                'Reason': f"Negative net profit % ({round(net_profit_percentage, 2)}%) on last date ({format_date_readable(last_date)})"
                            }
                            
                            last_date_negative_campaigns.append(last_date_campaign)

        # Write last date negative campaigns to sheet (FOURTH TABLE)
        if last_date_negative_campaigns:
            # Sort by net profit percentage (worst first)
            last_date_negative_campaigns.sort(key=lambda x: x['Last Date Net Profit %'])
            
            for campaign in last_date_negative_campaigns:
                safe_write(negative_profit_sheet, current_row, 0, campaign['Product'], last_date_data_format)
                safe_write(negative_profit_sheet, current_row, 1, campaign['Campaign Name'], last_date_data_format)
                safe_write(negative_profit_sheet, current_row, 2, campaign['CPP'], last_date_data_format)
                safe_write(negative_profit_sheet, current_row, 3, campaign['BE'], last_date_data_format)
                safe_write(negative_profit_sheet, current_row, 4, campaign['Amount Spent (USD)'], last_date_data_format)
                safe_write(negative_profit_sheet, current_row, 5, campaign['Net Profit %'], last_date_data_format)
                safe_write(negative_profit_sheet, current_row, 6, campaign['Last Date'], last_date_data_format)
                safe_write(negative_profit_sheet, current_row, 7, campaign['Last Date Net Profit %'], last_date_data_format)
                safe_write(negative_profit_sheet, current_row, 8, campaign['Last Date Amount Spent (USD)'], last_date_data_format)
                safe_write(negative_profit_sheet, current_row, 9, campaign['Last Date Purchases'], last_date_data_format)
                safe_write(negative_profit_sheet, current_row, 10, campaign['Reason'], last_date_data_format)
                current_row += 1
        else:
            safe_write(negative_profit_sheet, current_row, 0, f"No campaigns found with negative net profit % on last date ({format_date_readable(last_date) if last_date else 'N/A'})", last_date_data_format)
            current_row += 1

        # Add summary for last date table
        current_row += 2
        safe_write(negative_profit_sheet, current_row, 0, "SUMMARY - LAST DATE TABLE", last_date_header_format)
        safe_write(negative_profit_sheet, current_row + 1, 0, f"Last date analyzed: {format_date_readable(last_date) if last_date else 'N/A'}", last_date_data_format)
        safe_write(negative_profit_sheet, current_row + 2, 0, f"Campaigns with negative net profit % on last date: {len(last_date_negative_campaigns)}", last_date_data_format)
        safe_write(negative_profit_sheet, current_row + 3, 0, f"Campaigns excluded (already in other tables): {len(already_processed_campaigns)}", last_date_data_format)
        
        # Add overall summary
        current_row += 5
        safe_write(negative_profit_sheet, current_row, 0, "OVERALL SUMMARY", negative_profit_header_format)
        
        # Count total campaigns correctly (using filtered data)
        total_campaigns = 0
        for product, product_df in df_main.groupby("Product"):
            total_campaigns += len(product_df.groupby("Campaign Name"))

        safe_write(negative_profit_sheet, current_row + 1, 0, f"Total campaigns analyzed: {total_campaigns}", negative_profit_data_format)
        safe_write(negative_profit_sheet, current_row + 2, 0, f"Total campaigns with negative net profit %: {len(all_negative_campaigns)}", negative_profit_data_format)
        safe_write(negative_profit_sheet, current_row + 3, 0, f"Severe negative campaigns (-100% to -1%): {len(severe_negative_campaigns)}", negative_profit_data_format)
        safe_write(negative_profit_sheet, current_row + 4, 0, f"Moderate negative campaigns (-1% to 0%, excluding 0%): {len(moderate_negative_campaigns)}", moderate_negative_data_format)
        safe_write(negative_profit_sheet, current_row + 5, 0, f"Positive campaigns (0% and above): {len(positive_campaigns)}", positive_data_format)
        safe_write(negative_profit_sheet, current_row + 6, 0, f"Last date negative campaigns: {len(last_date_negative_campaigns)}", last_date_data_format)
        safe_write(negative_profit_sheet, current_row + 7, 0, f"Days threshold used: {selected_days} out of {len(unique_dates)} total unique dates", negative_profit_data_format)
        safe_write(negative_profit_sheet, current_row + 8, 0, f"Date range analyzed: {min(unique_dates)} to {max(unique_dates)}" if unique_dates else "No dates found", negative_profit_data_format)

        # Set column widths for negative net profit sheet
        negative_profit_sheet.set_column(0, 0, 20)  # Product
        negative_profit_sheet.set_column(1, 1, 35)  # Campaign Name
        negative_profit_sheet.set_column(2, 2, 15)  # CPP
        negative_profit_sheet.set_column(3, 3, 15)  # BE
        negative_profit_sheet.set_column(4, 4, 20)  # Amount Spent (USD)
        negative_profit_sheet.set_column(5, 5, 15)  # Net Profit %
        negative_profit_sheet.set_column(6, 6, 15)  # Total Dates / Last Date
        negative_profit_sheet.set_column(7, 7, 15)  # Days Checked / Last Date Net Profit %
        negative_profit_sheet.set_column(8, 8, 25)  # Days with Negative Net Profit % / Last Date Amount Spent
        negative_profit_sheet.set_column(9, 9, 40)  # Negative Net Profit Dates / Last Date Purchases
        negative_profit_sheet.set_column(10, 10, 40)  # Comment / Reason
        negative_profit_sheet.set_column(11, 11, 40)  # Reason (for severe campaigns only)
        
        # ==== NEW SHEET: Profit and Loss Products ====
        # SHEET 1: POSITIVE NET PROFIT PRODUCTS
        positive_profit_sheet = workbook.add_worksheet("Positive Net Profit Products")
        
        # Formats for positive profit sheet
        positive_profit_header_format = workbook.add_format({
            "bold": True, "align": "center", "valign": "vcenter",
            "fg_color": "#4CAF50", "font_name": "Calibri", "font_size": 11
        })
        positive_profit_data_format = workbook.add_format({
            "align": "left", "valign": "vcenter",
            "fg_color": "#E8F5E8", "font_name": "Calibri", "font_size": 11,
            "num_format": "#,##0.00"
        })
        
        # Headers for positive profit sheet
        positive_profit_headers = ["Product Name", "Total Product Cost Input", "Total Net Profit"]
        
        for col_num, header in enumerate(positive_profit_headers):
            safe_write(positive_profit_sheet, 0, col_num, header, positive_profit_header_format)
        
        # Filter and sort positive products by net profit (highest to lowest)
        positive_products = [(product, net_profit) for product, net_profit in product_net_profit_values.items() if net_profit >= 0]
        positive_products.sort(key=lambda x: x[1], reverse=True)
        
        # Write positive product data
        current_row = 1
        for product, net_profit in positive_products:
            cost_input = product_cost_input_values.get(product, 0)
            
            safe_write(positive_profit_sheet, current_row, 0, str(product), positive_profit_data_format)
            safe_write(positive_profit_sheet, current_row, 1, cost_input, positive_profit_data_format)
            safe_write(positive_profit_sheet, current_row, 2, net_profit, positive_profit_data_format)
            current_row += 1
        
        # Add summary for positive products
        current_row += 2
        safe_write(positive_profit_sheet, current_row, 0, "SUMMARY - POSITIVE NET PROFIT PRODUCTS", positive_profit_header_format)
        current_row += 1
        
        total_positive_products = len(positive_products)
        total_positive_net_profit = sum([profit for _, profit in positive_products])
        total_positive_cost_input = sum([product_cost_input_values.get(product, 0) for product, _ in positive_products])
        avg_positive_net_profit = total_positive_net_profit / total_positive_products if total_positive_products > 0 else 0
        avg_positive_cost_input = total_positive_cost_input / total_positive_products if total_positive_products > 0 else 0
        
        safe_write(positive_profit_sheet, current_row, 0, f"Total Positive Products: {total_positive_products}", positive_profit_data_format)
        safe_write(positive_profit_sheet, current_row + 1, 0, f"Total Net Profit (Positive Products): {round(total_positive_net_profit, 2)}", positive_profit_data_format)
        safe_write(positive_profit_sheet, current_row + 2, 0, f"Total Product Cost Input (Positive Products): {round(total_positive_cost_input, 2)}", positive_profit_data_format)
        safe_write(positive_profit_sheet, current_row + 3, 0, f"Average Net Profit per Product: {round(avg_positive_net_profit, 2)}", positive_profit_data_format)
        safe_write(positive_profit_sheet, current_row + 4, 0, f"Average Product Cost Input per Product: {round(avg_positive_cost_input, 2)}", positive_profit_data_format)
        
        # Set column widths for positive profit sheet
        positive_profit_sheet.set_column(0, 0, 30)  # Product Name
        positive_profit_sheet.set_column(1, 1, 25)  # Total Product Cost Input
        positive_profit_sheet.set_column(2, 2, 20)  # Total Net Profit
        
        # SHEET 2: NEGATIVE NET PROFIT PRODUCTS WITH RATIO AND CONDITIONAL SPLITTING
        negative_profit_sheet_products = workbook.add_worksheet("Negative Net Profit Products")
        
        # Formats for negative profit sheet
        negative_profit_header_format_products = workbook.add_format({
            "bold": True, "align": "center", "valign": "vcenter",
            "fg_color": "#FF6B6B", "font_name": "Calibri", "font_size": 11
        })
        negative_profit_data_format_products = workbook.add_format({
            "align": "left", "valign": "vcenter",
            "fg_color": "#FFEBEE", "font_name": "Calibri", "font_size": 11,
            "num_format": "#,##0.00",
            "font_color": "#D32F2F"
        })
        # Format for products with ratio >= 0.2
        moderate_negative_format_products = workbook.add_format({
            "bold": True, "align": "center", "valign": "vcenter",
            "fg_color": "#FFA500", "font_name": "Calibri", "font_size": 11
        })
        moderate_negative_data_format_products = workbook.add_format({
            "align": "left", "valign": "vcenter",
            "fg_color": "#FFF3E0", "font_name": "Calibri", "font_size": 11,
            "num_format": "#,##0.00",
            "font_color": "#F57C00"
        })
        
        # Filter negative products and calculate ratio
        negative_products_with_ratio = []
        for product, net_profit in product_net_profit_values.items():
            if net_profit < 0:
                cost_input = product_cost_input_values.get(product, 0)
                # Calculate ratio: Total Net Profit / Total Product Cost Input
                if cost_input != 0:
                    ratio = net_profit / cost_input
                else:
                    ratio = 0  # Handle division by zero
                
                negative_products_with_ratio.append({
                    'product': product,
                    'cost_input': cost_input,
                    'net_profit': net_profit,
                    'ratio': ratio
                })
        
        # Sort by ratio (worst first - most negative ratios first)
        negative_products_with_ratio.sort(key=lambda x: x['ratio'])
        
        # Split into two groups based on ratio threshold (0.2)
        ratio_less_than_02 = [p for p in negative_products_with_ratio if p['ratio'] < 0.2]
        ratio_greater_equal_02 = [p for p in negative_products_with_ratio if p['ratio'] >= 0.2]
        
        current_row = 0
        
        # TABLE 1: Products with ratio < 0.2 (worse performance)
        safe_write(negative_profit_sheet_products, current_row, 0, "NEGATIVE NET PROFIT PRODUCTS - RATIO < 0.2 (CRITICAL)", negative_profit_header_format_products)
        current_row += 1
        
        # Headers with ratio column
        negative_headers_with_ratio = ["Product Name", "Total Product Cost Input", "Total Net Profit", "Net Profit / Cost Input Ratio"]
        
        for col_num, header in enumerate(negative_headers_with_ratio):
            safe_write(negative_profit_sheet_products, current_row, col_num, header, negative_profit_header_format_products)
        current_row += 1
        
        # Write products with ratio < 0.2
        if ratio_less_than_02:
            for product_data in ratio_less_than_02:
                safe_write(negative_profit_sheet_products, current_row, 0, str(product_data['product']), negative_profit_data_format_products)
                safe_write(negative_profit_sheet_products, current_row, 1, product_data['cost_input'], negative_profit_data_format_products)
                safe_write(negative_profit_sheet_products, current_row, 2, product_data['net_profit'], negative_profit_data_format_products)
                safe_write(negative_profit_sheet_products, current_row, 3, round(product_data['ratio'], 4), negative_profit_data_format_products)
                current_row += 1
        else:
            safe_write(negative_profit_sheet_products, current_row, 0, "No products found with ratio < 0.2", negative_profit_data_format_products)
            current_row += 1
        
        # Add summary for ratio < 0.2
        current_row += 2
        safe_write(negative_profit_sheet_products, current_row, 0, "SUMMARY - RATIO < 0.2 (CRITICAL)", negative_profit_header_format_products)
        current_row += 1
        
        total_critical_products = len(ratio_less_than_02)
        total_critical_net_profit = sum([p['net_profit'] for p in ratio_less_than_02])
        total_critical_cost_input = sum([p['cost_input'] for p in ratio_less_than_02])
        avg_critical_ratio = sum([p['ratio'] for p in ratio_less_than_02]) / total_critical_products if total_critical_products > 0 else 0
        
        safe_write(negative_profit_sheet_products, current_row, 0, f"Products with ratio < 0.2: {total_critical_products}", negative_profit_data_format_products)
        safe_write(negative_profit_sheet_products, current_row + 1, 0, f"Total Net Profit (Critical): {round(total_critical_net_profit, 2)}", negative_profit_data_format_products)
        safe_write(negative_profit_sheet_products, current_row + 2, 0, f"Total Product Cost Input (Critical): {round(total_critical_cost_input, 2)}", negative_profit_data_format_products)
        safe_write(negative_profit_sheet_products, current_row + 3, 0, f"Average Ratio (Critical): {round(avg_critical_ratio, 4)}", negative_profit_data_format_products)
        
        # TABLE 2: Products with ratio >= 0.2 (better performance among negatives)
        current_row += 7  # Add gap between tables
        
        safe_write(negative_profit_sheet_products, current_row, 0, "NEGATIVE NET PROFIT PRODUCTS - RATIO >= 0.2 (MODERATE)", moderate_negative_format_products)
        current_row += 1
        
        # Headers for second table
        for col_num, header in enumerate(negative_headers_with_ratio):
            safe_write(negative_profit_sheet_products, current_row, col_num, header, moderate_negative_format_products)
        current_row += 1
        
        # Write products with ratio >= 0.2
        if ratio_greater_equal_02:
            for product_data in ratio_greater_equal_02:
                safe_write(negative_profit_sheet_products, current_row, 0, str(product_data['product']), moderate_negative_data_format_products)
                safe_write(negative_profit_sheet_products, current_row, 1, product_data['cost_input'], moderate_negative_data_format_products)
                safe_write(negative_profit_sheet_products, current_row, 2, product_data['net_profit'], moderate_negative_data_format_products)
                safe_write(negative_profit_sheet_products, current_row, 3, round(product_data['ratio'], 4), moderate_negative_data_format_products)
                current_row += 1
        else:
            safe_write(negative_profit_sheet_products, current_row, 0, "No products found with ratio >= 0.2", moderate_negative_data_format_products)
            current_row += 1
        
        # Add summary for ratio >= 0.2
        current_row += 2
        safe_write(negative_profit_sheet_products, current_row, 0, "SUMMARY - RATIO >= 0.2 (MODERATE)", moderate_negative_format_products)
        current_row += 1
        
        total_moderate_products = len(ratio_greater_equal_02)
        total_moderate_net_profit = sum([p['net_profit'] for p in ratio_greater_equal_02])
        total_moderate_cost_input = sum([p['cost_input'] for p in ratio_greater_equal_02])
        avg_moderate_ratio = sum([p['ratio'] for p in ratio_greater_equal_02]) / total_moderate_products if total_moderate_products > 0 else 0
        
        safe_write(negative_profit_sheet_products, current_row, 0, f"Products with ratio >= 0.2: {total_moderate_products}", moderate_negative_data_format_products)
        safe_write(negative_profit_sheet_products, current_row + 1, 0, f"Total Net Profit (Moderate): {round(total_moderate_net_profit, 2)}", moderate_negative_data_format_products)
        safe_write(negative_profit_sheet_products, current_row + 2, 0, f"Total Product Cost Input (Moderate): {round(total_moderate_cost_input, 2)}", moderate_negative_data_format_products)
        safe_write(negative_profit_sheet_products, current_row + 3, 0, f"Average Ratio (Moderate): {round(avg_moderate_ratio, 4)}", moderate_negative_data_format_products)
        
        # OVERALL SUMMARY FOR ALL NEGATIVE PRODUCTS
        current_row += 7
        safe_write(negative_profit_sheet_products, current_row, 0, "OVERALL SUMMARY - ALL NEGATIVE PRODUCTS", negative_profit_header_format_products)
        current_row += 1
        
        total_negative_products = len(negative_products_with_ratio)
        total_all_negative_net_profit = sum([p['net_profit'] for p in negative_products_with_ratio])
        total_all_negative_cost_input = sum([p['cost_input'] for p in negative_products_with_ratio])
        avg_all_negative_ratio = sum([p['ratio'] for p in negative_products_with_ratio]) / total_negative_products if total_negative_products > 0 else 0
        
        safe_write(negative_profit_sheet_products, current_row, 0, f"Total Negative Products: {total_negative_products}", negative_profit_data_format_products)
        safe_write(negative_profit_sheet_products, current_row + 1, 0, f"Total Net Profit (All Negative): {round(total_all_negative_net_profit, 2)}", negative_profit_data_format_products)
        safe_write(negative_profit_sheet_products, current_row + 2, 0, f"Total Product Cost Input (All Negative): {round(total_all_negative_cost_input, 2)}", negative_profit_data_format_products)
        safe_write(negative_profit_sheet_products, current_row + 3, 0, f"Average Ratio (All Negative): {round(avg_all_negative_ratio, 4)}", negative_profit_data_format_products)
        safe_write(negative_profit_sheet_products, current_row + 4, 0, f"Critical Products (ratio < 0.2): {total_critical_products}", negative_profit_data_format_products)
        safe_write(negative_profit_sheet_products, current_row + 5, 0, f"Moderate Products (ratio >= 0.2): {total_moderate_products}", moderate_negative_data_format_products)
        
        # Set column widths for negative profit sheet
        negative_profit_sheet_products.set_column(0, 0, 30)  # Product Name
        negative_profit_sheet_products.set_column(1, 1, 25)  # Total Product Cost Input
        negative_profit_sheet_products.set_column(2, 2, 20)  # Total Net Profit
        negative_profit_sheet_products.set_column(3, 3, 25)  # Net Profit / Cost Input Ratio
        
    return output.getvalue()




# ---- DOWNLOAD SECTIONS ----
st.header("📥 Download Processed Files")

# ---- SHOPIFY DOWNLOAD ----
if df_shopify is not None:
    export_df = df_shopify.drop(columns=["Product Name", "Canonical Product"], errors="ignore")

    # Use new date-column structure if dates are present
    has_dates = 'Date' in export_df.columns
    if has_dates:
        shopify_excel = convert_shopify_to_excel_with_date_columns_fixed(export_df)
        button_label = "📥 Download Shopify File with Date Columns & Excel Formulas (Excel)"
        file_name = "shopify_date_columns_with_formulas_FIXED.xlsx"
    else:
        shopify_excel = convert_shopify_to_excel(export_df)
        button_label = "📥 Download admin Shopify File "
        file_name = "processed_shopify_merged.xlsx"
    
    st.download_button(
        label=button_label,
        data=shopify_excel,
        file_name=file_name,
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )
else:
    st.warning("⚠️ Please upload Shopify files to process.")

# ---- CAMPAIGN DOWNLOAD ----
if campaign_files:
    def convert_df_to_excel(df):
        output = BytesIO()
        with pd.ExcelWriter(output, engine="openpyxl") as writer:
            df.to_excel(writer, index=False, sheet_name="Processed Data")
        return output.getvalue()

    

    # Download final campaign data (structured format like Shopify)
    if 'df_final_campaign' in locals() and not df_final_campaign.empty:
        # Use new date-column structure if dates are present
        has_dates = 'Date' in df_final_campaign.columns
        if has_dates:
            final_campaign_excel = convert_final_campaign_to_excel_with_date_columns_fixed(df_final_campaign)
            button_label = "🎯 Download Campaign File with Date Columns & Excel Formulas (Excel)"
            file_name = "campaign_date_columns_with_formulas_FIXED.xlsx"
        else:
            final_campaign_excel = convert_final_campaign_to_excel(df_final_campaign, selected_days=selected_days)
            button_label = "🎯 Download admin Campaign File "
            file_name = "final_campaign_data_merged.xlsx"
        
        if final_campaign_excel:
            st.download_button(
                label=button_label,
                data=final_campaign_excel,
                file_name=file_name,
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )

# ---- SUMMARY SECTION ----
if campaign_files or shopify_files or old_merged_files:
    st.header("📊 Processing Summary")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.metric("Campaign Files Uploaded", len(campaign_files) if campaign_files else 0)
        if df_campaign is not None:
            st.metric("Total Campaigns", len(df_campaign))
    
    with col2:
        st.metric("Shopify Files Uploaded", len(shopify_files) if shopify_files else 0)
        if df_shopify is not None:
            st.metric("Total Product Variants", len(df_shopify))
    
    with col3:
        st.metric("Reference Files Uploaded", len(old_merged_files) if old_merged_files else 0)
        if df_old_merged is not None:
            st.metric("Reference Records", len(df_old_merged))

    # Show date information
    if df_shopify is not None and 'Date' in df_shopify.columns:
        unique_dates = df_shopify['Date'].unique()
        unique_dates = [str(d) for d in unique_dates if pd.notna(d) and str(d).strip() != '']
        st.info(f"📅 Found {len(unique_dates)} unique dates: {', '.join(sorted(unique_dates)[:5])}{'...' if len(unique_dates) > 5 else ''}")



                                'Product': str(product),
                                'Campaign Name': str(campaign_name),
                                'CPP': round(cpp, 2),
                                'BE': be,
                                'Amount Spent (USD)': round(total_amount_spent_usd, 2),  # NEW: Added total amount spent
                                'Net Profit %': round(campaign_net_profit_percentage, 2),  # NEW: Added Net Profit %
                                'Last Date': format_date_readable(last_date),
                                'Last Date Net Profit %': round(net_profit_percentage, 2),
                                'Last Date Amount Spent (USD)': round(amount_spent, 2),
                                'Last Date Purchases': int(purchases),
                                'Reason': f"Negative net profit % ({round(net_profit_percentage, 2)}%) on last date ({format_date_readable(last_date)})"
                            }
                            
                            last_date_negative_campaigns.append(last_date_campaign)

        # Write last date negative campaigns to sheet (SECOND TABLE)
        if last_date_negative_campaigns:
            # Sort by net profit percentage (worst first)
            last_date_negative_campaigns.sort(key=lambda x: x['Last Date Net Profit %'])
            
            for campaign in last_date_negative_campaigns:
                safe_write(negative_profit_sheet, current_row, 0, campaign['Product'], last_date_data_format)
                safe_write(negative_profit_sheet, current_row, 1, campaign['Campaign Name'], last_date_data_format)
                safe_write(negative_profit_sheet, current_row, 2, campaign['CPP'], last_date_data_format)
                safe_write(negative_profit_sheet, current_row, 3, campaign['BE'], last_date_data_format)
                safe_write(negative_profit_sheet, current_row, 4, campaign['Amount Spent (USD)'], last_date_data_format)  # NEW: Total Amount Spent
                safe_write(negative_profit_sheet, current_row, 5, campaign['Net Profit %'], last_date_data_format)  # NEW: Net Profit %
                safe_write(negative_profit_sheet, current_row, 6, campaign['Last Date'], last_date_data_format)
                safe_write(negative_profit_sheet, current_row, 7, campaign['Last Date Net Profit %'], last_date_data_format)
                safe_write(negative_profit_sheet, current_row, 8, campaign['Last Date Amount Spent (USD)'], last_date_data_format)
                safe_write(negative_profit_sheet, current_row, 9, campaign['Last Date Purchases'], last_date_data_format)
                safe_write(negative_profit_sheet, current_row, 10, campaign['Reason'], last_date_data_format)
                current_row += 1
        else:
            # No last date negative campaigns found
            safe_write(negative_profit_sheet, current_row, 0, f"No campaigns found with negative net profit % on last date ({format_date_readable(last_date) if last_date else 'N/A'})", last_date_data_format)
            current_row += 1

        # Add summary for second table
        safe_write(negative_profit_sheet, current_row + 1, 0, "SUMMARY - LAST DATE TABLE", last_date_header_format)
        safe_write(negative_profit_sheet, current_row + 2, 0, f"Last date analyzed: {format_date_readable(last_date) if last_date else 'N/A'}", last_date_data_format)
        safe_write(negative_profit_sheet, current_row + 3, 0, f"Campaigns with negative net profit % on last date: {len(last_date_negative_campaigns)}", last_date_data_format)
        safe_write(negative_profit_sheet, current_row + 4, 0, f"Campaigns excluded (already in days checked table): {len(first_table_campaigns)}", last_date_data_format)

        # Set column widths for negative net profit sheet - UPDATED for new Net Profit % column
        negative_profit_sheet.set_column(0, 0, 20)  # Product
        negative_profit_sheet.set_column(1, 1, 35)  # Campaign Name
        negative_profit_sheet.set_column(2, 2, 15)  # CPP
        negative_profit_sheet.set_column(3, 3, 15)  # BE
        negative_profit_sheet.set_column(4, 4, 20)  # Amount Spent (USD)
        negative_profit_sheet.set_column(5, 5, 15)  # Net Profit %
        negative_profit_sheet.set_column(6, 6, 15)  # Total Dates / Last Date
        negative_profit_sheet.set_column(7, 7, 15)  # Days Checked / Last Date Net Profit %
        negative_profit_sheet.set_column(8, 8, 25)  # Days with Negative Net Profit % / Last Date Amount Spent
        negative_profit_sheet.set_column(9, 9, 40)  # Negative Net Profit Dates / Last Date Purchases
        negative_profit_sheet.set_column(10, 10, 40)  # Reason
        
    return output.getvalue()



# ---- DOWNLOAD SECTIONS ----
st.header("📥 Download Processed Files")

# ---- SHOPIFY DOWNLOAD ----
if df_shopify is not None:
    export_df = df_shopify.drop(columns=["Product Name", "Canonical Product"], errors="ignore")

    # Use new date-column structure if dates are present
    has_dates = 'Date' in export_df.columns
    if has_dates:
        shopify_excel = convert_shopify_to_excel_with_date_columns_fixed(export_df)
        button_label = "📥 Download Shopify File with Date Columns & Excel Formulas (Excel)"
        file_name = "shopify_date_columns_with_formulas_FIXED.xlsx"
    else:
        shopify_excel = convert_shopify_to_excel(export_df)
        button_label = "📥 Download admin Shopify File "
        file_name = "processed_shopify_merged.xlsx"
    
    st.download_button(
        label=button_label,
        data=shopify_excel,
        file_name=file_name,
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )
else:
    st.warning("⚠️ Please upload Shopify files to process.")

# ---- CAMPAIGN DOWNLOAD ----
if campaign_files:
    def convert_df_to_excel(df):
        output = BytesIO()
        with pd.ExcelWriter(output, engine="openpyxl") as writer:
            df.to_excel(writer, index=False, sheet_name="Processed Data")
        return output.getvalue()

    

    # Download final campaign data (structured format like Shopify)
    if 'df_final_campaign' in locals() and not df_final_campaign.empty:
        # Use new date-column structure if dates are present
        has_dates = 'Date' in df_final_campaign.columns
        if has_dates:
            final_campaign_excel = convert_final_campaign_to_excel_with_date_columns_fixed(df_final_campaign)
            button_label = "🎯 Download Campaign File with Date Columns & Excel Formulas (Excel)"
            file_name = "campaign_date_columns_with_formulas_FIXED.xlsx"
        else:
            final_campaign_excel = convert_final_campaign_to_excel(df_final_campaign, selected_days=selected_days)
            button_label = "🎯 Download admin Campaign File "
            file_name = "final_campaign_data_merged.xlsx"
        
        if final_campaign_excel:
            st.download_button(
                label=button_label,
                data=final_campaign_excel,
                file_name=file_name,
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )

# ---- SUMMARY SECTION ----
if campaign_files or shopify_files or old_merged_files:
    st.header("📊 Processing Summary")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.metric("Campaign Files Uploaded", len(campaign_files) if campaign_files else 0)
        if df_campaign is not None:
            st.metric("Total Campaigns", len(df_campaign))
    
    with col2:
        st.metric("Shopify Files Uploaded", len(shopify_files) if shopify_files else 0)
        if df_shopify is not None:
            st.metric("Total Product Variants", len(df_shopify))
    
    with col3:
        st.metric("Reference Files Uploaded", len(old_merged_files) if old_merged_files else 0)
        if df_old_merged is not None:
            st.metric("Reference Records", len(df_old_merged))

    # Show date information
    if df_shopify is not None and 'Date' in df_shopify.columns:
        unique_dates = df_shopify['Date'].unique()
        unique_dates = [str(d) for d in unique_dates if pd.notna(d) and str(d).strip() != '']
        st.info(f"📅 Found {len(unique_dates)} unique dates: {', '.join(sorted(unique_dates)[:5])}{'...' if len(unique_dates) > 5 else ''}")








