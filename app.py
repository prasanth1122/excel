import streamlit as st
import pandas as pd
from io import BytesIO
import re
from rapidfuzz import fuzz, process
from xlsxwriter.utility import xl_col_to_name
import math

st.title("📊 Enhanced Campaign + Shopify Data Processor")
st.markdown("**Now supports multiple file uploads for each category!**")

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

def standardize_campaign_columns(df):
    """Standardize campaign column names and handle currency conversion"""
    df = df.copy()
    
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
    
    # Group by Campaign name and sum amounts (handle duplicates)
    required_cols = ["Campaign name", "Amount spent (USD)"]
    if all(col in merged_df.columns for col in required_cols):
        # Check if Purchases column exists
        has_purchases = "Purchases" in merged_df.columns
        
        if has_purchases:
            # Group and sum both amount and purchases
            merged_df = merged_df.groupby("Campaign name", as_index=False).agg({
                "Amount spent (USD)": "sum",
                "Purchases": "sum"
            })
        else:
            # Group and sum only amount
            merged_df = merged_df.groupby("Campaign name", as_index=False).agg({
                "Amount spent (USD)": "sum"
            })
    
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
            all_shopify.append(df)
            file_info.append(f"{file.name} ({len(df)} rows)")
    
    if not all_shopify:
        return None
    
    # Combine all Shopify files
    merged_df = pd.concat(all_shopify, ignore_index=True)
    
    # Group by Product title + Product variant title and merge
    required_cols = ["Product title", "Product variant title", "Net items sold"]
    if all(col in merged_df.columns for col in required_cols):
        # Group and sum net items sold, keep first price
        agg_dict = {"Net items sold": "sum"}
        if "Product variant price" in merged_df.columns:
            agg_dict["Product variant price"] = "first"  # Keep first price found
        
        merged_df = merged_df.groupby(["Product title", "Product variant title"], as_index=False).agg(agg_dict)
    
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

        # ---- GROUP BY CANONICAL PRODUCT ----
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

        # ---- FINAL CAMPAIGN DATA STRUCTURE ----
        final_campaign_data = []
        has_purchases = "Purchases" in df_campaign.columns

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
                final_campaign_data.append(row)

        df_final_campaign = pd.DataFrame(final_campaign_data)

        if not df_final_campaign.empty:
            order = (
                df_final_campaign.groupby("Product")["Amount Spent (INR)"].sum().sort_values(ascending=False).index
            )
            df_final_campaign["Product"] = pd.Categorical(df_final_campaign["Product"], categories=order, ordered=True)
            df_final_campaign = df_final_campaign.sort_values("Product").reset_index(drop=True)
            df_final_campaign["Delivered Orders"] = ""
            df_final_campaign["Delivery Rate"] = ""

        st.subheader("🎯 Final Campaign Data Structure")
        st.write(df_final_campaign.drop(columns=["Product"], errors="ignore"))

# ---- PROCESS MULTIPLE SHOPIFY FILES ----
if shopify_files:
    df_shopify = merge_shopify_files(shopify_files)
    
    if df_shopify is not None:
        required_cols = ["Product title", "Product variant title", "Product variant price", "Net items sold"]
        available_cols = [col for col in required_cols if col in df_shopify.columns]
        df_shopify = df_shopify[available_cols]

        # Add extra columns
        df_shopify["In Order"] = ""
        df_shopify["Product Cost (Input)"] = ""
        df_shopify["Delivery Rate"] = ""
        df_shopify["Delivered Orders"] = ""
        df_shopify["Net Revenue"] = ""
        df_shopify["Ad Spend (USD)"] = 0.0  # Changed from INR to USD for display
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

        # ---- ALLOCATE AD SPEND ----
        if grouped_campaign is not None:
            ad_spend_map = dict(zip(grouped_campaign["Product"], grouped_campaign["Total Amount Spent (INR)"]))

            for product, product_df in df_shopify.groupby("Canonical Product"):
                total_items = product_df["Net items sold"].sum()
                if total_items > 0 and product in ad_spend_map:
                    total_spend_inr = ad_spend_map[product]
                    total_spend_usd = total_spend_inr / 100  # Convert to USD for display
                    ratio = product_df["Net items sold"] / total_items
                    df_shopify.loc[product_df.index, "Ad Spend (USD)"] = total_spend_usd * ratio

        # ---- SORT PRODUCTS BY NET ITEMS SOLD (DESC) ----
        product_order = (
            df_shopify.groupby("Product title")["Net items sold"]
            .sum()
            .sort_values(ascending=False)
            .index
        )

        df_shopify["Product title"] = pd.Categorical(df_shopify["Product title"], categories=product_order, ordered=True)
        df_shopify = df_shopify.sort_values(by=["Product title"]).reset_index(drop=True)

        st.subheader("🛒 Merged Shopify Data with Ad Spend (USD) & Extra Columns")
        
        # Show delivery rate and product cost import summary
        if df_old_merged is not None:
            delivery_rate_filled = df_shopify["Delivery Rate"].astype(str).str.strip()
            delivery_rate_filled = delivery_rate_filled[delivery_rate_filled != ""]
            
            product_cost_filled = df_shopify["Product Cost (Input)"].astype(str).str.strip()
            product_cost_filled = product_cost_filled[product_cost_filled != ""]
            
            st.info(f"📊 Delivery rates imported: {len(delivery_rate_filled)} out of {len(df_shopify)} variants")
            if len(product_cost_filled) > 0:
                st.info(f"📊 Product costs imported: {len(product_cost_filled)} out of {len(df_shopify)} variants")
        
        st.write(df_shopify)

# ✅ Build lookup of weighted avg price per product (only if Shopify data exists)
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

# ✅ Build lookup of weighted avg product cost per product
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

# ✅ Build Shopify totals lookup for Delivered Orders & Delivery Rate
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

def convert_shopify_to_excel(df):
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
        low_sales_product_format = workbook.add_format({
            "bold": True, "align": "left", "valign": "vcenter",
            "fg_color": "#F4CCCC", "font_name": "Calibri", "font_size": 11
        })
        low_sales_variant_format = workbook.add_format({
            "align": "left", "valign": "vcenter",
            "fg_color": "#FCE5CD", "font_name": "Calibri", "font_size": 11
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
        ad_spend_col = df.columns.get_loc("Ad Spend (USD)")  # Changed from INR to USD
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
            total_product_sales = product_df["Net items sold"].sum()
            is_low_sales = total_product_sales < 5

            p_format = low_sales_product_format if is_low_sales else product_total_format
            v_format = low_sales_variant_format if is_low_sales else variant_format

            product_total_row_idx = row
            product_total_rows.append(product_total_row_idx)

            worksheet.write(product_total_row_idx, 0, product, p_format)
            worksheet.write(product_total_row_idx, 1, "ALL VARIANTS (TOTAL)", p_format)

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
                    p_format
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
                p_format
            )

            # Product weighted avg Product variant price
            price_col_letter = xl_col_to_name(price_col)
            worksheet.write_formula(
                product_total_row_idx, price_col,
                f"=IF(SUM({sold_col_letter}{excel_first}:{sold_col_letter}{excel_last})=0,0,"
                f"SUMPRODUCT({price_col_letter}{excel_first}:{price_col_letter}{excel_last},"
                f"{sold_col_letter}{excel_first}:{sold_col_letter}{excel_last})/"
                f"SUM({sold_col_letter}{excel_first}:{sold_col_letter}{excel_last}))",
                p_format
            )

            # Product weighted avg Product Cost (Input)
            pc_input_col_letter = xl_col_to_name(product_cost_input_col)
            worksheet.write_formula(
                product_total_row_idx, product_cost_input_col,
                f"=IF(SUM({sold_col_letter}{excel_first}:{sold_col_letter}{excel_last})=0,0,"
                f"SUMPRODUCT({pc_input_col_letter}{excel_first}:{pc_input_col_letter}{excel_last},"
                f"{sold_col_letter}{excel_first}:{sold_col_letter}{excel_last})/"
                f"SUM({sold_col_letter}{excel_first}:{sold_col_letter}{excel_last}))",
                p_format
            )

            # Product Net Profit %
            rev_col_letter = xl_col_to_name(revenue_col)
            np_col_letter = xl_col_to_name(net_profit_col)
            excel_row = product_total_row_idx + 1
            worksheet.write_formula(
                product_total_row_idx, net_profit_percent_col,
                f"=IF(N({rev_col_letter}{excel_row})=0,0,"
                f"N({np_col_letter}{excel_row})/N({rev_col_letter}{excel_row})*100)",
                p_format
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
                        worksheet.write(variant_row_idx, col_idx, "", v_format)
                    elif col_idx == variant_title_col:
                        worksheet.write(variant_row_idx, col_idx, variant.get("Product variant title", ""), v_format)
                    elif col_name == "Net items sold":
                        worksheet.write(variant_row_idx, col_idx, variant.get("Net items sold", 0), v_format)
                    elif col_name == "Product variant price":
                        worksheet.write(variant_row_idx, col_idx, variant.get("Product variant price", 0), v_format)
                    elif col_name == "Ad Spend (USD)":
                        worksheet.write(variant_row_idx, col_idx, variant.get("Ad Spend (USD)", 0.0), v_format)
                    elif col_name == "Delivery Rate":
                        worksheet.write(variant_row_idx, col_idx, variant.get("Delivery Rate", ""), v_format)
                    elif col_name == "Product Cost (Input)":
                        worksheet.write(variant_row_idx, col_idx, variant.get("Product Cost (Input)", ""), v_format)
                    elif col_name == "Delivered Orders":
                        rate_term = f"IF(N({rate_ref})>1,N({rate_ref})/100,N({rate_ref}))"
                        worksheet.write_formula(
                            variant_row_idx, col_idx,
                            f"=ROUND(N({sold_ref})*{rate_term},1)",
                            v_format
                        )
                    elif col_name == "Net Revenue":
                        worksheet.write_formula(
                            variant_row_idx, col_idx,
                            f"=N({price_ref})*N({delivered_ref})",
                            v_format
                        )
                    elif col_name == "Shipping Cost":
                        worksheet.write_formula(
                            variant_row_idx, col_idx,
                            f"={shipping_rate}*N({sold_ref})",
                            v_format
                        )
                    elif col_name == "Operational Cost":
                        worksheet.write_formula(
                            variant_row_idx, col_idx,
                            f"={operational_rate}*N({sold_ref})",
                            v_format
                        )
                    elif col_name == "Product Cost (Output)":
                        worksheet.write_formula(
                            variant_row_idx, col_idx,
                            f"=N({pc_input_ref})*N({delivered_ref})",
                            v_format
                        )
                    elif col_name == "Net Profit":
                        # Calculate using INR values for ad spend (multiply USD by 100)
                        worksheet.write_formula(
                            variant_row_idx, col_idx,
                            f"=N({revenue_ref})-N({ad_spend_ref})*100-N({shipping_ref})-N({pc_output_ref})-N({op_ref})",
                            v_format
                        )
                    elif col_name == "Net Profit (%)":
                        worksheet.write_formula(
                            variant_row_idx, col_idx,
                            f"=IF(N({revenue_ref})=0,0,N({net_profit_ref})/N({revenue_ref})*100)",
                            v_format
                        )
                    else:
                        worksheet.write(variant_row_idx, col_idx, variant.get(col_name, ""), v_format)
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

def convert_final_campaign_to_excel(df, original_campaign_df=None):
    if df.empty:
        return None
    
    output = BytesIO()
    with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
        workbook = writer.book
        
        # ==== MAIN SHEET: Campaign Data ====
        worksheet = workbook.add_worksheet("Campaign Data")
        writer.sheets["Campaign Data"] = worksheet

        # ==== Formats ====
        header_format = workbook.add_format({
            "bold": True, "align": "center", "valign": "vcenter",
            "fg_color": "#DDD9C4", "font_name": "Calibri", "font_size": 11
        })
        grand_total_format = workbook.add_format({
            "bold": True, "align": "left", "valign": "vcenter",
            "fg_color": "#FFC000", "font_name": "Calibri", "font_size": 11,
            "num_format": "#,##0.00"  # 2 decimal places
        })
        product_total_format = workbook.add_format({
            "bold": True, "align": "left", "valign": "vcenter",
            "fg_color": "#FFD966", "font_name": "Calibri", "font_size": 11,
            "num_format": "#,##0.00"  # 2 decimal places
        })
        campaign_format = workbook.add_format({
            "align": "left", "valign": "vcenter",
            "fg_color": "#D9E1F2", "font_name": "Calibri", "font_size": 11,
            "num_format": "#,##0.00"  # 2 decimal places
        })

        # ==== Build Columns ====
        columns = [col for col in df.columns if col != "Product"]
        
        # Add new columns if they don't exist (remove Cost Per Purchase INR, keep only USD)
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

        # ==== Column Indexes ====
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

        # ==== GRAND TOTAL ROW ====
        grand_total_row_idx = 1
        safe_write(worksheet, grand_total_row_idx, 0, "GRAND TOTAL", grand_total_format)
        if campaign_name_col is not None:
            safe_write(worksheet, grand_total_row_idx, campaign_name_col, "ALL PRODUCTS", grand_total_format)

        row = grand_total_row_idx + 1
        product_total_rows = []
        
        # Track campaigns that have Shopify data vs those that don't
        matched_campaigns = []
        unmatched_campaigns = []

        # ==== Group by product ====
        for product, product_df in df.groupby("Product"):
            # Check if this product has Shopify data
            has_shopify_data = (product in shopify_totals or 
                              product in avg_price_lookup or 
                              product in avg_product_cost_lookup)
            
            # MODIFIED: Calculate Cost Per Purchase (USD) and sort by it instead of Amount Spent
            product_df = product_df.copy()  # Make a copy to avoid modifying original
            
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
            
            # Categorize campaigns for the unmatched sheet
            for _, campaign_row in product_df.iterrows():
                campaign_info = {
                    'Product': str(product) if pd.notna(product) else '',
                    'Campaign Name': str(campaign_row.get('Campaign Name', '')) if pd.notna(campaign_row.get('Campaign Name', '')) else '',
                    'Amount Spent (USD)': round(float(campaign_row.get('Amount Spent (USD)', 0)), 2) if pd.notna(campaign_row.get('Amount Spent (USD)', 0)) else 0.0,
                    'Purchases': int(campaign_row.get('Purchases', 0)) if pd.notna(campaign_row.get('Purchases', 0)) else 0,
                    'Has Shopify Data': has_shopify_data
                }
                
                if has_shopify_data:
                    matched_campaigns.append(campaign_info)
                else:
                    unmatched_campaigns.append(campaign_info)
            
            product_total_row_idx = row
            product_total_rows.append(product_total_row_idx)

            # Product total row
            safe_write(worksheet, product_total_row_idx, 0, product, product_total_format)
            if campaign_name_col is not None:
                safe_write(worksheet, product_total_row_idx, campaign_name_col, "ALL CAMPAIGNS (TOTAL)", product_total_format)

            n_campaigns = len(product_df)
            first_campaign_row_idx = product_total_row_idx + 1
            last_campaign_row_idx = product_total_row_idx + n_campaigns

            # ==== Totals for numeric columns ====
            for col_idx in cols_to_sum:
                col_letter = xl_col_to_name(col_idx)
                excel_first = first_campaign_row_idx + 1
                excel_last = last_campaign_row_idx + 1
                worksheet.write_formula(
                    product_total_row_idx, col_idx,
                    f"=ROUND(SUM({col_letter}{excel_first}:{col_letter}{excel_last}),2)",
                    product_total_format
                )

            # ==== Cost Per Purchase calculations for product total ====
            if cost_per_purchase_usd_col is not None and amount_usd_col is not None and purchases_col is not None:
                amount_usd_ref = f"{xl_col_to_name(amount_usd_col)}{product_total_row_idx+1}"
                purchases_ref = f"{xl_col_to_name(purchases_col)}{product_total_row_idx+1}"
                worksheet.write_formula(
                    product_total_row_idx, cost_per_purchase_usd_col,
                    f"=IF(N({purchases_ref})=0,0,ROUND(N({amount_usd_ref})/N({purchases_ref}),2))",
                    product_total_format
                )

            # Delivery Rate (from Shopify lookup if available)
            if rate_col is not None and product in shopify_totals:
                safe_write(
                   worksheet, product_total_row_idx, rate_col,
                   round(shopify_totals[product]["Delivery Rate"], 4),  # keep 4 decimals for accuracy
                   product_total_format
                )

            # Delivered Orders (calculated as Delivery Rate × Purchases total)
            if delivered_col is not None and purchases_col is not None and rate_col is not None:
                purchases_ref = f"{xl_col_to_name(purchases_col)}{product_total_row_idx+1}"
                rate_ref = f"{xl_col_to_name(rate_col)}{product_total_row_idx+1}"
                worksheet.write_formula(
                     product_total_row_idx, delivered_col,
                    f"=ROUND(N({purchases_ref})*N({rate_ref}),2)",
                     product_total_format
                             )

            if avg_price_col is not None and product in avg_price_lookup:
                safe_write(worksheet, product_total_row_idx, avg_price_col, round(avg_price_lookup[product], 2), product_total_format)
                if net_rev_col is not None and delivered_col is not None:
                    deliv_ref = f"{xl_col_to_name(delivered_col)}{product_total_row_idx+1}"
                    avg_price_ref = f"{xl_col_to_name(avg_price_col)}{product_total_row_idx+1}"
                    worksheet.write_formula(
                        product_total_row_idx, net_rev_col,
                        f"=ROUND(N({deliv_ref})*N({avg_price_ref}),2)",
                        product_total_format
                    )

            if prod_cost_input_col is not None and product in avg_product_cost_lookup:
                safe_write(
                    worksheet, product_total_row_idx, prod_cost_input_col,
                    round(avg_product_cost_lookup[product], 2),
                    product_total_format
                )

            # Product total "Total Product Cost" = SUM of all campaign totals
            if total_prod_cost_col is not None:
                col_letter = xl_col_to_name(total_prod_cost_col)
                excel_first = first_campaign_row_idx + 1
                excel_last = last_campaign_row_idx + 1
                worksheet.write_formula(
                    product_total_row_idx, total_prod_cost_col,
                    f"=ROUND(SUM({col_letter}{excel_first}:{col_letter}{excel_last}),2)",
                    product_total_format
                )

            # ==== Add constant values for shipping and operational costs (per item) ====
            if shipping_per_item_col is not None:
                safe_write(worksheet, product_total_row_idx, shipping_per_item_col, round(shipping_rate, 2), product_total_format)
            
            if operational_per_item_col is not None:
                safe_write(worksheet, product_total_row_idx, operational_per_item_col, round(operational_rate, 2), product_total_format)

            # ==== Product total Net Profit (%) calculation ====
            if net_profit_pct_col is not None and net_profit_col is not None and net_rev_col is not None:
                net_profit_ref = f"{xl_col_to_name(net_profit_col)}{product_total_row_idx+1}"
                net_rev_ref = f"{xl_col_to_name(net_rev_col)}{product_total_row_idx+1}"
                worksheet.write_formula(
                    product_total_row_idx, net_profit_pct_col,
                    f"=IF(N({net_rev_ref})=0,0,ROUND(N({net_profit_ref})/N({net_rev_ref})*100,2))",
                    product_total_format
                )

            # ==== Campaign rows ====
            row += 1
            for _, campaign in product_df.iterrows():
                safe_write(worksheet, row, product_name_col, "", campaign_format)

                if campaign_name_col is not None:
                    safe_write(worksheet, row, campaign_name_col, campaign.get("Campaign Name", ""), campaign_format)
                if amount_usd_col is not None:
                    safe_write(worksheet, row, amount_usd_col, round(campaign.get("Amount Spent (USD)", 0), 2), campaign_format)

                if purchases_col is not None:
                    safe_write(worksheet, row, purchases_col, campaign.get("Purchases", 0), campaign_format)
                    
                    # ==== Cost Per Purchase calculations for campaign row ====
                    if cost_per_purchase_usd_col is not None and amount_usd_col is not None:
                        amount_usd_ref = f"{xl_col_to_name(amount_usd_col)}{row+1}"
                        purchases_ref = f"{xl_col_to_name(purchases_col)}{row+1}"
                        worksheet.write_formula(
                            row, cost_per_purchase_usd_col,
                            f"=IF(N({purchases_ref})=0,0,ROUND(N({amount_usd_ref})/N({purchases_ref}),2))",
                            campaign_format
                        )
                    
                    if delivered_col is not None and rate_col is not None:
                        rate_ref = f"{xl_col_to_name(rate_col)}{product_total_row_idx+1}"
                        purch_ref = f"{xl_col_to_name(purchases_col)}{row+1}"
                        worksheet.write_formula(
                            row, delivered_col,
                            f"=ROUND(N({purch_ref})*N({rate_ref}),2)",
                            campaign_format
                        )

                if rate_col is not None:
                    safe_write(worksheet, row, rate_col, "", campaign_format)

                if avg_price_col is not None and product in avg_price_lookup:
                    safe_write(worksheet, row, avg_price_col, round(avg_price_lookup[product], 2), campaign_format)
                    if net_rev_col is not None and delivered_col is not None:
                        deliv_ref = f"{xl_col_to_name(delivered_col)}{row+1}"
                        avg_price_ref = f"{xl_col_to_name(avg_price_col)}{row+1}"
                        worksheet.write_formula(
                            row, net_rev_col,
                            f"=ROUND(N({deliv_ref})*N({avg_price_ref}),2)",
                            campaign_format
                        )

                if prod_cost_input_col is not None and product in avg_product_cost_lookup:
                    safe_write(
                        worksheet, row, prod_cost_input_col,
                        round(avg_product_cost_lookup[product], 2),
                        campaign_format
                    )

                # Campaign row "Total Product Cost" = Product Cost (Input) × Delivered Orders
                if total_prod_cost_col is not None and prod_cost_input_col is not None and delivered_col is not None:
                    pc_input_ref = f"{xl_col_to_name(prod_cost_input_col)}{row+1}"
                    deliv_ref = f"{xl_col_to_name(delivered_col)}{row+1}"
                    worksheet.write_formula(
                        row, total_prod_cost_col,
                        f"=ROUND(N({pc_input_ref})*N({deliv_ref}),2)",
                        campaign_format
                    )

                # ==== Shipping and operational costs ====
                
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

                # ==== Net Profit and Net Profit (%) calculations ====
                
                # Net Profit = Net Revenue - Ad Spent (in INR) - Shipping Cost - Operation Cost - Total Product Cost
                if net_profit_col is not None:
                    # Build the formula components
                    formula_parts = []
                    
                    # Start with Net Revenue
                    if net_rev_col is not None:
                        formula_parts.append(f"N({xl_col_to_name(net_rev_col)}{row+1})")
                    else:
                        formula_parts.append("0")
                    
                    # Subtract Ad Spent (convert USD to INR by multiplying by 100)
                    if amount_usd_col is not None:
                        formula_parts.append(f"-N({xl_col_to_name(amount_usd_col)}{row+1})*100")
                    
                    # Subtract Total Shipping Cost
                    if total_shipping_col is not None:
                        formula_parts.append(f"-N({xl_col_to_name(total_shipping_col)}{row+1})")
                    
                    # Subtract Total Operational Cost
                    if total_operational_col is not None:
                        formula_parts.append(f"-N({xl_col_to_name(total_operational_col)}{row+1})")
                    
                    # Subtract Total Product Cost
                    if total_prod_cost_col is not None:
                        formula_parts.append(f"-N({xl_col_to_name(total_prod_cost_col)}{row+1})")
                    
                    net_profit_formula = "=ROUND(" + "".join(formula_parts) + ",2)" if len(formula_parts) > 1 else "=0"
                    worksheet.write_formula(row, net_profit_col, net_profit_formula, campaign_format)
                
                # Net Profit (%) = Net Profit / Net Revenue * 100
                if net_profit_pct_col is not None and net_profit_col is not None and net_rev_col is not None:
                    net_profit_ref = f"{xl_col_to_name(net_profit_col)}{row+1}"
                    net_rev_ref = f"{xl_col_to_name(net_rev_col)}{row+1}"
                    worksheet.write_formula(
                        row, net_profit_pct_col,
                        f"=IF(N({net_rev_ref})=0,0,ROUND(N({net_profit_ref})/N({net_rev_ref})*100,2))",
                        campaign_format
                    )

                row += 1

        # ==== GRAND TOTAL CALCULATIONS ====
        if product_total_rows:
            for col_idx in cols_to_sum:
                col_letter = xl_col_to_name(col_idx)
                total_refs = [f"{col_letter}{r+1}" for r in product_total_rows]
                worksheet.write_formula(
                    grand_total_row_idx, col_idx,
                    f"=ROUND(SUM({','.join(total_refs)}),2)",
                    grand_total_format
                )

            # Grand total Cost Per Purchase calculations
            if cost_per_purchase_usd_col is not None and amount_usd_col is not None and purchases_col is not None:
                amount_usd_ref = f"{xl_col_to_name(amount_usd_col)}{grand_total_row_idx+1}"
                purchases_ref = f"{xl_col_to_name(purchases_col)}{grand_total_row_idx+1}"
                worksheet.write_formula(
                    grand_total_row_idx, cost_per_purchase_usd_col,
                    f"=IF(N({purchases_ref})=0,0,ROUND(N({amount_usd_ref})/N({purchases_ref}),2))",
                    grand_total_format
                )

            # Grand total weighted averages
            if purchases_col is not None:
                purchases_col_letter = xl_col_to_name(purchases_col)
                product_refs_purchases = [f"{purchases_col_letter}{r+1}" for r in product_total_rows]
                
                # Grand total weighted avg Average Price
                if avg_price_col is not None:
                    avg_price_col_letter = xl_col_to_name(avg_price_col)
                    product_refs_avg_price = [f"{avg_price_col_letter}{r+1}" for r in product_total_rows]
                    worksheet.write_formula(
                        grand_total_row_idx, avg_price_col,
                        f"=IF(SUM({','.join(product_refs_purchases)})=0,0,"
                        f"ROUND(SUMPRODUCT({','.join(product_refs_avg_price)},{','.join(product_refs_purchases)})/"
                        f"SUM({','.join(product_refs_purchases)}),2))",
                        grand_total_format
                    )

                # Grand total weighted avg Product Cost (Input)
                if prod_cost_input_col is not None:
                    prod_cost_input_col_letter = xl_col_to_name(prod_cost_input_col)
                    product_refs_prod_cost = [f"{prod_cost_input_col_letter}{r+1}" for r in product_total_rows]
                    worksheet.write_formula(
                        grand_total_row_idx, prod_cost_input_col,
                        f"=IF(SUM({','.join(product_refs_purchases)})=0,0,"
                        f"ROUND(SUMPRODUCT({','.join(product_refs_prod_cost)},{','.join(product_refs_purchases)})/"
                        f"SUM({','.join(product_refs_purchases)}),2))",
                        grand_total_format
                    )

            # Grand total shipping and operational per item (constants)
            if shipping_per_item_col is not None:
                safe_write(worksheet, grand_total_row_idx, shipping_per_item_col, round(shipping_rate, 2), grand_total_format)
            
            if operational_per_item_col is not None:
                safe_write(worksheet, grand_total_row_idx, operational_per_item_col, round(operational_rate, 2), grand_total_format)

            # Grand total Net Profit %
            if net_profit_pct_col is not None and net_profit_col is not None and net_rev_col is not None:
                net_profit_ref = f"{xl_col_to_name(net_profit_col)}{grand_total_row_idx+1}"
                net_rev_ref = f"{xl_col_to_name(net_rev_col)}{grand_total_row_idx+1}"
                worksheet.write_formula(
                    grand_total_row_idx, net_profit_pct_col,
                    f"=IF(N({net_rev_ref})=0,0,ROUND(N({net_profit_ref})/N({net_rev_ref})*100,2))",
                    grand_total_format
                )

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
                           "Purchases", "Cost Per Purchase (USD)", "Reason"]
        
        for col_num, header in enumerate(unmatched_headers):
            safe_write(unmatched_sheet, 0, col_num, header, unmatched_header_format)
        
        # Write summary first
        summary_row = 1
        safe_write(unmatched_sheet, summary_row, 0, "SUMMARY", unmatched_header_format)
        safe_write(unmatched_sheet, summary_row + 1, 0, f"Total Campaigns: {len(matched_campaigns) + len(unmatched_campaigns)}", matched_summary_format)
        safe_write(unmatched_sheet, summary_row + 2, 0, f"Matched with Shopify: {len(matched_campaigns)}", matched_summary_format)
        safe_write(unmatched_sheet, summary_row + 3, 0, f"Unmatched with Shopify: {len(unmatched_campaigns)}", unmatched_data_format)
        
        # Write unmatched campaigns
        current_row = summary_row + 5
        
        if unmatched_campaigns:
            safe_write(unmatched_sheet, current_row, 0, "CAMPAIGNS WITHOUT SHOPIFY DATA", unmatched_header_format)
            current_row += 1
            
            for campaign in unmatched_campaigns:
                cost_per_purchase_usd = 0
                if campaign['Purchases'] > 0:
                    cost_per_purchase_usd = round(campaign['Amount Spent (USD)'] / campaign['Purchases'], 2)
                
                safe_write(unmatched_sheet, current_row, 0, "UNMATCHED", unmatched_data_format)
                safe_write(unmatched_sheet, current_row, 1, campaign['Product'], unmatched_data_format)
                safe_write(unmatched_sheet, current_row, 2, campaign['Campaign Name'], unmatched_data_format)
                safe_write(unmatched_sheet, current_row, 3, campaign['Amount Spent (USD)'], unmatched_data_format)
                safe_write(unmatched_sheet, current_row, 4, campaign['Purchases'], unmatched_data_format)
                safe_write(unmatched_sheet, current_row, 5, cost_per_purchase_usd, unmatched_data_format)
                safe_write(unmatched_sheet, current_row, 6, "No matching Shopify product found", unmatched_data_format)
                current_row += 1
        
        # Write matched campaigns summary
        if matched_campaigns:
            current_row += 2
            safe_write(unmatched_sheet, current_row, 0, "CAMPAIGNS WITH SHOPIFY DATA (FOR REFERENCE)", unmatched_header_format)
            current_row += 1
            
            for campaign in matched_campaigns[:10]:  # Show only first 10 to save space
                cost_per_purchase_usd = 0
                if campaign['Purchases'] > 0:
                    cost_per_purchase_usd = round(campaign['Amount Spent (USD)'] / campaign['Purchases'], 2)
                
                safe_write(unmatched_sheet, current_row, 0, "MATCHED", matched_summary_format)
                safe_write(unmatched_sheet, current_row, 1, campaign['Product'], matched_summary_format)
                safe_write(unmatched_sheet, current_row, 2, campaign['Campaign Name'], matched_summary_format)
                safe_write(unmatched_sheet, current_row, 3, campaign['Amount Spent (USD)'], matched_summary_format)
                safe_write(unmatched_sheet, current_row, 4, campaign['Purchases'], matched_summary_format)
                safe_write(unmatched_sheet, current_row, 5, cost_per_purchase_usd, matched_summary_format)
                safe_write(unmatched_sheet, current_row, 6, "Successfully matched with Shopify", matched_summary_format)
                current_row += 1
            
            if len(matched_campaigns) > 10:
                safe_write(unmatched_sheet, current_row, 0, f"... and {len(matched_campaigns) - 10} more matched campaigns", matched_summary_format)
        
        # Set column widths for unmatched sheet
        unmatched_sheet.set_column(0, 0, 12)  # Status
        unmatched_sheet.set_column(1, 1, 25)  # Product
        unmatched_sheet.set_column(2, 2, 35)  # Campaign Name
        unmatched_sheet.set_column(3, 3, 18)  # Amount USD
        unmatched_sheet.set_column(4, 4, 12)  # Purchases
        unmatched_sheet.set_column(5, 5, 20)  # Cost Per Purchase USD
        unmatched_sheet.set_column(6, 6, 30)  # Reason

    return output.getvalue()

# ---- DOWNLOAD SECTIONS ----
st.header("📥 Download Processed Files")

# ---- SHOPIFY DOWNLOAD ----
if df_shopify is not None:
    export_df = df_shopify.drop(columns=["Product Name", "Canonical Product"], errors="ignore")

    shopify_excel = convert_shopify_to_excel(export_df)
    st.download_button(
        label="📥 Download Processed Shopify File (Excel)",
        data=shopify_excel,
        file_name="processed_shopify_merged.xlsx",
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

    # Download processed campaign data (simple format)
    if grouped_campaign is not None:
        excel_data = convert_df_to_excel(grouped_campaign)
        st.download_button(
            label="📥 Download Processed Campaign File (Excel)",
            data=excel_data,
            file_name="processed_campaigns_merged.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )

    # Download final campaign data (structured format like Shopify)
    if 'df_final_campaign' in locals() and not df_final_campaign.empty:
        final_campaign_excel = convert_final_campaign_to_excel(df_final_campaign)
        if final_campaign_excel:
            st.download_button(
                label="🎯 Download Final Campaign File (Structured Excel)",
                data=final_campaign_excel,
                file_name="final_campaign_data_merged.xlsx",
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

st.markdown("---")
st.markdown("**Enhanced with Multiple File Support** 🚀")
st.markdown("Upload multiple files of each type and they will be intelligently merged!")

