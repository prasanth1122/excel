import streamlit as st
import pandas as pd
from io import BytesIO
import re
from rapidfuzz import fuzz, process
from xlsxwriter.utility import xl_col_to_name
import math

st.title("📊 Campaign + Shopify Data Processor")

# ---- UPLOAD ----
campaign_file = st.file_uploader("Upload Campaign Data (Excel/CSV)", type=["xlsx", "csv"])
shopify_file = st.file_uploader("Upload Shopify Data (Excel/CSV)", type=["xlsx", "csv"])

# ---- UPLOAD OLD MERGED DATA ----
st.subheader("📋 Import Delivery Rates & Product Costs from Previous Data (Optional)")
old_merged_file = st.file_uploader(
    "Upload Old Merged Data (Excel/CSV) - to import delivery rates and product costs",
    type=["xlsx", "csv"],
    help="Upload your previous merged data file to automatically import delivery rates and product costs for matching products"
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

# ---- STATE ----
df_campaign, df_shopify, df_old_merged = None, None, None
grouped_campaign = None

# ---- USER INPUT ----
shipping_rate = st.number_input("Shipping Rate per Item", min_value=0, value=77, step=1)
operational_rate = st.number_input("Operational Cost per Item", min_value=0, value=65, step=1)

# ---- LOAD OLD MERGED DATA ----
if old_merged_file:
    try:
        if old_merged_file.name.endswith(".csv"):
            df_old_merged = pd.read_csv(old_merged_file)
        else:
            df_old_merged = pd.read_excel(old_merged_file)

        required_old_cols = ["Product title", "Product variant title", "Delivery Rate"]
        if all(col in df_old_merged.columns for col in required_old_cols):
            current_product = None
            for idx, row in df_old_merged.iterrows():
                if pd.notna(row["Product title"]) and row["Product title"].strip() != "":
                    if row["Product variant title"] == "ALL VARIANTS (TOTAL)":
                        current_product = row["Product title"]
                    else:
                        current_product = row["Product title"]
                else:
                    if current_product:
                        df_old_merged.loc[idx, "Product title"] = current_product

            df_old_merged = df_old_merged[
                (df_old_merged["Product variant title"] != "ALL VARIANTS (TOTAL)") &
                (df_old_merged["Product variant title"] != "ALL PRODUCTS") &
                (df_old_merged["Delivery Rate"].notna()) & (df_old_merged["Delivery Rate"] != "")
            ]
            df_old_merged["Product title_norm"] = df_old_merged["Product title"].astype(str).str.strip().str.lower()
            df_old_merged["Product variant title_norm"] = df_old_merged["Product variant title"].astype(str).str.strip().str.lower()

            has_product_cost = "Product Cost (Input)" in df_old_merged.columns
            st.success(f"✅ Loaded {len(df_old_merged)} records with delivery rates from old merged data")
            if has_product_cost:
                product_cost_count = df_old_merged["Product Cost (Input)"].notna().sum()
                st.success(f"✅ Found {product_cost_count} records with product costs")

            preview_cols = ["Product title", "Product variant title", "Delivery Rate"]
            if has_product_cost:
                preview_cols.append("Product Cost (Input)")
            st.write(df_old_merged[preview_cols].head())
        else:
            st.warning("⚠️ Old merged file doesn't contain required columns: Product title, Product variant title, Delivery Rate")
            df_old_merged = None
    except Exception as e:
        st.error(f"❌ Error reading old merged file: {str(e)}")
        df_old_merged = None

# ---- CAMPAIGN DATA ----
if campaign_file:
    if campaign_file.name.endswith(".csv"):
        df_campaign = pd.read_csv(campaign_file)
    else:
        df_campaign = pd.read_excel(campaign_file)

    st.subheader("📂 Original Campaign Data")
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

    

# ---- SHOPIFY DATA ----
if shopify_file:
    if shopify_file.name.endswith(".csv"):
        df_shopify = pd.read_csv(shopify_file)
    else:
        df_shopify = pd.read_excel(shopify_file)

    required_cols = ["Product title", "Product variant title", "Product variant price", "Net items sold"]
    available_cols = [col for col in required_cols if col in df_shopify.columns]
    df_shopify = df_shopify[available_cols]

    # Add extra columns
    df_shopify["In Order"] = ""
    df_shopify["Product Cost (Input)"] = ""
    df_shopify["Delivery Rate"] = ""
    df_shopify["Delivered Orders"] = ""
    df_shopify["Net Revenue"] = ""
    df_shopify["Ad Spend (INR)"] = 0.0
    df_shopify["Shipping Cost"] = ""
    df_shopify["Operational Cost"] = ""
    df_shopify["Product Cost (Output)"] = ""
    df_shopify["Net Profit"] = ""
    df_shopify["Net Profit (%)"] = ""

    # ---- IMPORT DELIVERY RATES AND PRODUCT COSTS FROM OLD DATA ----
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
        
        st.success(f"✅ Successfully imported delivery rates for {delivery_matched_count} product variants from old data")
        if has_product_cost and product_cost_matched_count > 0:
            st.success(f"✅ Successfully imported product costs for {product_cost_matched_count} product variants from old data")
        elif has_product_cost:
            st.info("ℹ️ No product cost matches found in old data")

    # ---- STEP 3: CLEAN SHOPIFY PRODUCT TITLES TO MATCH CAMPAIGN ----
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
                ratio = product_df["Net items sold"] / total_items
                df_shopify.loc[product_df.index, "Ad Spend (INR)"] = total_spend_inr * ratio
    
    

    # ---- SORT PRODUCTS BY NET ITEMS SOLD (DESC) ----
    product_order = (
        df_shopify.groupby("Product title")["Net items sold"]
        .sum()
        .sort_values(ascending=False)
        .index
    )

    df_shopify["Product title"] = pd.Categorical(df_shopify["Product title"], categories=product_order, ordered=True)
    df_shopify = df_shopify.sort_values(by=["Product title"]).reset_index(drop=True)

    st.subheader("🛒 Shopify Data with Ad Spend (INR) & Extra Columns")
    
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
        ad_spend_col = df.columns.get_loc("Ad Spend (INR)")
        net_profit_percent_col = df.columns.get_loc("Net Profit (%)")
        product_title_col = df.columns.get_loc("Product title")
        variant_title_col = df.columns.get_loc("Product variant title")

        cols_to_sum = [
            "Net items sold", "Delivered Orders", "Net Revenue", "Ad Spend (INR)",
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

            if product in avg_price_lookup:
                worksheet.write(product_total_row_idx, price_col, avg_price_lookup[product], p_format)

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
                    elif col_name == "Ad Spend (INR)":
                        worksheet.write(variant_row_idx, col_idx, variant.get("Ad Spend (INR)", 0.0), v_format)
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
                        worksheet.write_formula(
                            variant_row_idx, col_idx,
                            f"=N({revenue_ref})-N({ad_spend_ref})-N({shipping_ref})-N({pc_output_ref})-N({op_ref})",
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

            sold_col_letter = xl_col_to_name(sold_col)
            rate_col_letter = xl_col_to_name(rate_col)
            product_refs_sold = [f"{sold_col_letter}{r+1}" for r in product_total_rows]
            product_refs_rate = [f"{rate_col_letter}{r+1}" for r in product_total_rows]
            worksheet.write_formula(
                grand_total_row_idx, rate_col,
                f"=IF(SUM({','.join(product_refs_sold)})=0,0,"
                f"SUMPRODUCT({','.join(product_refs_rate)},{','.join(product_refs_sold)})/"
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
            elif col in ("Product variant price", "Net Revenue", "Ad Spend (INR)", "Shipping Cost", "Operational Cost", "Net Profit"):
                worksheet.set_column(i, i, 15)
            else:
                worksheet.set_column(i, i, 12)

    return output.getvalue()

    
export_df = df_shopify.drop(columns=["Product Name", "Canonical Product"], errors="ignore")

    # ---- DOWNLOAD BUTTONS ----
shopify_excel = convert_shopify_to_excel(export_df)
st.download_button(
        label="📥 Download Processed Shopify File (Excel)",
        data=shopify_excel,
        file_name="processed_shopify.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )
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
        
        # Add new columns if they don't exist (including renamed Cost Per Purchase columns)
        new_columns = ["Cost Per Purchase (INR)", "Cost Per Purchase (USD)", "Average Price", "Net Revenue", "Product Cost (Input)", "Total Product Cost", 
                      "Shipping Cost Per Item", "Total Shipping Cost", "Operational Cost Per Item", 
                      "Total Operational Cost", "Net Profit", "Net Profit (%)"]
        
        for new_col in new_columns:
            if new_col not in columns:
                columns.append(new_col)

        # Remove old "Cost Per Item" column if it exists
        if "Cost Per Item" in columns:
            columns.remove("Cost Per Item")

        # Reorder columns to place cost per purchase columns right after "Purchases"
        if "Purchases" in columns:
            purchases_index = columns.index("Purchases")
            
            # Remove cost per purchase columns from their current positions
            if "Cost Per Purchase (INR)" in columns:
                columns.remove("Cost Per Purchase (INR)")
            if "Cost Per Purchase (USD)" in columns:
                columns.remove("Cost Per Purchase (USD)")
            
            # Insert both cost per purchase columns after Purchases
            columns.insert(purchases_index + 1, "Cost Per Purchase (INR)")
            columns.insert(purchases_index + 2, "Cost Per Purchase (USD)")

        for col_num, value in enumerate(columns):
            safe_write(worksheet, 0, col_num, value, header_format)

        # ==== Column Indexes ====
        product_name_col = 0
        campaign_name_col = columns.index("Campaign Name") if "Campaign Name" in columns else None
        amount_usd_col = columns.index("Amount Spent (USD)") if "Amount Spent (USD)" in columns else None
        amount_inr_col = columns.index("Amount Spent (INR)") if "Amount Spent (INR)" in columns else None
        purchases_col = columns.index("Purchases") if "Purchases" in columns else None
        cost_per_purchase_inr_col = columns.index("Cost Per Purchase (INR)") if "Cost Per Purchase (INR)" in columns else None
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
        for c in ["Amount Spent (USD)", "Amount Spent (INR)", "Purchases", "Total Shipping Cost", "Total Operational Cost", "Net Profit"]:
            if c in columns:
                cols_to_sum.append(columns.index(c))

        row = 1
        
        # Track campaigns that have Shopify data vs those that don't
        matched_campaigns = []
        unmatched_campaigns = []

        # ==== Group by product ====
        for product, product_df in df.groupby("Product"):
            # Check if this product has Shopify data
            has_shopify_data = (product in shopify_totals or 
                              product in avg_price_lookup or 
                              product in avg_product_cost_lookup)
            
            # MODIFIED: Calculate Cost Per Purchase (INR) and sort by it instead of Amount Spent
            product_df = product_df.copy()  # Make a copy to avoid modifying original
            
            # Calculate Cost Per Purchase (INR) for sorting
            if "Amount Spent (INR)" in product_df.columns and "Purchases" in product_df.columns:
                # Handle division by zero - campaigns with 0 purchases get infinite cost per purchase (sorted last)
                product_df['_temp_cost_per_purchase'] = product_df.apply(
                    lambda row: float('inf') if row["Purchases"] == 0 else row["Amount Spent (INR)"] / row["Purchases"], 
                    axis=1
                )
                # Sort by Cost Per Purchase (INR) in increasing order
                product_df = product_df.sort_values("_temp_cost_per_purchase", ascending=True)
                # Remove temporary column
                product_df = product_df.drop(columns=['_temp_cost_per_purchase'])
            else:
                # Fallback to original sorting if required columns don't exist
                if "Amount Spent (USD)" in product_df.columns:
                    product_df = product_df.sort_values("Amount Spent (USD)", ascending=True)
                elif "Amount Spent (INR)" in product_df.columns:
                    product_df = product_df.sort_values("Amount Spent (INR)", ascending=True)
            
            # Categorize campaigns for the unmatched sheet
            for _, campaign_row in product_df.iterrows():
                campaign_info = {
                    'Product': str(product) if pd.notna(product) else '',
                    'Campaign Name': str(campaign_row.get('Campaign Name', '')) if pd.notna(campaign_row.get('Campaign Name', '')) else '',
                    'Amount Spent (USD)': round(float(campaign_row.get('Amount Spent (USD)', 0)), 2) if pd.notna(campaign_row.get('Amount Spent (USD)', 0)) else 0.0,
                    'Amount Spent (INR)': round(float(campaign_row.get('Amount Spent (INR)', 0)), 2) if pd.notna(campaign_row.get('Amount Spent (INR)', 0)) else 0.0,
                    'Purchases': int(campaign_row.get('Purchases', 0)) if pd.notna(campaign_row.get('Purchases', 0)) else 0,
                    'Has Shopify Data': has_shopify_data
                }
                
                if has_shopify_data:
                    matched_campaigns.append(campaign_info)
                else:
                    unmatched_campaigns.append(campaign_info)
            
            product_total_row_idx = row

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
            if cost_per_purchase_inr_col is not None and amount_inr_col is not None and purchases_col is not None:
                amount_inr_ref = f"{xl_col_to_name(amount_inr_col)}{product_total_row_idx+1}"
                purchases_ref = f"{xl_col_to_name(purchases_col)}{product_total_row_idx+1}"
                worksheet.write_formula(
                    product_total_row_idx, cost_per_purchase_inr_col,
                    f"=IF(N({purchases_ref})=0,0,ROUND(N({amount_inr_ref})/N({purchases_ref}),2))",
                    product_total_format
                )

            if cost_per_purchase_usd_col is not None and amount_usd_col is not None and purchases_col is not None:
                amount_usd_ref = f"{xl_col_to_name(amount_usd_col)}{product_total_row_idx+1}"
                purchases_ref = f"{xl_col_to_name(purchases_col)}{product_total_row_idx+1}"
                worksheet.write_formula(
                    product_total_row_idx, cost_per_purchase_usd_col,
                    f"=IF(N({purchases_ref})=0,0,ROUND(N({amount_usd_ref})/N({purchases_ref}),2))",
                    product_total_format
                )

            # ==== Shopify totals injection ====
            if delivered_col is not None and product in shopify_totals:
                safe_write(worksheet, product_total_row_idx, delivered_col, round(shopify_totals[product]["Delivered Orders"], 2), product_total_format)
            if rate_col is not None and product in shopify_totals:
                safe_write(worksheet, product_total_row_idx, rate_col, round(shopify_totals[product]["Delivery Rate"], 2), product_total_format)

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
                if amount_inr_col is not None:
                    safe_write(worksheet, row, amount_inr_col, round(campaign.get("Amount Spent (INR)", 0), 2), campaign_format)

                if purchases_col is not None:
                    safe_write(worksheet, row, purchases_col, campaign.get("Purchases", 0), campaign_format)
                    
                    # ==== Cost Per Purchase calculations for campaign row ====
                    if cost_per_purchase_inr_col is not None and amount_inr_col is not None:
                        amount_inr_ref = f"{xl_col_to_name(amount_inr_col)}{row+1}"
                        purchases_ref = f"{xl_col_to_name(purchases_col)}{row+1}"
                        worksheet.write_formula(
                            row, cost_per_purchase_inr_col,
                            f"=IF(N({purchases_ref})=0,0,ROUND(N({amount_inr_ref})/N({purchases_ref}),2))",
                            campaign_format
                        )

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
                
                # Net Profit = Net Revenue - Ad Spent - Shipping Cost - Operation Cost - Total Product Cost
                if net_profit_col is not None:
                    # Build the formula components
                    formula_parts = []
                    
                    # Start with Net Revenue
                    if net_rev_col is not None:
                        formula_parts.append(f"N({xl_col_to_name(net_rev_col)}{row+1})")
                    else:
                        formula_parts.append("0")
                    
                    # Subtract Ad Spent (using INR if available, otherwise USD)
                    if amount_inr_col is not None:
                        formula_parts.append(f"-N({xl_col_to_name(amount_inr_col)}{row+1})")
                    elif amount_usd_col is not None:
                        formula_parts.append(f"-N({xl_col_to_name(amount_usd_col)}{row+1})")
                    
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

        worksheet.freeze_panes(1, 0)
        for i, col in enumerate(columns):
            if col == "Campaign Name":
                worksheet.set_column(i, i, 35)
            elif col in ["Total Shipping Cost", "Total Operational Cost", "Shipping Cost Per Item", "Operational Cost Per Item"]:
                worksheet.set_column(i, i, 18)
            elif col in ["Net Profit", "Net Profit (%)", "Cost Per Purchase (INR)", "Cost Per Purchase (USD)"]:
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
                           "Amount Spent (INR)", "Purchases", "Cost Per Purchase (INR)", "Cost Per Purchase (USD)", "Reason"]
        
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
                cost_per_purchase_inr = 0
                cost_per_purchase_usd = 0
                if campaign['Purchases'] > 0:
                    cost_per_purchase_inr = round(campaign['Amount Spent (INR)'] / campaign['Purchases'], 2)
                    cost_per_purchase_usd = round(campaign['Amount Spent (USD)'] / campaign['Purchases'], 2)
                
                safe_write(unmatched_sheet, current_row, 0, "UNMATCHED", unmatched_data_format)
                safe_write(unmatched_sheet, current_row, 1, campaign['Product'], unmatched_data_format)
                safe_write(unmatched_sheet, current_row, 2, campaign['Campaign Name'], unmatched_data_format)
                safe_write(unmatched_sheet, current_row, 3, campaign['Amount Spent (USD)'], unmatched_data_format)
                safe_write(unmatched_sheet, current_row, 4, campaign['Amount Spent (INR)'], unmatched_data_format)
                safe_write(unmatched_sheet, current_row, 5, campaign['Purchases'], unmatched_data_format)
                safe_write(unmatched_sheet, current_row, 6, cost_per_purchase_inr, unmatched_data_format)
                safe_write(unmatched_sheet, current_row, 7, cost_per_purchase_usd, unmatched_data_format)
                safe_write(unmatched_sheet, current_row, 8, "No matching Shopify product found", unmatched_data_format)
                current_row += 1
        
        # Write matched campaigns summary
        if matched_campaigns:
            current_row += 2
            safe_write(unmatched_sheet, current_row, 0, "CAMPAIGNS WITH SHOPIFY DATA (FOR REFERENCE)", unmatched_header_format)
            current_row += 1
            
            for campaign in matched_campaigns[:10]:  # Show only first 10 to save space
                cost_per_purchase_inr = 0
                cost_per_purchase_usd = 0
                if campaign['Purchases'] > 0:
                    cost_per_purchase_inr = round(campaign['Amount Spent (INR)'] / campaign['Purchases'], 2)
                    cost_per_purchase_usd = round(campaign['Amount Spent (USD)'] / campaign['Purchases'], 2)
                
                safe_write(unmatched_sheet, current_row, 0, "MATCHED", matched_summary_format)
                safe_write(unmatched_sheet, current_row, 1, campaign['Product'], matched_summary_format)
                safe_write(unmatched_sheet, current_row, 2, campaign['Campaign Name'], matched_summary_format)
                safe_write(unmatched_sheet, current_row, 3, campaign['Amount Spent (USD)'], matched_summary_format)
                safe_write(unmatched_sheet, current_row, 4, campaign['Amount Spent (INR)'], matched_summary_format)
                safe_write(unmatched_sheet, current_row, 5, campaign['Purchases'], matched_summary_format)
                safe_write(unmatched_sheet, current_row, 6, cost_per_purchase_inr, matched_summary_format)
                safe_write(unmatched_sheet, current_row, 7, cost_per_purchase_usd, matched_summary_format)
                safe_write(unmatched_sheet, current_row, 8, "Successfully matched with Shopify", matched_summary_format)
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
        unmatched_sheet.set_column(6, 6, 15)  # Cost Per Item
        unmatched_sheet.set_column(7, 7, 30)  # Reason

    return output.getvalue()


# ---- CAMPAIGN DOWNLOAD ----
if campaign_file:
    def convert_df_to_excel(df):
        output = BytesIO()
        with pd.ExcelWriter(output, engine="openpyxl") as writer:
            df.to_excel(writer, index=False, sheet_name="Processed Data")
        return output.getvalue()

    # Download processed campaign data (simple format)
    excel_data = convert_df_to_excel(grouped_campaign)
    st.download_button(
        label="📥 Download Processed Campaign File (Excel)",
        data=excel_data,
        file_name="processed_campaigns.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )

    # Download final campaign data (structured format like Shopify)
    if 'df_final_campaign' in locals() and not df_final_campaign.empty:
        final_campaign_excel = convert_final_campaign_to_excel(df_final_campaign)
        if final_campaign_excel:
            st.download_button(
                label="🎯 Download Final Campaign File (Structured Excel)",
                data=final_campaign_excel,
                file_name="final_campaign_data.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )
            
            




