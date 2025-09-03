import streamlit as st
import pandas as pd
from io import BytesIO
import re
from rapidfuzz import fuzz, process
from xlsxwriter.utility import xl_col_to_name

st.title("📊 Campaign + Shopify Data Processor")

# ---- UPLOAD ----
campaign_file = st.file_uploader("Upload Campaign Data (Excel/CSV)", type=["xlsx", "csv"])
shopify_file = st.file_uploader("Upload Shopify Data (Excel/CSV)", type=["xlsx", "csv"])

# ---- NEW: UPLOAD OLD MERGED DATA ----
st.subheader("📋 Import Delivery Rates from Previous Data (Optional)")
old_merged_file = st.file_uploader(
    "Upload Old Merged Data (Excel/CSV) - to import delivery rates", 
    type=["xlsx", "csv"],
    help="Upload your previous merged data file to automatically import delivery rates for matching products"
)

df_campaign, df_shopify, df_old_merged = None, None, None
grouped_campaign = None

# ---- USER INPUT FOR RATES ----
shipping_rate = st.number_input("Shipping Rate per Item", min_value=0, value=77, step=1)
operational_rate = st.number_input("Operational Cost per Item", min_value=0, value=65, step=1)

# ---- LOAD OLD MERGED DATA ----
if old_merged_file:
    try:
        if old_merged_file.name.endswith(".csv"):
            df_old_merged = pd.read_csv(old_merged_file)
        else:
            df_old_merged = pd.read_excel(old_merged_file)
        
        # Check if required columns exist
        required_old_cols = ["Product title", "Product variant title", "Delivery Rate"]
        available_old_cols = [col for col in required_old_cols if col in df_old_merged.columns]
        
        if len(available_old_cols) == len(required_old_cols):
            # Process the hierarchical structure of the old merged file
            # Fill down product titles from "ALL VARIANTS (TOTAL)" rows to variant rows
            current_product = None
            for idx, row in df_old_merged.iterrows():
                if pd.notna(row["Product title"]) and row["Product title"].strip() != "":
                    if row["Product variant title"] == "ALL VARIANTS (TOTAL)":
                        # This is a product header row
                        current_product = row["Product title"]
                    else:
                        # This might be a variant row with product title filled
                        current_product = row["Product title"]
                else:
                    # This is a variant row with empty product title - fill it
                    if current_product:
                        df_old_merged.loc[idx, "Product title"] = current_product
            
            # Filter out summary rows and rows where delivery rate is empty/null
            df_old_merged = df_old_merged[
                (df_old_merged["Product variant title"] != "ALL VARIANTS (TOTAL)") &
                (df_old_merged["Product variant title"] != "ALL PRODUCTS") &
                (df_old_merged["Delivery Rate"].notna()) & 
                (df_old_merged["Delivery Rate"] != "")
            ]
            
            # Create normalized versions for matching (case insensitive)
            df_old_merged["Product title_norm"] = df_old_merged["Product title"].astype(str).str.strip().str.lower()
            df_old_merged["Product variant title_norm"] = df_old_merged["Product variant title"].astype(str).str.strip().str.lower()
            
            st.success(f"✅ Loaded {len(df_old_merged)} records with delivery rates from old merged data")
            st.write("Preview of old data with delivery rates:")
            st.write(df_old_merged[["Product title", "Product variant title", "Delivery Rate"]].head())
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
        result = process.extractOne(
            name, mapping.keys(), scorer=fuzz.token_sort_ratio, score_cutoff=85
        )
        if result:
            best_match, score = result[0], result[1]
            mapping[name] = mapping[best_match]
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

    # ---- IMPORT DELIVERY RATES FROM OLD DATA ----
    if df_old_merged is not None:
        # Create normalized versions for matching (case insensitive)
        df_shopify["Product title_norm"] = df_shopify["Product title"].astype(str).str.strip().str.lower()
        df_shopify["Product variant title_norm"] = df_shopify["Product variant title"].astype(str).str.strip().str.lower()
        
        # Create a lookup dictionary from old data
        delivery_rate_lookup = {}
        for _, row in df_old_merged.iterrows():
            key = (row["Product title_norm"], row["Product variant title_norm"])
            delivery_rate_lookup[key] = row["Delivery Rate"]
        
        # Match and update delivery rates
        matched_count = 0
        for idx, row in df_shopify.iterrows():
            key = (row["Product title_norm"], row["Product variant title_norm"])
            if key in delivery_rate_lookup:
                df_shopify.loc[idx, "Delivery Rate"] = delivery_rate_lookup[key]
                matched_count += 1
        
        # Clean up temporary normalized columns
        df_shopify = df_shopify.drop(columns=["Product title_norm", "Product variant title_norm"])
        
        st.success(f"✅ Successfully imported delivery rates for {matched_count} product variants from old data")

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
    
    # ---- FILTER PRODUCTS/VARIANTS WITH NET ITEMS SOLD < 5 ----
    product_sales = df_shopify.groupby("Product title")["Net items sold"].sum()

    # Keep only products with total >= 5
    valid_products = product_sales[product_sales >= 5].index

    # Filter Shopify data
    df_shopify = df_shopify[df_shopify["Product title"].isin(valid_products)].reset_index(drop=True)

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
    
    # Show delivery rate import summary
    if df_old_merged is not None:
        delivery_rate_filled = df_shopify["Delivery Rate"].astype(str).str.strip()
        delivery_rate_filled = delivery_rate_filled[delivery_rate_filled != ""]
        st.info(f"📊 Delivery rates imported: {len(delivery_rate_filled)} out of {len(df_shopify)} variants")
    
    st.write(df_shopify)

    # ---- CONVERT SHOPIFY TO EXCEL (STRUCTURED WITH FORMULAS) ----
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

            # Write header row
            for col_num, value in enumerate(df.columns.values):
                worksheet.write(0, col_num, value, header_format)

            # determine column indexes once
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

            # numeric columns we sum at product & grand level
            cols_to_sum = [
                "Net items sold", "Net Revenue", "Ad Spend (INR)",
                "Shipping Cost", "Operational Cost", "Product Cost (Output)", "Net Profit"
            ]
            cols_to_sum_idx = [df.columns.get_loc(c) for c in cols_to_sum]

            # Grand total row index (0-based)
            grand_total_row_idx = 1
            worksheet.write(grand_total_row_idx, 0, "GRAND TOTAL", grand_total_format)
            worksheet.write(grand_total_row_idx, 1, "ALL PRODUCTS", grand_total_format)

            # start writing product blocks after grand total row
            row = grand_total_row_idx + 1
            product_total_rows = []  # keep track of product total rows

            # group by product and write structured blocks
            for product, product_df in df.groupby("Product title"):
                product_total_row_idx = row
                product_total_rows.append(product_total_row_idx)

                # product total row label
                worksheet.write(product_total_row_idx, 0, product, product_total_format)
                worksheet.write(product_total_row_idx, 1, "ALL VARIANTS (TOTAL)", product_total_format)

                n_variants = len(product_df)
                first_variant_row_idx = product_total_row_idx + 1
                last_variant_row_idx = product_total_row_idx + n_variants

                # write product-level SUM formulas for numeric columns
                for col_idx in cols_to_sum_idx:
                    col_letter = xl_col_to_name(col_idx)
                    excel_first = first_variant_row_idx + 1
                    excel_last = last_variant_row_idx + 1
                    worksheet.write_formula(
                        product_total_row_idx, col_idx,
                        f"=SUM({col_letter}{excel_first}:{col_letter}{excel_last})",
                        product_total_format
                    )

                # ✅ FIX: product-level Net Profit % formula
                rev_col_letter = xl_col_to_name(revenue_col)
                np_col_letter = xl_col_to_name(net_profit_col)
                excel_row = product_total_row_idx + 1
                worksheet.write_formula(
                    product_total_row_idx, net_profit_percent_col,
                    f"=IF(N({rev_col_letter}{excel_row})=0,0,N({np_col_letter}{excel_row})/N({rev_col_letter}{excel_row})*100)",
                    product_total_format
                )

                # write variant rows
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
                        elif col_name == "Ad Spend (INR)":
                            worksheet.write(variant_row_idx, col_idx, variant.get("Ad Spend (INR)", 0.0), variant_format)
                        elif col_name == "Delivery Rate":
                            # Use the imported delivery rate if available, otherwise leave empty
                            delivery_rate_val = variant.get("Delivery Rate", "")
                            worksheet.write(variant_row_idx, col_idx, delivery_rate_val, variant_format)
                        elif col_name == "Product Cost (Input)":
                            worksheet.write(variant_row_idx, col_idx, "", variant_format)
                        elif col_name == "Delivered Orders":
                            rate_term = f"IF(N({rate_ref})>1, N({rate_ref})/100, N({rate_ref}))"
                            worksheet.write_formula(
                                variant_row_idx, col_idx,
                                f"=INT(N({sold_ref})*{rate_term})",
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
                            worksheet.write_formula(
                                variant_row_idx, col_idx,
                                f"=N({pc_input_ref})*N({delivered_ref})",
                                variant_format
                            )
                        elif col_name == "Net Profit":
                            worksheet.write_formula(
                                variant_row_idx, col_idx,
                                f"=N({revenue_ref})-N({ad_spend_ref})-N({shipping_ref})-N({pc_output_ref})-N({op_ref})",
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

            # ---- GRAND TOTAL = sum of product total rows ----
            if product_total_rows:
                for col_idx in cols_to_sum_idx:
                    col_letter = xl_col_to_name(col_idx)
                    total_refs = [f"{col_letter}{r+1}" for r in product_total_rows]
                    formula = f"=SUM({','.join(total_refs)})"
                    worksheet.write_formula(
                        grand_total_row_idx, col_idx,
                        formula,
                        grand_total_format
                    )
                # ✅ FIX: grand total Net Profit %
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

# ---- CAMPAIGN DOWNLOAD ----
if campaign_file:
    def convert_df_to_excel(df):
        output = BytesIO()
        with pd.ExcelWriter(output, engine="openpyxl") as writer:
            df.to_excel(writer, index=False, sheet_name="Processed Data")
        return output.getvalue()

    excel_data = convert_df_to_excel(grouped_campaign)
    st.download_button(
        label="📥 Download Processed Campaign File (Excel)",
        data=excel_data,
        file_name="processed_campaigns.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )

