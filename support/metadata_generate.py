# Build complete and optimized metadata for all provided tables based on sample data and schema notes

# Define type mapping and consistency rules for PostgreSQL
def define_column(logical_name, physical_name, index, dtype, mode="", pk=False, sample_value="", date_format="", comment=""):
    return {
        "logical_name_en": logical_name,
        "physical_name": physical_name,
        "mode": mode,
        "type": dtype,
        "convert_boolean_to_flag": comment,
        "date_format": date_format,
        "index": str(index),
        "primary_key": "Y" if pk else "",
        "sample_value": sample_value,
        "timezone": ""
    }

# Construct all metadata
metadata_pg = {
    "customers": {
        "columns": [
            define_column("Customer ID", "cstmr_id", 1, "INTEGER", mode="REQUIRED", pk=True),
            define_column("Name", "cstmr_name", 2, "TEXT", mode="REQUIRED"),
            define_column("Email", "cstmr_email", 3, "TEXT", mode="REQUIRED"),
            define_column("Telephone", "cstmr_tel", 4, "TEXT"),
            define_column("City", "cstmr_city", 5, "TEXT"),
            define_column("Country", "cstmr_cntry", 6, "TEXT"),
            define_column("Gender", "cstmr_gender", 7, "CHAR(1)", comment="M or F"),
            define_column("Date of Birth", "cstmr_birthdate", 8, "DATE", date_format="YYYY-MM-DD"),
            define_column("Job Title", "cstmr_job_title", 9, "TEXT")
        ]
    },
    "discounts": {
        "columns": [
            define_column("Start Date", "dscnt_start_date", 1, "DATE", mode="REQUIRED", date_format="YYYY-MM-DD"),
            define_column("End Date", "dscnt_end_date", 2, "DATE", date_format="YYYY-MM-DD"),
            define_column("Discount", "dscnt_value", 3, "NUMERIC(3,2)", mode="REQUIRED", comment="Range: 0 to 1"),
            define_column("Description", "dscnt_desc", 4, "TEXT"),
            define_column("Category", "dscnt_ctgry", 5, "TEXT"),
            define_column("Sub Category", "dscnt_sub_ctgry", 6, "TEXT")
        ]
    },
    "employees": {
        "columns": [
            define_column("Employee ID", "emply_id", 1, "INTEGER", mode="REQUIRED", pk=True),
            define_column("Store ID", "str_id", 2, "INTEGER"),
            define_column("Name", "emply_name", 3, "TEXT"),
            define_column("Position", "emply_pstn", 4, "TEXT")
        ]
    },
    "products": {
        "columns": [
            define_column("Product ID", "prd_id", 1, "INTEGER", mode="REQUIRED", pk=True),
            define_column("Category", "prd_ctgry", 2, "TEXT", mode="REQUIRED"),
            define_column("Sub Category", "prd_sub_ctgry", 3, "TEXT", mode="REQUIRED"),
            define_column("Description PT", "prd_desc_pt", 4, "TEXT"),
            define_column("Description DE", "prd_desc_de", 5, "TEXT"),
            define_column("Description FR", "prd_desc_fr", 6, "TEXT"),
            define_column("Description ES", "prd_desc_es", 7, "TEXT"),
            define_column("Description EN", "prd_desc_en", 8, "TEXT"),
            define_column("Description ZH", "prd_desc_zh", 9, "TEXT"),
            define_column("Color", "prd_color", 10, "TEXT"),
            define_column("Sizes", "prd_sizes", 11, "TEXT", comment="e.g., S|M|L"),
            define_column("Production Cost", "prd_cost", 12, "NUMERIC(10,2)", mode="REQUIRED")
        ]
    },
    "stores": {
        "columns": [
            define_column("Store ID", "str_id", 1, "INTEGER", mode="REQUIRED", pk=True),
            define_column("Country", "str_cntry", 2, "TEXT"),
            define_column("City", "str_city", 3, "TEXT"),
            define_column("Store Name", "str_name", 4, "TEXT"),
            define_column("Number of Employees", "str_emply_num", 5, "INTEGER"),
            define_column("ZIP Code", "str_zip_cd", 6, "TEXT"),
            define_column("Latitude", "str_lat", 7, "NUMERIC(9,6)", mode="REQUIRED"),
            define_column("Longitude", "str_lon", 8, "NUMERIC(9,6)", mode="REQUIRED")
        ]
    },
    "transactions": {
        "columns": [
            define_column("Invoice ID", "trn_invc_id", 1, "TEXT", mode="REQUIRED", pk=True),
            define_column("Line", "trn_line", 2, "INTEGER", mode="REQUIRED", pk=True),
            define_column("Customer ID", "cstmr_id", 3, "INTEGER", mode="REQUIRED"),
            define_column("Product ID", "trn_prd_id", 4, "INTEGER", mode="REQUIRED"),
            define_column("Size", "trn_size", 5, "TEXT"),
            define_column("Color", "trn_clr", 6, "TEXT"),
            define_column("Unit Price", "trn_unit_prc", 7, "NUMERIC(10,2)", mode="REQUIRED"),
            define_column("Quantity", "trn_qty", 8, "INTEGER", mode="REQUIRED"),
            define_column("Date", "trn_date", 9, "TIMESTAMP", mode="REQUIRED", date_format="YYYY-MM-DD HH:MM:SS"),
            define_column("Discount", "trn_dscnt", 10, "NUMERIC(3,2)", comment="Range (0,1)", mode="REQUIRED"),
            define_column("Line Total", "trn_line_ttl", 11, "NUMERIC(10,2)"),
            define_column("Store ID", "str_id", 12, "INTEGER"),
            define_column("Employee ID", "emply_id", 13, "INTEGER"),
            define_column("Currency", "trn_crncy", 14, "TEXT"),
            define_column("Currency Symbol", "trn_crncy_sbl", 15, "TEXT"),
            define_column("SKU", "trn_sku", 16, "TEXT"),
            define_column("Transaction Type", "trn_type", 17, "TEXT"),
            define_column("Payment Method", "trn_pymnt_mthd", 18, "TEXT"),
            define_column("Invoice Total", "trn_invc_ttl", 19, "NUMERIC(10,2)")
        ]
    }
}

import json

with open("table_metadata.json", "w", encoding="utf-8") as f:
    json.dump(metadata_pg, f, indent=2, ensure_ascii=False)
    
print("✅ Metadata saved to table_metadata.json")
