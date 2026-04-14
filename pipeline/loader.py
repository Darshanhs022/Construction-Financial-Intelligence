import os
import io
import pandas as pd
import psycopg2
from psycopg2 import sql
from config1 import RAW_DIR, FILES, DB_CONFIG

schemes = {
    "project_master": {
        "required_cols": ["project_id", "project_name", "location", "project_type",
                          "planned_start_date", "planned_end_date", "revised_end_date",
                          "currency", "status", "total_original_budget"],
        "date_cols":    ["planned_start_date", "planned_end_date", "revised_end_date"],
        "numeric_cols": ["total_original_budget"],
    },
    "budget_cost_codes": {
        "required_cols": ["project_id", "cost_code", "cost_description",
                          "original_budget", "approved_variations", "revised_budget",
                          "revision_date", "revision_reason", "cost_category"],
        "date_cols":    ["revision_date"],
        "numeric_cols": ["original_budget", "approved_variations", "revised_budget"],
    },
    "vendor_contracts": {
        "required_cols": ["contract_id", "project_id", "cost_code", "vendor_id",
                          "vendor_name", "original_contract_value", "approved_amendments",
                          "revised_contract_value", "contract_start_date",
                          "contract_end_date", "payment_terms_days", "contract_status"],
        "date_cols":    ["contract_start_date", "contract_end_date"],
        "numeric_cols": ["original_contract_value", "approved_amendments",
                         "revised_contract_value", "payment_terms_days"],
    },
    "purchase_orders": {
        "required_cols": ["po_number", "contract_id", "project_id", "cost_code",
                          "vendor_id", "po_amount", "po_date", "po_status",
                          "expected_delivery_date"],
        "date_cols":    ["po_date", "expected_delivery_date"],
        "numeric_cols": ["po_amount"],
    },
    "vendor_invoices": {
        "required_cols": ["invoice_number", "po_number", "project_id", "cost_code",
                          "vendor_id", "invoice_amount", "invoice_date",
                          "payment_status", "due_date"],
        "date_cols":    ["invoice_date", "due_date", "payment_date"],
        "numeric_cols": ["invoice_amount"],
    },
    "accruals": {
        "required_cols": ["accrual_id", "project_id", "cost_code", "vendor_id",
                          "accrual_month", "accrued_amount", "accrual_reason",
                          "reversal_month"],
        "date_cols":    ["accrual_month", "reversal_month"],
        "numeric_cols": ["accrued_amount"],
    },
}

staging_tables = {
    "project_master":    "stg_project_master",
    "budget_cost_codes": "stg_budget_cost_codes",
    "vendor_contracts":  "stg_vendor_contracts",
    "purchase_orders":   "stg_purchase_orders",
    "vendor_invoices":   "stg_vendor_invoices",
    "accruals":          "stg_accruals",
}

def process(name):
    path=os.path.join(RAW_DIR,FILES[name])
    if not os.path.exists(path):
        raise FileNotFoundError(f"Raw file not found:{path}")
    
    df=pd.read_csv(path)
    schema=schemes[name]

    missing=[c for c in schema['required_cols'] if c not in df.columns]
    if missing:
        raise ValueError(f"[{name}] missing columns:{missing}")
    
    for col in df.select_dtypes(include=['object']).columns:
        if col.endswith(('_id','_number','_code')):
            df[col]=df[col].astype(str).str.strip()  

    for col in schema.get('date_cols',[]):
        if col in df.columns:
            df[col]=pd.to_datetime(df[col],errors='coerce')

    for col in schema.get('numeric_cols',[]):
        if col in df.columns:
            df[col]=pd.to_numeric(df[col],errors='coerce')   

    return df


def df_sql(df,table,conn):

    df=df.where(pd.notnull(df),None)

    columns=list(df.columns)
    col_str=",".join(f'"{c}"' for c in columns)

    with conn.cursor() as cur:
        cur.execute(f'TRUNCATE TABLE {table} RESTART IDENTITY CASCADE;')
        buffer=io.StringIO()
        df.to_csv(buffer,index=False,header=False,na_rep="\\N")
        buffer.seek(0)

        copy_sql=(
            f"copy {table} ({col_str})"  
            f"from STDIN with (format csv,null '\\N')"
        )
        cur.copy_expert(copy_sql,buffer)

    conn.commit()
    return(len(df))

def load_all():
    print('ETL - LOADING RAW DATA INTO POSTGRES')
    conn=psycopg2.connect(**DB_CONFIG) 

    try:
        for name,table in staging_tables.items():
            df=process(name)
            rows=df_sql(df,table,conn)
            print(f"{name} {rows}rows →  {table} ")
    finally:
        conn.close()

if __name__ == "__main__":
    load_all()               
             
