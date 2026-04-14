import psycopg2
import psycopg2.extras
import pandas as pd
from datetime import date
from config1 import DB_CONFIG, THRESHOLDS, CR_DIVISOR, RUN_DATE

class Exception_log:

    def __init__(self,conn):
        self.conn = conn
        self.summary=[]

    def log(self,rule,category,df,exposure_col=None):
        violation_count=len(df)
        exposure_cr=0.0

        if violation_count > 0 and exposure_col and exposure_col in df.columns:
            exposure_cr = round(df[exposure_col].fillna(0).sum(), 2)

        status='FAIL' if violation_count > 0 else 'PASS'

        self.summary.append({
            'rule':rule,
            'category':category,
            'violation_count':violation_count,
            'exposure_cr_inr':exposure_cr,
            'status':status,
            'run_date':RUN_DATE,
        })
        
        if status=='FAIL':
            print(f'[{category}] {rule} {violation_count} issues | {exposure_cr} cr')

    def write_summary_db(self):
        df=pd.DataFrame(self.summary)
        with self.conn.cursor() as cur:
            cur.execute("TRUNCATE TABLE exceptions_summary;")
            for _,row in df.iterrows():
                cur.execute(
                    '''INSERT INTO exceptions_summary
                        (rule,category,violation_count,exposure_cr_inr,status,run_date)
                       values(%s,%s,%s,%s,%s,%s) 
                    ''',
                    (row["rule"], row["category"], int(row["violation_count"]),
                     float(row["exposure_cr_inr"]), row["status"], row["run_date"])
                )
        self.conn.commit()
        return df     
                        
def query(conn,sql):                                                                      #pd.read_sql
    with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
        cur.execute(sql)
        rows=cur.fetchall()
        if rows:
            return pd.DataFrame(rows,columns=[desc[0] for desc in cur.description])
    return pd.DataFrame()        
    
# ══════════════════════════════════════════════════════════════════════════════
#  BUDGET CONTROLS  
# ══════════════════════════════════════════════════════════════════════════════
def check_budget(conn,ex):
    util_threshold=THRESHOLDS["budget_utilization_warning"]
    var_threshold=THRESHOLDS["variation_spike_pct"]
    
    df=query(conn,"""
        select 
            b.project_id,
            b.cost_code,
            b.cost_description,
            round(b.revised_budget /10000000.0,2) as revised_budget_cr,
            round(sum(c.revised_contract_value) /10000000.0,2) as total_contract_cr,
            round(
            (sum(c.revised_contract_value) - b.revised_budget) / 10000000.0, 2
            ) AS over_by_cr
        from stg_budget_cost_codes b
        left join stg_vendor_contracts c
        on b.project_id=c.project_id and b.cost_code=c.cost_code
        group by b.project_id,b.cost_code,b.cost_description,b.revised_budget
        having sum(c.revised_contract_value) > b.revised_budget
        order by over_by_cr desc
        """)
    ex.log("budget_contract_exceeds_budget","BUDGET",df,"over_by_cr")    

    df=query(conn,f"""
       select 
            b.project_id,
	        b.cost_code,
	        b.cost_description,
	        round(b.revised_budget /10000000.0,2) as revised_budget_cr,
	        round(sum(c.revised_contract_value)/10000000.0,2) as total_contract_cr,
	        round(
                sum(c.revised_contract_value)*100.0/ nullif(b.revised_budget,0),1
	        )as utilisation_pct
       from stg_budget_cost_codes as b
       left join stg_vendor_contracts as c 
             on b.project_id=c.project_id and b.cost_code=c.cost_code
       group by b.project_id,b.cost_code,b.cost_description,b.revised_budget
       having sum(c.revised_contract_value)/nullif(b.revised_budget,0) between {util_threshold} and 1.0
       order by utilisation_pct desc   
       """)     
    ex.log("budget_near_exhaustion","BUDGET",df,"total_contract_cr")

    df=query(conn,f"""
       select 
            project_id,
	        cost_code,
	        cost_description,
	        round(original_budget /10000000.0,2) as original_budget_cr,
	        round(approved_variations /10000000.0,2)as approved_variations_cr,
	        round(revised_budget /10000000.0,2)as revised_budget_cr,
	        round(
                 abs(approved_variations)*100.0/nullif(original_budget,0),1
	        )as variation_pct,
	        revision_reason
      from stg_budget_cost_codes	 
      where abs(approved_variations) /nullif(original_budget,0) > {var_threshold}
      order by variation_pct desc
       """)     
    ex.log("budget_high_variation_pct","BUDGET",df,"approved_variations_cr")

# ══════════════════════════════════════════════════════════════════════════════
#  CONTRACT CONTROLS 
# ══════════════════════════════════════════════════════════════════════════════ 
def check_contracts(conn,ex):
    var_threshold=THRESHOLDS["variation_spike_pct"]
    
    df=query(conn,f"""
      select
           contract_id,
           project_id,
           cost_code,
           vendor_name,
           round(original_contract_value / 10000000.0, 2) as original_contract_cr,
           round(approved_amendments     / 10000000.0, 2) as approved_amendments_cr,
           round(
                abs(approved_amendments) * 100.0 / nullif(original_contract_value, 0), 1
           ) AS amendment_pct
	  from stg_vendor_contracts
	  where abs(approved_amendments) /nullif(original_contract_value,0)>0.20
	  order by amendment_pct desc;
    """)
    ex.log("contract_amendment_spike", "CONTRACT", df, "approved_amendments_cr")

# ══════════════════════════════════════════════════════════════════════════════
#  PO CONTROLS  
# ══════════════════════════════════════════════════════════════════════════════  

def check_po(conn,ex):

    df = query(conn, """
        select
            p.po_number,
            p.contract_id,
            p.project_id,
            p.cost_code,
            p.po_status,
            p.po_date,
            p.expected_delivery_date,
            round(p.po_amount / 10000000.0, 2) AS po_amount_cr
        from stg_purchase_orders p
        where p.po_status = 'Open' and p.po_number not in (
              select distinct po_number FROM stg_vendor_invoices
          )
        order by p.po_amount desc
    """)
    ex.log("po_open_no_invoice", "PO", df, "po_amount_cr")

    df = query(conn, """
        select
            p.po_number,
            p.project_id,
            p.cost_code,
            round(p.po_amount / 10000000.0, 2) AS po_amount_cr,
            round(coalesce(sum(i.invoice_amount), 0) / 10000000.0, 2) as total_invoiced_cr,
            round(
                (coalesce(sum(i.invoice_amount), 0) - p.po_amount)
                / 10000000.0, 2
            ) as over_invoiced_by_cr
        from stg_purchase_orders p
        left join stg_vendor_invoices i ON p.po_number = i.po_number
        group by p.po_number, p.project_id, p.cost_code, p.po_amount
        having sum(i.invoice_amount) > p.po_amount
        order by over_invoiced_by_cr DESC
    """)
    ex.log("po_over_invoiced", "PO", df, "over_invoiced_by_cr")

# ══════════════════════════════════════════════════════════════════════════════
#  INVOICE CONTROLS 
# ══════════════════════════════════════════════════════════════════════════════

def check_invoices(conn, ex):
    cutoff = THRESHOLDS["overdue_cutoff_date"]

    df = query(conn, f"""
        select
            i.invoice_number,
            i.project_id,
            i.cost_code,
            i.vendor_id,
            round(i.invoice_amount / 10000000.0, 4) as invoice_amount_cr,
            i.invoice_date,
            i.due_date,
            current_date - i.due_date::date          AS days_overdue,
            case
                when current_date - i.due_date::date <= 30 then '0-30 days'
                when current_date - i.due_date::date <= 60 then '31-60 days'
                when current_date - i.due_date::date <= 90 then '61-90 days'
                else '90+ days'
            end as aging_bucket
        from stg_vendor_invoices i
        where i.payment_status = 'Unpaid'
          and i.due_date < date '{cutoff}'
        order by days_overdue desc
    """)
    ex.log("invoice_overdue_unpaid", "INVOICE", df, "invoice_amount_cr")

    df = query(conn, """
        select
            i.invoice_number,
            i.project_id,
            i.cost_code,
            i.vendor_id,
            round(i.invoice_amount / 10000000.0, 4) as invoice_amount_cr,
            i.invoice_date,
            p.po_date,
            p.po_date - i.invoice_date::date as days_before_po
        from stg_vendor_invoices i
        join stg_purchase_orders p ON i.po_number = p.po_number
        where i.invoice_date < p.po_date
        order by days_before_po desc
    """)
    ex.log("invoice_before_po_date", "INVOICE", df, "invoice_amount_cr")

# ══════════════════════════════════════════════════════════════════════════════
#  ACCRUAL CONTROLS  
# ══════════════════════════════════════════════════════════════════════════════

def check_accurals(conn,ex):
    df=query(conn,"""
    select
            accrual_id,
            project_id,
            cost_code,
            vendor_id,
            accrual_month,
            reversal_month,
            round(accrued_amount / 10000000.0, 4) AS accrued_amount_cr,
            accrual_reason,
            current_date - reversal_month::date    AS days_past_reversal
        FROM stg_accruals
        where reversal_month < current_date
        order by days_past_reversal desc
    """)
    ex.log("accrual_unreversed_open","ACCRUAL",df,"accrued_amount_cr")


def run_all_validations():
    print("COMMERCIAL CONTROL VALIDATION")

    conn=psycopg2.connect(**DB_CONFIG)
    ex=Exception_log(conn)

    try:
        check_budget(conn,ex)
        check_contracts(conn,ex)
        check_po(conn,ex)
        check_invoices(conn,ex)
        check_accurals(conn,ex)

        summary=ex.write_summary_db()
    finally:
        conn.close() 
    fails=summary[summary["status"]=='FAIL']  
    passes=summary[summary["status"]=='PASS']  
    
    print(f"{len(fails)} rules FAILED | {len(passes)} rules PASSED")
    total_exposure = fails['exposure_cr_inr'].astype(float).sum()
    print(f"Total financial exposure = ₹{total_exposure:.2f} cr")

    return summary

if __name__=="__main__":
    run_all_validations()

