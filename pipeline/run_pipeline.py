import sys
import os

sys.path.insert(0,os.path.dirname(os.path.abspath(__file__)))

from loader1    import load_all
from Validator1 import run_all_validations
from config1    import RUN_DATE

def print_summary(summary):
    fails = summary[summary["status"] == "FAIL"]
    passes = summary[summary["status"] == "PASS"]
    exp = fails["exposure_cr_inr"].sum()

    print(f"""
    Pipeline complete \n
    Run Date     :{RUN_DATE}\n
    Rules Passes :{len(passes)}\n
    Rules Failed :{len(fails)}\n
    Total Exposure :₹{exp} cr INR     
    """)

def run():
    load_all()
    summary = run_all_validations()
    print_summary(summary)

if __name__ == "__main__":
    run()    


