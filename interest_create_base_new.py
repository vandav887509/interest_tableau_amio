import logging
from sqlalchemy import text
from declaration import engine_pg

# ── Mode indicator ─────────────────────────────────────────────
# Set to True to insert test records only (no real data)
# Set to False to run full pipeline with real data
TEST_MODE = False

# ── Logging setup ──────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger(__name__)

# ── Test records (Stage 3) ─────────────────────────────────────
TEST_RECORDS = [
    {
        "int_id_contract":     "TEST-001",
        "int_report_date":     "2025-12-31",
        "int_stage":           3,
        "int_ecl":             8.2,
        "int_rate_effect":     0.1,
        "int_rate_nominal":    0.1,
        "int_amount_balance": 99.83,
        "int_ecl_prev":        8.2,
        "int_prev_stage":      3,
    },
]

TEST_INSERT_SQL = """
    INSERT INTO public.interest_base (
        int_id_contract,
        int_report_date,
        int_stage,
        int_ecl,
        int_rate_effect,
        int_rate_nominal,
        int_amount_balance,
        int_ecl_prev,
        int_prev_stage
    ) VALUES (
        :int_id_contract,
        :int_report_date,
        :int_stage,
        :int_ecl,
        :int_rate_effect,
        :int_rate_nominal,
        :int_amount_balance,
        :int_ecl_prev,
        :int_prev_stage
    );
"""

TEST_CALC_SQL = """
    UPDATE public.interest_base
    SET
        interest_rev_ifrs = CASE
            WHEN int_stage = 3
            THEN (int_rate_effect / 12) * (int_amount_balance - int_ecl_prev)
            ELSE (int_rate_effect / 12) * int_amount_balance
        END,
        int_impair = int_ecl - int_ecl_prev,
        revers_impair = CASE
            WHEN int_stage <> 3
            THEN GREATEST(int_ecl_prev - int_ecl, 0)
            ELSE 0
        END
    WHERE int_id_contract LIKE 'TEST-%';
"""

# ── Live SQL steps ─────────────────────────────────────────────
LIVE_STEPS = [
    {
        "name": "Step 1: Truncate interest_base",
        "sql": """
            TRUNCATE TABLE public.interest_base;
        """
    },
    {
        "name": "Step 2: Insert base records from ECL_Collective_202512 (Stage > 1)",
        "sql": """
            INSERT INTO public.interest_base (int_id_contract, int_report_date, int_stage, int_ecl)
            SELECT trim("ID договора"), "Дата отчётности", "Stage", "ECL"
            FROM public."ECL_Collective_202512"
            WHERE "Stage" > 1;
        """
    },
    {
        "name": "Step 3: Update interest rates from CollectiveLoan_202512",
        "sql": """
            UPDATE public.interest_base
            SET 
                int_rate_effect  = s."Interest_Rate_Effective",
                int_rate_nominal = s."Interest_Rate_Nominal",
                int_amount_interest = s."Amount_Interest",
                int_amount_balance = s."Amount_Balance"
            FROM public."CollectiveLoan_202512" s
            WHERE trim(s."ID_Contract") = trim(public.interest_base.int_id_contract) and s."Date_Report" = '2025-12-31';
        """
    },
    {
        "name": "Step 4: Update balance amount from CollectiveLoan_202511",
        "sql": """
            UPDATE public.interest_base
            SET int_amount_balance_prev = s."Amount_Balance",
            int_amount_interest_prev = s."Amount_Interest"
            FROM public."CollectiveLoan_202511" s
            WHERE trim(s."ID_Contract") = trim(public.interest_base.int_id_contract) and s."Date_Report" = '2025-11-30';
        """
    },
    {
        "name": "Step 5: Update previous ECL and stage from ECL_Collective_202511",
        "sql": """
            UPDATE public.interest_base
            SET 
                int_ecl_prev   = s."ECL",
                int_prev_stage = s."Stage"
            FROM public."ECL_Collective_202511" s
            WHERE trim(s."ID договора") = trim(public.interest_base.int_id_contract);
        """
    },
    {
        "name": "Step 6: Update EAD from and stage from ECL_Collective_202511",
        "sql": """
            UPDATE public.interest_base
            SET 
                int_ead_1   = s."1"
            FROM public."EAD_Result_202511" s
            WHERE trim(s."ID договора") = trim(public.interest_base.int_id_contract);
        """
    },
    {
        "name": "Step 6: Calculate interest_rev_ifrs, int_impair, revers_impair",
        "sql": """
            UPDATE public.interest_base
            SET
                interest_rev_ifrs = CASE
                    WHEN int_stage = 3 AND (int_amount_balance - int_ecl_prev) > 0
                    THEN (POWER(1 + int_rate_effect/100, 1.0/12) - 1) * (int_amount_balance - int_ecl_prev)
                    ELSE (1)
                END,
                int_impair = int_ecl - int_ecl_prev,
                revers_impair = CASE
                    WHEN int_stage <> 3 OR (int_ecl - int_ecl_prev) < 0
                    THEN GREATEST(int_ecl_prev - int_ecl, 0)
                    ELSE 0
                END;
        """
    },

    {
        "name": "Step 6: Delete non stage 3 to 2",
        "sql": """
                DELETE FROM public.interest_base where "int_prev_stage" < 3 AND "int_stage" < 3
        """
    },
    {
        "name": "Step 7: Calculate Stage_3 Interest rate ",
        "sql": """
            UPDATE public.interest_base
            SET
                stage_interest_gap = (int_amount_interest - int_amount_interest_prev) - interest_rev_ifrs
        """
    },
    {   "name": "Step 8: Calculate interest_rev_ifrs, int_impair, revers_impair",
        "sql": """
            UPDATE public.interest_base
            SET
                openning_nca = int_amount_balance_prev - int_ecl_prev, impair_pl = int_ecl - int_ecl_prev,
                cure_reversal = GREATEST(-(int_ecl - int_ecl_prev), 0), monthly_interest_amio = int_amount_interest - int_amount_interest_prev;
        """
    },
     {   "name": "Step 8-1: Calculate interest_rev_ifrs, int_impair, revers_impair",
        "sql": """
                UPDATE public.interest_base
SET monthly_interest_gt =
    int_amount_balance *
    (POWER(1 + (int_rate_effect / 100.0), 1.0/12.0) - 1), closing_nca = int_amount_balance-int_ecl;
        """
    },
    {   "name": "Step 8-2: Calculate interest_rev_ifrs, int_impair, revers_impair",
        "sql": """
                UPDATE public.interest_base
SET ifrs_interest =
    openning_nca *
    (POWER(1 + (int_rate_effect / 100.0), 1.0/12.0) - 1);
        """
    },
    {   "name": "Step 8-2: Calculate interest_rev_ifrs, int_impair, revers_impair",
        "sql": """
                UPDATE public.interest_base
SET ifrs_interest = 0 where ifrs_interest < 0;
        """
    },
    {   "name": "Step 8-2: Calculate interest_rev_ifrs, int_impair, revers_impair",
        "sql": """
                UPDATE public.interest_base
SET stage3_interest_gap_bas = monthly_interest_amio - ifrs_interest;
        """
    },
    {   "name": "Step 8-2: Calculate interest_rev_ifrs, int_impair, revers_impair",
        "sql": """
                UPDATE public.interest_base
SET stage3_interest_gap_bas = 0 where stage3_interest_gap_bas < 0;
        """
    },
    {   "name": "Step 8-2: Calculate interest_rev_ifrs, int_impair, revers_impair",
        "sql": """
                UPDATE public.interest_base
SET stage3_interest_gap_bas_calc = monthly_interest_gt - ifrs_interest;
        """
    },
    {
        "name": "Step 9: Insert calculated results into interest_rate",
        "sql": """
            INSERT INTO public.interest_rate (
                int_id_contract,
                report_month,
                stage_current,
                interest_revenue,
                iloss,
                cure_revers
            )
            SELECT
                int_id_contract,
                int_report_date,
                int_stage,
                interest_rev_ifrs,
                int_impair,
                revers_impair
            FROM public.interest_base;
        """
    },
]

# ── Main runner ────────────────────────────────────────────────
def run():
    with engine_pg.begin() as conn:
        try:
            if TEST_MODE:
                # ── TEST MODE: insert test records only ────────
                log.info(">>> Running in TEST MODE <<<")
                log.info("Truncating interest_base for clean test...")
                conn.execute(text("TRUNCATE TABLE public.interest_base;"))

                log.info("Inserting test records...")
                for record in TEST_RECORDS:
                    log.info(f"  Inserting: {record['int_id_contract']}")
                    conn.execute(text(TEST_INSERT_SQL), record)

                log.info("Running calculations for test records...")
                conn.execute(text(TEST_CALC_SQL))

                log.info("TEST MODE completed successfully. Transaction committed.")

            else:
                # ── LIVE MODE: run full pipeline ───────────────
                log.info(">>> Running in LIVE MODE <<<")
                for step in LIVE_STEPS:
                    log.info(f"Running: {step['name']}")
                    result = conn.execute(text(step["sql"]))
                    log.info(f"  Done — rows affected: {result.rowcount}")

                log.info("LIVE MODE completed successfully. Transaction committed.")

        except Exception as e:
            log.error("Error occurred — transaction rolled back.")
            log.error(f"Error: {e}")
            raise

if __name__ == "__main__":
    run()
