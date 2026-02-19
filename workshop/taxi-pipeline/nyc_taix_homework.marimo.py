import marimo

__generated_with = "0.19.11"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo
    import dlt
    import ibis

    return dlt, mo


@app.cell
def _(dlt):
    # Reuse the existing dlt pipeline configured in `taxi_pipelinE.py`.
    # Assumes you have already run the pipeline at least once.
    pipeline = dlt.pipeline(
        pipeline_name="taxi_pipeline",
        destination="duckdb",
        dataset_name="nyc_taxi_data",
    )

    dataset = pipeline.dataset()
    ibis_con = dataset.ibis()

    ibis_con
    return ibis_con, pipeline


@app.cell
def _(ibis_con, pipeline):
    # Main trips table; adjust if your table name differs.
    table_name = "nyc_taxi_rides"
    db = pipeline.dataset_name

    t = ibis_con.table(table_name, database=db)
    t
    return (t,)


@app.cell
def _(mo, t):
    # Q1: start date and end date of the dataset
    # Try common pickup datetime column names.
    for col_q1 in [
        "trip_pickup_date_time",
        "tpep_pickup_datetime",
        "lpep_pickup_datetime",
        "pickup_time",
    ]:
        if col_q1 in t.columns:
            pickup_col = col_q1
            break
    else:
        raise ValueError("Could not find a pickup datetime column on the table.")

    q1_expr = t[[pickup_col]].aggregate(
        start_date=t[pickup_col].cast("date").min(),
        end_date=t[pickup_col].cast("date").max(),
    )
    q1 = q1_expr.execute().iloc[0]

    start_date = str(q1["start_date"])
    end_date = str(q1["end_date"])

    options_q1 = [
        ("2009-01-01", "2009-01-31"),
        ("2009-06-01", "2009-07-01"),
        ("2024-01-01", "2024-02-01"),
        ("2024-06-01", "2024-07-01"),
    ]
    chosen_q1 = next((opt for opt in options_q1 if opt == (start_date, end_date)), None)

    mo.md(
        f"""
        ## Question 1

        **Computed range**: `{start_date}` â†’ `{end_date}`  
        **Matching option**: `{chosen_q1}`  
        """
    )
    return


@app.cell
def _(mo, t):
    # Q2: proportion of trips paid with credit card
    # Try common payment columns.
    for col_q2 in ["payment_type", "payment", "payment_type_description"]:
        if col_q2 in t.columns:
            payment_col = col_q2
            break
    else:
        raise ValueError("Could not find a payment type column on the table.")

    sample_vals = t[[payment_col]].distinct().limit(50).execute()[payment_col].tolist()
    is_numeric = any(isinstance(v, (int, float)) for v in sample_vals if v is not None)

    if is_numeric:
        is_credit = t[payment_col] == 1
    else:
        is_credit = t[payment_col].cast("string").lower().isin(
            ["credit card", "credit", "card", "crd", "cc"]
        )

    q2_expr = t.aggregate(
        credit_prop=is_credit.ifelse(1.0, 0.0).mean(),
    )
    credit_prop = float(q2_expr.execute().iloc[0]["credit_prop"])
    credit_pct = credit_prop * 100.0

    options_q2 = [16.66, 26.66, 36.66, 46.66]
    chosen_q2 = min(options_q2, key=lambda x: abs(x - credit_pct))

    mo.md(
        f"""
        ## Question 2

        **Computed proportion (credit card)**: `{credit_pct:.2f}%`  
        **Closest option**: `{chosen_q2:.2f}%`  
        """
    )
    return


@app.cell
def _(mo, t):
    # Q3: total amount of money generated in tips
    # Try common tip columns.
    for col in ["tip_amt", "tip", "tips"]:
        if col in t.columns:
            tip_col = col
            break
    else:
        raise ValueError("Could not find a tip column on the table.")

    q3_expr = t.aggregate(total_tips=t[tip_col].cast("float64").sum())
    total_tips = float(q3_expr.execute().iloc[0]["total_tips"])

    options_q3 = [4063.41, 6063.41, 8063.41, 10063.41]
    chosen_q3 = min(options_q3, key=lambda x: abs(x - total_tips))

    mo.md(
        f"""
        ## Question 3

        **Computed total tips**: `${total_tips:,.2f}`  
        **Closest option**: `${chosen_q3:,.2f}`  
        """
    )
    return


if __name__ == "__main__":
    app.run()
