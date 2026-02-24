from pyspark.sql.functions import col, regexp_replace, concat, lit, repeat, substring, length

def clean_currency(df, column_name):
    """Removes '$' and ',' and converts to Decimal(10,2)."""
    df_cleaned = df.withColumn(
        column_name, 
        regexp_replace(col(column_name), "[$,]", "")
    )
    return df_cleaned

def mask_card_number(df, column_name):
    """Masks all digits of the card number except the last 4"""
    card = col(column_name)
    df_masked = df.withColumn(
        column_name,
        concat(
            repeat(lit("*"), length(card) - 4),
            substring(card, -4, 4)
        )
    )
    return df_masked