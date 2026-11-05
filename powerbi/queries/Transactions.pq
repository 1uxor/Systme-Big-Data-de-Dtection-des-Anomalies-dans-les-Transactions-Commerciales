let
    Source = LoadCsv("transactions.csv", {
        {"transaction_id", type text}, {"timestamp", type datetime}, {"transaction_date", type date},
        {"year", Int64.Type}, {"month", Int64.Type}, {"day", Int64.Type}, {"hour", Int64.Type},
        {"customer_id", type text}, {"product_id", type text}, {"category", type text},
        {"quantity", Int64.Type}, {"unit_price", type number}, {"total_amount", type number},
        {"payment_method", type text}, {"city", type text}
    })
in
    Source
