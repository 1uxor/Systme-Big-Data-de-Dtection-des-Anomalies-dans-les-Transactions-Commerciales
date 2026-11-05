let
    Source = LoadCsv("transaction_anomalies.csv", {
        {"transaction_id", type text}, {"timestamp", type datetime}, {"transaction_date", type date},
        {"customer_id", type text}, {"product_id", type text}, {"category", type text},
        {"city", type text}, {"payment_method", type text}, {"amount", type number},
        {"category_mean", type number}, {"category_std", type number},
        {"threshold", type number}, {"reason", type text}
    })
in
    Source
