let
    Source = LoadCsv("customer_anomalies.csv", {
        {"customer_id", type text}, {"transaction_count", Int64.Type},
        {"total_amount", type number}, {"reason", type text}
    })
in
    Source
