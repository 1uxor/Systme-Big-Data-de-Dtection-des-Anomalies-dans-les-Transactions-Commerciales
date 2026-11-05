let
    Source = LoadCsv("period_anomalies.csv", {
        {"previous_period", type text}, {"period", type text},
        {"previous_count", Int64.Type}, {"current_count", Int64.Type},
        {"variation_percent", type number}, {"reason", type text}
    })
in
    Source
