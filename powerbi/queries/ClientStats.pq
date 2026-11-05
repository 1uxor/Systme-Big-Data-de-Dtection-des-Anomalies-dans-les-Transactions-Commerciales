let
    Source = LoadCsv("client_stats.csv", {
        {"customer_id", type text}, {"count", Int64.Type},
        {"total_amount", type number}, {"average_amount", type number}
    })
in
    Source
