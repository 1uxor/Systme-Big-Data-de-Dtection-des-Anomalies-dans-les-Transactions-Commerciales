let
    Source = LoadCsv("period_stats.csv", {
        {"period", type text}, {"count", Int64.Type},
        {"total_amount", type number}, {"average_amount", type number}
    })
in
    Source
