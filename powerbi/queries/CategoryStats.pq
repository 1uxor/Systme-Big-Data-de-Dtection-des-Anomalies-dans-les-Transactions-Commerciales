let
    Source = LoadCsv("category_stats.csv", {
        {"category", type text}, {"count", Int64.Type}, {"total_amount", type number},
        {"mean", type number}, {"std", type number}
    })
in
    Source
