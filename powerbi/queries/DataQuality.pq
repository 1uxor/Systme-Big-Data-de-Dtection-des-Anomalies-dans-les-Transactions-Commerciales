let
    Source = LoadCsv("data_quality.csv", {{"metric", type text}, {"value", type text}})
in
    Source
