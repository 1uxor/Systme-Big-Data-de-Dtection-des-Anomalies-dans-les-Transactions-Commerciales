let
    Source = LoadCsv("categories.csv", {{"category", type text}})
in
    Source
