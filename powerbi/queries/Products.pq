let
    Source = LoadCsv("products.csv", {
        {"product_id", type text}, {"category", type text}, {"reference_price", type number}
    })
in
    Source
