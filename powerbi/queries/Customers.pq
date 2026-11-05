let
    Source = LoadCsv("customers.csv", {
        {"customer_id", type text}, {"segment", type text}, {"city", type text}
    })
in
    Source
