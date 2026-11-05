(fileName as text, columnTypes as list) as table =>
let
    Source = Csv.Document(
        File.Contents(pDataFolder & "/" & fileName),
        [Delimiter = ",", Encoding = 65001, QuoteStyle = QuoteStyle.Csv]
    ),
    Headers = Table.PromoteHeaders(Source, [PromoteAllScalars = true]),
    Typed = Table.TransformColumnTypes(Headers, columnTypes, "en-US")
in
    Typed
