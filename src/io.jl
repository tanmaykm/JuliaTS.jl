# relies on other packages at present
# should have built in io functions in future

@require DataFrames begin
TArray(keynames, df::DataFrames.DataFrame) = TArray(keynames, Pair[n=>df[n] for n in DataFrames.names(df)]...)
end

@require TimeSeries begin
TArray(ts::TimeSeries.TimeArray) = TArray((:timestamp,), ts)
TArray(keynames, ts::TimeSeries.TimeArray) = TArray(keynames, keynames[1]=>ts.timestamp, Pair[symbol(n)=>ts[n].values for n in TimeSeries.colnames(ts)]...)
end
