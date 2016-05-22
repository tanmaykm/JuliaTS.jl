# JuliaTS

[![Build Status](https://travis-ci.org/tanmaykm/JuliaTS.jl.svg?branch=master)](https://travis-ci.org/tanmaykm/JuliaTS.jl)

Implements some time series and relational algebra operations over [NDSparseData](https://github.com/JuliaComputing/NDSparseData.jl).

#### construct

````julia
TArray(colpairs::Pair...)
TArray(keynames::Tuple, colpairs::Pair...)
TArray(data::NDSparse, keynames::Tuple, valnames::Tuple)
TArray(from::TArray, colpairs::Pair...)
TArray(from::TimeSeries.TimeArray)
TArray(keynames::Tuple, from::TimeSeries.TimeArray)
TArray(keynames::Tuple, from::DataFrames.DataFrame)
````

#### get/set

Use Julia `getindex` and `setindex!` with single or range of values to match with every dimension. E.g.:
````julia
val = ta[DateTime("2016-01-01"):DateTime("2016-01-02"), [1, 2, 3], :open]
ta[DateTime("2016-01-01"), 1, :open] = (11.2, 31.2, 21.2, 30.0)
````

Get column vector: `getvals(ta::TArray, colname)`
Set column vector: `setvals(ta::TArray, colname, vals::Vector)`
Any column can be fetched by `get`, but `set` can be used only on value columns (keys can not be set).

#### set operations

````julia
project(ta::TArray, keynames, valnames)
union(ta1::TArray, ta2::TArray)
intersect(ta1::TArray, ta2::TArray)
difference(ta1::TArray, ta2::TArray)
````

#### rename columns

````julia
rename(ta::TArray, keynames, valnames)
rename!(ta::TArray, keynames, valnames)
rename(ta::TArray, pfx)
rename!(ta::TArray, pfx)
````

#### groupby

````julia
# fn: the aggregator function, called per group-column
# fns: tuple of functions, one per value column, when different aggregation functions are neeed for each column
groupby(ta::TArray, keynames, fn)
groupby(ta::TArray, keynames, fns::Tuple{Function})
````

#### join

````julia
# jointype can be :inner, :left, :right
# only inner join supported now
naturaljoin(taout::TArray, ta1::TArray, ta2::TArray, jointype, fn::Function)
````

#### shift

````julia
timeshift(ta::TArray, col, by)
timeshift!(ta::TArray, col, by)
````

#### window aggregation

````julia
# fn: the aggregaion function
window(taout::TArray, ta::TArray, wspec::Window, wintype, fn)

# window specification:
immutable Window{K,S,W}
    first::K        # start of first window
    last::K         # end of the last window
    step::S         # increment window start
    width::W        # window width
end

````
