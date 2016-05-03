# JuliaTS

[![Build Status](https://travis-ci.org/tanmaykm/JuliaTS.jl.svg?branch=master)](https://travis-ci.org/tanmaykm/JuliaTS.jl)

Implements some time series and relational algebra operations over [NDSparseData](https://github.com/JuliaComputing/NDSparseData.jl).

#### construct

````
TArray(colpairs::Pair...)
TArray(keynames::Tuple, colpairs::Pair...)
TArray(data::NDSparse, keynames::Tuple, valnames::Tuple)
TArray(from::TArray, colpairs::Pair...)
````

#### get/set

Use Julia `getindex` and `setindex!` with single or range of values to match with every dimension. E.g.:
````
val = ta[DateTime("2016-01-01"):DateTime("2016-01-02"), [1, 2, 3], :open]
ta[DateTime("2016-01-01"), 1, :open] = (11.2, 31.2, 21.2, 30.0)
````

Get column vector: `getvals(ta, colid)`
Set column vector: `setvals(ta, colid, vals::Vector)`

#### set operations

````
project(ta::TArray, keynames, valnames)
union(ta1::TArray, ta2::TArray)
intersect(ta1::TArray, ta2::TArray)
difference(ta1::TArray, ta2::TArray)
````

#### rename columns

````
rename(ta::TArray, keynames, valnames)
rename!(ta::TArray, keynames, valnames)
rename(ta::TArray, pfx)
rename!(ta::TArray, pfx)
````

#### groupby

````
# fn: the aggregator function, called per group-column
# fns: tuple of functions, one per value column, when different aggregation functions are neeed for each column
groupby(ta::TArray, keynames, fn)
groupby(ta::TArray, keynames, fns::Tuple{Function})
````

#### join

````
thetajoin(ta1::TArray, ta2::TArray, cond::Function, relnames=())
````

#### window aggregation

````
window(tain::TArray, w, f)
window!(taout::TArray, tain::TArray, w, f)
window!(ta::TArray, w, f)

# w: one of IdxWindow or KeyWindow
# f: the aggregaion function
````

