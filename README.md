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

#### selection

````
select(ta::TArray, r::IndexRange)
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

