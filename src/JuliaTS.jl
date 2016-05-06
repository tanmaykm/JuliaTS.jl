module JuliaTS

using NDSparseData
using Base.Dates

import Base: zero, getindex, setindex!, eltype, keys, values, push!, similar, sizehint!, first, last, searchsortedfirst, resize!, copy!,
             select, union, intersect, deepcopy, start, next, done, show, in
import NDSparseData: flush!

export TArray, window, nrows, ncols, index, index!, Period,
        groupby, groupby!, select, project, project!, union, intersect, naturaljoin,
        timeshift, timeshift!,
        rename, rename!, difference,
        Window, window,
        getvals, setvals

include("utils.jl")
include("ts.jl")

end # module
