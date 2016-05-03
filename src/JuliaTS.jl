module JuliaTS

using NDSparseData

import Base: zero, getindex, setindex!, eltype, keys, values, push!, similar, sizehint!, first, last, searchsortedfirst, resize!, copy!,
             select, union, intersect, deepcopy
import NDSparseData: flush!

export TArray, window, nrows, ncols, index, index!, Period,
        groupby, groupby!, select, project, project!, union, intersect, thetajoin,
        rename, rename!, difference,
        IdxWindow, KeyWindow, window, window!,
        getvals, setvals

include("utils.jl")
include("ts.jl")

end # module
