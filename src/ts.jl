immutable Period{T<:Tuple}
    first::T
    last::T
end

Base.in{T}(x::T, p::Period{T}) = (p.first <= x <= p.last)
append{T}(p::Period, f::T, l::T) = Period((p.first..., f), (p.last..., l))
append(p::Period, r::Range) = append(p, first(r), last(r))
append(p::Period, s) = append(p, s, s)


immutable TArray{T, D<:Tuple, C<:Tuple, V<:AbstractVector}
    data::NDSparse{T,D,C,V}
    raw_data::Dict

    keynames::Tuple
    valnames::Tuple
end

# construct a TArray giving column-name=>column-data pairs
function TArray(colpairs::Pair...)
    idxcol = colpairs[1]
    keynames = (idxcol.first,)
    TArray(keynames, colpairs...)
end

function TArray(keynames::Tuple, colpairs::Pair...)
    raw_data = Dict(colpairs...)
    valnames = tuple(setdiff([c.first for c in colpairs], keynames)...)
    valcols = [raw_data[c] for c in valnames]
    keycols = [raw_data[c] for c in keynames]

    vals = Indexes(valcols...)
    nd = NDSparse(keycols..., vals)
    TArray(nd, raw_data, keynames, valnames)
end

function TArray(data::NDSparse, keynames::Tuple, valnames::Tuple)
    raw_data = Dict([Pair(v...) for v in zip(keynames, data.indexes.columns)]..., [Pair(v...) for v in zip(valnames, data.data.columns)]...)
    TArray(data, raw_data, keynames, valnames)
end

# TODO: optimize to avoid indexing again
TArray(from::TArray, colpairs::Pair...) = TArray(from.keynames, from.raw_data..., colpairs...)

Base.show(io::IO, ta::TArray) = show_data(io, ta, ta.data)
function show_data{T,D<:Tuple}(io::IO, ta::TArray, t::NDSparse{T,D})
    flush!(t)
    print("TArray $(nrows(ta))x$(ncols(ta)) ")
    println(io, D, " => ", T)
    print(" $(ta.keynames) => $(ta.valnames)")
    n = length(t.indexes)
    for i in 1:min(n,10)
        println(io); print(io, " $(NDSparseData.row(t.indexes, i)) => $(t.data[i])")
    end
    if n > 20
        println(io); print(io, " ⋮")
        for i in (n-9):n
            println(io); print(io, " $(NDSparseData.row(t.indexes, i)) => $(t.data[i])")
        end
    end
end

nrows(ta::TArray) = length(ta.data.indexes)
ncols(ta::TArray) = length(ta.keynames) + length(ta.valnames)

deepcopy(ta::TArray) = TArray(ta.keynames, [x.first=>copy(x.second) for x in ta.raw_data]...)

index!(ta::TArray, keynames) = TArray((keynames...,), ta.raw_data...)
index(ta::TArray, keynames) = index!(deepcopy(ta), keynames)

eltype(ta::TArray) = eltype(ta.data)
keys(ta::TArray) = ta.data.indexes
values(ta::TArray) = ta.data.data
key(ta, i::Integer) = keys(ta)[i]
function flush!(tas::TArray...)
    for ta in tas
        flush!(ta.data)
    end
end

# searching
function searchsortedfirst(ta::TArray, idx::Tuple)
    x = ta.data.indexes
    lidx = length(idx)
    if lidx < length(ta.keynames)
        z = zero(eltype(x))
        searchsortedfirst(x, tuple(idx..., z[lidx+1:end]...))
    else
        searchsortedfirst(x, idx)
    end
end

# select / indexing
function _span{T}(ta::TArray, r::Period{T})
    r1 = r.first
    r2 = r.last
    idx = ta.data.indexes
    lidx = length(idx)

    i1 = searchsortedfirst(idx, r1)
    i2 = searchsortedfirst(idx, r2)
    ((i2 > lidx) || (idx[i2] > r2)) && (i2 -= 1)
    i1:i2
end
function _span{T<:Tuple}(ta::TArray, r::T)
    idx = ta.data.indexes
    lidx = length(idx)
    i2 = i1 = searchsortedfirst(idx, r)
    ((i2 > lidx) || (idx[i2] > r2)) && (i2 -= 1)
    i1:i2
end
_span(ta::TArray, r::Vector{Period}) = union([_span(ta, c) for c in r]...)

function getindex(ta::TArray, conditions...)
    flush!(ta)
    NI = length(ta.keynames)
    if length(conditions) != NI
        throw(DimensionMismatch("Cannot match $(length(conditions)) to $NI dimension data"))
    end

    periods = Period[Period((),())]
    nranges = 0
    for cond in conditions
        if isa(cond, Vector)
            pp = copy(periods)
            periods = Period[]
            for p in pp
                for c in cond 
                    push!(periods, append(p, c))
                end
            end
        else
            if isa(cond, Range)
                nranges += 1
                if nranges > 1
                    throw(ArgumentError("Can not index with multiple ranges"))
                end
            end
            periods = Period[append(period, cond) for period in periods]
        end
    end
    spn = _span(ta, periods)

    data = ta.data
    idxs = data.indexes[spn]
    vals = data.data[spn]

    TArray(ta.keynames, [Pair(v...) for v in zip(ta.keynames, idxs)]..., [Pair(v...) for v in zip(ta.valnames, vals)]...)
end
setindex!(ta::TArray, rhs, idx::Tuple) = (ta.data[idx...] = rhs)

# set/get single value column vectors
function getvals(ta::TArray, colname)
    idx = findfirst(ta.valnames, colname)
    ta.data.data.columns[idx]
end
function setvals(ta::TArray, colname, vals::Vector)
    idx = findfirst(ta.valnames, colname)
    if idx > 0
        ta.data.data.columns[idx] = vals
        return ta
    else
        return TArray(ta, colname=>vals)
    end
end

# groupby
groupby(ta::TArray, keynames) = index(ta, keynames)
groupby!(ta::TArray, keynames) = index!(ta, keynames)

# project
# duplicates are eliminated as per keyname
# TODO: allow zero keys? or row id as default key?
function project(ta::TArray, keynames, valnames)
    flush!(ta)
    projkeys = [n=>copy(ta.raw_data[n]) for n in keynames]
    projvals = [n=>copy(ta.raw_data[n]) for n in valnames]
    TArray(keynames, projkeys..., projvals...)
end

function project!(ta::TArray, keynames, valnames)
    flush!(ta)
    projkeys = [n=>ta.raw_data[n] for n in keynames]
    projvals = [n=>ta.raw_data[n] for n in valnames]
    TArray(keynames, projkeys..., projvals...)
end

# union
function union(ta1::TArray, ta2::TArray)
    flush!(ta1, ta2)
    rel = merge(ta1.data, ta2.data)
    TArray(rel, ta1.keynames, ta1.valnames)
end

# intersect
function intersect(ta1::TArray, ta2::TArray)
    flush!(ta1, ta2)
    rel = intersect(ta1.data, ta2.data)
    TArray(rel, ta1.keynames, ta1.valnames)
end

# difference
function difference(ta1::TArray, ta2::TArray)
    flush!(ta1, ta2)
    rel = difference(ta1.data, ta2.data)
    TArray(rel, ta1.keynames, ta1.valnames)
end

# TODO: drop

# rename
function rename(ta::TArray, keynames, valnames)
    flush!(ta)
    rename!(deepcopy(ta), keynames, valnames)
end

function rename!(ta::TArray, keynames, valnames)
    flush!(ta)
    TArray(ta.data, ta.raw_data, keynames, valnames)
end

_namepfx(names, pfx) = [pfx*"."*n for n in names]
rename(ta::TArray, pfx) = rename(ta, _namepfx(ta.keynames, pfx), _namepfx(ta.valnames, pfx))
rename!(ta::TArray, pfx) = rename!(ta, _namepfx(ta.keynames, pfx), _namepfx(ta.valnames, pfx))

# cross products
# theta join
function thetajoin(ta1::TArray, ta2::TArray, cond::Function, relnames=())
    if isempty(relnames)
        flush!(ta1, ta2)
    else
        ta1 = rename!(ta1, relnames[1])
        ta2 = rename!(ta2, relnames[2])
    end
    x = ta1.data
    y = ta2.data
    xi = x.indexes
    yi = y.indexes

    result_indexnames = tuple(ta1.keynames..., ta2.keynames...)
    result_valnames = tuple(ta1.valnames..., ta2.valnames...)
    default_vals = tuple(ta1.data.default..., ta2.data.default...)
    guess = max(length(xi), length(yi))
    result_indexes = Indexes(map(c->sizehint!(similar(c,0),guess), xi.columns)..., map(c->sizehint!(similar(c,0),guess), yi.columns)...)
    result_vals = Indexes(map(c->sizehint!(similar(c,0),guess), x.data.columns)..., map(c->sizehint!(similar(c,0),guess), y.data.columns)...)
    result = NDSparse(result_indexes, result_vals, default_vals)

    for xi in x.indexes
        xd = x[xi...]
        for yi = y.indexes
            yd = y[yi...]
            if cond(xi, xd, yi, yd)
                ridx = tuple(xi..., yi...)
                result[ridx...] = tuple(xd..., yd...)
            end
        end
    end
    TArray(result, result_indexnames, result_valnames)
end

# shift joins
# shiftall
# shift

# windowing
immutable Window{K,S,W}
    first::K        # start of first window
    step::S         # increment window start
    width::W        # window width
end
typealias IdxWindow Window{Int,Int,Int}
typealias KeyWindow Window{Tuple,Tuple,Tuple}

window(tain::TArray, w, f) = window!(TArray(similar(tain.data), tain.keynames, tain.valnames), tain, w, f)

function window!(taout::TArray, tain::TArray, w::KeyWindow, f)
    i1 = w.first
    idxs = tain.data.indexes
    vals = tain.data.data
    iend = idxs[end]

    tdout = taout.data
    oidxs = tdout.indexes
    ovals = tdout.data
    nidxs = length(oidxs.columns)

    guess = nrows(tain)
    for c in oidxs.columns
        resize!(c, 0)
        sizehint!(c, guess)
    end
    for c in ovals.columns
        resize!(c, 0)
        sizehint!(c, guess)
    end

    ww = [w.width...]
    ws = [w.step...]
    while i1 <= iend
        i2 = tuple(([i1...] .+ ww)...)
        sr = _span(tain, Period(i1, i2))
        cols = f(idxs[sr]..., vals[sr]...)
        push!(oidxs, tuple(cols[1:nidxs]...))
        push!(ovals, tuple(cols[(nidxs+1):end]...))
        i1 = tuple(([i1...] .+ ws)...)
    end
    taout
end

function window!(ta::TArray, w::KeyWindow, f)
    i1 = w.first
    idxs = ta.data.indexes
    vals = ta.data.data
    iend = idxs[end]
    outidx = 1
    nidxs = length(ta.keynames)

    ww = [w.width...]
    ws = [w.step...]
    while i1 <= iend
        i2 = tuple(([i1...] .+ ww)...)
        sr = _span(tain, Period(i1, i2))
        cols = f(idxs[sr]..., vals[sr]...)
        idxs[outidx] = tuple(cols[1:nidxs]...)
        vals[outidx] = tuple(cols[(nidxs+1):end]...)
        i1 = tuple(([i1...] .+ ws)...)
        outidx += 1
    end
    outidx -= 1
    for c in idxs.columns
        resize!(c, outidx)
    end
    for c in vals.columns
        resize!(c, outidx)
    end
    ta
end

function window!(taout::TArray, tain::TArray, w::IdxWindow, f)
    i1 = w.first
    iend = nrows(tain)
    idxs = tain.data.indexes
    vals = tain.data.data

    tdout = taout.data
    oidxs = tdout.indexes
    ovals = tdout.data
    nidxs = length(oidxs.columns)

    guess = div(nrows(tain), w.step)+1
    for c in oidxs.columns
        resize!(c, 0)
        sizehint!(c, guess)
    end
    for c in ovals.columns
        resize!(c, 0)
        sizehint!(c, guess)
    end

    while i1 <= iend
        i2 = min((i1 + w.width), iend)
        cols = f(idxs[i1:i2]..., vals[i1:i2]...)
        push!(oidxs, tuple(cols[1:nidxs]...))
        push!(ovals, tuple(cols[(nidxs+1):end]...))
        i1 += w.step
    end
    taout
end

function window!(ta::TArray, w::IdxWindow, f)
    i1 = w.first
    iend = nrows(ta)
    idxs = ta.data.indexes
    vals = ta.data.data
    outidx = 1
    nidxs = length(ta.keynames)

    while i1 <= iend
        i2 = min((i1 + w.width), iend)
        cols = f(idxs[i1:i2]..., vals[i1:i2]...)
        idxs[outidx] = tuple(cols[1:nidxs]...)
        vals[outidx] = tuple(cols[(nidxs+1):end]...)
        i1 += w.step
        outidx += 1
    end
    outidx -= 1
    for c in idxs.columns
        resize!(c, outidx)
    end
    for c in vals.columns
        resize!(c, outidx)
    end
    ta
end
