immutable IndexRange
    first::Tuple
    last::Tuple
end
first(ir::IndexRange) = ir.first
last(ir::IndexRange) = ir.last
function ranges(r::IndexRange)
    r1 = first(r)
    r2 = last(r)
    [r1[idx]:r2[idx] for idx in 1:length(r1)]
end

immutable TArray
    data::NDSparse
    raw_data::Dict

    keynames::Tuple
    valnames::Tuple
end

nrows(ta::TArray) = length(ta.data.indexes)
ncols(ta::TArray) = length(ta.keynames) + length(ta.valnames)

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

TArray(from::TArray, colpairs::Pair...) = TArray(from.keynames, from.raw_data..., colpairs...)

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

# indexing
getindex(ta::TArray, idx::Tuple) = ta.data[idx...]
getindex(ta::TArray, idx::Tuple, ::Colon) = tuple(idx..., ta.data[idx]...)
setindex!(ta::TArray, rhs, idx::Tuple) = (ta.data[idx...] = rhs)

getindex(ta::TArray, i::Integer) = (flush!(ta); values(ta)[i])
getindex(ta::TArray, i::Integer, ::Colon) = tuple(ta.data.indexes[i]..., ta.data.data[i]...)
setindex!(ta::TArray, rhs, i::Integer) = (ta[keys(ta)[i]] = rhs)

getindex(ta::TArray, r::Range) = (flush!(ta); ta.data[r])
getindex(ta::TArray, r::IndexRange) = (flush!(ta); ta.data[_span(ta, r)])

function getindex(ta::TArray, ::Colon, c::Integer)
    l = length(ta.keynames)
    (c > l) ? ta.data.data.columns[c-l] : ta.data.indexes.columns[c]
end
function getindex(ta::TArray, ::Colon, c)
    p = findfirst(ta.keynames, c)
    if p > 0
        ta.data.indexes.columns[p]
    else
        p = findfirst(ta.valnames, c)
        ta.data.data.columns[p]
    end
end

function _span(ta::TArray, r::IndexRange)
    r1 = first(r)
    r2 = last(r)
    idx = ta.data.indexes
    lidx = length(idx)

    i1 = searchsortedfirst(idx, r1)
    i2 = searchsortedfirst(idx, r2)
    ((i2 <= lidx) && (idx[i2] >= r2)) || (i2 -= 1)
    i1:i2
end

# groupby
groupby(ta::TArray, keynames) = index(ta, keynames)
groupby!(ta::TArray, keynames) = index!(ta, keynames)

# select
function select(ta::TArray, r::IndexRange)
    flush!(ta)
    r1 = first(r)
    r2 = last(r)

    data = ta.data
    i1 = searchsortedfirst(data.indexes, r1)
    i2 = searchsortedlast(data.indexes, r2)

    idxs = data.indexes[i1:i2]
    vals = data.data[i1:i2]

    TArray(ta.keynames, [Pair(v...) for v in zip(ta.keynames, idxs)]..., [Pair(v...) for v in zip(ta.valnames, vals)]...)
end

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
    result
end

# shift joins
# shiftall
# shift

# windowing
# fixed width windows
immutable IdxWindow
    first::Int      # start of first window
    step::Int       # increment window start
    width::Int      # window width
end

# key range windows
immutable KeyWindow{K<:Tuple, S<:Tuple, W<:Tuple}
    first::K        # start of first window
    step::S         # increment window start
    width::W        # window width
end

window(tain::TArray, w, f) = window!(TArray(similar(tain.data), tain.keynames, tain.valnames), tain, w, f)

function window!{K,S,W}(taout::TArray, tain::TArray, w::KeyWindow{K,S,W}, f)
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
        sr = _span(tain, IndexRange(i1, i2))
        cols = f(idxs[sr]..., vals[sr]...)
        push!(oidxs, tuple(cols[1:nidxs]...))
        push!(ovals, tuple(cols[(nidxs+1):end]...))
        i1 = tuple(([i1...] .+ ws)...)
    end
    taout
end

function window!{K,S,W}(ta::TArray, w::KeyWindow{K,S,W}, f)
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
        sr = _span(tain, IndexRange(i1, i2))
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
