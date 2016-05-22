immutable Period{T<:Tuple}
    first::T
    last::T
end

in{T}(x::T, p::Period{T}) = (p.first <= x <= p.last)
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
    colnames = [c.first for c in colpairs]
    notkey = [!(c in keynames) for c in colnames]
    valnames = tuple(colnames[notkey]...)
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

show(io::IO, ta::TArray) = show_data(io, ta, ta.data)
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

index!(ta::TArray, keynames) = ((keynames...,) == ta.keynames) ? ta : TArray((keynames...,), ta.raw_data...)
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
# get can fetch any column, but set can be called only on value columns
#=
function getvals(ta::TArray, colname)
    idx = findfirst(ta.valnames, colname)
    ta.data.data.columns[idx]
end
=#
getvals(ta::TArray, colname) = ta.raw_data[colname]
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
_aggregate(cols::Tuple, fn::Function) = ([fn(col) for col in cols]...)
_aggregate(cols::Tuple, fns::Tuple) = ([fns[i](cols[i]) for i in 1:length(cols)]...)
function _aggregate!(ta::TArray, fn)
    I, D  = ta.data.indexes, ta.data.data
    maxi = nrows(ta)
    nvals = length(ta.valnames)
    i = 1
    a = 0
    while i < maxi
        j = searchsortedlast(I, I[i], i, maxi, Base.Order.ForwardOrdering())
        a += 1
        D[a] = _aggregate(D[i:j], fn)
        i = j + 1
    end
    for c in I.columns
        resize!(c, a)
    end
    for c in D.columns
        resize!(c, a)
    end
    ta
end

groupby(ta::TArray, keynames, fn) = _aggregate!(index(ta, keynames), fn)
groupby!(ta::TArray, keynames, fn) = _aggregate!(index!(ta, keynames), fn)

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

# natural join
function naturaljoin(taout, ta1, ta2, jointype, fn)
    # only inner join supported as of now
    (jointype == :inner) || throw(ArgumentError("Unsupported join type $jointype"))

    I1 = ta1.data.indexes
    I2 = ta2.data.indexes
    D1 = ta1.data.data
    D2 = ta2.data.data
    
    I = intersect(I1, I2)
    N = length(I)
    resize!(taout.data.indexes, N)
    resize!(taout.data.data, N)
    I_ = taout.data.indexes
    D = taout.data.data

    i1 = i2 = i = 1
    
    for i in 1:N
        ival = I_[i] = I[i]
        while !NDSparseData.isequal_tup(I1[i1], ival)
            i1 += 1
        end
        while !NDSparseData.isequal_tup(I2[i2], ival)
            i2 += 1
        end
        D[i] = fn(D1[i1], D2[i2])
    end
    taout
end

# theta join
#= not very useful
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
=#

# shift joins
# shiftall
# shift
function timeshift!{T<:Union{Date,DateTime},D<:DatePeriod}(col::Vector{T}, by::D)
    for idx in 1:length(col)
        col[idx] += by
    end
    col
end

timeshift(ta::TArray, col, by) = timeshift!(deepcopy(ta), col, by)
function timeshift!(ta::TArray, col, by)
    timeshift!(ta.raw_data[col], by)
    ta
end

# windowing
immutable Window{K,S,W}
    first::K        # start of first window
    last::K         # end of the last window
    step::S         # increment window start
    width::W        # window width
end

Window{K,S}(first::K, last::K, step::S) = Window{K,S,S}(first, last, step, step)

start{K}(w::Window{K}) = w.first
done{K}(w::Window{K}, state::K) = (NDSparseData.cmp_tup(state, w.last) > -1)
#=
@generated function next{K,S,W,N}(w::Window{K,S,W}, state::NTuple{N})
    quote
        _newstate = [state...]
        for n in N:-1:1
            if _newstate[n] < w.last[n]
                nextn = min(_newstate[n] + w.step[n], w.last[n])
                if nextn > _newstate[n]
                    _newstate[n] = nextn
                    break
                end
            else
                if n == 1
                    _newstate = [w.last...]
                else
                    _newstate[n] = w.first[n]
                end
            end
        end
        newstate = tuple(_newstate...)::K
        newend = tuple([min(state[i] + w.width[i], w.last[i]) for i in 1:N]...)::K
        return Period(state, newend), newstate
    end
end


function _nextstate{T,S}(state::T, step::S, last::T)
    stepped = (state+step)::T
    nextstate = min(stepped, last)
    nextstate, (nextstate > state)
end

@generated function _nextstate{T<:Tuple,S<:Tuple}(state::T, step::S, last::T, n::Int)
    statetypes = T.parameters
    steptypes = S.parameters
    quote
        nextstate = ()
        
    end
end
=#

function next{K,S,W}(w::Window{K,S,W}, state::K)
    N = length(state)
    _newstate = [state...]
    for n in N:-1:1
        if _newstate[n] < w.last[n]
            nextn = min(_newstate[n] + w.step[n], w.last[n])
            if nextn > _newstate[n]
                _newstate[n] = nextn
                break
            end
        else
            if n == 1
                _newstate = [w.last...]
            else
                _newstate[n] = w.first[n]
            end
        end
    end
    newstate = tuple(_newstate...)::K
    newend = tuple([min(state[i] + w.width[i], w.last[i]) for i in 1:N]...)::K
    Period(state, newend), newstate
end


# window type decides the output key values; can be :first, :last 
# TODO: more window types
function window(taout::TArray, ta::TArray, wspec::Window, wintype, fn)
    I = taout.data.indexes
    D = taout.data.data
    Din = ta.data.data
    state = start(wspec)
    while !done(wspec, state)
        w, state = next(wspec, state)
        push!(I, getfield(w, wintype)) # NOTE: getfield may not work for other window types
        sr = _span(ta, w)
        push!(D, _aggregate(Din[sr], fn))
    end
    taout
end
