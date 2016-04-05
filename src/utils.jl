
# TODO: use WithDefault instead of overriding zero
function zero{T<:AbstractString}(::Type{T})
    convert(T, "")
end

zero(::Type{Char}) = ' '
zero(::Type{DateTime}) = DateTime()

function zero{T<:Tuple}(::Type{T})
    tuple([zero(x) for x in T.parameters]...)
end

similar(I::Indexes, d::Integer) = Indexes(map((c)->similar(c,d), I.columns)...)
push!(I::Indexes, r) = NDSparseData.pushrow!(I, r)
sizehint!(I::Indexes, i) = (map((c)->sizehint!(c,i), I.columns); I)
setrow!(I::Indexes, rhs, i::Integer) = tuple([I.columns[j][i]=rhs[j] for j in 1:length(I.columns)]...)
setindex!(I::Indexes, rhs, i::Integer) = setrow!(I, rhs, i)
copy!(i1::Indexes, i2::Indexes) = (map((idx)->copy!(i1.columns[idx], i2.columns[idx]), 1:ndims(i1)); i1)
resize!(I::Indexes, d::Integer) = (map((c)->resize!(c,d), I.columns); I)

function intersect{T,S,D}(x::NDSparse{T,D}, y::NDSparse{S,D})
    xidx = x.indexes
    yidx = y.indexes
    K = intersect(xidx, yidx)

    n = length(K)
    lx, ly = length(xidx), length(yidx)

    dflt = x.default
    data = similar(x.data, n)
    i = j = 1

    for k = 1:n
        r = K[k]
        found = false

        (j <= ly) && (cmp(yidx[j], r) < 0) && (j = searchsortedfirst(yidx, r))
        if (j <= ly) && (cmp(yidx[j], r) == 0)
            data[k] = y.data[j]
            j += 1
        else
            i = searchsortedfirst(xidx, r)
            data[k] = x.data[i]
            i += 1
        end
    end
    NDSparse(K, data, dflt)
end

function difference{T,S,D}(x::NDSparse{T,D}, y::NDSparse{S,D})
    I = x.indexes
    J = y.indexes
    pos = _diffpos(I, J)
    guess = length(pos)

    K = Indexes(map(c->sizehint!(similar(c,0),guess), I.columns)...)::typeof(I)
    for p in pos
        NDSparseData.pushrow!(K, I[p])
    end

    Dx = x.data
    Dd = Indexes(map(c->sizehint!(similar(c,0),guess), Dx.columns)...)::typeof(Dx)
    for p in pos
        NDSparseData.pushrow!(Dd, Dx[p])
    end
    NDSparse(K, Dd, x.default)
end

function difference{D}(I::Indexes{D}, J::Indexes{D})
    pos = _diffpos(I, J)
    guess = length(pos)
    K = Indexes(map(c->sizehint!(similar(c,0),guess), I.columns)...)::typeof(I)
    for p in pos
        NDSparseData.pushrow!(K, I[p])
    end
    return K
end

function _diffpos{D}(I::Indexes{D}, J::Indexes{D})
    lI, lJ = length(I), length(J)
    K = Int[]
    sizehint!(K, lI)
    i = j = 1
    while i <= lI
        ri = I[i]
        if j <= lJ
            rj = J[j]
            c = cmp(ri, rj)
            if c == 0
                i += 1
            elseif c < 0
                push!(K, i)
                i += 1
            else
                j += 1
            end
        else
            push!(K, i)
        end
    end
    return K
end

