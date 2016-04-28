using Faker
using JuliaTS
using NDSparseData
using Chrono

function randomdates(N)
    dates = [Faker.date_time() for x in 1:N];
    sort!(dates)
    #map((x)->DateTime(replace(x, " ", "T")), unique(dates))
    map((x)->parsedate(x), unique(dates))
end

function createseries(N)
    dates = randomdates(N)
    N = length(dates)

    y1 = map(x->round(Int, x), rand(N) * 100);
    y2 = rand(N) * 100;
    y3 = rand(N) * 10;
    TArray("time"=>dates, "y1"=>y1, "y2"=>y2, "y3"=>y3)
end

function indexing(ta)
    info("indexing")
    N = 10^4;

    ni = 10
    nj = nrows(ta) - 10
    i = ta.data.indexes[ni]
    j = ta.data.indexes[nj]

    info("ta get single row")
    ta[i]
    ta[ni]
    @time for idx in 1:N ta[i] end
    @time for idx in 1:N ta[ni] end

    #info("ta get row range")
    #r = IdxPeriod(i,j)
    #info("ta get row range $r")
    #ta[r]
    #ta[ni:nj]
    #@time for idx in 1:N ta[r] end
    #@time for idx in 1:N ta[ni:nj] end

    info("ta setindex")
    nr = nrows(ta)
    rv = [(round(Int, rand()*100), rand()*100, rand()*10) for idx in 1:N]
    @time for idx in 1:N
        row = (idx <= nr) ? idx : (nr % idx)
        ta[row] = rv[idx]
    end

    rd = [(x,) for x in randomdates(N)]
    @time for idx in 1:length(rd)
        ta[rd[idx]] = rv[idx]
    end

    info("ta flush!")
    @time flush!(ta)
    @show nrows(ta)

    nothing
end

function reindexing(ta)
    info("reindexing...")
    ta1 = index(ta, ("y1", "y2"))
    #@show ta1[1:10]
    ta2 = index(ta, ("y3", "y2"))
    #@show ta2[1:10]
    nothing
end

#function windowing(ts, ta)
#    info("windowing")
#    i = ta.data.indexes[10][1]
#    j = ta.data.indexes[nrows(ta)-10][1]
#    info("ta time range")
#    window(ta, i:j)
#    @time window(ta, i:j)
#    nothing
#end

function relalg(ta)
    ni = 10
    nj = nrows(ta) - 10
    i = ta.data.indexes[ni]
    j = ta.data.indexes[nj]
    r = IdxPeriod(i,j)

    ta1 = select(ta, r)
    ta2 = project(ta1, ("date", "y2"), ("y1",))
end


N = 10^2
ta = createseries(N)

indexing(ta)
reindexing(ta)
#windowing(ts, ta)

