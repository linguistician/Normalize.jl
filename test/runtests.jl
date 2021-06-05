using DataFrames, Normalize, Test

@testset "convert tabular data to dataframe" begin
    function test_dir()
        path = pwd()
        if endswith(path, "test")
            return view(path, 1:findfirst("test", path)[end])
        else
            return "$(path)/test"
        end
    end

    example = DataFrame(a=[1, 2, 3, 4, 5], b=[6, 7, 8, 9, 10])
    @test tabular_to_dataframe("$(test_dir())/dummy/test.csv") == example
    @test tabular_to_dataframe("$(test_dir())/dummy/test.xlsx", "test") == example
    @test tabular_to_dataframe("$(test_dir())/dummy/test.xls", "test") == example
    @test tabular_to_dataframe("$(test_dir())/dummy/test.ods", "test") == example
    @test tabular_to_dataframe("$(test_dir())/dummy/test.sav") == example
    @test tabular_to_dataframe("$(test_dir())/dummy/co3.dta") isa DataFrame && 
        tabular_to_dataframe("$(test_dir())/dummy/co3.dta") != DataFrame()
    @test tabular_to_dataframe("fake.doc") == DataFrame()
end

@testset "replace missing" begin
    @test replace_missing!(DataFrame(a=[1.0, missing]), 3.14) == DataFrame(a=[1.0, 3.14])
    @test replace_missing!(DataFrame(a=["hi", missing]), "bye")[2, 1] === missing
    @test replace_missing!(DataFrame(a=[missing]), 1)[1, 1] === missing
    @test replace_missing!(DataFrame(), 1) == DataFrame()
end
    
@testset "convert columns of eltype String or Any to float" begin
    @test sheetcols_to_float!(DataFrame(a=["1", " "]), blank_to=2.2) ==
        DataFrame(a=[1, 2.2])
    @test sheetcols_to_float!(DataFrame(a=[1.23, " "]), blank_to=1) ==
        DataFrame(a=[1.23, 1.0])
    @test sheetcols_to_float!(DataFrame(a=1:4), blank_to=5.1) == DataFrame(a=1:4)
end

@testset "positive skew transformations" begin
    @test isone(Normalize.log_base_10(10))
    @test_throws DomainError Normalize.log_base_10(0)
    @test_throws DomainError Normalize.log_base_10(0.0)
    @test_throws DomainError Normalize.log_base_10(-1)

    @test Normalize.add_n_log_base_10(98, -1) == 2.0
    @test_throws ArgumentError Normalize.add_n_log_base_10(4, 5)

    @test isone(Normalize.natural_log(exp(1)))
    @test_throws DomainError Normalize.natural_log(0)
    @test_throws DomainError Normalize.natural_log(0.0)
    @test_throws DomainError Normalize.natural_log(-1)

    @test Normalize.add_n_natural_log(exp(2), 1) == 2.0
    @test_throws ArgumentError Normalize.add_n_natural_log(0, 1)

    @test Normalize.square_root(4) == 2.0
    @test_throws DomainError Normalize.square_root(-1)

    @test Normalize.add_n_square_root(3, 0) == 2.0
    @test_throws ArgumentError Normalize.add_n_square_root(-1, 0)

    @test Normalize.invert(-2) == -0.5
    @test_throws DivideError Normalize.invert(0)
    @test_throws DivideError Normalize.invert(0.0)

    @test Normalize.add_n_invert(9, 0) == 0.1
    @test_throws ArgumentError Normalize.add_n_invert(1.22, 1.32)

    @test Normalize.square_n_invert(2) == 0.25
    @test_throws DivideError Normalize.square_n_invert(0)
    @test_throws DivideError Normalize.square_n_invert(0.0)
    
    @test Normalize.Normalize.square_n_add_n_invert(2, 1) == 0.25
    @test_throws ArgumentError Normalize.Normalize.square_n_add_n_invert(-2, -1)
    @test_throws DivideError Normalize.Normalize.square_n_add_n_invert(0, -1)
    @test_throws DivideError Normalize.Normalize.square_n_add_n_invert(0.0, -1.0)

    @test Normalize.square_root_n_invert(4) == 0.5
    @test_throws DivideError Normalize.square_root_n_invert(0)
    @test_throws DivideError Normalize.square_root_n_invert(0.0)
    @test_throws DomainError Normalize.square_root_n_invert(-1)

    @test Normalize.add_n_square_root_n_invert(23, -1) == 0.2
    @test_throws ArgumentError Normalize.add_n_square_root_n_invert(-1, 0)

    @test Normalize.square_root_n_add_n_invert(4, 1) == 0.5
    @test_throws ArgumentError Normalize.square_root_n_add_n_invert(3, 4)
    @test_throws DomainError Normalize.square_root_n_add_n_invert(-1, -2)
    @test_throws DomainError Normalize.square_root_n_add_n_invert(0, -1)
end

@testset "stretch transformations" begin
    @test Normalize.logit(3) == Normalize.log_base_10(1.5)
    @test_throws DivideError Normalize.logit(1)
    @test_throws DomainError Normalize.logit(0)
    @test_throws DomainError Normalize.logit(0.0)

    @test Normalize.add_n_logit(2.75) == Normalize.logit(3)
    @test_throws DivideError Normalize.add_n_logit(0.75)
    @test_throws DomainError Normalize.add_n_logit(-0.25)
end

@testset "negative skew transformations" begin
    @test Normalize.square(5.0) == 25.0
    @test Normalize.square(-2) == 4

    @test Normalize.cube(-3) == -27
    @test Normalize.antilog(2) == 100.0

    @test Normalize.reflect_n_square_root(-1, 2) == 2.0
    @test_throws ArgumentError Normalize.reflect_n_square_root(1, 0)

    @test isone(Normalize.reflect_n_log_base_10(0, 9))
    @test_throws ArgumentError Normalize.reflect_n_log_base_10(1, 0)
    
    @test Normalize.reflect_n_invert(0, 1) == 0.5
    @test_throws ArgumentError Normalize.reflect_n_invert(1, 0)
end

@testset "check skews" begin
    @test is_positive_skew(2.0)
    @test is_positive_skew(-1) == false
    @test is_positive_skew(0) == false

    @test is_negative_skew(2.0) == false
    @test is_negative_skew(-1)
    @test is_negative_skew(0) == false

    nt = [(skewness_ratio = 1.67,), (skewness_ratio = -3.8,)]
    @test are_negative_skews(nt)

    nt2 = [(skewness_ratio = -2.7,), (skewness_ratio = 0,)]
    @test are_positive_skews(nt2) == false
end

@testset "check normal" begin
    @test is_normal(-1.23, 0.98)
    @test is_normal(0.63, -1.98)
    @test is_normal(0, 0)
    @test is_normal(4, 6) == false
    @test is_normal(3.14, 2.22, normal_ratio=4)

    nt = [
        (skewness_ratio = 1.19, kurtosis_ratio = 0.55,), 
        (skewness_ratio = 0.98, kurtosis_ratio = 1.2,),
    ]
    @test are_normal(nt)
    @test are_nonnormal(nt) == false
    nt2 = [
        (skewness_ratio = 1.19, kurtosis_ratio = 2.2,),
        (skewness_ratio = 0.98, kurtosis_ratio = 1.2,),
    ]
    @test are_normal(nt2) == false
    @test are_nonnormal(nt2)
end

@testset "get skew transformations" begin
    df = DataFrame(a=[-1, 1])
    @test isempty(get_positive_skew_transformations(df)["one arg"])
    @test (Normalize.add_n_square_root, -1) in
    get_positive_skew_transformations(df)["two args"]
    @test (Normalize.add_n_square_root_n_invert, -1) in
    get_positive_skew_transformations(df)["two args"]
    @test (Normalize.add_n_invert, -1) in
        get_positive_skew_transformations(df)["two args"]
    @test (Normalize.square_n_add_n_invert, -1) in
        get_positive_skew_transformations(df)["two args"]
    @test (Normalize.add_n_log_base_10, -1) in
        get_positive_skew_transformations(df)["two args"]
    @test (Normalize.add_n_natural_log, -1) in
        get_positive_skew_transformations(df)["two args"]
    df2 = DataFrame(a=[0.5, 0.75])
    @test get_positive_skew_transformations(df2)["one arg"] == [Normalize.square_root]
    @test (Normalize.square_root_n_add_n_invert, 0.5) in
        get_positive_skew_transformations(df2)["two args"]
    @test (Normalize.add_n_invert, 0.5) in
        get_positive_skew_transformations(df2)["two args"]
    @test get_positive_skew_transformations(df2)["two args"][end] ==
        (Normalize.square_n_add_n_invert, 0.5)
    @test (Normalize.add_n_log_base_10, 0.5) in
        get_positive_skew_transformations(df2)["two args"]
    @test (Normalize.add_n_natural_log, 0.5) in
        get_positive_skew_transformations(df2)["two args"]
    df3 = DataFrame(a=[3.14, 4])
    @test Normalize.square_root in get_positive_skew_transformations(df3)["one arg"]
    @test Normalize.square_root_n_invert in
        get_positive_skew_transformations(df3)["one arg"]
    @test Normalize.invert in get_positive_skew_transformations(df3)["one arg"]
    @test Normalize.square_n_invert in get_positive_skew_transformations(df3)["one arg"]
    @test Normalize.log_base_10 in get_positive_skew_transformations(df3)["one arg"]
    @test Normalize.natural_log in get_positive_skew_transformations(df3)["one arg"]
    @test isempty(get_positive_skew_transformations(df3)["two args"])
    df4 = DataFrame(a=[0, -1])
    @test Normalize.square_n_add_n_invert ∉
        get_positive_skew_transformations(df4)["two args"]

    @test Normalize.square in get_negative_skew_transformations(df)["one arg"]
    @test Normalize.cube in get_negative_skew_transformations(df)["one arg"]
    @test Normalize.antilog in get_negative_skew_transformations(df)["one arg"]
    @test (Normalize.reflect_n_square_root, 1) in
        get_negative_skew_transformations(df)["two args"]
    @test (Normalize.reflect_n_log_base_10, 1) in
        get_negative_skew_transformations(df)["two args"]
    @test (Normalize.reflect_n_invert, 1) in
        get_negative_skew_transformations(df)["two args"]

    @test get_stretch_skew_transformations(df3)["one arg"][begin] == 
        Normalize.add_n_logit
    @test get_stretch_skew_transformations(df3)["one arg"][end] == Normalize.logit
    @test isempty(get_stretch_skew_transformations(df3)["two args"])
    @test Normalize.logit ∉ get_stretch_skew_transformations(df)["one arg"]
    @test Normalize.add_n_logit ∉ get_stretch_skew_transformations(df2)["one arg"]

    df5 = DataFrame(a=[-0.5, 2.47, 2.54, 2.91, 3.13])
    transformations = get_skew_transformations(df5)
    @test !isempty(transformations["one arg"])
    @test !isempty(transformations["two args"])

    df6 = DataFrame(a=[-0.5, 2.47, 2.54, 2.91, 3.13, 3.29, 1])
    transformations2 = get_skew_transformations(df6)
    @test isempty(transformations2["one arg"])
    @test isempty(transformations2["two args"])
end

@testset "skewness and kurtosis" begin
    df = DataFrame(a=[1, -1, 4, 0, 2], b=[-2, 5, 0, -3, 7])
    skew = skewness(df.a)
    @test skew.skewness_stat == skewness_stat(df.a)
    @test skew.skewness_error == skewness_error(df.a)
    @test round(√(skewness_variance(df.a)), digits=3) == skew.skewness_error
    @test_throws ArgumentError skewness(())
    @test_throws ArgumentError skewness((NaN,))
    @test_throws ArgumentError skewness_stat([])
    @test_throws ArgumentError skewness_stat([NaN, NaN])
    @test_throws ArgumentError skewness_variance((1, 2))
    @test_throws ArgumentError skewness_variance((1, 2, NaN))

    kurt = kurtosis(df.b)
    @test kurt.kurtosis_stat == kurtosis_stat(df.b)
    @test kurt.kurtosis_error == kurtosis_error(df.b)
    @test round(√(kurtosis_variance(df.b)), digits=3) == kurt.kurtosis_error
    @test_throws ArgumentError kurtosis(Set(()))
    @test_throws ArgumentError kurtosis(Set((NaN,)))
    @test_throws ArgumentError kurtosis_stat([])
    @test_throws ArgumentError kurtosis_stat([NaN, NaN])
    @test_throws ArgumentError kurtosis_variance((-1, 2))
    @test_throws ArgumentError kurtosis_variance(Set((1, 0, NaN)))
    @test_throws ArgumentError kurtosis_variance(Set((1.1, 2.0, 3.67, NaN)))
end

@testset "apply transformations" begin
    function check_skewness_kurtosis(func, df::AbstractDataFrame)
        applied = apply(func, df)
        col_temp = applied[:transformed_gdf][:, 1]
        skew = skewness(col_temp)
        kurt = kurtosis(col_temp)
        quant = applied[:skewness_and_kurtosis][1]
        for s in keys(skew), k in keys(kurt)
            @test skew[s] == quant[s]
            @test kurt[k] == quant[k]
        end
    end

    function check_skewness_kurtosis(func, df::AbstractDataFrame, func_other_arg)
        applied = apply(func, df, func_other_arg)
        col_temp = applied[:transformed_gdf][:, 1]
        skew = skewness(col_temp)
        kurt = kurtosis(col_temp)
        quant = applied[:skewness_and_kurtosis][1]
        for s in keys(skew), k in keys(kurt)
            @test skew[s] == quant[s]
            @test kurt[k] == quant[k]
        end
    end

    function check_skewness_kurtosis(func, gd)
        applied = apply(func, gd)
        df_temp = DataFrame(applied[:transformed_gdf])
        for pos in (1, 5)
            if pos == 5
                quant = applied[:skewness_and_kurtosis][2]
            else
                quant = applied[:skewness_and_kurtosis][1]
            end
            skew = skewness(df_temp[pos:(pos + 3), 2])
            kurt = kurtosis(df_temp[pos:(pos + 3), 2])
            for s in keys(skew), k in keys(kurt)
                @test skew[s] == quant[s]
                @test kurt[k] == quant[k]
            end
        end
    end

    function check_skewness_kurtosis(func, gd, func_other_arg)
        applied = apply(func, gd, func_other_arg)
        df_temp = DataFrame(applied[:transformed_gdf])
        for pos in (1, 5)
            if pos == 5
                quant = applied[:skewness_and_kurtosis][2]
            else
                quant = applied[:skewness_and_kurtosis][1]
            end
            skew = skewness(df_temp[pos:(pos + 3), 2])
            kurt = kurtosis(df_temp[pos:(pos + 3), 2])
            for s in keys(skew), k in keys(kurt)
                @test skew[s] == quant[s]
                @test kurt[k] == quant[k]
            end
        end
    end

    df = DataFrame(a=[-0.5, 2.47, 2.54, 2.91, 3.13])
    check_skewness_kurtosis(Normalize.square, df)
    @test_throws ArgumentError apply(Normalize.square, df; marker="-")
    @test_throws ArgumentError apply(Normalize.square, df; marker="-a")
    check_skewness_kurtosis(Normalize.add_n_invert, df, -0.5)
    @test_throws ArgumentError apply(Normalize.add_n_invert, df, -0.5; marker="mark")
    @test_throws ArgumentError apply(Normalize.add_n_invert, df, -0.5; marker="~hi?")
            
    df2 = DataFrame(group=[1,1,1,1,2,2,2,2], a=1:8)
    gd = groupby(df2, :group)
    check_skewness_kurtosis(Normalize.cube, gd)
    check_skewness_kurtosis(Normalize.reflect_n_invert, gd, 8)
end
    
@testset "record transformations" begin
    df = DataFrame(a=[-0.5, 2.47, 2.54, 2.91, 3.13])
    function check_record(df::AbstractDataFrame, func; normal_ratio=2)
        record1 = record(df, Function[func]; normal_ratio)
        normal_vars = record1["normalized"]
        @test length(normal_vars) == ncol(record1["normal gdf"][1])
        for nt1 in normal_vars["$func"]
            @test nt1[:name] == "a"
            @test haskey(nt1, :skewness_stat)
            @test haskey(nt1, :skewness_error)
            @test haskey(nt1, :skewness_ratio)
            @test haskey(nt1, :kurtosis_stat)
            @test haskey(nt1, :kurtosis_error)
            @test haskey(nt1, :kurtosis_ratio)
        end

        for df in record1["normal gdf"]
            @test df isa AbstractDataFrame
        end
        @test isempty(record1["nonnormal gdf"])
    end

    function check_record(df::AbstractDataFrame, func, func_other_arg; normal_ratio=2)
        record1 = record(df, [(func, func_other_arg)]; normal_ratio)
        normal_vars = record1["normalized"]
        @test length(normal_vars) == ncol(record1["normal gdf"][1])
        func_name = join(split("$func", "_n_"), " and ")
        for nt1 in normal_vars[func_name]
            @test nt1[:name] == "a"
            @test haskey(nt1, :skewness_stat)
            @test haskey(nt1, :skewness_error)
            @test haskey(nt1, :skewness_ratio)
            @test haskey(nt1, :kurtosis_stat)
            @test haskey(nt1, :kurtosis_error)
            @test haskey(nt1, :kurtosis_ratio)
        end

        for df in record1["normal gdf"]
            @test df isa AbstractDataFrame
        end
        @test isempty(record1["nonnormal gdf"])
    end

    function check_records(df::AbstractDataFrame, transform_series; normal_ratio=2)
    check_record(df, transform_series["one arg"]; normal_ratio)
        check_record(df, transform_series["two args"]...; normal_ratio)
    end

    function check_record(gd, func; normal_ratio=2)
        record1 = record(gd, Function[func]; normal_ratio)
        normal_vars = record1["normalized"]
        @test length(normal_vars) == length(valuecols(record1["normal gdf"][1]))
        for (index, nt1) in enumerate(record1["normalized"]["$func"])
            @test nt1[:name] == "a (group = $index)"
            @test haskey(nt1, :skewness_stat)
            @test haskey(nt1, :skewness_error)
            @test haskey(nt1, :skewness_ratio)
            @test haskey(nt1, :kurtosis_stat)
            @test haskey(nt1, :kurtosis_error)
            @test haskey(nt1, :kurtosis_ratio)
        end

        for gd in record1["normal gdf"]
            @test gd isa GroupedDataFrame
        end
        @test isempty(record1["nonnormal gdf"])
    end

    function check_record(gd, func, func_other_arg; normal_ratio=2)
        record1 = record(gd, [(func, func_other_arg)]; normal_ratio)
        normal_vars = record1["normalized"]
        @test length(normal_vars) == length(valuecols(record1["normal gdf"][1]))
        no_under = join(split("$func", "_"), " ")
        func_name = join(split(no_under, " n "), " and ")
        for (index, nt1) in enumerate(record1["normalized"][func_name])
            @test nt1[:name] == "a (group = $index)"
            @test haskey(nt1, :skewness_stat)
            @test haskey(nt1, :skewness_error)
            @test haskey(nt1, :skewness_ratio)
            @test haskey(nt1, :kurtosis_stat)
            @test haskey(nt1, :kurtosis_error)
            @test haskey(nt1, :kurtosis_ratio)
        end
        
        for gd in record1["normal gdf"]
            @test gd isa GroupedDataFrame
        end
        @test isempty(record1["nonnormal gdf"])
    end
    
    function check_records(gd, transform_series; normal_ratio=2)
        check_record(gd, transform_series["one arg"]; normal_ratio)
        check_record(gd, transform_series["two args"]...; normal_ratio)
    end

    check_record(df, Normalize.square)
    check_record(df, Normalize.add_n_invert, -0.5, normal_ratio=3)
    functions = Dict(
        "one arg" => Normalize.square,
        "two args" => (Normalize.add_n_invert, -0.5),
            )
    check_records(df, functions, normal_ratio=3)

    df2 = DataFrame(group=[1,1,1,1,2,2,2,2], a=1:8)
    gd = groupby(df2, :group)
    check_record(gd, Normalize.invert)
    check_record(gd, Normalize.reflect_n_square_root, 8)
    functions2 = Dict(
        "one arg" => Normalize.invert,
        "two args" => (Normalize.reflect_n_square_root, 8),
    )
    check_records(gd, functions)
end            