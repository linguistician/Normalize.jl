using DataFrames, Normalize, Test

@testset "legible functions" begin
    @test make_phrase("cube", "", "") === "cube"
    @test make_phrase("square_root", "_", " ") === "square root"
    @test make_phrase("add_then_log_base_10", "_", " ") === "add then log base 10"
    @test make_list_phrase("cube") === "cube"
    @test make_list_phrase("add then log base 10") === "add and log base 10"
    @test make_list_phrase("add then square then invert") === "add, square, and invert"
    @test make_list_phrase("add then square root then invert") === 
        "add, square root, and invert"
    @test make_legible("add_then_natural_log") === "add and natural log"
end

@testset "convert tabular data to dataframe" begin
    example = DataFrame(a=[1, 2, 3, 4, 5], b=[6, 7, 8, 9, 10])
    @test tabular_to_dataframe("test/dummy/test.csv") == example
    @test tabular_to_dataframe("test/dummy/test.xlsx", "test") == example
    @test isnothing(tabular_to_dataframe("test/dummy/test.xls", "test"))
end

@testset "positive skew transformations" begin
    @test isone(log_base_10(10))
    @test_throws DomainError log_base_10(0)
    @test_throws DomainError log_base_10(0.0)
    @test_throws DomainError log_base_10(-1)

    @test add_then_log_base_10(98, -1) === 2.0
    @test_throws Exception add_then_log_base_10(4, 5)

    @test isone(natural_log(exp(1)))
    @test_throws DomainError natural_log(0)
    @test_throws DomainError natural_log(0.0)
    @test_throws DomainError natural_log(-1)

    @test add_then_natural_log(exp(2), 1) === 2.0
    @test_throws Exception add_then_natural_log(0, 1)

    @test square_root(4) === 2.0
    @test_throws DomainError square_root(-1)

    @test add_then_square_root(3, 0) === 2.0
    @test_throws Exception add_then_square_root(-1, 0)

    @test invert(-2) === -0.5
    @test_throws DivideError invert(0)
    @test_throws DivideError invert(0.0)

    @test add_then_invert(9, 0) === 0.1
    @test_throws Exception add_then_invert(1.22, 1.32)

    @test square_then_invert(2) === 0.25
    @test_throws DivideError square_then_invert(0)
    @test_throws DivideError square_then_invert(0.0)

    @test add_then_square_then_invert(2, 1) === 0.25
    @test_throws Exception add_then_square_then_invert(-2, -1)
    @test_throws DivideError add_then_square_then_invert(0, -1)
    @test_throws DivideError add_then_square_then_invert(0.0, -1.0)

    @test square_root_then_invert(4) === 0.5
    @test_throws DivideError square_root_then_invert(0)
    @test_throws DivideError square_root_then_invert(0.0)
    @test_throws DomainError square_root_then_invert(-1)

    @test add_then_square_root_then_invert(23, -1) === 0.2
    @test_throws Exception add_then_square_root_then_invert(-1, 0)

    @test square_root_then_add_then_invert(4, 1) === 0.5
    @test_throws Exception square_root_then_add_then_invert(3, 4)
    @test_throws DomainError square_root_then_add_then_invert(-1, -2)
    @test_throws DomainError square_root_then_add_then_invert(0, -1)
end

@testset "stretch transformations" begin
    @test logit(3) === log_base_10(1.5)
    @test_throws DivideError logit(1)
    @test_throws DomainError logit(0)
    @test_throws DomainError logit(0.0)

    @test add_then_logit(2.75) === logit(3)
    @test_throws DivideError add_then_logit(0.75)
    @test_throws DomainError add_then_logit(-0.25)
end

@testset "negative skew transformations" begin
    @test square(5.0) === 25.0
    @test square(-2) === 4

    @test cube(-3) === -27
    @test antilog(2) === 100.0

    @test reflect_then_square_root(-1, 2) === 2.0
    @test_throws Exception reflect_then_square_root(1, 0)

    @test isone(reflect_then_log_base_10(0, 9))
    @test_throws Exception reflect_then_log_base_10(1, 0)

    @test reflect_then_invert(0, 1) === 0.5
    @test_throws Exception reflect_then_invert(1, 0)
end

@testset "converting String cols to Float" begin
    df = DataFrame(a=[" ", "1", " "], b=["2", " ", "3"])
    @test blank_to_nan!([" ", "1", " "]) == ["NaN", "1", "NaN"]
    @test isequal(string_to_float!(df), DataFrame(a=[NaN, 1, NaN], b=[2, NaN, 3]))
end

@testset "rename columns with function" begin
    df = DataFrame(a_new_function=[4, 16, 25], b_new_function=[1, 64, 36])
    @test rename_with_function(square, "example-+_square") === "example__square"
    @test rename_with_function(cube, "example-+_two-+_cube") === "example-+_two__cube"
    @test rename_with_function(logit, "ex__square-+_logit") === "ex__square__logit"
    @test rename_with_function(square_root, "example-+_square_root") ===
            "example__square_root"

    rename!(df, ["a_new__square", "b_new__square"])
    @test locate_end_of_original_colname("a_new__square") === 5
    @test get_original_colnames(df) == ["a_new", "b_new"]
    @test get_applied_transformations(df) == "square"

    rename!(df, ["a_new__square__cube", "b_new__square__cube"])
    @test get_original_colnames(df) == ["a_new", "b_new"]
    @test get_applied_transformations(df) == "square; cube"

    rename!(df, ["a_new__cube__logit", "b_new__square_root__log"])
    @test get_applied_transformations(df) == "cube; logit"
end

@testset "check skews" begin
    @test is_positive_skew(2.0)
    @test is_positive_skew(-1) === false
    @test is_positive_skew(0) === false

    @test is_negative_skew(2.0) === false
    @test is_negative_skew(-1)
    @test is_negative_skew(0) === false

    nt = [(skewness_ratio = 1.67,), (skewness_ratio = -3.8,)]
    @test contains_skew(is_positive_skew, nt)
    @test are_negative_skew(nt)

    nt2 = [(skewness_ratio = -2.7,), (skewness_ratio = 0,)]
    @test are_positive_skew(nt2) === false
end

@testset "check invalids" begin
    @test is_invalid_for_add_then_logit(-0.25)
    @test is_invalid_for_add_then_logit(0.75)

    @test is_invalid_for_logit(0)
    @test is_invalid_for_logit(0.0)
    @test is_invalid_for_logit(1)
    @test is_invalid_for_logit(1.0)

    df = DataFrame(a=[0, -0.25, 2])
    @test contains_invalid_for_add_then_logit(df)
    @test contains_invalid_for_logit(df)
    df2 = DataFrame(a=[3, -2, 0.4])
    @test contains_invalid_for_add_then_logit(df2) === false
    @test contains_invalid_for_logit(df2) === false
end

@testset "get maximum/minimum" begin
    df = DataFrame(a=[1, -1, 4], b=[-2, 5, 0])
    @test get_extremum(minimum, df) === -2
    @test get_extremum(maximum, df) === 5
    df2 = DataFrame(a=[NaN, 2, -2])
    @test get_extremum(minimum, df2) === -2.0
    @test get_extremum(maximum, df2) === 2.0
end

@testset "check normal" begin
    @test is_normal(-1.23, 0.98)
    @test is_normal(0.63, -1.98)
    @test is_normal(0, 0)
    @test is_normal(4, 6) === false
    @test is_normal(3.14, 2.22, normal_ratio=4)

    nt = [(skewness_ratio = 1.19,), (skewness_ratio = 0.98,)]
    nt2 = [(kurtosis_ratio = 0.55,), (kurtosis_ratio = 1.2,)]
    @test are_normal(nt, nt2)
    @test are_nonnormal(nt, nt2) === false
    nt3 = [(kurtosis_ratio = 2.2,), (kurtosis_ratio = 1.2,)]
    @test are_normal(nt, nt3) === false
    @test are_nonnormal(nt, nt3)
end

@testset "filter cols" begin
    df = DataFrame(a=[1, -1, 4], b=[-2, 5, 0])
    @test get_filtered_cols(iseven, df) == [[4], [-2, 0]] 
    @test get_filtered_cols(isequal, df, 4) == [[4], []]
    @test contains_predicated(iseven, df)
    @test contains_predicated(isequal, df, 4)
end

@testset "get skew transformations" begin
    @test isempty(get_positive_skew_transformations(-1)["one arg"])
    @test get_positive_skew_transformations(-1)["two args"] == 
            [
                add_then_square_root,
                add_then_square_root_then_invert,
                add_then_invert,
                add_then_square_then_invert,
                add_then_log_base_10,
                add_then_natural_log,
            ]
    @test get_positive_skew_transformations(0.5)["one arg"] == [square_root]
    @test get_positive_skew_transformations(0.5)["two args"] ==
            [
                square_root_then_add_then_invert,
                add_then_invert,
                add_then_square_then_invert,
                add_then_log_base_10,
                add_then_natural_log,
            ]
    @test get_positive_skew_transformations(3.14)["one arg"] ==
            [
                square_root,
                square_root_then_invert,
                invert,
                square_then_invert,
                log_base_10,
                natural_log,
            ]
    @test isempty(get_positive_skew_transformations(3.14)["two args"])
    @test isempty(get_error_positive_skew_transformations(DivideError(), 0.1)["one arg"])
    @test get_error_positive_skew_transformations(DivideError(), 0.1)["two args"] ==
            [
                add_then_square_root,
                add_then_log_base_10,
                add_then_natural_log,
            ]
    @test get_error_positive_skew_transformations(DivideError(), 2.2)["one arg"] ==
            [
                square_root,
                log_base_10,
                natural_log,
            ]
    @test isempty(get_error_positive_skew_transformations(DivideError(), 2.2)["two args"])
end

@testset "add cols" begin
    d = Dict("df_transformed" => DataFrame())
    other = Dict("df_transformed" => DataFrame(a=[1, 2]))
    @test merge_cols!(d, other) == DataFrame(a=[1, 2])
    another = Dict("df_transformed" => DataFrame(b=[3, 4]))
    @test merge_cols!(d, another) == DataFrame(a=[1, 2], b=[3, 4])
    @test d["df_transformed"] == DataFrame(a=[1, 2], b=[3, 4])
    @test_throws ArgumentError merge_cols!(d, other) == DataFrame(a=[1, 2], b=[3, 4])
end

@testset "skewness and kurtosis" begin
    df = DataFrame(a=[1, -1, 4, 0, 2], b=[-2, 5, 0, -3, 7])
    skewness = calculate_skewness(df.a)
    @test skewness.skewness_stat === calculate_skewness_stat(df.a)
    @test skewness.skewness_error === calculate_skewness_error(df.a)
    @test round(√(calculate_skewness_variance(df.a)), digits=3) === 
        skewness.skewness_error

    kurtosis = calculate_kurtosis(df.b)
    @test kurtosis.kurtosis_stat === calculate_kurtosis_stat(df.b)
    @test kurtosis.kurtosis_error === calculate_kurtosis_error(df.b)
    @test round(√(calculate_kurtosis_variance(df.b)), digits=3) ===
        kurtosis.kurtosis_error

    @test calculate_ratio(1, 3) === 0.333
end

@testset "labeling" begin
    stats = [(ratio = 0.5,), (number = 1.0,)]
    names = ["fraction", "version"]
    ids = add_label(stats, names)
    @test ids[1] === (ratio = 0.5, name = "fraction")
    @test ids[2] === (number = 1.0, name = "version")
    @test ids[1] != (0.5, "fraction")

    df = DataFrame(a_new__square=[4, 16, 25], b_old__square=[1, 64, 36])
    nt = [(color = "blue",), (color = "red",)]
    nt2 = [(condition = "new",), (condition = "broken",)]
    updated = label_findings(df, nt, nt2)
    @test updated[1].color === "blue"
    @test updated[1].condition === "new"
    @test updated[1].name == "a_new"
    @test updated[2] == (
        color = "red",
        condition = "broken",
        name = "b_old",
    )
end

@testset "merge results" begin
    d = Dict("normal" => Dict(), "df_transformed" => DataFrame())
    other = Dict("normal" => Dict("ratio" => 1.3), 
                 "df_transformed" => DataFrame(a=[1, 2]))
    merge_results!(d, other)
    @test d["normal"]["ratio"] === 1.3
    @test d["df_transformed"] == DataFrame(a=[1, 2])
end

@testset "update normal" begin
    d = Dict("normal" => Dict())
    other = Dict(
        "skewness" => [(skewness_ratio = 1.15,)],
        "kurtosis" => [(kurtosis_ratio = 0.87,)],
        "df_transformed" => DataFrame(a__square=[25, 16, 9])
        )
    update_normal!(d, other)
    @test d["normal"]["square"] == 
        [(skewness_ratio = 1.15, kurtosis_ratio = 0.87, name = "a")]
    other["kurtosis"] = [(kurtosis_ratio = 2.7,)]
    dd = Dict()
    update_normal!(dd, other)
    @test iszero(length(dd))

end

@testset "apply and record transformations" begin
    df = DataFrame(a=[-0.5, 2.47, 2.54, 2.91, 3.13], b=[0.21, 1.04, 1.11, 0.27, 5.27])
    result = apply_transformation(square, df)
    @test result["skewness"] == [
        (skewness_stat = -1.315, skewness_error = 0.913, skewness_ratio = -1.44), 
        (skewness_stat = 2.225, skewness_error = 0.913, skewness_ratio = 2.437)
        ]
    @test result["kurtosis"] == [
        (kurtosis_stat = 2.149, kurtosis_error = 2.0, kurtosis_ratio = 1.074), 
        (kurtosis_stat = 4.959, kurtosis_error = 2.0, kurtosis_ratio = 2.48)
        ]
    result2 = apply_transformation(add_then_invert, df, -0.5)
    @test result2["skewness"] == [
        (skewness_stat = 2.226, skewness_error = 0.913, skewness_ratio = 2.438), 
        (skewness_stat = -0.824, skewness_error = 0.913, skewness_ratio = -0.903)
        ]
    @test result2["kurtosis"] == [
        (kurtosis_stat = 4.963, kurtosis_error = 2.0, kurtosis_ratio = 2.482), 
        (kurtosis_stat = 0.341, kurtosis_error = 2.0, kurtosis_ratio = 0.17)
        ]

    record = record_transformations(df, [square])
    @test iszero(length(record["normal"]))
    @test record["df_transformed"] == result["df_transformed"]

    record2 = record_transformations(df, [add_then_invert], -0.5, normal_ratio=3)
    @test record2["normal"]["add and invert"] == [
        (skewness_stat = 2.226, skewness_error = 0.913, skewness_ratio = 2.438,
        kurtosis_stat = 4.963, kurtosis_error = 2.0, kurtosis_ratio = 2.482,
        name = "a"), 
        (skewness_stat = -0.824, skewness_error = 0.913, skewness_ratio = -0.903,
        kurtosis_stat = 0.341, kurtosis_error = 2.0, kurtosis_ratio = 0.17,
        name = "b")
        ]

    record3 = record_transformations(df, [square], normal_ratio=3)
    
    functions = Dict("one arg" => [square], "two args" => [add_then_invert])
    record_2_and_3 = record_all_transformations(df, functions, -0.5, normal_ratio=3)
    @test record_2_and_3["normal"]["square"] == record3["normal"]["square"]
    @test record_2_and_3["normal"]["add and invert"] == record2["normal"]["add and invert"]
    @test record_2_and_3["df_transformed"] == 
        hcat(record3["df_transformed"], record2["df_transformed"])
end

@testset "transform positive skew" begin
    df = DataFrame(b=[0.21, 1.04, 1.11, 0.27, 5.27])
    result = transform_positive_skew(df)
    @test length(result["normal"]) === 6
    @test names(result["df_transformed"]) ==
        ["b__square_root",
        "b__square_root_then_add_then_invert",
        "b__add_then_invert",
        "b__add_then_square_then_invert",
        "b__add_then_log_base_10",
        "b__add_then_natural_log"]
    
    df2 = DataFrame(a=[-0.5, 2.47, 2.54, 2.91, 3.13])
    result2 = transform_positive_skew(df2)
    @test iszero(length(result2["normal"]))
    @test names(result2["df_transformed"]) ==
        [
            "a__add_then_square_root",
            "a__add_then_square_root_then_invert",
            "a__add_then_invert",
            "a__add_then_square_then_invert",
            "a__add_then_log_base_10", 
            "a__add_then_natural_log"
        ]
end

@testset "transform negative skew" begin
    df = DataFrame(b=[0.21, 1.04, 1.11, 0.27, 5.27])
    result = transform_negative_skew(df)
    @test iszero(length(result["normal"]))
    @test names(result["df_transformed"]) ==
        [
            "b__square",
            "b__cube",
            "b__antilog",
            "b__reflect_then_square_root",
            "b__reflect_then_log_base_10", 
            "b__reflect_then_invert"
        ]
    
    df2 = DataFrame(a=[-0.5, 2.47, 2.54, 2.91, 3.13])
    result2 = transform_negative_skew(df2)
    @test length(result2["normal"]) === 5
    @test haskey(result2["normal"], "reflect and invert")
    @test haskey(result2["normal"], "cube")
    @test haskey(result2["normal"], "square")
    @test haskey(result2["normal"], "antilog")
    @test haskey(result2["normal"], "reflect and log base 10")
    @test names(result2["df_transformed"]) ==
        [
            "a__square",
            "a__cube",
            "a__antilog",
            "a__reflect_then_square_root",
            "a__reflect_then_log_base_10", 
            "a__reflect_then_invert"
        ]
end

@testset "stretch skew" begin
    df = DataFrame(b=[0.21, 1.04, 1.11, 0.27, 5.27])
    result = stretch_skew(df)
    @test length(result["normal"]) == 2
    @test names(result["df_transformed"]) ==
        [
            "b__add_then_logit",
            "b__logit",
        ]
    
    df2 = DataFrame(a=[-0.5, 2.47, 2.54, 2.91, 3.13])
    result2 = stretch_skew(df2)
    @test iszero(length(result2["normal"]))
end

@testset "normalize" begin
    df = DataFrame(b=[0.21, 1.04, 1.11, 0.27, 5.27])
    result = normalize(df)
    result_positive = transform_positive_skew(df)
    result_stretch = stretch_skew(df)
    @test result[1] == result_positive
    @test result[2] == result_stretch

    df2 = DataFrame(a=[-0.5, 2.47, 2.54, 2.91, 3.13])
    result2 = normalize(df2)
    result2_negative = transform_negative_skew(df2)
    @test result2[1] == result2_negative
end
