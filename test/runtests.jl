import Normalize as nm
using DataFrames
using Test

@testset "read spreadsheet" begin
    example = DataFrame(a=[1, 2, 3, 4, 5], b=[6, 7, 8, 9, 10])
    @test nm.loadSpreadsheet("test/dummy/test.csv") == example
    @test nm.loadSpreadsheet("test/dummy/test.xlsx", "test") == example
    @test isnothing(nm.loadSpreadsheet("test/dummy/test.xls", "test"))
end

@testset "positive skew transformations" begin
    @test isone(nm.logInBase10(10))
    @test_throws DomainError nm.logInBase10(0)
    @test_throws DomainError nm.logInBase10(0.0)
    @test_throws DomainError nm.logInBase10(-1)

    @test nm.addThenLog(98, -1) === 2.0
    @test_throws Exception nm.addThenLog(4, 5)

    @test isone(nm.naturalLog(exp(1)))
    @test_throws DomainError nm.naturalLog(0)
    @test_throws DomainError nm.naturalLog(0.0)
    @test_throws DomainError nm.naturalLog(-1)

    @test nm.addThenNaturalLog(exp(2), 1) === 2.0
    @test_throws Exception nm.addThenNaturalLog(0, 1)

    @test nm.squareRoot(4) === 2.0
    @test_throws DomainError nm.squareRoot(-1)

    @test nm.addThenSquareRoot(3, 0) === 2.0
    @test_throws Exception nm.addThenSquareRoot(-1, 0)

    @test nm.invert(-2) === -0.5
    @test_throws DivideError nm.invert(0)
    @test_throws DivideError nm.invert(0.0)

    @test nm.addThenInvert(9, 0) === 0.1
    @test_throws Exception nm.addThenInvert(1.22, 1.32)

    @test nm.squareThenInvert(2) === 0.25
    @test_throws DivideError nm.squareThenInvert(0)
    @test_throws DivideError nm.squareThenInvert(0.0)

    @test nm.addThenSquareThenInvert(2, 1) === 0.25
    @test_throws Exception nm.addThenSquareThenInvert(-2, -1)
    @test_throws DivideError nm.addThenSquareThenInvert(0, -1)
    @test_throws DivideError nm.addThenSquareThenInvert(0.0, -1.0)

    @test nm.squareRootThenInvert(4) === 0.5
    @test_throws DivideError nm.squareRootThenInvert(0)
    @test_throws DivideError nm.squareRootThenInvert(0.0)
    @test_throws DomainError nm.squareRootThenInvert(-1)

    @test nm.addThenSquareRootThenInvert(23, -1) === 0.2
    @test_throws Exception nm.addThenSquareRootThenInvert(-1, 0)

    @test nm.squareRootThenAddThenInvert(4, 1) === 0.5
    @test_throws Exception nm.squareRootThenAddThenInvert(3, 4)
    @test_throws DomainError nm.squareRootThenAddThenInvert(-1, -2)
    @test_throws DomainError nm.squareRootThenAddThenInvert(0, -1)
end

@testset "stretch transformations" begin
    @test nm.logit(3) === nm.logInBase10(1.5)
    @test_throws DivideError nm.logit(1)
    @test_throws DomainError nm.logit(0)
    @test_throws DomainError nm.logit(0.0)

    @test nm.addThenLogit(2.75) === nm.logit(3)
    @test_throws DivideError nm.addThenLogit(0.75)
    @test_throws DomainError nm.addThenLogit(-0.25)
end

@testset "negative skew transformations" begin
    @test nm.square(5.0) === 25.0
    @test nm.square(-2) === 4

    @test nm.cube(-3) === -27
    @test nm.antilog(2) === 100.0

    @test nm.reflectThenSquareRoot(-1, 2) === 2.0
    @test_throws Exception nm.reflectThenSquareRoot(1, 0)

    @test isone(nm.reflectThenLog(0, 9))
    @test_throws Exception nm.reflectThenLog(1, 0)

    @test nm.reflectThenInvert(0, 1) === 0.5
    @test_throws Exception nm.reflectThenInvert(1, 0)
end

@testset "converting String cols to Float" begin
    df = DataFrame(a=[" ", "1", " "], b=["2", " ", "3"])
    @test nm.blankToNaN!([" ", "1", " "]) == ["NaN", "1", "NaN"]
    @test isequal(nm.stringToFloat!(df), DataFrame(a=[NaN, 1, NaN], b=[2, NaN, 3]))
end

@testset "rename columns with function" begin
    df = DataFrame(a_new_function=[4, 16, 25], b_new_function=[1, 64, 36])
    @test nm.renameWithFunction(nm.square, "example_square") === "example__square"
    @test nm.renameWithFunction(nm.cube, "example_two_cube") === "example_two__cube"
    @test nm.renameWithFunction(nm.logit, "ex__square_logit") === "ex__square__logit"
    @test nm.getTransformedColnames(nm.square, df) == [:a_new__square, :b_new__square]

    rename!(df, ["a_new__square", "b_new__square"])
    @test nm.locateEndOfOriginalColname("a_new__square") === 5
    @test nm.getOriginalColnames(df) == ["a_new", "b_new"]
    @test nm.getAppliedTransformations(df) == "square"

    rename!(df, ["a_new__square__cube", "b_new__square__cube"])
    @test nm.getOriginalColnames(df) == ["a_new", "b_new"]
    @test nm.getAppliedTransformations(df) == "square__cube"

    rename!(df, ["a_new__cube__logit", "b_new__squareRoot__log"])
    @test nm.getAppliedTransformations(df) == "cube__logit"
end

@testset "check skews" begin
    @test nm.isPositiveSkew(2.0)
    @test nm.isPositiveSkew(-1) === false
    @test nm.isPositiveSkew(0) === false

    @test nm.isNegativeSkew(2.0) === false
    @test nm.isNegativeSkew(-1)
    @test nm.isNegativeSkew(0) === false

    nt = [(skewness_ratio = 1.67,), (skewness_ratio = -3.8,)]
    @test nm.containsSkew(nm.isPositiveSkew, nt)
    @test nm.areNegativeSkew(nt)

    nt2 = [(skewness_ratio = -2.7,), (skewness_ratio = 0,)]
    @test nm.arePositiveSkew(nt2) === false
end

@testset "check invalids" begin
    @test nm.isInvalidForAddThenLogit(-0.25)
    @test nm.isInvalidForAddThenLogit(0.75)

    @test nm.isInvalidForLogit(0)
    @test nm.isInvalidForLogit(0.0)
    @test nm.isInvalidForLogit(1)
    @test nm.isInvalidForLogit(1.0)

    df = DataFrame(a=[0, -0.25, 2])
    @test nm.containsInvalidForAddThenLogit(df)
    @test nm.containsInvalidForLogit(df)
    df2 = DataFrame(a=[3, -2, 0.4])
    @test nm.containsInvalidForAddThenLogit(df2) === false
    @test nm.containsInvalidForLogit(df2) === false
end

@testset "get maximum/minimum" begin
    df = DataFrame(a=[1, -1, 4], b=[-2, 5, 0])
    @test nm.getExtremum(minimum, df) === -2
    @test nm.getExtremum(maximum, df) === 5
    df2 = DataFrame(a=[NaN, 2, -2])
    @test nm.getExtremum(minimum, df2) === -2.0
    @test nm.getExtremum(maximum, df2) === 2.0
end

@testset "check normal" begin
    @test nm.isNormal(-1.23, 0.98)
    @test nm.isNormal(0.63, -1.98)
    @test nm.isNormal(0, 0)
    @test nm.isNormal(4, 6) === false
    @test nm.isNormal(3.14, 2.22, normal_ratio=4)

    nt = [(skewness_ratio = 1.19,), (skewness_ratio = 0.98,)]
    nt2 = [(kurtosis_ratio = 0.55,), (kurtosis_ratio = 1.2,)]
    @test nm.areNormal(nt, nt2)
    @test nm.areNonnormal(nt, nt2) === false
    nt3 = [(kurtosis_ratio = 2.2,), (kurtosis_ratio = 1.2,)]
    @test nm.areNormal(nt, nt3) === false
    @test nm.areNonnormal(nt, nt3)
end

@testset "filter cols" begin
    df = DataFrame(a=[1, -1, 4], b=[-2, 5, 0])
    @test nm.getFilteredCols(iseven, df) == [[4], [-2, 0]] 
    @test nm.getFilteredCols(isequal, df, 4) == [[4], []]
    @test nm.containsPredicated(iseven, df)
    @test nm.containsPredicated(isequal, df, 4)
end

@testset "get positive and error positive skew transformations" begin
    @test isempty(nm.getPositiveSkewTransformations(-1)["one arg"])
    @test nm.getPositiveSkewTransformations(-1)["two args"] == 
            [
                nm.addThenSquareRoot,
                nm.addThenSquareRootThenInvert,
                nm.addThenInvert,
                nm.addThenSquareThenInvert,
                nm.addThenLog,
                nm.addThenNaturalLog,
            ]
    @test nm.getPositiveSkewTransformations(0.5)["one arg"] == [nm.squareRoot]
    @test nm.getPositiveSkewTransformations(0.5)["two args"] ==
            [
                nm.squareRootThenAddThenInvert,
                nm.addThenInvert,
                nm.addThenSquareThenInvert,
                nm.addThenLog,
                nm.addThenNaturalLog,
            ]
    @test nm.getPositiveSkewTransformations(3.14)["one arg"] ==
            [
                nm.squareRoot,
                nm.squareRootThenInvert,
                nm.invert,
                nm.squareThenInvert,
                nm.logInBase10,
                nm.naturalLog,
            ]
    @test isempty(nm.getPositiveSkewTransformations(3.14)["two args"])
    @test isempty(nm.getErrorPositiveSkewTransformations(DivideError(), 0.1)["one arg"])
    @test nm.getErrorPositiveSkewTransformations(DivideError(), 0.1)["two args"] ==
            [
                nm.addThenSquareRoot,
                nm.addThenLog,
                nm.addThenNaturalLog,
            ]
    @test nm.getErrorPositiveSkewTransformations(DivideError(), 2.2)["one arg"] ==
            [
                nm.squareRoot,
                nm.logInBase10,
                nm.naturalLog,
            ]
    @test isempty(nm.getErrorPositiveSkewTransformations(DivideError(), 2.2)["two args"])
end

@testset "add cols" begin
    d = Dict("df_transformed" => DataFrame())
    other = Dict("df_transformed" => DataFrame(a=[1, 2]))
    @test nm.mergeCols!(d, other) == DataFrame(a=[1, 2])
    another = Dict("df_transformed" => DataFrame(b=[3, 4]))
    @test nm.mergeCols!(d, another) == DataFrame(a=[1, 2], b=[3, 4])
    @test d["df_transformed"] == DataFrame(a=[1, 2], b=[3, 4])
    @test_throws ArgumentError nm.mergeCols!(d, other) == DataFrame(a=[1, 2], b=[3, 4])
end

@testset "skewness and kurtosis" begin
    df = DataFrame(a=[1, -1, 4, 0, 2], b=[-2, 5, 0, -3, 7])
    skewness = nm.calculateSkewness(df.a)
    @test skewness.skewness_stat === nm.calculateSkewnessStat(df.a)
    @test skewness.skewness_error === nm.calculateSkewnessError(df.a)
    @test round(√(nm.calculateSkewnessVariance(df.a)), digits=3) === 
        skewness.skewness_error

    kurtosis = nm.calculateKurtosis(df.b)
    @test kurtosis.kurtosis_stat === nm.calculateKurtosisStat(df.b)
    @test kurtosis.kurtosis_error === nm.calculateKurtosisError(df.b)
    @test round(√(nm.calculateKurtosisVariance(df.b)), digits=3) ===
        kurtosis.kurtosis_error

    @test nm.calculateRatio(1, 3) === 0.333
end

@testset "labeling" begin
    stats = [(ratio = 0.5,), (number = 1.0,)]
    names = ["fraction", "version"]
    ids = nm.addLabel(stats, names)
    @test ids[1] === (ratio = 0.5, name = "fraction")
    @test ids[2] === (number = 1.0, name = "version")
    @test ids[1] != (0.5, "fraction")

    df = DataFrame(a_new__square=[4, 16, 25], b_old__square=[1, 64, 36])
    nt = [(color = "blue",), (color = "red",)]
    nt2 = [(condition = "new",), (condition = "broken",)]
    updated = nm.labelFindings(df, nt, nt2)
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
    nm.mergeResults!(d, other)
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
    nm.updateNormal!(d, other)
    @test d["normal"]["square"] == 
        [(skewness_ratio = 1.15, kurtosis_ratio = 0.87, name = "a")]
    other["kurtosis"] = [(kurtosis_ratio = 2.7,)]
    dd = Dict()
    nm.updateNormal!(dd, other)
    @test iszero(length(dd))

end

@testset "apply and record transformations" begin
    df = DataFrame(a=[-0.5, 2.47, 2.54, 2.91, 3.13], b=[0.21, 1.04, 1.11, 0.27, 5.27])
    result = nm.applyTransformation(nm.square, df)
    @test result["skewness"] == [
        (skewness_stat = -1.315, skewness_error = 0.913, skewness_ratio = -1.44), 
        (skewness_stat = 2.225, skewness_error = 0.913, skewness_ratio = 2.437)
        ]
    @test result["kurtosis"] == [
        (kurtosis_stat = 2.149, kurtosis_error = 2.0, kurtosis_ratio = 1.074), 
        (kurtosis_stat = 4.959, kurtosis_error = 2.0, kurtosis_ratio = 2.48)
        ]
    result2 = nm.applyTransformation(nm.addThenInvert, df, -0.5)
    @test result2["skewness"] == [
        (skewness_stat = 2.226, skewness_error = 0.913, skewness_ratio = 2.438), 
        (skewness_stat = -0.824, skewness_error = 0.913, skewness_ratio = -0.903)
        ]
    @test result2["kurtosis"] == [
        (kurtosis_stat = 4.963, kurtosis_error = 2.0, kurtosis_ratio = 2.482), 
        (kurtosis_stat = 0.341, kurtosis_error = 2.0, kurtosis_ratio = 0.17)
        ]

    record = nm.recordTransformations(df, [nm.square])
    @test iszero(length(record["normal"]))
    @test record["df_transformed"] == result["df_transformed"]

    record2 = nm.recordTransformations(df, [nm.addThenInvert], -0.5, normal_ratio=3)
    @test record2["normal"]["addThenInvert"] == [
        (skewness_stat = 2.226, skewness_error = 0.913, skewness_ratio = 2.438,
        kurtosis_stat = 4.963, kurtosis_error = 2.0, kurtosis_ratio = 2.482,
        name = "a"), 
        (skewness_stat = -0.824, skewness_error = 0.913, skewness_ratio = -0.903,
        kurtosis_stat = 0.341, kurtosis_error = 2.0, kurtosis_ratio = 0.17,
        name = "b")
        ]

    record3 = nm.recordTransformations(df, [nm.square], normal_ratio=3)
    
    functions = Dict("one arg" => [nm.square], "two args" => [nm.addThenInvert])
    record_2_and_3 = nm.recordAllTransformations(df, functions, -0.5, normal_ratio=3)
    @test record_2_and_3["normal"]["square"] == record3["normal"]["square"]
    @test record_2_and_3["normal"]["addThenInvert"] == record2["normal"]["addThenInvert"]
    @test record_2_and_3["df_transformed"] == 
        hcat(record3["df_transformed"], record2["df_transformed"])
end

@testset "transform positive skew" begin
    df = DataFrame(b=[0.21, 1.04, 1.11, 0.27, 5.27])
    result = nm.transformPositiveSkew(df)
    @test length(result["normal"]) === 6
    @test names(result["df_transformed"]) ==
        ["b__squareRoot",
        "b__squareRootThenAddThenInvert",
        "b__addThenInvert",
        "b__addThenSquareThenInvert",
        "b__addThenLog",
        "b__addThenNaturalLog"]
    
    df2 = DataFrame(a=[-0.5, 2.47, 2.54, 2.91, 3.13])
    result2 = nm.transformPositiveSkew(df2)
    @test iszero(length(result2["normal"]))
    @test names(result2["df_transformed"]) ==
        [
            "a__addThenSquareRoot",
            "a__addThenSquareRootThenInvert",
            "a__addThenInvert",
            "a__addThenSquareThenInvert",
            "a__addThenLog", 
            "a__addThenNaturalLog"
        ]
end

@testset "transform negative skew" begin
    df = DataFrame(b=[0.21, 1.04, 1.11, 0.27, 5.27])
    result = nm.transformNegativeSkew(df)
    @test iszero(length(result["normal"]))
    @test names(result["df_transformed"]) ==
        [
            "b__square",
            "b__cube",
            "b__antilog",
            "b__reflectThenSquareRoot",
            "b__reflectThenLog", 
            "b__reflectThenInvert"
        ]
    
    df2 = DataFrame(a=[-0.5, 2.47, 2.54, 2.91, 3.13])
    result2 = nm.transformNegativeSkew(df2)
    @test length(result2["normal"]) === 5
    @test collect(keys(result2["normal"])) ==
        [
            "reflectThenInvert",
            "reflectThenLog",
            "cube",
            "square",
            "antilog",
        ]
    @test names(result2["df_transformed"]) ==
        [
            "a__square",
            "a__cube",
            "a__antilog",
            "a__reflectThenSquareRoot",
            "a__reflectThenLog", 
            "a__reflectThenInvert"
        ]
end

@testset "stretch skew" begin
    df = DataFrame(b=[0.21, 1.04, 1.11, 0.27, 5.27])
    result = nm.stretchSkew(df)
    @test length(result["normal"]) == 2
    @test names(result["df_transformed"]) ==
        [
            "b__addThenLogit",
            "b__logit",
        ]
    
    df2 = DataFrame(a=[-0.5, 2.47, 2.54, 2.91, 3.13])
    result2 = nm.stretchSkew(df2)
    @test iszero(length(result2["normal"]))
end

@testset "normalize" begin
    df = DataFrame(b=[0.21, 1.04, 1.11, 0.27, 5.27])
    result = nm.normalize(df)
    result_positive = nm.transformPositiveSkew(df)
    result_stretch = nm.stretchSkew(df)
    @test result[1] == result_positive
    @test result[2] == result_stretch

    df2 = DataFrame(a=[-0.5, 2.47, 2.54, 2.91, 3.13])
    result2 = nm.normalize(df2)
    result2_negative = nm.transformNegativeSkew(df2)
    @test result2[1] == result2_negative
end
