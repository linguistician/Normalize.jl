module Normalize

using CSV, DataFrames, SciPy, ExcelFiles

"""
    loadSpreadsheet(path::AbstractString) -> AbstractDataFrame


"""
function loadSpreadsheet(path::AbstractString, sheet::AbstractString="")
    if endswith(path, ".csv")
        CSV.read(path, DataFrame)
    elseif endswith(path, ".xlsx")
        DataFrame(load(path, sheet))
    else
        println("Files ending with .csv or .xlsx can only be used.")
    end
end

"""
    normalize(df::AbstractDataFrame, normal_ratio::Real=2) -> AbstractVector


"""
function normalize(df::AbstractDataFrame, normal_ratio::Real=2)
    try
        skews = calculateSkewness.(eachcol(df))
        kurts = calculateKurtosis.(eachcol(df))
        normal = []
        if areNonnormal(skews, kurts, normal_ratio)
            if arePositiveSkew(skews)
                push!(normal, transformPositiveSkew(df, normal_ratio))
            elseif areNegativeSkew(skews)
                push!(normal, transformNegativeSkew(df, normal_ratio))
            end
            push!(normal, stretchSkew(df, normal_ratio))
        end
        normal
    catch e
        []
    end
end

"""
    calculateSkewness(col::AbstractVector, round_to::Integer=3, biased::Bool=false,
                      nan_rule::AbstractString="omit") -> NamedTuple


"""
function calculateSkewness(col::AbstractVector, round_to::Integer=3, biased::Bool=false,
                           nan_rule::AbstractString="omit")
    stat = calculateSkewnessStat(col, round_to, biased, nan_rule)
    error = calculateSkewnessError(col, round_to)
    ratio = calculateRatio(stat, error, round_to)
    (
        skewness_stat = stat,
        skewness_error = error,
        skewness_ratio = ratio,
    )
end

"""
    calculateSkewnessStat(col::AbstractVector, round_to::Integer=3, biased::Bool=false,
                          nan_rule::AbstractString="omit")


"""
function calculateSkewnessStat(col::AbstractVector, round_to::Integer=3,
                               biased::Bool=false, nan_rule::AbstractString="omit")
    if any(isnan.(col))
        round(SciPy.stats.skew(col, bias=biased, nan_policy=nan_rule)[1], digits=round_to)
    else
        round(SciPy.stats.skew(col, bias=biased, nan_policy=nan_rule), digits=round_to)
    end
end

"""
    calculateSkewnessError(col::AbstractVector, round_to::Integer=3)

Compute skewness error of `col`, rounded to `round_to` digits.
"""
function calculateSkewnessError(col::AbstractVector, round_to::Integer=3)
    variance = calculateSkewnessVariance(col)
    round(√variance, digits=round_to)
end

"""
    calculateSkewnessVariance(col::AbstractVector)

Compute the skewness variance of `col`.

It is calculated from the formula:

``\\frac{6N(N-1)}{(N-2)(N+1)(N+3)}``
"""
function calculateSkewnessVariance(col::AbstractVector)
    omission = count(isnan.(col))
    N = length(col) - omission
    6 * N * (N - 1) * invert((N - 2) * (N + 1) * (N + 3))
end

"""
    calculateRatio(statistic::Real, std_error::Real, round_to::Integer=3)

Compute the ratio of `statistic` and `std_error`, rounded to `round_to` digits.
"""
function calculateRatio(statistic::Real, std_error::Real, round_to::Integer=3)
   round(statistic * invert(std_error), digits=round_to)
end

"""
    calculateKurtosis(col::AbstractVector, round_to::Integer=3, biased::Bool=false,
                      nan_rule::AbstractString="omit") -> NamedTuple


"""
function calculateKurtosis(col::AbstractVector, round_to::Integer=3, biased::Bool=false,
                           nan_rule::AbstractString="omit")
    stat = calculateKurtosisStat(col, round_to, biased, nan_rule)
    error = calculateKurtosisError(col, round_to)
    ratio = calculateRatio(stat, error, round_to)
    (
        kurtosis_stat = stat,
        kurtosis_error = error,
        kurtosis_ratio = ratio,
    )
end

"""
    calculateKurtosisStat(col::AbstractVector, round_to::Integer=3, biased::Bool=false,
                          nan_rule::AbstractString="omit")


"""
function calculateKurtosisStat(col::AbstractVector, round_to::Integer=3,
                               biased::Bool=false, nan_rule::AbstractString="omit")
    if any(isnan.(col))
        round(SciPy.stats.kurtosis(col, bias=biased, nan_policy=nan_rule)[1], 
              digits=round_to)
    else
        round(SciPy.stats.kurtosis(col, bias=biased, nan_policy=nan_rule), 
              digits=round_to)
    end
end

"""
    calculateKurtosisError(col::AbstractVector, round_to::Integer=3)

Compute the kurtosis error of `col`, rounded to `round_to` digits.
"""
function calculateKurtosisError(col::AbstractVector, round_to::Integer=3)
    variance = calculateKurtosisVariance(col)
    round(√variance, digits=round_to)
end

"""
    calculateKurtosisVariance(col::AbstractVector)

Compute the kurtosis variance of `col`.

It is calculated from the formula:

``\\frac{}{(N-3)(N+5)}``
"""
function calculateKurtosisVariance(col::AbstractVector)
    omission = count(isnan.(col))
    N = length(col) - omission
    skewness_variance = calculateSkewnessVariance(col)
    4 * (N^2 - 1) * skewness_variance * invert((N - 3) * (N + 5))
end

"""
    areNonnormal(skewnesses::AbstractVector, kurtoses::AbstractVector,
                 normal_ratio::Real=2) -> Bool

Return `true` if ratios in `skewnesses` and `kurtoses` are not within the range of
±`normal_ratio`, and false otherwise.
"""
function areNonnormal(skewnesses::AbstractVector, kurtoses::AbstractVector,
                      normal_ratio::Real=2)
    !areNormal(skewnesses, kurtoses, normal_ratio)
end

"""
    areNormal(skewnesses::AbstractVector, kurtoses::AbstractVector;
              normal_ratio::Real=2) -> Bool

Return `true` if items in `skewnesses` and `kurtoses` are normal, and `false` otherwise.
"""
function areNormal(skewnesses::AbstractVector, kurtoses::AbstractVector,
                   normal_ratio::Real=2)
    combined = merge.(skewnesses, kurtoses)
    normals = [isNormal(var.skewness_ratio, var.kurtosis_ratio, normal_ratio=normal_ratio)
               for var in combined]
    all(normals)
end

"""
    isNormal(skewness_ratio::Real, kurtosis_ratio::Real, normal_ratio::Real=2) -> Bool

Return `true` if `skewness_ratio` and `kurtosis_ratio` are within the range of
±`normal_ratio`, and false otherwise.
"""
function isNormal(skewness_ratio::Real, kurtosis_ratio::Real; normal_ratio::Real=2)
    (-normal_ratio ≤ skewness_ratio ≤ normal_ratio) &&
    (-normal_ratio ≤ kurtosis_ratio ≤ normal_ratio)
end

"""
    arePositiveSkew(skewnesses::AbstractVector) -> Bool

Return `true` if `skewnesses` contains any positive ratios, and `false` otherwise.
"""
function arePositiveSkew(skewnesses::AbstractVector)
    containsSkew(isPositiveSkew, skewnesses)
end

"""
    containsSkew(f::Function, skewnesses::AbstractVector) -> Bool


"""
function containsSkew(f::Function, skewnesses::AbstractVector)
    ratios = [var.skewness_ratio
              for var in skewnesses]
    skews = f.(ratios)
    any(skews)
end

"""
    isPositiveSkew(skewness_ratio::Real) -> Bool

Return `true` if `skewness_ratio` is positive, and `false` otherwise.
"""
function isPositiveSkew(skewness_ratio::Real)
    skewness_ratio > 0
end

"""
    transformPositiveSkew(df::AbstractDataFrame, normal_ratio::Real=2) -> AbstractDict

Apply positive skew transformations to `df`, and return
"""
function transformPositiveSkew(df::AbstractDataFrame, normal_ratio::Real=2)
    try
        smallest = getExtremum(minimum, df)
        positive_transformations = getPositiveSkewTransformations(smallest)
        recordAllTransformations(df, 
                                 positive_transformations, 
                                 smallest, 
                                 normal_ratio=normal_ratio)
    catch e
        smallest = getExtremum(minimum, df)
        positive_transformations = getErrorPositiveSkewTransformations(e, smallest)
        recordAllTransformations(df, 
                                 positive_transformations, 
                                 smallest, 
                                 normal_ratio=normal_ratio)
    end
end

"""
    getExtremum(f::Function, df::AbstractDataFrame)


"""
function getExtremum(f::Function, df::AbstractDataFrame)
    filtered_cols = getFilteredCols(isfinite, df)
    col_with_extremum = f.(filtered_cols)
    f(col_with_extremum)
end

"""
    getFilteredCols(f::Function,
                    df::AbstractDataFrame[, f_second_arg::Real]) -> AbstractVector


"""
function getFilteredCols(f::Function, df::AbstractDataFrame)
    [df[(f.(df[!, name])), name]
     for name in names(df)]
end

function getFilteredCols(f::Function, df::AbstractDataFrame, f_second_arg::Real)
    [df[(f.(df[!, name], f_second_arg)), name]
     for name in names(df)]
end

"""
    getPositiveSkewTransformations(min::Real) -> AbstractDict

Return positive skew transformations for the data set based on `min`.
"""
function getPositiveSkewTransformations(min::Real)
    if min < 0
        Dict(
            "one arg" => [],
            "two args" => [
                addThenSquareRoot,
                addThenSquareRootThenInvert,
                addThenInvert,
                addThenSquareThenInvert,
                addThenLog,
                addThenNaturalLog,
            ],
        )
    elseif 0 ≤ min < 1
        Dict(
            "one arg" => [
                squareRoot,
            ],
            "two args" => [
                squareRootThenAddThenInvert,
                addThenInvert,
                addThenSquareThenInvert,
                addThenLog,
                addThenNaturalLog,
            ],
        )
    else
        Dict(
            "one arg" => [
                squareRoot,
                squareRootThenInvert,
                invert,
                squareThenInvert,
                logInBase10,
                naturalLog,
            ],
            "two args" => [],
        )
    end
end

# min < 0
"""
    addThenSquareRoot(x::Real, min::Real)

Add one to the difference of `x` and `min`, and compute its square root.

Throws error if `min > x`.
"""
function addThenSquareRoot(x::Real, min::Real)
    if min > x
        throw(error("min must be smaller."))
    end
    √(x + 1 - min)
end

"""
    addThenSquareRootThenInvert(x::Real, min::Real)

Add one to the difference of `x` and `min`, and compute its reciprocal square root.

Throws error if `min > x`.
"""
function addThenSquareRootThenInvert(x::Real, min::Real)
    invert(addThenSquareRoot(x, min))
end

"""
    addThenInvert(x::Real, min::Real)

Add one to the difference of `x` and `min`, and compute its reciprocal.

Throws error if `min > x`.
"""
function addThenInvert(x::Real, min::Real)
    if min > x
        throw(error("min must be smaller."))
    end
    invert(x + 1 - min)
end

"""
    addThenSquareThenInvert(x::Real, min::Real)

Add one to the difference of squares of `x` and `min`, and compute its reciprocal.

Throws `DivideError` if x^2 + 1 - min^2 is 0.
"""
function addThenSquareThenInvert(x::Real, min::Real)
    if min > x
        throw(error("The second parameter (min) must be smaller."))
    end
    invert(x^2 + 1 - min^2)
end

"""
    addThenLog(x::Real, min::Real)

Add one to the difference of `x` and `min`, and compute its logarithm to base 10.

Throws error if `min > x`.
"""
function addThenLog(x::Real, min::Real)
    if min > x
        throw(error("min must be smaller."))
    end
    logInBase10(x + 1 - min)
end

"""
    logInBase10(x::Real)

Compute the logarithm of `x` to base 10.

Throws `DomainError` if `x ≤ 0`.
"""
function logInBase10(x::Real)
    if iszero(x)
        throw(DomainError(x))
    end
    log10(x)
end

"""
    addThenNaturalLog(x::Real, min::Real)

Add one to the difference of `x` and `min`, and compute its natural logarithm.

Throws error if `min > x`.
"""
function addThenNaturalLog(x::Real, min::Real)
    if min > x
        throw(error("min must be smaller."))
    end
    naturalLog(x + 1 - min)
end

"""
    naturalLog(x::Real)

Compute the natural logarithm of `x`.

Throws `DomainError` if `x ≤ 0`.
"""
function naturalLog(x::Real)
    if iszero(x)
        throw(DomainError(x))
    end
    log(x)
end

# 0 ≤ min < 1 (with addThenInvert, addThenSquareThenInvert, addThenLog, addThenNaturalLog)

"""
    squareRoot(x::Real)

Compute the square root of `x`.

Throws `DomainError` if `x` is negative.
"""
function squareRoot(x::Real)
    √x
end

"""
    squareRootThenAddThenInvert(x::Real, min::Real)

Add one to the difference of square roots of `x` and `min`, and compute its reciprocal.

Throws `DomainError` if `x` or `min` is negative, and ErrorException if `min > x`.
"""
function squareRootThenAddThenInvert(x::Real, min::Real)
    if min > x
        throw(error("The second parameter (min) must be smaller."))
    end
    invert(√x + 1 - √min)
end

# min ≥ 1 (with squareRoot, logInBase10, naturalLog)

"""
    squareRootThenInvert(x::Real)

Compute the reciprocal square root of `x`.

Throws `DomainError` if `x` is negative, and `DivideError` if `x` is 0.
"""
function squareRootThenInvert(x::Real)
    invert(√x)
end

"""
    invert(x::Real)

Compute the reciprocal of `x`.

Throws `DivideError` if `x` is 0.
"""
function invert(x::Real)
    if iszero(x)
        throw(DivideError())
    end
    inv(x)
end

"""
    squareThenInvert(x::Real)

Compute the reciprocal square of `x`.

Throws `DivideError` if `x` is 0.
"""
function squareThenInvert(x::Real)
    invert(x^2)
end

"""
    recordAllTransformations(df::AbstractDataFrame, transformations::AbstractDict,
                             extremum::Real; normal_ratio::Real=2) -> AbstractDict


"""
function recordAllTransformations(df::AbstractDataFrame, transformations::AbstractDict,
                                  extremum::Real; normal_ratio::Real=2)
    record = recordTransformations(df, 
                                   transformations["one arg"], 
                                   normal_ratio=normal_ratio)
    other_record = recordTransformations(df, 
                                         transformations["two args"], 
                                         extremum,
                                         normal_ratio=normal_ratio)
    mergeResults!(record, other_record)
end

"""
    recordTransformations(df::AbstractDataFrame,
                          transformations::AbstractVector[, extremum::Real]; 
                          normal_ratio::Real=2) -> AbstractDict


"""
function recordTransformations(df::AbstractDataFrame, transformations::AbstractVector;
                               normal_ratio::Real=2)
    record = Dict(
        "normal" => Dict(),
        "df_transformed" => DataFrame(),
    )
    for transformation in transformations
        results = applyTransformation(transformation, df)
        updateNormal!(record, results, normal_ratio)
        mergeCols!(record, results)
    end
    record
end

function recordTransformations(df::AbstractDataFrame, transformations::AbstractVector,
                               extremum::Real; normal_ratio::Real=2)
    record = Dict(
        "normal" => Dict(),
        "df_transformed" => DataFrame(),
    )
    for transformation in transformations
        results = applyTransformation(transformation, df, extremum)
        updateNormal!(record, results, normal_ratio)
        mergeCols!(record, results)
    end
    record
end

"""
    applyTransformation(f::Function, df::AbstractDataFrame
                        [, f_second_arg::Real]) -> AbstractDict

Apply `f` to `df`, and return the skewness, kurtosis, and transformed data frame.
"""
function applyTransformation(f::Function, df::AbstractDataFrame)
    df_altered = select(df,
                        names(df) .=> ByRow(f))
    rename!(df_altered, getTransformedColnames(f, df_altered))
    skew = calculateSkewness.(eachcol(df_altered))
    kurt = calculateKurtosis.(eachcol(df_altered))
    Dict(
        "skewness" => skew,
        "kurtosis" => kurt,
        "df_transformed" => df_altered,
    )
end

function applyTransformation(f::Function, df::AbstractDataFrame, f_second_arg::Real)
    df_altered = select(df,
                        names(df) .=> ByRow(x -> f(x, f_second_arg)))
    rename!(df_altered, getTransformedColnames(f, df_altered))
    skew = calculateSkewness.(eachcol(df_altered))
    kurt = calculateKurtosis.(eachcol(df_altered))
    Dict(
        "skewness" => skew,
        "kurtosis" => kurt,
        "df_transformed" => df_altered,
    )
end

"""
    getTransformedColnames(f::Function, df::AbstractDataFrame) -> AbstractVector

Return an array of new column names with `f` for `df`.
"""
function getTransformedColnames(f::Function, df::AbstractDataFrame)
    [Symbol(renameWithFunction(f, colname))
     for colname in names(df)]
end

"""
    renameWithFunction(f::Function, name::AbstractString) -> AbstractString

Create a string with `name` and `f`.
"""
function renameWithFunction(f::Function, name::AbstractString)
    marker_before_function = findlast("_", name)[1]
    end_of_original_colname = marker_before_function - 1
    old_name = view(name, 1:end_of_original_colname)
    # change if double underscore is used in original colname
    marker = "__"
    function_name = string(f)
    old_name * marker * function_name
end

"""
    updateNormal!(d::AbstractDict, other::AbstractDict, normal_ratio::Real=2)

Add the results from `other` to `d` whose ratios are within the range of ±`normal_ratio`.
"""
function updateNormal!(d::AbstractDict, other::AbstractDict, normal_ratio::Real=2)
    skews = other["skewness"]
    kurts = other["kurtosis"]
    if areNormal(skews, kurts, normal_ratio)
        df_altered = other["df_transformed"]
        transformations = getAppliedTransformations(df_altered)
        d["normal"][transformations] = labelFindings(df_altered, skews, kurts)
    end
    nothing
end

"""
    getAppliedTransformations(df::AbstractDataFrame) -> SubString

Return the transformations that were applied to `df`.
"""
function getAppliedTransformations(df::AbstractDataFrame)
    new_colname = names(df)[1]
    before_marker = locateEndOfOriginalColname(new_colname)
    after_marker = before_marker + 3
    SubString(new_colname, after_marker)
end

"""
    locateEndOfOriginalColname(name::AbstractString) -> Integer

Return the index in `name` that is the last character of the original column name.
"""
function locateEndOfOriginalColname(name::AbstractString)
    # change if double underscore is used in original colname
    marker_after_old_colname = "__"
    position = findfirst(marker_after_old_colname, name)[1]
    position - 1
end

"""
    labelFindings(df::AbstractDataFrame, skewnesses::AbstractVector,
                  kurtoses::AbstractVector) -> AbstractVector


"""
function labelFindings(df::AbstractDataFrame, skewnesses::AbstractVector,
                       kurtoses::AbstractVector)
    combined = merge.(skewnesses, kurtoses)
    original_colnames = getOriginalColnames(df)
    addLabel(combined, original_colnames)
end

"""
    getOriginalColnames(df::AbstractDataFrame) -> AbstractVector

Return the original column names of `df`.
"""
function getOriginalColnames(df::AbstractDataFrame)
    [view(colname, 1:locateEndOfOriginalColname(colname))
     for colname in names(df)]
end

"""
    addLabel(ratios::AbstractVector, names::AbstractVector) -> AbstractVector


"""
function addLabel(ratios::AbstractVector, names::AbstractVector)
    labeled = []
    for i in 1:length(ratios)
        entry = (
            name = names[i],
        )
        updated = merge(ratios[i], entry)
        push!(labeled, updated)
    end
    labeled
end

"""
    mergeCols!(d::AbstractDict, other::AbstractDict) -> AbstractDataFrame

Concatenate `df` and a data frame from `results`.
"""
function mergeCols!(d::AbstractDict, other::AbstractDict)
    d["df_transformed"] = hcat(d["df_transformed"], other["df_transformed"])
end

"""
    mergeResults!(d::AbstractDict, other::AbstractDict) -> AbstractDict


"""
function mergeResults!(d::AbstractDict, other::AbstractDict)
    merge!(d["normal"], other["normal"])
    mergeCols!(d, other)
    d
end

"""
    getErrorPositiveSkewTransformations(error::Exception, min::Real) -> AbstractDict

Return positive skew transformations for the data set based on `error` and `min`.
"""
function getErrorPositiveSkewTransformations(error::Exception, min::Real)
    if isa(error, DivideError)
        if min < 1
            Dict(
                "one arg" => [],
                "two args" => [
                    addThenSquareRoot,
                    addThenLog,
                    addThenNaturalLog,
                ],
            )
        else
            Dict(
                "one arg" => [
                    squareRoot,
                    logInBase10,
                    naturalLog,
                ],
                "two args" => [],
            )
        end
    end
end

"""
    areNegativeSkew(skewnesses::AbstractVector) -> Bool

Return `true` if `skewnesses` contains any negative ratios, and `false` otherwise.
"""
function areNegativeSkew(skewnesses::AbstractVector)
    containsSkew(isNegativeSkew, skewnesses)
end

"""
    isNegativeSkew(skewness_ratio::Real) -> Bool

Return `true` if `skewness_ratio` is negative, and `false` otherwise.
"""
function isNegativeSkew(skewness_ratio::Real)
    skewness_ratio < 0
end

"""
    transformNegativeSkew(df::AbstractDataFrame, normal_ratio::Real=2) -> AbstractDict


"""
function transformNegativeSkew(df::AbstractDataFrame, normal_ratio::Real=2)
    largest = getExtremum(maximum, df)
    negative_transformations = Dict(
        "one arg" => [
            square,
            cube,
            antilog,
        ],
        "two args" => [
            reflectThenSquareRoot,
            reflectThenLog,
            reflectThenInvert,
        ],
    )
    recordAllTransformations(df, 
                             negative_transformations, 
                             largest, 
                             normal_ratio=normal_ratio)
end

"""
    square(x::Real)

Compute the square of `x`.
"""
function square(x::Real)
    x^2
end

"""
    cube(x::Real)

Compute the cube of `x`.
"""
function cube(x::Real)
    x^3
end

"""
    antilog(x::Real)

Compute the antilog of `x` to base 10.
"""
function antilog(x::Real)
    exp10(x)
end

"""
    reflectThenSquareRoot(x::Real, max::Real)

Add one to the difference of `max` and `x`, and return its square root.

Throws error if `max < x`.
"""
function reflectThenSquareRoot(x::Real, max::Real)
    if max < x
        throw(error("max must be greater."))
    end
    √(max + 1 - x)
end

"""
    reflectThenLog(x::Real, max::Real)

Add one to the difference of `max` and `x`, and compute its logarithm to base 10.

Throws error if `max < x`.
"""
function reflectThenLog(x::Real, max::Real)
    if max < x
        throw(error("max must be greater."))
    end
    logInBase10(max + 1 - x)
end

"""
    reflectThenInvert(x::Real, max::Real)

Add one to the difference of `max` and `x`, and compute its reciprocal.

Throws error if `max < x`.
"""
function reflectThenInvert(x::Real, max::Real)
    if max < x
        throw(error("max must be greater."))
    end
    invert(max + 1 - x)
end

"""
    getErrorNegativeSkewTransformations(error::Exception) -> AbstractDict

Return negative skew transformations for the data set based on `error` and `max`.
"""
function getErrorNegativeSkewTransformations(error::Exception)
    if isa(error, DomainError)
        Dict(
            "one arg" => [
                square,
                cube,
                antilog,
            ],
            "two args" => [
                reflectThenInvert,
            ],
        )
    elseif isa(error, DivideError)
        Dict(
            "one arg" => [
                square,
                cube,
                antilog,
            ],
            "two args" => [
                reflectThenSquareRoot,
                reflectThenLog,
            ],
        )
    end
end

"""
    stretchSkew(df::AbstractDataFrame, normal_ratio::Real=2) -> AbstractDict


"""
function stretchSkew(df::AbstractDataFrame, normal_ratio::Real=2)
    stretch_transformations = [
        addThenLogit,
        logit,
    ]
    if containsInvalidForAddThenLogit(df)
        popfirst!(stretch_transformations)
    end
    if containsInvalidForLogit(df)
        pop!(stretch_transformations)
    end
    recordTransformations(df, stretch_transformations, normal_ratio=normal_ratio)
end

"""
    addThenLogit(x::Real)

Increase `x` by `0.25`, and compute the logit in base 10.

Throws `DomainError` if `x` = -0.25, and `DivideError` if `x` = 0.75.
"""
function addThenLogit(x::Real)
    logit(x + 0.25)
end

"""
    logit(x::Real)

Compute the logit of `x` to base 10.

Throws `DomainError` if `x` = 0, and `DivideError` if `x` = 1.
"""
function logit(x::Real)
    logInBase10(abs(x * invert(1 - x)))
end

"""
    containsInvalidForAddThenLogit(df::AbstractDataFrame) -> Bool

"""
function containsInvalidForAddThenLogit(df::AbstractDataFrame)
    containsPredicated(isInvalidForAddThenLogit, df)
end

"""
    containsPredicated(f::Function, df::AbstractDataFrame
                       [, f_second_arg::Real]) -> Bool

Return `true` if `f` returns any values from `df`, and false otherwise.
"""
function containsPredicated(f::Function, df::AbstractDataFrame)
    filtered_cols = getFilteredCols(f, df)
    sum(length.(filtered_cols)) > 0
end

function containsPredicated(f::Function, df::AbstractDataFrame, f_second_arg::Real)
    filtered_cols = getFilteredCols(f, df, f_second_arg)
    sum(length.(filtered_cols)) > 0
end

"""
    isInvalidForAddThenLogit(x::Real) -> Bool


"""
function isInvalidForAddThenLogit(x::Real)
    x === -0.25 || x === 0.75
end

"""
    containsInvalidForLogit(df::AbstractDataFrame) -> Bool

Return `true` if `0` or `1` exists in `df`, and `false` otherwise.
"""
function containsInvalidForLogit(df::AbstractDataFrame)
    containsPredicated(isInvalidForLogit, df)
end

"""
    isInvalidForLogit(x::Real) -> Bool

Return `true` if `x` is 0 or 1.
"""
function isInvalidForLogit(x::Real)
    iszero(x) || isone(x)
end

"""
    printSkewnessKurtosis(df::AbstractDataFrame)

Print the skewness and kurtosis (statistic, standard error, ratio) of each column in `df`.
"""
function printSkewnessKurtosis(df::AbstractDataFrame)
    for colname in names(df)
        skew = calculateSkewness(df[!, colname])
        kurt = calculateKurtosis(df[!, colname])
        label = (
            name = colname,
        )
        combined = merge(skew, kurt, label)
        printSummary(combined)
    end
end

"""
    printSummary(sk::NamedTuple)


"""
function printSummary(sk::NamedTuple)
    println("""
        --------
        Variable: $(sk.name)
        Skewness Statistic: $(sk.skewness_stat)
        Skewness Std. Error: $(sk.skewness_error)
        Skewness Ratio: $(sk.skewness_ratio)

        Kurtosis Statistic: $(sk.kurtosis_stat)
        Kurtosis Std. Error: $(sk.kurtosis_error)
        Kurtosis Ratio: $(sk.kurtosis_ratio)""")
end

"""
    stringToFloat!(df::AbstractDataFrame) -> AbstractDataFrame

Convert columns of type `String` to `Float64` in `df`.

If a string cannot be parsed to `Float64`, an error is raised.

# Examples
```jldoctest
julia> df = DataFrame(a=1:2, b=["4", "5.2"])
2×2 DataFrame
 Row │ a      b
     │ Int64  String
─────┼───────────────
   1 │     1  4
   2 │     2  5.2

julia> stringToFloat!(df)
2×2 DataFrame
 Row │ a      b
     │ Int64  Float64
─────┼────────────────
   1 │     1      4.0
   2 │     2      5.2
```
"""
function stringToFloat!(df::AbstractDataFrame)
    for colname in names(df)
        col = df[!, colname]
        if eltype(col) === String
            blankToNaN!(col)
            df[!, colname] = parse.(Float64, col)
        end
    end
    df
end

"""
    blankToNaN!(col::AbstractVector) -> AbstractVector

Replace blank values with `"NaN"` in `col`.

# Examples
```jldoctest
julia> df = DataFrame(a=[" ", "1", " "], b=["2", " ", "3"]);

julia> replaceBlank!(df)
3×2 DataFrame
 Row │ a       b
     │ String  String
─────┼────────────────
   1 │ NaN     2
   2 │ 1       NaN
   3 │ NaN     3
```
"""
function blankToNaN!(col::AbstractVector)
    replace!(col, " " => "NaN")
end

"""
    printFindings(results::AbstractVector)


"""
function printFindings(results::AbstractVector)
    try
        normal = results[1]["normal"]
        for (transformation, altered) in normal
            println("APPLIED: $transformation")
            printSummary.(altered)
            println("\n")
        end
    catch e
        println("Unable to normalize.")
    end
end

end
