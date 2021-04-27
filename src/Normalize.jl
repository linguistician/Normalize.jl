module Normalize

using CSV
using DataFrames
using ExcelFiles
using OdsIO
using SciPy
using StatFiles

export apply, normalize, record, record_all
export are_negative_skew, are_positive_skew, is_negative_skew, is_positive_skew
export are_nonnormal, are_normal, is_normal
export get_negative_skew_transformations, get_positive_skew_transformations
export get_skew_transformations, get_stretch_skew_transformations
export kurtosis, kurtosis_error, kurtosis_stat, kurtosis_variance
export print_findings, print_skewness_kurtosis
export skewness, skewness_error, skewness_stat, skewness_variance
export string_to_float!, tabular_to_dataframe

"""
    normalize(df; normal_ratio::Real=2, marker="__") -> AbstractDict

Normalize a data frame `df` for its skewness and kurtosis ratios to be within the range of
±`normal_ratio`.

Functions will be separately applied to `df`, and the column names of the transformed
data frame will be suffixed with a string `marker` and the applied functions.

A dictionary is returned with the following key-value pairs:
- `"normal"` => a dictionary of named tuples containing skewnesses and kurtoses
- `"df_transformed"` => a transformed data frame from applying functions to `df`
"""
function normalize(df; normal_ratio::Real=2, marker="__")
    transformations = get_skew_transformations(df; normal_ratio=normal_ratio)
    record_all(df, transformations; normal_ratio=normal_ratio, marker=marker)
end

"""
    get_skew_transformations(df; normal_ratio::Real=2) -> AbstractDict

Return functions to apply to a data frame `df` if the skewness and kurtosis ratios of a 
`df` are not within the range of ±`normal_ratio`.

The returned dictionary has the following key-value pairs:
- `"one arg"` => a collection of functions that require one argument
- `"two args"` => a collection of functions that require one argument
"""
function get_skew_transformations(df; normal_ratio::Real=2)
    skews = skewness.(eachcol(df))
    kurts = kurtosis.(eachcol(df))
    transformations = Dict(
        "one arg" => Function[],
        "two args" => Any[],
    )
    if are_nonnormal(skews, kurts; normal_ratio=normal_ratio)
        if are_positive_skew(skews)
            positive = get_positive_skew_transformations(df)
            mergewith!(append!, transformations, positive)
        elseif are_negative_skew(skews)
            negative = get_negative_skew_transformations(df)
            mergewith!(append!, transformations, negative)
        end
        stretch = get_stretch_skew_transformations(df)
        mergewith!(append!, transformations, stretch)
    end
end

"""
    skewness(col, round_to=3) -> NamedTuple

Compute the skewness of a collection `col`, rounded to `round_to` digits.

The returned named tuple has the following fields:
- `skewness_stat`: skewness statistic
- `skewness_error`: skewness standard error
- `skewness_ratio`: skewness ratio
"""
function skewness(col, round_to=3)
    stat = skewness_stat(col, round_to)
    error = skewness_error(col, round_to)
    ratio = round(stat * invert(error), digits=round_to)
    (
        skewness_stat=stat,
        skewness_error=error,
        skewness_ratio=ratio,
    )
end

"""
    skewness_stat(col, round_to=3)

Computes skewness statistic of a collection `col`, rounded to `round_to` digits.
"""
function skewness_stat(col, round_to=3)
    if any(isnan, col)
        round(SciPy.stats.skew(col, bias=false, nan_policy="omit")[1], digits=round_to)
    else
        round(SciPy.stats.skew(col, bias=false, nan_policy="omit"), digits=round_to)
    end
end

"""
    skewness_error(col, round_to=3)

Compute the skewness error of a collection `col`, rounded to `round_to` digits.
"""
function skewness_error(col, round_to=3)
    variance = skewness_variance(col)
    round(√variance, digits=round_to)
end

"""
    skewness_variance(col)

Compute the skewness variance of a collection `col`.

It is calculated from the formula:

``\\frac{6N(N-1)}{(N-2)(N+1)(N+3)}``
"""
function skewness_variance(col)
    omission = count(isnan.(col))
    N = length(col) - omission
    6 * N * (N - 1) * invert((N - 2) * (N + 1) * (N + 3))
end

"""
    kurtosis(col, round_to=3) -> NamedTuple

Compute the kurtosis of a collection `col`, rounded to `round_to` digits.

The returned named tuple has the following fields:
- `kurtosis_stat`: kurtosis statistic
- `kurtosis_error`: kurtosis standard error
- `kurtosis_ratio`: kurtosis ratio
"""
function kurtosis(col, round_to=3)
    stat = kurtosis_stat(col, round_to)
    error = kurtosis_error(col, round_to)
    ratio = round(stat * invert(error), digits=round_to)
    (
        kurtosis_stat=stat,
        kurtosis_error=error,
        kurtosis_ratio=ratio,
    )
end

"""
    kurtosis_stat(col, round_to=3)

Compute the kurtosis statistic of a collection `col`, rounded to `round_to` digits.
"""
function kurtosis_stat(col, round_to=3)
    if any(isnan, col)
        round(SciPy.stats.kurtosis(col, bias=false, nan_policy="omit")[1], digits=round_to)
    else
        round(SciPy.stats.kurtosis(col, bias=false, nan_policy="omit"), digits=round_to)
    end
end

"""
    kurtosis_error(col, round_to=3)

Compute the kurtosis error of a collection `col`, rounded to `round_to` digits.
"""
function kurtosis_error(col, round_to=3)
    variance = kurtosis_variance(col)
    round(√variance, digits=round_to)
end

"""
    kurtosis_variance(col)

Compute the kurtosis variance of a collection `col`.

It is calculated from the formula:

``\\frac{4(N^2-1)*skewness_variance}{(N-3)(N+5)}``
"""
function kurtosis_variance(col)
    omission = count(isnan.(col))
    N = length(col) - omission
    skewness_var = skewness_variance(col)
    4 * (N^2 - 1) * skewness_var * invert((N - 3) * (N + 5))
end

"""
    are_nonnormal(skewnesses, kurtoses; normal_ratio::Real=2) -> Bool

Return `true` if ratios are not within the range of ±`normal_ratio` in collections
`skewnesses` and `kurtoses` containing named tuples, and false otherwise.
"""
function are_nonnormal(skewnesses, kurtoses; normal_ratio::Real=2)
    !are_normal(skewnesses, kurtoses; normal_ratio=normal_ratio)
end

"""
    are_normal(skewnesses, kurtoses; normal_ratio::Real=2) -> Bool

Return `true` if ratios are within the range of ±`normal_ratio` in collections
`skewnesses` and `kurtoses` containing named tuples, and false otherwise.
"""
function are_normal(skewnesses, kurtoses; normal_ratio::Real=2)
    combined = merge.(skewnesses, kurtoses)
    all(
        i -> is_normal(i.skewness_ratio, i.kurtosis_ratio; normal_ratio=normal_ratio),
        combined,
    )
end

"""
    is_normal(skewness_ratio::Real, kurtosis_ratio::Real; normal_ratio::Real=2) -> Bool

Return `true` if `skewness_ratio` and `kurtosis_ratio` are within the range of
±`normal_ratio`, and false otherwise.
"""
function is_normal(skewness_ratio::Real, kurtosis_ratio::Real; normal_ratio::Real=2)
    (-normal_ratio ≤ skewness_ratio ≤ normal_ratio) &&
    (-normal_ratio ≤ kurtosis_ratio ≤ normal_ratio)
end

"""
    are_positive_skew(skewnesses) -> Bool

Return `true` if there are any positive ratios in a collection `skewnesses` containing
named tuples, and `false` otherwise.
"""
function are_positive_skew(skewnesses)
    contains(is_positive_skew, skewnesses)
end

"""
    contains(func, skewnesses) -> Bool

Return `true` if there are ratios that are `true` for the function `func` in a collection
`skewnesses` containing named tuples, and `false` otherwise.
"""
function contains(func, skewnesses)
    any(func, var.skewness_ratio for var in skewnesses)
end

"""
    is_positive_skew(skewness_ratio::Real) -> Bool

Return `true` if `skewness_ratio` is positive, and `false` otherwise.
"""
function is_positive_skew(skewness_ratio::Real)
    skewness_ratio > 0
end

"""
    get_positive_skew_transformations(df) -> AbstractDict

Return functions to transform positive skew for a data frame `df`.

The returned dictionary has the following key-value pairs:
- `"one arg"` => a collection of functions that require one argument
- `"two args"` => a collection of functions that require one argument
"""
function get_positive_skew_transformations(df)
    min = extrema(df)[1]
    if min < 0
        positive = Dict(
            "one arg" => Function[],
            "two args" => Any[
                add_then_square_root,
                add_then_square_root_then_invert,
                add_then_invert,
                add_then_log_base_10,
                add_then_natural_log,
                square_then_add_then_invert,
            ],
        )
        if contains(cannot_square_then_add_then_invert, df, min)
            pop!(positive["two args"])
        end
    elseif 0 ≤ min < 1
        positive = Dict(
            "one arg" => Function[
                square_root,
            ],
            "two args" => Any[
                square_root_then_add_then_invert,
                add_then_invert,
                add_then_log_base_10,
                add_then_natural_log,
                square_then_add_then_invert,
            ],
        )
    else
        positive = Dict(
            "one arg" => Function[
                square_root,
                square_root_then_invert,
                invert,
                square_then_invert,
                log_base_10,
                natural_log,
            ],
            "two args" => Any[],
        )
    end
    positive["two args"] = map(func -> (func, min), positive["two args"])
    positive
end

"""
    extrema(df) -> Tuple

Compute the minimum and maximum of a data frame `df`.
"""
function extrema(df)
    filtered_cols = filter_cols(isfinite, df)
    mins_from_cols = minimum.(filtered_cols)
    min = minimum(mins_from_cols)
    maxes_from_cols = maximum.(filtered_cols)
    max = maximum(maxes_from_cols)
    (min, max)
end

"""
    filter_cols(func, df[, func_other_arg]) -> AbstractVector

Returns a vector of elements in a data frame `df` for which a function `func` is `true`.

`func` is passed up to two arguments. If `func` takes two arguments, then `func_other_arg`
is the second argument.
"""
function filter_cols(func, df)
    filter.(func, eachcol(df))
end

function filter_cols(func, df, func_other_arg)
    filter.(x -> func(x, func_other_arg), eachcol(df))
end

# min < 0
"""
    add_then_square_root(x::Real, min::Real)

Compute ``\\sqrt{x + 1 - min}``.

Throw error if `min > x`.
"""
function add_then_square_root(x::Real, min::Real)
    if min > x
        throw(error("min must be smaller."))
    end
    √(x + 1 - min)
end

"""
    add_then_square_root_then_invert(x::Real, min::Real)

Compute ``\\frac{1}{\\sqrt{x + 1 - min}}``.

Throw error if `min > x`.
"""
function add_then_square_root_then_invert(x::Real, min::Real)
    invert(add_then_square_root(x, min))
end

"""
    add_then_invert(x::Real, min::Real)

Compute ``\\frac{1}{x + 1 - min}``

Throw error if `min > x`.
"""
function add_then_invert(x::Real, min::Real)
    if min > x
        throw(error("min must be smaller."))
    end
    invert(x + 1 - min)
end

"""
    add_then_log_base_10(x::Real, min::Real)

Compute ``\\log_{10}(x + 1 - min)``.

Throw error if `min > x`.
"""
function add_then_log_base_10(x::Real, min::Real)
    if min > x
        throw(error("min must be smaller."))
    end
    log_base_10(x + 1 - min)
end

"""
    log_base_10(x::Real)

Compute the logarithm of `x` to base 10.

Throw `DomainError` if `x ≤ 0`.
"""
function log_base_10(x::Real)
    if iszero(x)
        throw(DomainError(x))
    end
    log10(x)
end

"""
    add_then_natural_log(x::Real, min::Real)

Compute ``\\ln(x + 1 - min)``.

Throw error if `min > x`.
"""
function add_then_natural_log(x::Real, min::Real)
    if min > x
        throw(error("min must be smaller."))
    end
    natural_log(x + 1 - min)
end

"""
    natural_log(x::Real)

Compute the natural logarithm of `x`.

Throw `DomainError` if `x ≤ 0`.
"""
function natural_log(x::Real)
    if iszero(x)
        throw(DomainError(x))
    end
    log(x)
end

"""
    square_then_add_then_invert(x::Real, min::Real)

Compute ``\\frac{1}{x^2 + 1 - min^2}``.

Throw `DivideError` if x^2 + 1 - min^2 is 0.
"""
function square_then_add_then_invert(x::Real, min::Real)
    if min > x
        throw(error("min must be smaller."))
    end
    invert(x^2 + 1 - min^2)
end

"""
    contains(func, df::AbstractDataFrame[, func_other_arg]) -> Bool

Return `true` if `func` returns any values from `df`, and false otherwise.
"""
function contains(func, df::AbstractDataFrame)
    filtered_cols = filter_cols(func, df)
    sum(length.(filtered_cols)) > 0
end

function contains(func, df::AbstractDataFrame, func_other_arg)
    filtered_cols = filter_cols(func, df, func_other_arg)
    sum(length.(filtered_cols)) > 0
end

"""
    cannot_square_then_add_then_invert(x::Real, min::Real)

Return `true` if ``x^2 + 1 - min^2`` is 0, and false otherwise.
"""
function cannot_square_then_add_then_invert(x::Real, min::Real)
    x^2 + 1 - min^2 === 0
end

# 0 ≤ min < 1 (with add_then_invert, add_then_log_base_10, add_then_natural_log
#              square_then_add_then_invert)

"""
    square_root(x::Real)

Compute ``\\sqrt{x}``.

Throw `DomainError` if `x` is negative.
"""
function square_root(x::Real)
    √x
end

"""
    square_root_then_add_then_invert(x::Real, min::Real)

Compute ``\\frac{1}{\\sqrt{x} + 1 - \\sqrt{min}}``.

Throw `DomainError` if `x` or `min` is negative, and `ErrorException` if `min > x`.
"""
function square_root_then_add_then_invert(x::Real, min::Real)
    if min > x
        throw(error("min must be smaller."))
    end
    invert(√x + 1 - √min)
end

# min ≥ 1 (with square_root, log_base_10, natural_log)

"""
    square_root_then_invert(x::Real)

Compute ``\\frac{1}{\\sqrt{x}}``.

Throw `DomainError` if `x` is negative, and `DivideError` if `x` is 0.
"""
function square_root_then_invert(x::Real)
    invert(√x)
end

"""
    invert(x::Real)

Compute ``\\frac{1}{x}``.

Throw `DivideError` if `x` is 0.
"""
function invert(x::Real)
    if iszero(x)
        throw(DivideError())
    end
    inv(x)
end

"""
    square_then_invert(x::Real)

Compute ``\\frac{1}{x^2}``.

Throw `DivideError` if `x` is 0.
"""
function square_then_invert(x::Real)
    invert(x^2)
end

"""
    are_negative_skew(skewnesses) -> Bool

Return `true` if there are any negative ratios in a collection `skewnesses` containing
named tuples, and `false` otherwise.
"""
function are_negative_skew(skewnesses)
    contains(is_negative_skew, skewnesses)
end

"""
    is_negative_skew(skewness_ratio::Real) -> Bool

Return `true` if `skewness_ratio` is negative, and `false` otherwise.
"""
function is_negative_skew(skewness_ratio::Real)
    skewness_ratio < 0
end

"""
    get_negative_skew_transformations(df) -> AbstractDict

Return functions to transform negative skew for a data frame `df`.

The returned dictionary has the following key-value pairs:
- `"one arg"` => a collection of functions that require one argument
- `"two args"` => a collection of functions that require one argument
"""
function get_negative_skew_transformations(df)
    max = extrema(df)[2]
    negative = Dict(
        "one arg" => Function[
            square,
            cube,
            antilog,
        ],
        "two args" => Any[
            reflect_then_square_root,
            reflect_then_log_base_10,
            reflect_then_invert,
        ],
    )
    negative["two args"] = map(func -> (func, max), negative["two args"])
    negative
end

"""
    square(x::Real)

Compute ``x^2``.
"""
function square(x::Real)
    x^2
end

"""
    cube(x::Real)

Compute ``x^3``.
"""
function cube(x::Real)
    x^3
end

"""
    antilog(x::Real)

Compute the antilog of `x` to base 10, in other words ``10^x``.
"""
function antilog(x::Real)
    exp10(x)
end

"""
    reflect_then_square_root(x::Real, max::Real)

Compute ``\\sqrt{max + 1 - x}``.

Throw error if `max < x`.
"""
function reflect_then_square_root(x::Real, max::Real)
    if max < x
        throw(error("max must be greater."))
    end
    √(max + 1 - x)
end

"""
    reflect_then_log_base_10(x::Real, max::Real)

Compute ``\\log_{10}(max + 1 - x)``.

Throw error if `max < x`.
"""
function reflect_then_log_base_10(x::Real, max::Real)
    if max < x
        throw(error("max must be greater."))
    end
    log_base_10(max + 1 - x)
end

"""
    reflect_then_invert(x::Real, max::Real)

Compute ``\\frac{1}{max + 1 - x}``.

Throw error if `max < x`.
"""
function reflect_then_invert(x::Real, max::Real)
    if max < x
        throw(error("max must be greater."))
    end
    invert(max + 1 - x)
end

"""
    get_stretch_transformations(df) -> AbstractDict

Return functions to stretch skew for a data frame `df`.

The returned dictionary has the following key-value pairs:
- `"one arg"` => a collection of functions that require one argument
- `"two args"` => a collection of functions that require one argument
"""
function get_stretch_skew_transformations(df)
    stretch = Dict(
        "one arg" => Function[
            add_then_logit,
            logit,
        ],
        "two args" => Any[]
    )
    if contains(cannot_add_then_logit, df)
        popfirst!(stretch["one arg"])
    end

    if contains(cannot_logit, df)
        pop!(stretch["one arg"])
    end
    stretch
end

"""
    add_then_logit(x::Real)

Compute the logit of `x + 0.25` in base 10, in other words
``\\log_{10}|\\frac{x + 0.25}{1 - (x + 0.25)}|``.

Throw `DomainError` if `x` is -0.25, and `DivideError` if `x` is 0.75.
"""
function add_then_logit(x::Real)
    logit(x + 0.25)
end

"""
    logit(x::Real)

Compute the logit of `x` to base 10, in other words ``\\log_{10}|\\frac{x}{1 - x}|``.

Throw `DomainError` if `x` is 0, and `DivideError` if `x` is 1.
"""
function logit(x::Real)
    log_base_10(abs(x * invert(1 - x)))
end

"""
    cannot_add_then_logit(x::Real) -> Bool

Return `true` if x is -0.25 or 0.75, and `false` otherwise.
"""
function cannot_add_then_logit(x::Real)
    x === -0.25 || x === 0.75
end

"""
    cannot_logit(x::Real) -> Bool

Return `true` if `x` is 0 or 1, and `false` otherwise.
"""
function cannot_logit(x::Real)
    iszero(x) || isone(x)
end

"""
    record_all(df, transform_series; normal_ratio::Real=2, marker="__") -> AbstractDict

Compute the skewness and kurtosis for each function in a dictionary `transform_series` applied
to a data frame `df`, and return a dictionary of transformed columns in a data frame and of
any resulting skewness and kurtosis whose ratios are within the range of ±`normal_ratio`.

`transform_series` has the following key-value pairs:
- `"one arg"` => a collection of functions that require one argument
- `"two args"` => a collection of functions that require one argument

Functions will be separately applied to `df`, and the column names of the transformed
data frame will be suffixed with a string `marker` and the applied functions.

A dictionary is returned with the following key-value pairs:
- `"normal"` => a dictionary of named tuples containing skewnesses and kurtoses
- `"df_transformed"` => a transformed data frame from applying functions to `df`
"""
function record_all(df, transform_series; normal_ratio::Real=2, marker="__")
    first_record = record(
        df, transform_series["one arg"]; normal_ratio=normal_ratio, marker=marker
    )
    other_record = record(
        df, transform_series["two args"]; normal_ratio=normal_ratio, marker=marker
    )
    merge_results!(first_record, other_record)
end

"""
    record(
        df, transformations::Vector{Function}; normal_ratio::Real=2, marker="__"
    ) -> AbstractDict
    record(df, transformations; normal_ratio::Real=2, marker="__") -> AbstractDict

Compute the skewness and kurtosis for each function in `transformations` applied to a data
frame `df`, and return a dictionary of transformed columns in a data frame and of any
resulting skewness and kurtosis whose ratios are within the range of ±`normal_ratio`.

If `transformation` is not of type `Vector{Function}`, then it is a collection of function
and extremum pairs. The extremum is the minimum or maximum value of `df` that corresponds
to the second argument of the paired function.

Functions will be separately applied to `df`, and the column names of the transformed
data frame will be suffixed with a string `marker` and the applied functions.

A dictionary is returned with the following key-value pairs:
- `"normal"` => a dictionary of named tuples containing skewnesses and kurtoses
- `"df_transformed"` => a transformed data frame from applying functions to `df`
"""
function record(df, transformations::Vector{Function}; normal_ratio::Real=2, marker="__")
    main_record = Dict(
        "normal" => Dict(),
        "df_transformed" => DataFrame(),
    )
    for transformation in transformations
        results = apply(transformation, df; marker=marker)
        update_normal!(main_record, results; normal_ratio=normal_ratio, marker=marker)
        merge_cols!(main_record, results)
    end
    main_record
end

function record(df, transformations; normal_ratio::Real=2, marker="__")
    main_record = Dict(
        "normal" => Dict(),
        "df_transformed" => DataFrame(),
    )
    for (transformation, extremum) in transformations
        results = apply(transformation, df, extremum; marker=marker)
        update_normal!(main_record, results; normal_ratio=normal_ratio, marker=marker)
        merge_cols!(main_record, results)
    end
    main_record
end

"""
    apply(func, df[, func_other_arg]; marker="__") -> AbstractDict

Apply a function `func` to a data frame `df`, and return the skewness, kurtosis, and
transformed data frame.

`func` is passed up to two arguments. If `func` takes two arguments, then `func_other_arg`
is the second argument.

Functions will be separately applied to `df`, and the column names of the transformed
data frame will be suffixed with a string `marker` and the applied functions.

A dictionary is returned with the following key-value pairs:
- `"skewness"` => a named tuple with the fields `skewness_stat` (statistic),
    `skewness_error` (standard error), and `skewness_ratio` (ratio)
- `"kurtosis"` => a named tuple with the fields `kurtosis_stat` (statistic),
    `kurtosis_error` (standard error), and `kurtosis_ratio` (ratio)
- `"df_transformed"` => a transformed data frame
"""
function apply(func, df; marker="__")
    df_altered = rename(colname -> "$(colname)-+", df)
    select!(df_altered, names(df_altered) .=> ByRow(func))
    rename!(colname -> rename_with(func, colname; marker=marker), df_altered)
    skew = skewness.(eachcol(df_altered))
    kurt = kurtosis.(eachcol(df_altered))
    Dict(
        "skewness" => skew,
        "kurtosis" => kurt,
        "df_transformed" => df_altered,
    )
end

function apply(func, df, func_other_arg; marker="__")
    df_altered = rename(colname -> "$(colname)-+", df)
    select!(df_altered, names(df_altered) .=> ByRow(x -> func(x, func_other_arg)))
    rename!(colname -> rename_with(func, colname; marker=marker), df_altered)
    skew = skewness.(eachcol(df_altered))
    kurt = kurtosis.(eachcol(df_altered))
    Dict(
        "skewness" => skew,
        "kurtosis" => kurt,
        "df_transformed" => df_altered,
    )
end

"""
    rename_with(func, name; marker="__") -> AbstractString

Create a new string from a string `name` and function `func`, separated by a string
`marker`.
"""
function rename_with(func, name; marker="__")
    if length(marker) < 2 || !all(ispunct, marker)
        throw(
            error("marker should contain at least 2 characters, and only punctuations.")
        )
    end
    old_name = get_original(name)
    function_name = string(func)
    old_name * marker * function_name
end

"""
    get_original(colname; marker="__") -> SubString

Return the original form of a string `colname`, positioned before a string `marker`.
"""
function get_original(colname; marker="__")
    view(colname, 1:find_original_end(colname; marker=marker))
end

"""
    find_original_end(colname; marker="__") -> Integer

Return the index in a string `colname` that is the last character of its original form,
positioned before a string `marker`.
"""
function find_original_end(colname; marker="__")
    if Base.contains(colname, "-+_")
        starting = findlast("-+_", colname)[1]
        starting - 1
    elseif Base.contains(colname, marker)
        starting = findfirst(marker, colname)[1]
        starting - 1
    else
        length(colname)
    end
end

"""
    update_normal!(main_dict, other_dict; normal_ratio::Real=2, marker="__")

Update a dictionary `main_dict` with the results from a dictionary `other_dict` whose
ratios are within the range of ±`normal_ratio`.

`main_dict` has a key `"normal"` with a dictionary value that can accept strings as keys
and collections of named tuples as values. The dictionary value will have the names of
functions that produced normal ratios as its keys.

`other_dict` has the following key-value pairs:
- `"skewness"` => a named tuple with the fields `skewness_stat` (statistic),
    `skewness_error` (standard error), and `skewness_ratio` (ratio)
- `"kurtosis"` => a named tuple with the fields `kurtosis_stat` (statistic),
    `kurtosis_error` (standard error), and `kurtosis_ratio` (ratio)
- `"df_transformed"` => a transformed data frame

Functions were separately applied to `df`, and the column names of the transformed
data frame were suffixed with a string `marker` and the applied functions.
"""
function update_normal!(main_dict, other_dict; normal_ratio::Real=2, marker="__")
    skews = other_dict["skewness"]
    kurts = other_dict["kurtosis"]
    if are_normal(skews, kurts; normal_ratio=normal_ratio)
        df_altered = other_dict["df_transformed"]
        transformations = get_applied_function_names(df_altered; marker=marker)
        main_dict["normal"][transformations] = label_findings(
            df_altered, skews, kurts; marker=marker
        )
    end
    nothing
end

"""
    get_applied_function_names(df; marker="__") -> AbstractString

Return the names of functions that were applied to a data frame `df`.

The names of functions are positioned after a string `marker` in the column names of `df`.
"""
function get_applied_function_names(df; marker="__")
    new_colname = names(df)[1]
    before_marker = find_original_end(new_colname; marker=marker)
    after_marker = before_marker + length(marker) + 1
    function_names = SubString(new_colname, after_marker)
    make_legible(function_names, " then "; marker=marker)
end

"""
    make_legible(snake_case, split_on=" "; marker="__") -> AbstractString

Return readable form of a string `snake_case`.

The underscores in `snake_case` becomes spaces. The new string then splits into an array of
substrings on occurrences of a string `split_on` before the readable form is returned. If
there is an additional separator besides an underscore, then the new string is split on a
string `marker` before becoming readable.
"""
function make_legible(snake_case, split_on=" "; marker="__")
    words = make_phrase(snake_case, "_", " ")
    listable = make_listing(words, split_on)
    chars = length(marker)
    if marker === repeat("_", chars)
        spaces = repeat(" ", chars)
    else
        spaces = marker
    end
    make_phrase(listable, spaces, "; ")
end

"""
    make_phrase(fragment, split_on, join_with) -> AbstractString

Split a string `fragment` into an array of substrings on occurrences of a string
`split_on`, and join the array into a new string, inserting a string `join_with`
between adjacent substrings.
"""
function make_phrase(fragment, split_on, join_with)
    words = split(fragment, split_on)
    join(words, join_with)
end

"""
    make_listing(phrase, split_on=" ") -> AbstractString

Create a listing of items in a string `phrase` that resulted from splitting it on the
occurrences of a string `split_on`.
"""
function make_listing(phrase, split_on=" ")
    words = split(phrase, split_on)
    if length(words) ≤ 2
       join(words, " and ")
    else
        join(words, ", ", ", and ")
    end
end

"""
    label_findings(df, skewnesses, kurtoses; marker="__") -> AbstractVector

Merge original column names of a data frame `df` and named tuples of collections
`skewnesses` and `kurtoses`.

The original column names are positioned before a string `marker` in the column names of
`df`.
"""
function label_findings(df, skewnesses, kurtoses; marker="__")
    combined = merge.(skewnesses, kurtoses)
    original_colnames = get_original.(names(df); marker=marker)
    label.(combined, original_colnames)
end

"""
    label(prelim, tag) -> NamedTuple

Merge a string `tag` as a `name` field to the named tuple `prelim`.
"""
function label(prelim, tag)
    entry = (
        name=tag,
    )
    merge(entry, prelim)
end

"""
    merge_cols!(main_dict, other_dict) -> AbstractDataFrame

Concatenate a data frame from a dictionary `main_dict` and a data frame from a dictionary
`other_dict`.

Both dictionaries have a key `"df_transformed"` with a data frame value.
"""
function merge_cols!(main_dict, other_dict)
    main_dict["df_transformed"] = hcat(
        main_dict["df_transformed"], 
        other_dict["df_transformed"],
    )
end

"""
    merge_results!(main_dict, other_dict) -> AbstractDict

Merge values in keys `"normal"` and `"df_transformed"` from dictionaries `main_dict` and
`other_dict`.

Both dictionaries have a key `"normal"` with a dictionary value of named tuples, and a key
`"df_transformed"` with a data frame value.
"""
function merge_results!(main_dict, other_dict)
    merge!(main_dict["normal"], other_dict["normal"])
    merge_cols!(main_dict, other_dict)
    main_dict
end

"""
    tabular_to_dataframe(path, sheet="") -> AbstractDataFrame

Load tabular data from a string `path`, and converts it into a data frame.

Acceptable file extensions are .csv, .dta, .ods, .sav, .xls, and .xlsx. If `path` ends in
.ods, .xls, or .xlsx, the worksheet with the name `sheet` is converted.
"""
function tabular_to_dataframe(path, sheet="")
    if endswith(path, ".csv")
        CSV.read(path, DataFrame)
    elseif endswith(path, r"\.xls.?")
        DataFrame(load(path, sheet))
    elseif endswith(path, ".sav") || endswith(path, ".dta")
        DataFrame(load(path))
    elseif endswith(path, ".ods")
        ods_read(path; sheetName=sheet, retType="DataFrame")
    else
        println(
            "Files ending with .csv, .dta, .ods, .sav, .xls, or .xlsx can only be used."
        )
        DataFrame()
    end
end

"""
    string_to_float!(df) -> AbstractDataFrame

Convert columns of type `String` to `Float64` in a data frame `df`.

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

julia> string_to_float!(df)
2×2 DataFrame
 Row │ a      b
     │ Int64  Float64
─────┼────────────────
   1 │     1      4.0
   2 │     2      5.2
```
"""
function string_to_float!(df)
    for colname in names(df)
        col = df[!, colname]
        if eltype(col) === String
            replace!(col, " " => "NaN")
            df[!, colname] = parse.(Float64, col)
        end
    end
    df
end

"""
    print_skewness_kurtosis(df)

Print the skewness and kurtosis (statistic, standard error, ratio) of each column in a
data frame `df`.
"""
function print_skewness_kurtosis(df)
    for colname in names(df)
        skew = skewness(df[!, colname])
        kurt = kurtosis(df[!, colname])
        tag = (
            name=colname,
        )
        combined = merge(skew, kurt, tag)
        print_summary(combined)
    end
end

"""
    print_summary(factor::NamedTuple)

Print the skewness and kurtosis in `factor`.

`factor` has the following fields:
- `name`
- `skewness_stat`
- `skewness_error`
- `skewness_ratio`
- `kurtosis_stat`
- `kurtosis_error`
- `kurtosis_ratio`
"""
function print_summary(factor::NamedTuple)
    println("""
        --------
        Variable: $(factor.name)
        Skewness Statistic: $(factor.skewness_stat)
        Skewness Std. Error: $(factor.skewness_error)
        Skewness Ratio: $(factor.skewness_ratio)

        Kurtosis Statistic: $(factor.kurtosis_stat)
        Kurtosis Std. Error: $(factor.kurtosis_error)
        Kurtosis Ratio: $(factor.kurtosis_ratio)
        """
    )
end

"""
    print_findings(findings)

Print the normal skewness and kurtosis in a dictionary `findings`.

`findings` has a key `normal` with named tuples for each normal transformation.
"""
function print_findings(findings)
    normal = findings["normal"]
    for (transformation, altered) in normal
        println("APPLIED: $transformation")
        print_summary.(altered)
        println("\n")
    end

    if isempty(findings["df_transformed"])
        println("No transformations applied, so unable to normalize.")
    elseif isempty(normal)
        println("Transformations applied, but no normal results.")
    end
end

end
