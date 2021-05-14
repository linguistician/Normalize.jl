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
export replace_missing!, sheetcols_to_float!
export normal_to_csv, tabular_to_dataframe

"""
    normalize(
        gdf; normal_ratio::Real=2, dependent::Bool=false, marker="__"
    ) -> AbstractDict

Normalize a (grouped) data frame `gdf` for its skewness and kurtosis ratios to be within
the range of ±`normal_ratio`.

If `dependent` is `true`, then the difference between the columns of `gdf` will be
normalized.

Functions will be separately applied to `gdf`, and the column names of the transformed
data frame will be suffixed with a string `marker` and the applied functions.

A dictionary is returned with the following key-value pairs:
- `"normalized"` => a dictionary of a collection value of named tuples containing
    skewnesses and kurtoses for each normal transformation key
- `"normal gdf"` => a transformed data frame or list of grouped data frames whose data are
    normal
- `"nonnormal gdf"` => a transformed data frame or list of grouped data frames whose data
    are nonnormal
"""
function normalize(gdf; normal_ratio::Real=2, dependent::Bool=false, marker="__")
    if dependent
        if isa(gdf, AbstractDataFrame) && ncol(gdf) === 2
            difference = _diff_pair(gdf)
            gdf = DataFrame([difference])
        else
            println(
                "There can only be one group and two dependent variables to normalize."
            )
            return Dict(
                "normalized" => Dict(),
                "normal gdf" => DataFrame(),
                "nonnormal gdf" => DataFrame(),
            )
        end
    end
    transformations = get_skew_transformations(gdf; normal_ratio)
    return record_all(gdf, transformations; normal_ratio, marker)
end

function _diff_pair(df)
    var = names(df)[1]
    var2 = names(df)[2]
    return "$(var2)_minus_$(var)" => df[var2] .- df[var]
end

"""
    get_skew_transformations(gdf; normal_ratio::Real=2) -> AbstractDict

Return functions to apply to a (grouped) data frame `gdf` if its skewness and kurtosis
ratios are not within the range of ±`normal_ratio`.

The returned dictionary has the following key-value pairs:
- `"one arg"` => a collection of functions that require one argument
- `"two args"` => a collection of functions that require two arguments
"""
function get_skew_transformations(gdf; normal_ratio::Real=2)
    skews_and_kurts = _get_skewness_kurtosis(gdf)
    transformations = Dict(
        "one arg" => Function[],
        "two args" => Any[],
    )
    df_new = DataFrame(gdf)
    if are_nonnormal(skews_and_kurts; normal_ratio)
        if are_positive_skew(skews_and_kurts)
            positive = get_positive_skew_transformations(df_new)
            mergewith!(append!, transformations, positive)
        elseif are_negative_skew(skews_and_kurts)
            negative = get_negative_skew_transformations(df_new)
            mergewith!(append!, transformations, negative)
        end
        stretch = get_stretch_skew_transformations(df_new)
        mergewith!(append!, transformations, stretch)
    end

    return transformations
end

function _get_skewness_kurtosis(df::AbstractDataFrame)
    skews = skewness.(eachcol(df))
    kurts = kurtosis.(eachcol(df))
    return merge.(
        skews,
        kurts,
    )
end

function _get_skewness_kurtosis(gd)
    gd_new = combine(gd, valuecols(gd) => x -> [skewness(x), kurtosis(x),]; ungroup=false)
    return [
        merge(gd_new[group][var]...)
        for group in keys(gd_new)
            for var in valuecols(gd_new)
    ]
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
    ratio = round(stat * invert(error); digits=round_to)
    return (
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
        return round(
            SciPy.stats.skew(col, bias=false, nan_policy="omit")[1]; digits=round_to
        )
    else
        return round(SciPy.stats.skew(col, bias=false, nan_policy="omit"); digits=round_to)
    end
end

"""
    skewness_error(col, round_to=3)

Compute the skewness error of a collection `col`, rounded to `round_to` digits.
"""
function skewness_error(col, round_to=3)
    variance = skewness_variance(col)
    return round(√variance; digits=round_to)
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
    return 6 * N * (N - 1) * invert((N - 2) * (N + 1) * (N + 3))
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

    return inv(x)
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
    ratio = round(stat * invert(error); digits=round_to)
    return (
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
        return round(
            SciPy.stats.kurtosis(col, bias=false, nan_policy="omit")[1]; digits=round_to
        )
    else
        return round(
            SciPy.stats.kurtosis(col, bias=false, nan_policy="omit"); digits=round_to
        )
    end
end

"""
    kurtosis_error(col, round_to=3)

Compute the kurtosis error of a collection `col`, rounded to `round_to` digits.
"""
function kurtosis_error(col, round_to=3)
    variance = kurtosis_variance(col)
    return round(√variance; digits=round_to)
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
    return 4 * (N^2 - 1) * skewness_var * invert((N - 3) * (N + 5))
end

"""
    are_nonnormal(skew_kurt_ratios; normal_ratio::Real=2) -> Bool

Return `true` if ratios are not within the range of ±`normal_ratio` in a collection
`skew_kurt_ratios` containing named tuples, and false otherwise.

The named tuples in `skew_kurt_ratios` have the following fields:
- `skewness_ratio`: skewness ratio
- `kurtosis_ratio`: kurtosis ratio
"""
function are_nonnormal(skew_kurt_ratios; normal_ratio::Real=2)
    return !are_normal(skew_kurt_ratios; normal_ratio)
end

"""
    are_normal(skew_kurt_ratios; normal_ratio::Real=2) -> Bool

Return `true` if ratios are within the range of ±`normal_ratio` in collections
`skew_kurt_ratios` containing named tuples, and false otherwise.

The named tuples in `skew_kurt_ratios` have the following fields:
- `skewness_ratio`: skewness ratio
- `kurtosis_ratio`: kurtosis ratio
"""
function are_normal(skew_kurt_ratios; normal_ratio::Real=2)
    return all(
        i -> is_normal(i.skewness_ratio, i.kurtosis_ratio; normal_ratio), skew_kurt_ratios
    )
end

"""
    is_normal(skewness_ratio::Real, kurtosis_ratio::Real; normal_ratio::Real=2) -> Bool

Return `true` if `skewness_ratio` and `kurtosis_ratio` are within the range of
±`normal_ratio`, and false otherwise.
"""
function is_normal(skewness_ratio::Real, kurtosis_ratio::Real; normal_ratio::Real=2)
    return (-normal_ratio ≤ skewness_ratio ≤ normal_ratio) &&
        (-normal_ratio ≤ kurtosis_ratio ≤ normal_ratio)
end

"""
    are_positive_skew(skewnesses) -> Bool

Return `true` if there are any positive ratios in a collection `skewnesses` containing
named tuples, and `false` otherwise.

`skewnesses` has named tuples with a field `skewness_ratio`.
"""
function are_positive_skew(skewnesses)
    return _contains(is_positive_skew, skewnesses)
end

function _contains(func, skewnesses)
    return any(func, var.skewness_ratio for var in skewnesses)
end

function _contains(func, df::AbstractDataFrame)
    filtered_cols = _filter_cols(func, df)
    return sum(length.(filtered_cols)) > 0
end

function _contains(func, df::AbstractDataFrame, func_other_arg)
    filtered_cols = _filter_cols(func, df, func_other_arg)
    return sum(length.(filtered_cols)) > 0
end

function _filter_cols(func, df)
    return filter.(func, eachcol(df))
end

function _filter_cols(func, df, func_other_arg)
    return filter.(x -> func(x, func_other_arg), eachcol(df))
end

"""
    is_positive_skew(skewness_ratio::Real) -> Bool

Return `true` if `skewness_ratio` is positive, and `false` otherwise.
"""
function is_positive_skew(skewness_ratio::Real)
    return skewness_ratio > 0
end

"""
    get_positive_skew_transformations(df) -> AbstractDict

Return functions to transform positive skew for a data frame `df`.

The returned dictionary has the following key-value pairs:
- `"one arg"` => a collection of functions that require one argument
- `"two args"` => a collection of functions that require two arguments
"""
function get_positive_skew_transformations(df)
    min = _extrema(df)[1]
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
        if _contains(_cannot_square_then_add_then_invert, df, min)
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
    return positive
end

function _extrema(df)
    filtered_cols = _filter_cols(isfinite, df)
    mins_from_cols = minimum.(filtered_cols)
    min = minimum(mins_from_cols)
    maxes_from_cols = maximum.(filtered_cols)
    max = maximum(maxes_from_cols)
    return (min, max)
end

# min < 0 
# (add_then_square_root, add_then_square_root_then_invert, add_then_invert,
# add_then_log_base_10, add_then_natural_log, square_then_add_then_invert)
"""
    add_then_square_root(x::Real, min::Real)

Compute ``\\sqrt{x + 1 - min}``.

Throw error if `min > x`.
"""
function add_then_square_root(x::Real, min::Real)
    if min > x
        throw(error("min must be smaller."))
    end

    return √(x + 1 - min)
end

"""
    add_then_square_root_then_invert(x::Real, min::Real)

Compute ``\\frac{1}{\\sqrt{x + 1 - min}}``.

Throw error if `min > x`.
"""
function add_then_square_root_then_invert(x::Real, min::Real)
    return invert(add_then_square_root(x, min))
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

    return invert(x + 1 - min)
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

    return log_base_10(x + 1 - min)
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

    return log10(x)
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

    return natural_log(x + 1 - min)
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

    return log(x)
end

"""
    square_then_add_then_invert(x::Real, min::Real)

Compute ``\\frac{1}{x^2 + 1 - min^2}``.

Throw `DivideError` if x^2 + 1 - min^2 is 0, and ErrorException if `min > x`.
    .
"""
function square_then_add_then_invert(x::Real, min::Real)
    if min > x
        throw(error("min must be smaller."))
    end

    return invert(x^2 + 1 - min^2)
end

function _cannot_square_then_add_then_invert(x::Real, min::Real)
    return x^2 + 1 - min^2 === 0
end

# 0 ≤ min < 1 
# (square_root, square_root_then_add_then_invert, add_then_invert, add_then_log_base_10,
# add_then_natural_log, square_then_add_then_invert)

"""
    square_root(x::Real)

Compute ``\\sqrt{x}``.

Throw `DomainError` if `x` is negative.
"""
function square_root(x::Real)
    return √x
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

    return invert(√x + 1 - √min)
end

# min ≥ 1 
# (square_root, square_root_then_invert, invert, square_then_invert, log_base_10,
# natural_log, invert)

"""
    square_root_then_invert(x::Real)

Compute ``\\frac{1}{\\sqrt{x}}``.

Throw `DomainError` if `x` is negative, and `DivideError` if `x` is 0.
"""
function square_root_then_invert(x::Real)
    return invert(√x)
end

"""
    square_then_invert(x::Real)

Compute ``\\frac{1}{x^2}``.

Throw `DivideError` if `x` is 0.
"""
function square_then_invert(x::Real)
    return invert(x^2)
end

"""
    are_negative_skew(skewnesses) -> Bool

Return `true` if there are any negative ratios in a collection `skewnesses` containing
named tuples, and `false` otherwise.
"""
function are_negative_skew(skewnesses)
    return _contains(is_negative_skew, skewnesses)
end

"""
    is_negative_skew(skewness_ratio::Real) -> Bool

Return `true` if `skewness_ratio` is negative, and `false` otherwise.
"""
function is_negative_skew(skewness_ratio::Real)
    return skewness_ratio < 0
end

"""
    get_negative_skew_transformations(df) -> AbstractDict

Return functions to transform negative skew for a data frame `df`.

The returned dictionary has the following key-value pairs:
- `"one arg"` => a collection of functions that require one argument
- `"two args"` => a collection of functions that require two arguments
"""
function get_negative_skew_transformations(df)
    max = _extrema(df)[2]
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
    return negative
end

"""
    square(x::Real)

Compute ``x^2``.
"""
function square(x::Real)
    return x^2
end

"""
    cube(x::Real)

Compute ``x^3``.
"""
function cube(x::Real)
    return x^3
end

"""
    antilog(x::Real)

Compute the antilog of `x` to base 10, in other words ``10^x``.
"""
function antilog(x::Real)
    return exp10(x)
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
    
    return √(max + 1 - x)
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

    return log_base_10(max + 1 - x)
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

    return invert(max + 1 - x)
end

"""
    get_stretch_transformations(df) -> AbstractDict

Return functions to stretch skew for a data frame `df`.

The returned dictionary has the following key-value pairs:
- `"one arg"` => a collection of functions that require one argument
- `"two args"` => a collection of functions that require two arguments
"""
function get_stretch_skew_transformations(df)
    stretch = Dict(
        "one arg" => Function[
            add_then_logit,
            logit,
        ],
        "two args" => Any[],
    )
    if _contains(_cannot_add_then_logit, df)
        popfirst!(stretch["one arg"])
    end

    if _contains(_cannot_logit, df)
        pop!(stretch["one arg"])
    end

    return stretch
end

"""
    add_then_logit(x::Real)

Compute the logit of `x + 0.25` in base 10, in other words
``\\log_{10}|\\frac{x + 0.25}{1 - (x + 0.25)}|``.

Throw `DomainError` if `x` is -0.25, and `DivideError` if `x` is 0.75.
"""
function add_then_logit(x::Real)
    return logit(x + 0.25)
end

"""
    logit(x::Real)

Compute the logit of `x` to base 10, in other words ``\\log_{10}|\\frac{x}{1 - x}|``.

Throw `DomainError` if `x` is 0, and `DivideError` if `x` is 1.
"""
function logit(x::Real)
    return log_base_10(abs(x * invert(1 - x)))
end

function _cannot_add_then_logit(x::Real)
    return x === -0.25 || x === 0.75
end

function _cannot_logit(x::Real)
    return iszero(x) || isone(x)
end

"""
    record_all(gdf, transform_series; normal_ratio::Real=2, marker="__") -> AbstractDict

Compute the skewness and kurtosis for each function in a dictionary `transform_series`
applied to a (grouped) data frame `gdf`, and return a dictionary of transformed columns in 
a data frame or grouped data frames, and of any resulting skewness and kurtosis whose
ratios are within the range of ±`normal_ratio`.

`transform_series` has the following key-value pairs:
- `"one arg"` => a collection of functions that require one argument
- `"two args"` => a collection of functions that require two arguments

Functions will be separately applied to `gdf`, and the column names of the transformed
(grouped) data frame will be suffixed with a string `marker` and the applied functions.

A dictionary is returned with the following key-value pairs:
- `"normalized"` => a dictionary of a collection value of named tuples containing
    skewnesses and kurtoses for each normal transformation key
- `"normal gdf"` => a transformed data frame or list of grouped data frames whose data are
    normal
- `"nonnormal gdf"` => a transformed data frame or list of grouped data frames whose data
    are nonnormal
"""
function record_all(gdf, transform_series; normal_ratio::Real=2, marker="__")
    first_record = record(gdf, transform_series["one arg"]; normal_ratio, marker)
    other_record = record(gdf, transform_series["two args"]; normal_ratio, marker)
    return _merge_results!(first_record, other_record)
end

"""
    record(
        df::AbstractDataFrame,
        transformations::Vector{Function};
        normal_ratio::Real=2,
        marker="__"
    ) -> AbstractDict
    record(
        df::AbstractDataFrame, transformations; normal_ratio::Real=2, marker="__"
    ) -> AbstractDict
    record(
        gd, transformations::Vector{Function}; normal_ratio::Real=2, marker="__"
    ) -> AbstractDict
    record(gd, transformations; normal_ratio::Real=2, marker="__") -> AbstractDict

Compute the skewness and kurtosis for each function in `transformations` applied to `df` or
grouped data frame `gd`, and return a dictionary of transformed columns in a data frame or
grouped data frames, and of any resulting skewness and kurtosis whose ratios are within the
range of ±`normal_ratio`.

If `transformation` is not of type `Vector{Function}`, then it is a collection of function
and extremum pairs. The extremum is the minimum or maximum value of `df` that corresponds
to the second argument of the paired function.

Functions will be separately applied to `df` or `gd`, and the column names of the
transformed (grouped) data frame will be suffixed with a string `marker` and the applied
functions.

A dictionary is returned with the following key-value pairs:
- `"normalized"` => a dictionary of a collection value of named tuples containing
    skewnesses and kurtoses for each normal transformation key
- `"normal gdf"` => a transformed data frame or list of grouped data frames whose data are
    normal
- `"nonnormal gdf"` => a transformed data frame or list of grouped data frames whose data
    are nonnormal
"""
function record(
    df::AbstractDataFrame,
    transformations::Vector{Function};
    normal_ratio::Real=2,
    marker="__"
)
    main_record = Dict(
        "normalized" => Dict(),
        "normal gdf" => DataFrame(),
        "nonnormal gdf" => DataFrame(),
    )
    for transformation in transformations
        results = apply(transformation, df; marker)
        _update_normal!(main_record, results; normal_ratio, marker)
    end

    return main_record
end

function record(df::AbstractDataFrame, transformations; normal_ratio::Real=2, marker="__")
    main_record = Dict(
        "normalized" => Dict(),
        "normal gdf" => DataFrame(),
        "nonnormal gdf" => DataFrame(),
    )
    for (transformation, extremum) in transformations
        results = apply(transformation, df, extremum; marker)
        _update_normal!(main_record, results; normal_ratio, marker)
    end

    return main_record
end

function record(gd, transformations::Vector{Function}; normal_ratio::Real=2, marker="__")
    main_record = Dict(
        "normalized" => Dict(),
        "normal gdf" => [],
        "nonnormal gdf" => [],
    )
    for transformation in transformations
        results = apply(transformation, gd; marker)
        _update_normal!(main_record, results; normal_ratio, marker)
    end

    return main_record
end

function record(gd, transformations; normal_ratio::Real=2, marker="__")
    main_record = Dict(
        "normalized" => Dict(),
        "normal gdf" => [],
        "nonnormal gdf" => [],
    )
    for (transformation, extremum) in transformations
        results = apply(transformation, gd, extremum; marker)
        _update_normal!(main_record, results; normal_ratio, marker)
    end

    return main_record
end

"""
    apply(func, df::AbstractDataFrame[, func_other_arg]; marker="__") -> NamedTuple
    apply(func, gd[, func_other_arg]; marker="__") -> NamedTuple

Apply a function `func` to `df` or grouped data frame `gd`, and return the skewness,
kurtosis, and transformed (grouped) data frame.

`func` is passed up to two arguments. If `func` takes two arguments, then `func_other_arg`
is the second argument.

Functions will be separately applied to `df` or `gd`, and the column names of the
transformed (grouped) data frame will be suffixed with a string `marker` and the applied
functions.

A named tuple is returned with the following fields:
- `skewness_and_kurtosis` => a named tuple with the fields `skewness_stat`,`skewness_error`,
    `skewness_ratio`, `kurtosis_stat`, `kurtosis_error`, and `kurtosis_ratio`
- `transformed_gdf` => a transformed (grouped) data frame
"""
function apply(func, df::AbstractDataFrame; marker="__")
    if length(marker) < 2 || !all(ispunct, marker)
        throw(
            error("marker should contain at least 2 characters, and only punctuations.")
        )
    end
    df_altered = rename(colname -> _rename_with("-+", colname; marker=""), df)
    select!(df_altered, names(df_altered) .=> ByRow(func))
    rename!(colname -> _rename_with(func, colname; marker), df_altered)
    return (
        skewness_and_kurtosis=_get_skewness_kurtosis(df_altered),
        transformed_gdf=df_altered,
    )
end

function apply(func, df::AbstractDataFrame, func_other_arg; marker="__")
    if length(marker) < 2 || !all(ispunct, marker)
        throw(
            error("marker should contain at least 2 characters, and only punctuations.")
        )
    end
    df_altered = rename(colname -> _rename_with("-+", colname; marker=""), df)
    select!(df_altered, names(df_altered) .=> ByRow(x -> func(x, func_other_arg)))
    rename!(colname -> _rename_with(func, colname; marker), df_altered)
    return (
        skewness_and_kurtosis=_get_skewness_kurtosis(df_altered),
        transformed_gdf=df_altered,
    )
end

function apply(func, gd; marker="__")
    if length(marker) < 2 || !all(ispunct, marker)
        throw(
            error("marker should contain at least 2 characters, and only punctuations.")
        )
    end
    gd_new = _rename_valuecols(gd, "-+"; marker="")
    gd_new = _rename_valuecols(
        select(gd_new, valuecols(gd_new) .=> ByRow(func); ungroup=false), func; marker
    )
    return (
        skewness_and_kurtosis=_get_skewness_kurtosis(gd_new),
        transformed_gdf=gd_new,
    )
end

function apply(func, gd, func_other_arg; marker="__")
    if length(marker) < 2 || !all(ispunct, marker)
        throw(
            error("marker should contain at least 2 characters, and only punctuations.")
        )
    end
    gd_new = _rename_valuecols(gd, "-+"; marker="")
    gd_new = _rename_valuecols(
        select(
            gd_new,
            valuecols(gd_new) .=> ByRow(x -> func(x, func_other_arg));
            ungroup=false,
        ),
        func;
        marker,
    )
    return (
        skewness_and_kurtosis=_get_skewness_kurtosis(gd_new),
        transformed_gdf=gd_new,
    )
end

function _rename_with(fragment, name; marker="__")
    old_name = _get_original(name; marker)
    return "$(old_name)$(marker)$(fragment)"
end

function _get_original(colname; marker="__")
    return view(colname, 1:_find_original_end(colname; marker))
end

function _find_original_end(colname; marker="__")
    if length(marker) < 2
        return length(colname)
    elseif Base.contains(colname, "-+_")
        starting = findlast("-+_", colname)[1]
        return starting - 1
    else
        starting = findfirst(marker, colname)[1]
        return starting - 1
    end
end

function _rename_valuecols(gd, fragment; marker="__")
    grouping = groupcols(gd)
    new_names = [var => _rename_with(fragment, string(var); marker)
                 for var in valuecols(gd)]
    df_altered = rename(DataFrame(gd), new_names)
    return groupby(df_altered, grouping)
end

function _update_normal!(entries, applied; normal_ratio::Real=2, marker="__")
    if are_normal(applied[:skewness_and_kurtosis]; normal_ratio)
        _label_findings!(entries, applied; marker)
        _store_transformed!(
            entries, "normal gdf", applied[:transformed_gdf]
        )
    else
        _store_transformed!(entries, "nonnormal gdf", applied[:transformed_gdf])
    end

    return nothing
end

function _label_findings!(entries, applied; marker="__")
    gdf_altered = applied[:transformed_gdf]
    transformations = _get_applied_function_names(gdf_altered; marker)
    original_colnames = _get_original_colnames(gdf_altered; marker)
    entries["normalized"][transformations] = _label.(
        applied[:skewness_and_kurtosis], original_colnames
    )
    return entries["normalized"][transformations]
end

function _get_applied_function_names(df::AbstractDataFrame; marker="__")
    new_colname = names(df)[1]
    before_marker = _find_original_end(new_colname; marker)
    after_marker = before_marker + length(marker) + 1
    function_names = SubString(new_colname, after_marker)
    return _make_legible(function_names, " then "; marker)
end

function _get_applied_function_names(gd; marker="__")
    new_colname = string(valuecols(gd)[1])
    before_marker = _find_original_end(new_colname; marker)
    after_marker = before_marker + length(marker) + 1
    function_names = SubString(new_colname, after_marker)
    return _make_legible(function_names, " then "; marker)
end

function _make_legible(snake_case, split_on=" "; marker="__")
    words = _make_phrase(snake_case, "_", " ")
    listable = _make_listing(words, split_on)
    chars = length(marker)
    if marker === repeat("_", chars)
        spaces = repeat(" ", chars)
    else
        spaces = marker
    end

    return _make_phrase(listable, spaces, "; ")
end

function _make_phrase(fragment, split_on, join_with)
    words = split(fragment, split_on)
    return join(words, join_with)
end

function _make_listing(phrase, split_on=" ")
    words = split(phrase, split_on)
    if length(words) ≤ 2
        return join(words, " and ")
    else
        return join(words, ", ", ", and ")
    end
end

function _get_original_colnames(gdf::AbstractDataFrame; marker="__")
    return _get_original.(names(gdf); marker)
end

function _get_original_colnames(gdf; marker="__")
    originals = _get_original.(string(var) for var in valuecols(gdf); marker)
    return _concat_groupnames(gdf, originals)
end

function _concat_groupnames(gd, colnames)
    new_names = []
    for varname in colnames
        for group in keys(gd)
            grouping = chop(string(group), head=length("GroupKey: ("))
            push!(new_names, "$varname ($grouping)")
        end
    end

    return new_names
end

function _label(prelim, tag)
    title = (
        name=tag,
    )
    return merge(title, prelim)
end

function _store_transformed!(main_dict, key, value::AbstractDataFrame)
    main_dict[key] = hcat(main_dict[key], value)
    return main_dict[key]
end

function _store_transformed!(main_dict, key, value)
    if isempty(value)
        return append!(main_dict[key], value)
    else
        return push!(main_dict[key], value)
    end
end

function _merge_results!(main_dict, other_dict)
    merge!(main_dict["normalized"], other_dict["normalized"])
    _store_transformed!(
        main_dict, "normal gdf", other_dict["normal gdf"]
    )
    _store_transformed!(
        main_dict, "nonnormal gdf", other_dict["nonnormal gdf"]
    )
    return main_dict
end

"""
    tabular_to_dataframe(path, sheet="") -> AbstractDataFrame

Load tabular data from a string `path`, and converts it into a data frame.

Acceptable file extensions are .csv, .dta, .ods, .sav, .xls, and .xlsx. If `path` ends in
.ods, .xls, or .xlsx, the worksheet with the name `sheet` is converted.
"""
function tabular_to_dataframe(path, sheet="")
    if endswith(path, ".csv")
        return CSV.read(path, DataFrame)
    elseif endswith(path, r"\.xls.?")
        return DataFrame(load(path, sheet))
    elseif endswith(path, ".sav") || endswith(path, ".dta")
        return DataFrame(load(path))
    elseif endswith(path, ".ods")
        return ods_read(path; sheetName=sheet, retType="DataFrame")
    else
        println(
            "Files ending with .csv, .dta, .ods, .sav, .xls, or .xlsx can only be used."
        )
        return DataFrame()
    end
end

"""
    sheetcols_to_float!(df; blank_to::Real) -> AbstractDataFrame

Convert columns of element type `String` or `Any` to `Float64` in a data frame `df`. Blank
values (i.e., " ") will be replaced with `blank_to` before converting to `Float64`.

`df` originated from data in a CSV, an Excel, or OpenDocument Spreadsheet file.

If a value cannot be converted to `Float64`, an error is raised.

# Examples
```jldoctest
julia> df = DataFrame(a=1:2, b=["4", "5.2"])
2×2 DataFrame
 Row │ a      b
     │ Int64  String
─────┼───────────────
   1 │     1  4
   2 │     2  5.2

julia> sheetcols_to_float!(df)
2×2 DataFrame
 Row │ a      b
     │ Int64  Float64
─────┼────────────────
   1 │     1      4.0
   2 │     2      5.2
```
"""
function sheetcols_to_float!(df; blank_to::Real)
    for colname in names(df)
        col = df[colname]
        if eltype(col) === String || eltype(col) === Any
            nonblanks = filter(cell -> cell !== " ", col)
            if eltype(nonblanks) === String
                replace!(col, " " => "$(blank_to)")
                df[colname] = parse.(Float64, col)
            else
                replace!(col, " " => blank_to)
                df[colname] = convert(Vector{Float64}, col)
            end
        end
    end

    return df
end

"""
    replace_missing!(df, new_value) -> AbstractDataFrame

Replace all occurrences of `missing` with a number `new_value` in a data frame `df`.
"""
function replace_missing!(df, new_value)
    for colname in names(df)
        col = df[colname]
        nonmissing = nonmissingtype(eltype(col))
        if nonmissing <: Real
            replace!(col, missing => new_value)
        end
    end

    return df
end

"""
    print_skewness_kurtosis(df::AbstractDataFrame; dependent::Bool=false)
    print_skewness_kurtosis(gd)

Print the skewness and kurtosis (statistic, standard error, ratio) of each column in `df`
or grouped data frame `gd`.

If `dependent` is `true`, then the skewness and kurtosis of the difference between the
columns of `df` will be printed.
"""
function print_skewness_kurtosis(df::AbstractDataFrame; dependent::Bool=false)
    if dependent
        if ncol(df) === 2
            _print_dependent(df)
        else
            println("""
                There can only be two dependent variables to measure skewness and kurtosis.
                """
            )
        end
    else
        _print_independent(df)
    end
end

function print_skewness_kurtosis(gd)
    gd_new = combine(gd, valuecols(gd) .=> x -> [skewness(x), kurtosis(x),]; ungroup=false)
    for group in keys(gd_new)
        for var in valuecols(gd_new)
            tagged = _describe_var(gd_new, group, var)
            _print_summary(tagged)
        end
    end
end

function _print_dependent(df)
    difference = _diff_pair(df)
    tagged = _describe_var(difference.first, difference.second)
    _print_summary(tagged)
end

function _describe_var(colname, col)
    tag = (
        name=colname,
    )
    skew = skewness(col)
    kurt = kurtosis(col)
    return merge(tag, skew, kurt)
end

function _describe_var(gd, group_column, value_column)
    varname = chop(string(value_column), tail=length("_function"))
    grouping = chop(string(group_column), head=length("GroupKey: ("))
    tag = (
        name="$varname ($grouping)",
    )
    skews_and_kurts = gd[group_column][value_column]
    return merge(tag, skews_and_kurts...)
end

function _print_summary(factor::NamedTuple)
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

function _print_independent(df)
    for colname in names(df)
        tagged = _describe_var(colname, df[colname])
        _print_summary(tagged)
    end
end

"""
    print_findings(findings)

Print the normal skewness and kurtosis in a dictionary `findings`.

`findings` has the following key-value pairs:
- `"normalized"` => a dictionary of a collection value of named tuples containing
    skewnesses and kurtoses for each normal transformation key
- `"normal gdf"` => a transformed data frame or list of grouped data frames whose data are
    normal
- `"nonnormal gdf"` => a transformed data frame or list of grouped data frames whose data
    are nonnormal
"""
function print_findings(findings)
    normal = findings["normalized"]
    for (transformation, altered) in normal
        println("APPLIED: $transformation")
        _print_summary.(altered)
        println("\n")
    end

    if  isempty(findings["nonnormal gdf"]) && isempty(findings["normal gdf"])
        println("No transformations applied, so unable to normalize.")
    elseif isempty(normal)
        println("Transformations applied, but no normal results.")
    end
end

"""
    normal_to_csv(path, findings; dependent::Bool=false)

Export normal data from a dictionary `findings` to a csv file in a string `path`.

If `path` already exists (i.e., a file has the same name), then the file will be
overwritten. Otherwise, a new file will be created.

If `dependent` is `true`, then a column of zeros will also be created in the csv for
dependent testing of data with only one group and two dependent variables.

If there are any `NaN`, they will be replaced with `missing` in the csv.
"""
function normal_to_csv(path, findings; dependent::Bool=false)
    if !endswith(path, ".csv")
        println("Only a filename ending in .csv can be used.")
        return nothing
    end
    normal_data = findings["normal gdf"]
    if isempty(normal_data)
        println("There is no normal data to export.")
        return nothing
    end

    if isa(normal_data, AbstractDataFrame)
        df_normal = normal_data
        if dependent
            df_zero = DataFrame(["for_dependent_test" => zeros(nrow(df_normal))])
            df_normal = hcat(df_normal, df_zero)
        end
    else
        df_normal = _flatten_grouped_dataframes(normal_data)
    end
    CSV.write(path, df_normal, transform=(col, val) -> _nan_to(missing, val))
    return nothing
end

function _flatten_grouped_dataframes(gdfs)
    df_new = DataFrame()
    df_new = _store_groupcols(df_new, gdfs)
    df_new = _store_valuecols(df_new, gdfs)
    return df_new
end

function _store_groupcols(df, gdfs)
    gd_example = gdfs[1]
    df_example = DataFrame(gd_example)
    for group in groupcols(gd_example)
        df = hcat(df, df_example[[group]])
    end

    return df
end

function _store_valuecols(df, gdfs)
    for grouped in gdfs
        df_convert = DataFrame(grouped)
        for var in valuecols(grouped)
            df = hcat(df, df_convert[[var]])
        end
    end

    return df
end

function _nan_to(new_value, original)
    if original === NaN
        return new_value
    else
        return original
    end
end

end