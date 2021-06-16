module Normalize

using Base: String
using CSV
using DataFrames
using ExcelFiles
using OdsIO
using SciPy
using StatFiles

export apply, normalize, record, record_all
export are_negative_skews, are_positive_skews, is_negative_skew, is_positive_skew
export are_normal, is_normal
export get_negative_skew_transformations, get_positive_skew_transformations
export get_skew_transformations, get_stretch_skew_transformations
export kurtosis, kurtosis_error, kurtosis_stat, kurtosis_variance
export print_findings, print_skewness_kurtosis
export skewness, skewness_error, skewness_stat, skewness_variance
export replace_missing!, sheetcol_to_float!
export normal_to_csv, tabular_to_dataframe

"""
    normalize(
        gdf; normal_ratio::Real=2, dependent::Bool=false, marker::AbstractString="__"
    ) -> AbstractDict

Normalize a (grouped) data frame `gdf` for its skewness and kurtosis ratios to be within
the range of ±`normal_ratio`.

If `dependent` is `true`, then the difference between the columns of `gdf` will be
normalized.

Functions will be separately applied to `gdf`, and the column names of the transformed
(grouped) data frame will be suffixed with a string `marker` and the applied functions.

A dictionary is returned with the following key-value pairs:
- `"normalized"` => a dictionary of a collection value of named tuples containing
    skewnesses and kurtoses for each normal transformation key
- `"normal gdf"` => a list of transformed (grouped) data frames whose data are normal
- `"nonnormal gdf"` => a list of transformed (grouped) data frames whose data are nonnormal

!!! compat "Julia 1.5"
    `normalize` uses `mergewith!`, which requires at least Julia 1.5.

!!! compat "Julia 1.5"
    `normalize` uses `contains`, which requires at least Julia 1.5.

# Examples
```jldoctest
julia> df = DataFrame(a=[-0.5, 2.47, 2.54, 2.91, 3.13]);

julia> results = normalize(df);

julia> keys(results["normalized"])
KeySet for a Dict{Any, Any} with 5 entries. Keys:
  "cube"
  "square"
  "antilog"
  "reflect and invert"
  "reflect and log base 10"

julia> length(results["normal gdf"])
5

julia> results["normal gdf"][1]
5×1 DataFrame
 Row │ a__square
     │ Float64
─────┼───────────
   1 │    0.25
   2 │    6.1009
   3 │    6.4516
   4 │    8.4681
   5 │    9.7969

julia> pairs(results["normalized"]["square"][1])
pairs(::NamedTuple) with 7 entries:
  :name           => "a"
  :skewness_stat  => -1.315
  :skewness_error => 0.913
  :skewness_ratio => -1.44
  :kurtosis_stat  => 2.149
  :kurtosis_error => 2.0
  :kurtosis_ratio => 1.074

julia> typeof(results["nonnormal gdf"][1])
DataFrame

julia> df2 = DataFrame(b=[6.24, 6.34, 6.39, 2.6, 8.35], c=[5.5, 5.8, 5, 5, 7.5]);

julia> results2 = normalize(df2, dependent=true);

julia> names(results2["normal gdf"][1])
1-element Vector{String}:
 "c_minus_b__add_n_logit"

julia> df3 = DataFrame(g=[1,2,1,2,1,2,1,2], kel=[5.5,5.8,5,5,7.5,1.08,0.94,0.56]);

julia> results3 = normalize(groupby(df3, :g));

julia> results3["nonnormal gdf"]
1-element Vector{Any}:
 GroupedDataFrame with 2 groups based on key: g
First Group (4 rows): g = 1
 Row │ g      kel__add_n_logit
     │ Int64  Float64
─────┼─────────────────────────
   1 │     1         0.0829742
   2 │     1         0.0917704
   3 │     1         0.0599979
   4 │     1         0.796793
⋮
Last Group (4 rows): g = 2
 Row │ g      kel__add_n_logit
     │ Int64  Float64
─────┼─────────────────────────
   1 │     2         0.078464
   2 │     2         0.0917704
   3 │     2         0.605338
   4 │     2         0.629731

julia> names(results3["normal gdf"][1])
2-element Vector{String}:
 "g"
 "kel__square_root"

julia> for nt in results3["normalized"]["square root"]
           println(nt[:name])
       end
kel (g = 1)
kel (g = 2)
```
"""
function normalize(
    gdf; normal_ratio::Real=2, dependent::Bool=false, marker::AbstractString="__"
)
    try
        if dependent
            if _has_only_one_group_and_two_vars(gdf)
                difference = _diff_pair(gdf)
                gdf = DataFrame([difference])
            else
                throw(
                    ArgumentError(
                        "One group with only two dependent variables can be normalized."
                    )
                )
            end
        end
        transformations = get_skew_transformations(gdf; normal_ratio)
        return record_all(gdf, transformations; normal_ratio, marker)
    catch e
        println(e)
        return Dict(
            "normalized" => Dict(),
            "normal gdf" => [],
            "nonnormal gdf" => [],
        )
    end
end

function _has_only_one_group_and_two_vars(gdf)
    return gdf isa AbstractDataFrame && ncol(gdf) === 2
end

function _diff_pair(df)
    var = names(df)[1]
    var2 = names(df)[2]
    return "$(var2)_minus_$(var)" => df[!, var2] .- df[!, var]
end

"""
    get_skew_transformations(gdf; normal_ratio::Real=2) -> AbstractDict

Return functions to apply to a (grouped) data frame `gdf` if its skewness and kurtosis
ratios are not within the range of ±`normal_ratio`.

The returned dictionary has the following key-value pairs:
- `"one arg"` => a collection of functions that require one argument
- `"two args"` => a collection of tuples containing functions that require two arguments,
    and their second arguments

!!! compat "Julia 1.5"
    `get_skew_transformations` uses `mergewith!`, which requires at least Julia 1.5.

Throw `ArgumentError` if there is a column that does not have more than 3 non-`NaN` values.

See also: [`get_negative_skew_transformations`](@ref),
[`get_positive_skew_transformations`](@ref), [`get_stretch_skew_transformations`](@ref)

# Examples
```jldoctest
julia> df = DataFrame(a=[1.074, 0.058, 2.321, 0.466, 0.226, 0.171, 0.439]);

julia> transformations = get_skew_transformations(df);

julia> transformations["one arg"]
3-element Vector{Function}:
 square_root (generic function with 1 method)
 add_n_logit (generic function with 1 method)
 logit (generic function with 1 method)

julia> transformations["two args"]
5-element Vector{Any}:
 (Normalize.square_root_n_add_n_invert, 0.058)
 (Normalize.add_n_invert, 0.058)
 (Normalize.add_n_log_base_10, 0.058)
 (Normalize.add_n_natural_log, 0.058)
 (Normalize.square_n_add_n_invert, 0.058)
```
"""
function get_skew_transformations(gdf; normal_ratio::Real=2)
    transformations = Dict(
        "one arg" => Function[],
        "two args" => Any[],
    )
    skews_and_kurts = _get_skewness_kurtosis(gdf)
    df_new = DataFrame(gdf)
    if _are_nonnormal(skews_and_kurts; normal_ratio)
        if are_positive_skews(skews_and_kurts)
            positive = get_positive_skew_transformations(df_new)
            mergewith!(append!, transformations, positive)
        elseif are_negative_skews(skews_and_kurts)
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
    gd_new = combine(gd, valuecols(gd) .=> x -> [skewness(x), kurtosis(x),]; ungroup=false)
    return [
        merge(gd_new[group][!, var]...)
        for group in keys(gd_new)
            for var in valuecols(gd_new)
    ]
end

"""
    skewness(col, round_to::Integer=3) -> NamedTuple

Compute the skewness of a collection `col`, rounded to `round_to` digits.

The returned named tuple has the following fields:
- `skewness_stat`: skewness statistic
- `skewness_error`: skewness standard error
- `skewness_ratio`: skewness ratio

Skewness statistic is corrected for bias with the adjusted Fisher-Pearson standardized
moment coefficient.

Throw `ArgumentError` if `col` does not have more than 2 non-`NaN` values.

Source: [`scipy.stats.skew`](https://docs.scipy.org/doc/scipy/reference/generated/scipy.stats.skew.html)

See also: [`skewness_stat`](@ref), [`skewness_error`](@ref)

# Examples
```jldoctest
julia> skewness([1.072, 0.486, 0.133, 0.223, 1.139])
(skewness_stat = 0.306, skewness_error = 0.913, skewness_ratio = 0.335)
```
"""
function skewness(col, round_to::Integer=3)
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
    skewness_stat(col, round_to::Integer=3)

Compute the skewness statistic of a collection `col`, rounded to `round_to` digits.

Skewness statistic is corrected for bias with the adjusted Fisher-Pearson standardized
moment coefficient.

Throw `ArgumentError` if `col` is empty or contains only `NaN`s.

Source: [`scipy.stats.skew`](https://docs.scipy.org/doc/scipy/reference/generated/scipy.stats.skew.html)

See also: [`skewness`](@ref)

# Examples
```jldoctest
julia> skewness_stat([1.072, 0.486, 0.133, 0.223, 1.139])
0.306
```
"""
function skewness_stat(col, round_to::Integer=3)
    if iszero(_length_with_nan_excluded(col))
        throw(ArgumentError("col cannot be empty nor contain only NaNs."))
    elseif any(isnan, col)
        return round(
            SciPy.stats.skew(col, bias=false, nan_policy="omit")[1]; digits=round_to
        )
    else
        return round(SciPy.stats.skew(col, bias=false, nan_policy="omit"); digits=round_to)
    end
end

function _length_with_nan_excluded(col)
    omission = count(isnan.(col))
    return length(col) - omission
end

"""
    skewness_error(col, round_to::Integer=3)

Compute the skewness error of a collection `col`, rounded to `round_to` digits.

Throw `ArgumentError` if the number of items excluding `NaN` values is 2.

See also: [`skewness`](@ref)

# Examples
```jldoctest
julia> skewness_error([1.072, 0.486, 0.133, 0.223, 1.139])
0.913
```
"""
function skewness_error(col, round_to::Integer=3)
    variance = skewness_variance(col)
    return round(√variance; digits=round_to)
end

"""
    skewness_variance(col)

Compute the skewness variance of a collection `col`.

It is calculated from the formula:

```math
\\frac{6N(N-1)}{(N-2)(N+1)(N+3)}
```

Throw `ArgumentError` if the number of items excluding `NaN` values is 2.

# Examples
```jldoctest
julia> skewness_variance([1.072, 0.486, 0.133, 0.223, 1.139])
0.8333333333333333
```
"""
function skewness_variance(col)
    N = _length_with_nan_excluded(col)
    if N === 2
        throw(ArgumentError("The number of items excluding `NaN` values cannot be 2."))
    else
        return 6 * N * (N - 1) * invert((N - 2) * (N + 1) * (N + 3))
    end
end

"""
    invert(x::Real)

Compute ``\\frac{1}{x}``.

Throw `DivideError` if `x` is 0.
"""
function invert(x::Real)
    if iszero(x)
        throw(DivideError())
    else
        return inv(x)
    end
end

"""
    kurtosis(col, round_to::Integer=3) -> NamedTuple

Compute the kurtosis of a collection `col`, rounded to `round_to` digits.

The returned named tuple has the following fields:
- `kurtosis_stat`: kurtosis statistic
- `kurtosis_error`: kurtosis standard error
- `kurtosis_ratio`: kurtosis ratio

Kurtosis statistic is corrected for bias with k statistics, and Fisher's definition is
used.

Throw `ArgumentError` if `col` does not have more than 3 non-`NaN` values.

Source: [`scipy.stats.kurtosis`](https://docs.scipy.org/doc/scipy/reference/generated/scipy.stats.kurtosis.html)

See also: [`kurtosis_stat`](@ref), [`kurtosis_error`](@ref)

# Examples
```jldoctest
julia> kurtosis([1.072, 0.486, 0.133, 0.223, 1.139])
(kurtosis_stat = -2.952, kurtosis_error = 2.0, kurtosis_ratio = -1.476)
```
"""
function kurtosis(col, round_to::Integer=3)
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
    kurtosis_stat(col, round_to::Integer=3)

Compute the kurtosis statistic of a collection `col`, rounded to `round_to` digits.

Kurtosis statistic is corrected for bias with k statistics, and Fisher's definition is
used.

Throw `ArgumentError` if `col` is empty or contains only `NaN`s.

Source: [`scipy.stats.kurtosis`](https://docs.scipy.org/doc/scipy/reference/generated/scipy.stats.kurtosis.html)

See also: [`kurtosis`](@ref)

# Examples
```jldoctest
julia> kurtosis_stat([1.072, 0.486, 0.133, 0.223, 1.139])
-2.952
```
"""
function kurtosis_stat(col, round_to::Integer=3)
    if iszero(_length_with_nan_excluded(col))
        throw(ArgumentError("col cannot be empty nor contain only NaNs."))
    elseif any(isnan, col)
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
    kurtosis_error(col, round_to::Integer=3)

Compute the kurtosis error of a collection `col`, rounded to `round_to` digits.

Throw `ArgumentError` if the number of items excluding `NaN` values is 2 or 3.

See also: [`kurtosis`](@ref)

# Examples
```jldoctest
julia> kurtosis_error([1.072, 0.486, 0.133, 0.223, 1.139])
2.0
```
"""
function kurtosis_error(col, round_to::Integer=3)
    variance = kurtosis_variance(col)
    return round(√variance; digits=round_to)
end

"""
    kurtosis_variance(col)

Compute the kurtosis variance of a collection `col`.

It is calculated from the formula:

```math
\\frac{4(N^2-1)*skewnessVariance}{(N-3)(N+5)}
```

Throw `ArgumentError` if the number of items excluding `NaN` values is 2 or 3.

# Examples
```jldoctest
julia> kurtosis_variance([1.072, 0.486, 0.133, 0.223, 1.139])
4.0
```
"""
function kurtosis_variance(col)
    N = _length_with_nan_excluded(col)
    if N === 3
        throw(ArgumentError("The number of items excluding `NaN` values cannot be 3."))
    end
    skewness_var = skewness_variance(col)
    return 4 * (N^2 - 1) * skewness_var * invert((N - 3) * (N + 5))
end

function _are_nonnormal(skew_kurt_ratios; normal_ratio::Real=2)
    return !are_normal(skew_kurt_ratios; normal_ratio)
end

"""
    are_normal(skew_kurt_ratios; normal_ratio::Real=2) -> Bool

Return `true` if ratios are within the range of ±`normal_ratio` in collections
`skew_kurt_ratios` containing named tuples, and `false` otherwise.

The named tuples in `skew_kurt_ratios` have the following fields:
- `skewness_ratio`: skewness ratio
- `kurtosis_ratio`: kurtosis ratio

# Examples
```jldoctest
julia> sk = [
           (skewness_ratio=1.75, kurtosis_ratio=0.5),
           (skewness_ratio=0.3, kurtosis_ratio=0.6),
       ];

julia> are_normal(sk)
true

julia> sk2 = (
           (skewness_ratio=-3.14, kurtosis_ratio=1.11),
           (skewness_ratio=1.38, kurtosis_ratio=0.66),
       );

julia> are_normal(sk2)
false
```
"""
function are_normal(skew_kurt_ratios; normal_ratio::Real=2)
    return all(
        i -> is_normal(i[:skewness_ratio], i[:kurtosis_ratio]; normal_ratio),
        skew_kurt_ratios,
    )
end

"""
    is_normal(skewness_ratio::Real, kurtosis_ratio::Real; normal_ratio::Real=2) -> Bool

Return `true` if `skewness_ratio` and `kurtosis_ratio` are within the range of
±`normal_ratio`, and `false` otherwise.

# Examples
```jldoctest
julia> is_normal(1.38, 0.66)
true

julia> is_normal(-3.14, 1.11)
false
```
"""
function is_normal(skewness_ratio::Real, kurtosis_ratio::Real; normal_ratio::Real=2)
    return (-normal_ratio ≤ skewness_ratio ≤ normal_ratio) &&
        (-normal_ratio ≤ kurtosis_ratio ≤ normal_ratio)
end

"""
    are_positive_skews(skewnesses) -> Bool

Return `true` if there are any positive, non-zero ratios in a collection `skewnesses`
containing named tuples, and `false` otherwise.

`skewnesses` has named tuples with a field `skewness_ratio`.

# Examples
```jldoctest
julia> are_positive_skews([(skewness_ratio=0.513,), (skewness_ratio=-2.049,)])
true

julia> are_positive_skews([(skewness_ratio=-1.452,), (skewness_ratio=0,)])
false
```
"""
function are_positive_skews(skewnesses)
    return _contains(is_positive_skew, skewnesses)
end

function _contains(func, skewnesses)
    return any(i -> func(i[:skewness_ratio]), skewnesses)
end

function _contains(func, df::AbstractDataFrame)
    filtered_cols = _filter_cols(func, df)
    filtered_lengths = length.(filtered_cols)
    return sum(filtered_lengths) > 0
end

function _contains(func, df::AbstractDataFrame, func_other_arg)
    filtered_cols = _filter_cols(func, df, func_other_arg)
    filtered_lengths = length.(filtered_cols)
    return sum(filtered_lengths) > 0
end

function _filter_cols(func, df)
    return filter.(func, eachcol(df))
end

function _filter_cols(func, df, func_other_arg)
    return filter.(x -> func(x, func_other_arg), eachcol(df))
end

"""
    is_positive_skew(skewness_ratio::Real) -> Bool

Return `true` if `skewness_ratio` is positive and not zero, and `false` otherwise.

# Examples
```jldoctest
julia> is_positive_skew(1.25)
true

julia> is_positive_skew(-1.25)
false

julia> is_positive_skew(0)
false
```
"""
function is_positive_skew(skewness_ratio::Real)
    return skewness_ratio > 0
end

"""
    get_positive_skew_transformations(df) -> AbstractDict

Return functions to transform positive skew for a data frame `df` based on its minimum
value.

The returned dictionary has the following key-value pairs:
- `"one arg"` => a collection of functions that require one argument
- `"two args"` => a collection of tuples containing functions that require two arguments,
    and their second arguments

# Examples
```jldoctest
julia> df = DataFrame(a=[1.072, 0.486, 0.133, 0.223, 1.139]);

julia> positive = get_positive_skew_transformations(df);

julia> positive["one arg"]
1-element Vector{Function}:
 square_root (generic function with 1 method)

julia> positive["two args"]
5-element Vector{Tuple{Function, Float64}}:
 (Normalize.square_root_n_add_n_invert, 0.133)
 (Normalize.add_n_invert, 0.133)
 (Normalize.add_n_log_base_10, 0.133)
 (Normalize.add_n_natural_log, 0.133)
 (Normalize.square_n_add_n_invert, 0.133)
```
"""
function get_positive_skew_transformations(df)
    min = _extrema(df)[1]
    if min < 0
        positive = Dict(
            "one arg" => Function[],
            "two args" => Any[
                add_n_square_root,
                add_n_square_root_n_invert,
                add_n_invert,
                add_n_log_base_10,
                add_n_natural_log,
                square_n_add_n_invert,
            ],
        )
        if _contains(_cannot_square_n_add_n_invert, df, min)
            pop!(positive["two args"])
        end
    elseif 0 ≤ min < 1
        positive = Dict(
            "one arg" => Function[
                square_root,
            ],
            "two args" => Any[
                square_root_n_add_n_invert,
                add_n_invert,
                add_n_log_base_10,
                add_n_natural_log,
                square_n_add_n_invert,
            ],
        )
    else
        positive = Dict(
            "one arg" => Function[
                square_root,
                square_root_n_invert,
                invert,
                square_n_invert,
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
# (add_n_square_root, add_n_square_root_n_invert, add_n_invert,
# add_n_log_base_10, add_n_natural_log, square_n_add_n_invert)

"""
    add_n_square_root(x::Real, min::Real)

Compute ``\\sqrt{x + 1 - min}``.

Throw `ArgumentError` if `min > x`.
"""
function add_n_square_root(x::Real, min::Real)
    if min > x
        throw(ArgumentError("min must be smaller."))
    else
        return √(x + 1 - min)
    end
end

"""
    add_n_square_root_n_invert(x::Real, min::Real)

Compute ``\\frac{1}{\\sqrt{x + 1 - min}}``.

Throw `ArgumentError` if `min > x`.
"""
function add_n_square_root_n_invert(x::Real, min::Real)
    return invert(add_n_square_root(x, min))
end

"""
    add_n_invert(x::Real, min::Real)

Compute ``\\frac{1}{x + 1 - min}``

Throw `ArgumentError` if `min > x`.
"""
function add_n_invert(x::Real, min::Real)
    if min > x
        throw(ArgumentError("min must be smaller."))
    else
        return invert(x + 1 - min)
    end
end

"""
    add_n_log_base_10(x::Real, min::Real)

Compute ``\\log_{10}(x + 1 - min)``.

Throw `ArgumentError` if `min > x`.
"""
function add_n_log_base_10(x::Real, min::Real)
    if min > x
        throw(ArgumentError("min must be smaller."))
    else
        return log_base_10(x + 1 - min)
    end
end

"""
    log_base_10(x::Real)

Compute the logarithm of `x` to base 10, in other words ``\\log_{10}(x)``.

Throw `DomainError` if `x ≤ 0`.
"""
function log_base_10(x::Real)
    if iszero(x)
        throw(DomainError(x))
    else
        return log10(x)
    end
end

"""
    add_n_natural_log(x::Real, min::Real)

Compute ``\\ln(x + 1 - min)``.

Throw `ArgumentError` if `min > x`.
"""
function add_n_natural_log(x::Real, min::Real)
    if min > x
        throw(ArgumentError("min must be smaller."))
    else
        return natural_log(x + 1 - min)
    end
end

"""
    natural_log(x::Real)

Compute the natural logarithm of `x`, in other words ``\\ln(x)``.

Throw `DomainError` if `x ≤ 0`.
"""
function natural_log(x::Real)
    if iszero(x)
        throw(DomainError(x))
    else
        return log(x)
    end
end

"""
    square_n_add_n_invert(x::Real, min::Real)

Compute ``\\frac{1}{x^2 + 1 - min^2}``.

Throw `DivideError` if ``x^2 + 1 - min^2`` is 0, and `ArgumentError` if `min > x`.
"""
function square_n_add_n_invert(x::Real, min::Real)
    if min > x
        throw(ArgumentError("min must be smaller."))
    else
        return invert(x^2 + 1 - min^2)
    end
end

function _cannot_square_n_add_n_invert(x::Real, min::Real)
    return x^2 + 1 - min^2 === 0
end

# 0 ≤ min < 1 
# (square_root, square_root_n_add_n_invert, add_n_invert, add_n_log_base_10,
# add_n_natural_log, square_n_add_n_invert)

"""
    square_root(x::Real)

Compute ``\\sqrt{x}``.

Throw `DomainError` if `x` is negative.
"""
function square_root(x::Real)
    return √x
end

"""
    square_root_n_add_n_invert(x::Real, min::Real)

Compute ``\\frac{1}{\\sqrt{x} + 1 - \\sqrt{min}}``.

Throw `DomainError` if `x` or `min` is negative, and `ArgumentError` if `min > x`.
"""
function square_root_n_add_n_invert(x::Real, min::Real)
    if min > x
        throw(ArgumentError("min must be smaller."))
    else
        return invert(√x + 1 - √min)
    end
end

# min ≥ 1 
# (square_root, square_root_n_invert, invert, square_n_invert, log_base_10,
# natural_log, invert)

"""
    square_root_n_invert(x::Real)

Compute ``\\frac{1}{\\sqrt{x}}``.

Throw `DomainError` if `x` is negative, and `DivideError` if `x` is 0.
"""
function square_root_n_invert(x::Real)
    return invert(√x)
end

"""
    square_n_invert(x::Real)

Compute ``\\frac{1}{x^2}``.

Throw `DivideError` if `x` is 0.
"""
function square_n_invert(x::Real)
    return invert(x^2)
end

"""
    are_negative_skews(skewnesses) -> Bool

Return `true` if there are any negative ratios in a collection `skewnesses` containing
named tuples, and `false` otherwise.

`skewnesses` has named tuples with a field `skewness_ratio`.

# Examples
```jldoctest
julia> are_negative_skews([(skewness_ratio=0.513,), (skewness_ratio=-2.049,)])
true

julia> are_negative_skews([(skewness_ratio=0.0,), (skewness_ratio=0.978,)])
false
```
"""
function are_negative_skews(skewnesses)
    return _contains(is_negative_skew, skewnesses)
end

"""
    is_negative_skew(skewness_ratio::Real) -> Bool

Return `true` if `skewness_ratio` is negative, and `false` otherwise.

# Examples
```jldoctest
julia> is_negative_skew(1.25)
false

julia> is_negative_skew(-1.25)
true

julia> is_negative_skew(0)
false
```
"""
function is_negative_skew(skewness_ratio::Real)
    return skewness_ratio < 0
end

"""
    get_negative_skew_transformations(df) -> AbstractDict

Return functions to transform negative skew for a data frame `df` based on its maximum
value.

The returned dictionary has the following key-value pairs:
- `"one arg"` => a collection of functions that require one argument
- `"two args"` => a collection of tuples containing functions that require two arguments,
    and their second arguments

# Examples
```jldoctest
julia> df = DataFrame(a=[1.072, 0.486, 0.133, 0.223, 1.139]);

julia> negative = get_negative_skew_transformations(df);

julia> negative["one arg"]
3-element Vector{Function}:
 square (generic function with 1 method)
 cube (generic function with 1 method)
 antilog (generic function with 1 method)

julia> negative["two args"]
3-element Vector{Tuple{Function, Float64}}:
 (Normalize.reflect_n_square_root, 1.139)
 (Normalize.reflect_n_log_base_10, 1.139)
 (Normalize.reflect_n_invert, 1.139)
```
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
            reflect_n_square_root,
            reflect_n_log_base_10,
            reflect_n_invert,
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
    reflect_n_square_root(x::Real, max::Real)

Compute ``\\sqrt{max + 1 - x}``.

Throw `ArgumentError` if `max < x`.
"""
function reflect_n_square_root(x::Real, max::Real)
    if max < x
        throw(ArgumentError("max must be greater."))
    else
        return √(max + 1 - x)
    end
end

"""
    reflect_n_log_base_10(x::Real, max::Real)

Compute ``\\log_{10}(max + 1 - x)``.

Throw `ArgumentError` if `max < x`.
"""
function reflect_n_log_base_10(x::Real, max::Real)
    if max < x
        throw(ArgumentError("max must be greater."))
    else
        return log_base_10(max + 1 - x)
    end
end

"""
    reflect_n_invert(x::Real, max::Real)

Compute ``\\frac{1}{max + 1 - x}``.

Throw `ArgumentError` if `max < x`.
"""
function reflect_n_invert(x::Real, max::Real)
    if max < x
        throw(ArgumentError("max must be greater."))
    else
        return invert(max + 1 - x)
    end
end

"""
    get_stretch_skew_transformations(df) -> AbstractDict

Return functions to stretch skew for a data frame `df`.

The returned dictionary has the following key-value pairs:
- `"one arg"` => a collection of functions that require one argument
- `"two args"` => a collection of functions that require two arguments

# Examples
```jldoctest
julia> df = DataFrame(a=[1.072, 0.486, 0.133, 0.223, 1.139]);

julia> get_stretch_skew_transformations(df)
Dict{String, Vector{T} where T} with 2 entries:
  "one arg"  => Function[add_n_logit, logit]
  "two args" => Any[]
```
"""
function get_stretch_skew_transformations(df)
    stretch = Dict(
        "one arg" => Function[
            add_n_logit,
            logit,
        ],
        "two args" => Any[],
    )
    if _contains(_cannot_add_n_logit, df)
        popfirst!(stretch["one arg"])
    end

    if _contains(_cannot_logit, df)
        pop!(stretch["one arg"])
    end

    return stretch
end

"""
    add_n_logit(x::Real)

Compute the logit of `x + 0.25` in base 10, in other words
``\\log_{10}|\\frac{x + 0.25}{1 - (x + 0.25)}|``.

Throw `DomainError` if `x` is -0.25, and `DivideError` if `x` is 0.75.
"""
function add_n_logit(x::Real)
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

function _cannot_add_n_logit(x::Real)
    return x === -0.25 || x === 0.75
end

function _cannot_logit(x::Real)
    return iszero(x) || isone(x)
end

"""
    record_all(
        gdf, transform_series; normal_ratio::Real=2, marker::AbstractString="__"
    ) -> AbstractDict

Compute the skewness and kurtosis for each function in a dictionary `transform_series`
applied to a (grouped) data frame `gdf`, and return a dictionary of transformed (grouped)
data frames, and of any resulting skewness and kurtosis whose ratios are within the range
of ±`normal_ratio`.

`transform_series` has the following key-value pairs:
- `"one arg"` => a collection of functions that require one argument
- `"two args"` => a collection of tuples containing functions that require two arguments,
    and their second arguments

Functions will be separately applied to `gdf`, and the column names of the transformed
(grouped) data frame will be suffixed with a string `marker` and the applied functions.

A dictionary is returned with the following key-value pairs:
- `"normalized"` => a dictionary of a collection value of named tuples containing
    skewnesses and kurtoses for each normal transformation key
- `"normal gdf"` => a list of transformed (grouped) data frames whose data are normal
- `"nonnormal gdf"` => a list of transformed (grouped) data frames whose data are nonnormal

!!! compat "Julia 1.5"
    `record_all` uses `contains`, which requires at least Julia 1.5.

Throw `ArgumentError` if there is a column that does not have more than 3 non-`NaN` values,
or if `marker` is shorter than 2 characters, or does not only consist of punctuations; and
`OverflowError` if transforming `df` or `gd` results in a value too large to represent.

See also: [`record`](@ref)

# Examples
```jldoctest
julia> df = DataFrame(a=[-0.5, 2.47, 2.54, 2.91, 3.13]);

julia> series = Dict(
           "one arg" => Function[Normalize.antilog],
           "two args" => [(Normalize.reflect_n_square_root, 3.13)],
       );

julia> records = record_all(df, series);

julia> records["normal gdf"]
1-element Vector{Any}:
 5×1 DataFrame
 Row │ a__antilog
     │ Float64
─────┼─────────────
   1 │    0.316228
   2 │  295.121
   3 │  346.737
   4 │  812.831
   5 │ 1348.96

julia> pairs(records["normalized"]["antilog"][1])
pairs(::NamedTuple) with 7 entries:
  :name           => "a"
  :skewness_stat  => 0.852
  :skewness_error => 0.913
  :skewness_ratio => 0.933
  :kurtosis_stat  => -0.029
  :kurtosis_error => 2.0
  :kurtosis_ratio => -0.014

julia> records["nonnormal gdf"]
1-element Vector{Any}:
 5×1 DataFrame
 Row │ a__reflect_n_square_root
     │ Float64
─────┼──────────────────────────
   1 │                  2.15174
   2 │                  1.28841
   3 │                  1.26095
   4 │                  1.10454
   5 │                  1.0

julia> df2 = DataFrame(g=[1,2,1,2,1,2,1,2], kel=[5.5,5.8,5,5,7.5,1.08,0.94,0.56]);

julia> series2 = Dict(
           "two args" => [(Normalize.reflect_n_log_base_10, 7.5)],
           "one arg" => [],
       );

julia> records2 = record_all(groupby(df2, :g), series2);

julia> records2["normal gdf"][1]
GroupedDataFrame with 2 groups based on key: g
First Group (4 rows): g = 1
 Row │ g      kel__reflect_n_log_base_10
     │ Int64  Float64
─────┼───────────────────────────────────
   1 │     1                    0.477121
   2 │     1                    0.544068
   3 │     1                    0.0
   4 │     1                    0.878522
⋮
Last Group (4 rows): g = 2
 Row │ g      kel__reflect_n_log_base_10
     │ Int64  Float64
─────┼───────────────────────────────────
   1 │     2                    0.431364
   2 │     2                    0.544068
   3 │     2                    0.870404
   4 │     2                    0.899821

julia> pairs(records2["normalized"]["reflect and log base 10"][1])
pairs(::NamedTuple) with 7 entries:
  :name           => "kel (g = 1)"
  :skewness_stat  => -0.577
  :skewness_error => 1.014
  :skewness_ratio => -0.569
  :kurtosis_stat  => 1.523
  :kurtosis_error => 2.619
  :kurtosis_ratio => 0.582

julia> pairs(records2["normalized"]["reflect and log base 10"][2])
pairs(::NamedTuple) with 7 entries:
  :name           => "kel (g = 2)"
  :skewness_stat  => -0.183
  :skewness_error => 1.014
  :skewness_ratio => -0.18
  :kurtosis_stat  => -4.806
  :kurtosis_error => 2.619
  :kurtosis_ratio => -1.835

julia> records2["nonnormal gdf"]
Any[]
```
"""
function record_all(
    gdf, transform_series; normal_ratio::Real=2, marker::AbstractString="__"
)
    first_record = record(gdf, transform_series["one arg"]; normal_ratio, marker)
    other_record = record(gdf, transform_series["two args"]; normal_ratio, marker)
    return _merge_results!(first_record, other_record)
end

"""
    record(
        gdf, transformations::AbstractVector{Function};
        normal_ratio::Real=2, marker::AbstractString="__"
    ) -> AbstractDict
    record(
        gdf, transformations; normal_ratio::Real=2, marker::AbstractString="__"
    ) -> AbstractDict

Compute the skewness and kurtosis for each function in `transformations` applied to a
(grouped) data frame `gdf`, and return a dictionary of transformed (grouped) data frames,
and of any resulting skewness and kurtosis whose ratios are within the range of
±`normal_ratio`.

If `transformations` is not of type `AbstractVector{Function}`, then it is a collection of
function and second argument pairings.

Functions will be separately applied to `gdf`, and the column names of the transformed
(grouped) data frame will be suffixed with a string `marker` and the applied functions.

A dictionary is returned with the following key-value pairs:
- `"normalized"` => a dictionary of a collection value of named tuples containing
    skewnesses and kurtoses for each normal transformation key
- `"normal gdf"` => a list of transformed (grouped) data frames whose data are normal
- `"nonnormal gdf"` => a list of transformed (grouped) data frames whose data are nonnormal

!!! compat "Julia 1.5"
    `record` uses `contains`, which requires at least Julia 1.5.

Throw `ArgumentError` if there is a column that does not have more than 3 non-`NaN` values,
or if `marker` is shorter than 2 characters, or does not only consist of punctuations; and
`OverflowError` if transforming `df` or `gd` results in a value too large to represent.

See also: [`record_all`](@ref)

# Examples
```jldoctest
julia> df = DataFrame(g=[1,2,1,2,1,2,1,2], kel=[5.5,5.8,5,5,7.5,1.08,0.94,0.56]);

julia> catalog = record(groupby(df, :g), [(Normalize.add_n_natural_log, 0.56)]);

julia> catalog["normal gdf"]
1-element Vector{Any}:
 GroupedDataFrame with 2 groups based on key: g
First Group (4 rows): g = 1
 Row │ g      kel__add_n_natural_log
     │ Int64  Float64
─────┼───────────────────────────────
   1 │     1                1.78171
   2 │     1                1.69378
   3 │     1                2.07191
   4 │     1                0.322083
⋮
Last Group (4 rows): g = 2
 Row │ g      kel__add_n_natural_log
     │ Int64  Float64
─────┼───────────────────────────────
   1 │     2                 1.83098
   2 │     2                 1.69378
   3 │     2                 0.41871
   4 │     2                 0.0

julia> pairs(catalog["normalized"]["add and natural log"][1])
pairs(::NamedTuple) with 7 entries:
  :name           => "kel (g = 1)"
  :skewness_stat  => -1.737
  :skewness_error => 1.014
  :skewness_ratio => -1.713
  :kurtosis_stat  => 3.271
  :kurtosis_error => 2.619
  :kurtosis_ratio => 1.249

julia> pairs(catalog["normalized"]["add and natural log"][2])
pairs(::NamedTuple) with 7 entries:
  :name           => "kel (g = 2)"
  :skewness_stat  => -0.159
  :skewness_error => 1.014
  :skewness_ratio => -0.157
  :kurtosis_stat  => -4.877
  :kurtosis_error => 2.619
  :kurtosis_ratio => -1.862

julia> catalog["nonnormal gdf"]
Any[]

julia> df2 = DataFrame(a=[-0.5, 2.47, 2.54, 2.91, 3.13]);

julia> catalog2 = record(df2, Function[Normalize.logit]);

julia> catalog2["normalized"]
Dict{Any, Any}()

julia> catalog2["normal gdf"]
Any[]

julia> catalog2["nonnormal gdf"][1]
5×1 DataFrame
 Row │ a__logit
     │ Float64
─────┼───────────
   1 │ -0.477121
   2 │  0.22538
   3 │  0.217313
   4 │  0.18286
   5 │  0.167165
```
"""
function record(
    gdf, transformations::AbstractVector{Function};
    normal_ratio::Real=2, marker::AbstractString="__"
)
    main_record = Dict(
        "normalized" => Dict(),
        "normal gdf" => [],
        "nonnormal gdf" => [],
    )
    for transformation in transformations
        results = apply(transformation, gdf; marker)
        _update_normal!(main_record, results; normal_ratio, marker)
    end

    return main_record
end

function record(gdf, transformations; normal_ratio::Real=2, marker::AbstractString="__")
    main_record = Dict(
        "normalized" => Dict(),
        "normal gdf" => [],
        "nonnormal gdf" => [],
    )
    for (transformation, extremum) in transformations
        results = apply(transformation, gdf, extremum; marker)
        _update_normal!(main_record, results; normal_ratio, marker)
    end

    return main_record
end

"""
    apply(
        func, df::AbstractDataFrame[, func_other_arg]; marker::AbstractString="__"
    ) -> NamedTuple
    apply(func, gd[, func_other_arg]; marker::AbstractString="__") -> NamedTuple

Apply a function `func` to `df` or grouped data frame `gd`, and return the skewness,
kurtosis, and transformed (grouped) data frame.

`func` is passed up to two arguments. If `func` takes two arguments, then `func_other_arg`
is the second argument.

Functions will be separately applied to `df` or `gd`, and the column names of the
transformed (grouped) data frame will be suffixed with a string `marker` and the applied
functions.

A named tuple is returned with the following fields:
- `skewness_and_kurtosis`: a named tuple with the fields `skewness_stat`,
    `skewness_error`, `skewness_ratio`, `kurtosis_stat`, `kurtosis_error`, and
    `kurtosis_ratio`
- `transformed_gdf`: a transformed (grouped) data frame

!!! compat "Julia 1.5"
    `apply` uses `contains`, which requires at least Julia 1.5.

Throw `ArgumentError` if there is a column that does not have more than 3 non-`NaN` values,
or if `marker` is shorter than 2 characters, or does not only consist of punctuations; and
`OverflowError` if transforming `df` or `gd` results in a value too large to represent.

# Examples
```jldoctest
julia> df = DataFrame(a=1:4);

julia> results = apply(Normalize.square, df);

julia> pairs(results[:skewness_and_kurtosis][1])
pairs(::NamedTuple) with 6 entries:
  :skewness_stat  => 0.709
  :skewness_error => 1.014
  :skewness_ratio => 0.699
  :kurtosis_stat  => -0.592
  :kurtosis_error => 2.619
  :kurtosis_ratio => -0.226

julia> results[:transformed_gdf]
4×1 DataFrame
 Row │ a__square
     │ Int64
─────┼───────────
   1 │         1
   2 │         4
   3 │         9
   4 │        16

julia> df2 = DataFrame(group=[1, 1, 1, 1, 2, 2, 2, 2], a=1:8);

julia> results2 = apply(Normalize.add_n_invert, groupby(df2, :group), 1);

julia> pairs(results2[:skewness_and_kurtosis][1])
pairs(::NamedTuple) with 6 entries:
  :skewness_stat  => 1.469
  :skewness_error => 1.014
  :skewness_ratio => 1.449
  :kurtosis_stat  => 2.031
  :kurtosis_error => 2.619
  :kurtosis_ratio => 0.775

julia> pairs(results2[:skewness_and_kurtosis][2])
pairs(::NamedTuple) with 6 entries:
  :skewness_stat  => 0.574
  :skewness_error => 1.014
  :skewness_ratio => 0.566
  :kurtosis_stat  => -0.625
  :kurtosis_error => 2.619
  :kurtosis_ratio => -0.239

julia> results2[:transformed_gdf]
GroupedDataFrame with 2 groups based on key: group
First Group (4 rows): group = 1
 Row │ group  a__add_n_invert
     │ Int64  Float64
─────┼────────────────────────
   1 │     1         1.0
   2 │     1         0.5
   3 │     1         0.333333
   4 │     1         0.25
⋮
Last Group (4 rows): group = 2
 Row │ group  a__add_n_invert
     │ Int64  Float64
─────┼────────────────────────
   1 │     2         0.2
   2 │     2         0.166667
   3 │     2         0.142857
   4 │     2         0.125
```
"""
function apply(func, df::AbstractDataFrame; marker::AbstractString="__")
    if _is_not_long_punctuation(marker)
        throw(
            ArgumentError(
                "marker should contain at least 2 characters, and only punctuations."
            )
        )
    end
    df_altered = rename(colname -> _rename_with("-+", colname; marker=""), df)
    df_altered = _select_finite(func, df_altered)
    rename!(colname -> _rename_with(func, colname; marker), df_altered)
    return (
        skewness_and_kurtosis=_get_skewness_kurtosis(df_altered),
        transformed_gdf=df_altered,
    )
end

function apply(func, df::AbstractDataFrame, func_other_arg; marker::AbstractString="__")
    if _is_not_long_punctuation(marker)
        throw(
            ArgumentError(
                "marker should contain at least 2 characters, and only punctuations."
            )
        )
    end
    df_altered = rename(colname -> _rename_with("-+", colname; marker=""), df)
    df_altered = _select_finite(func, df_altered, func_other_arg)
    rename!(colname -> _rename_with(func, colname; marker), df_altered)
    return (
        skewness_and_kurtosis=_get_skewness_kurtosis(df_altered),
        transformed_gdf=df_altered,
    )
end

function apply(func, gd; marker::AbstractString="__")
    if _is_not_long_punctuation(marker)
        throw(
            ArgumentError(
                "marker should contain at least 2 characters, and only punctuations."
            )
        )
    end
    gd_new = _rename_valuecols(gd, "-+"; marker="")
    gd_new = _select_finite(func, gd_new)
    gd_new = _rename_valuecols(gd_new, func; marker)
    return (
        skewness_and_kurtosis=_get_skewness_kurtosis(gd_new),
        transformed_gdf=gd_new,
    )
end

function apply(func, gd, func_other_arg; marker::AbstractString="__")
    if _is_not_long_punctuation(marker)
        throw(
            ArgumentError(
                "marker should contain at least 2 characters, and only punctuations."
            )
        )
    end
    gd_new = _rename_valuecols(gd, "-+"; marker="")
    gd_new = _select_finite(func, gd_new, func_other_arg)
    gd_new = _rename_valuecols(gd_new, func; marker)
    return (
        skewness_and_kurtosis=_get_skewness_kurtosis(gd_new),
        transformed_gdf=gd_new,
    )
end

function _is_not_long_punctuation(separator)
    return length(separator) < 2 || !all(ispunct, separator)
end

function _rename_with(fragment, name; marker::AbstractString="__")
    old_name = _get_original(name; marker)
    return "$(old_name)$(marker)$(fragment)"
end

function _get_original(colname; marker::AbstractString="__")
    return SubString(colname, 1, _find_original_end(colname; marker))
end

function _find_original_end(colname; marker::AbstractString="__")
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

function _rename_valuecols(gd, fragment; marker::AbstractString="__")
    grouping = groupcols(gd)
    new_names = [
        var => _rename_with(fragment, string(var); marker)
        for var in valuecols(gd)
    ]
    df_altered = rename(DataFrame(gd), new_names)
    return groupby(df_altered, grouping)
end

function _select_finite(func, df::AbstractDataFrame)
    df = select(df, names(df) .=> ByRow(func))
    if _contains(isinf, df)
        throw(OverflowError("Transforming df results in a value too large to represent."))
    else
        return df
    end
end

function _select_finite(func, df::AbstractDataFrame, func_other_arg)
    df = select(df, names(df) .=> ByRow(x -> func(x, func_other_arg)))
    if _contains(isinf, df)
        throw(OverflowError("Transforming df results in a value too large to represent."))
    else
        return df
    end
end

function _select_finite(func, gd)
    grouping = groupcols(gd)
    gd = select(gd, valuecols(gd) .=> ByRow(func); ungroup=false)
    df_example = DataFrame(gd)
    if _contains(isinf, df_example[!, Not(grouping)])
        throw(OverflowError("Transforming gd results in a value too large to represent."))
    else
        return gd
    end
end

function _select_finite(func, gd, func_other_arg)
    grouping = groupcols(gd)
    gd = select(
        gd, valuecols(gd) .=> ByRow(x -> func(x, func_other_arg)); ungroup=false
    )
    df_example = DataFrame(gd)
    if _contains(isinf, df_example[!, Not(grouping)])
        throw(OverflowError("Transforming gd results in a value too large to represent."))
    else
        return gd
    end
end

function _update_normal!(
    entries, applied; normal_ratio::Real=2, marker::AbstractString="__"
)
    if are_normal(applied[:skewness_and_kurtosis]; normal_ratio)
        _label_findings!(entries, applied; marker)
        _store_transformed!(entries, "normal gdf", applied[:transformed_gdf])
    else
        _store_transformed!(entries, "nonnormal gdf", applied[:transformed_gdf])
    end

    return nothing
end

function _label_findings!(entries, applied; marker::AbstractString="__")
    gdf_altered = applied[:transformed_gdf]
    transformations = _get_applied_function_names(gdf_altered; marker)
    original_colnames = _get_original_colnames(gdf_altered; marker)
    entries["normalized"][transformations] = _label.(
        applied[:skewness_and_kurtosis], original_colnames
    )
    return entries["normalized"][transformations]
end

function _get_applied_function_names(df::AbstractDataFrame; marker::AbstractString="__")
    new_colname = names(df)[1]
    before_marker = _find_original_end(new_colname; marker)
    after_marker = before_marker + length(marker) + 1
    function_names = SubString(new_colname, after_marker)
    return _make_legible(function_names, " n "; marker)
end

function _get_applied_function_names(gd; marker::AbstractString="__")
    new_colname = string(valuecols(gd)[1])
    before_marker = _find_original_end(new_colname; marker)
    after_marker = before_marker + length(marker) + 1
    function_names = SubString(new_colname, after_marker)
    return _make_legible(function_names, " n "; marker)
end

function _make_legible(snake_case, listing_split_on=" "; marker::AbstractString="__")
    words = _make_phrase(snake_case, "_", " ")
    listable = _make_listing(words, listing_split_on)
    extra_chars = _underscores_to_spaces(marker)
    return _make_phrase(listable, extra_chars, "; ")
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

function _underscores_to_spaces(separator)
    chars = length(separator)
    if separator === repeat("_", chars)
        return repeat(" ", chars)
    else
        return separator
    end
end

function _get_original_colnames(gdf::AbstractDataFrame; marker::AbstractString="__")
    return _get_original.(names(gdf); marker)
end

function _get_original_colnames(gdf; marker::AbstractString="__")
    originals = _get_original.(string(var) for var in valuecols(gdf); marker)
    return _concat_groupnames(gdf, originals)
end

function _concat_groupnames(gd, colnames)
    new_names = []
    for varname in colnames
        for group in keys(gd)
            grouping = _get_groupnames(group)
            push!(new_names, "$varname ($grouping)")
        end
    end

    return new_names
end

function _get_groupnames(group)
    if isone(length(group))
        punctuations = 2
    else
        punctuations = 1
    end

    return chop(string(group), head=length("GroupKey: ("), tail=punctuations)
end

function _label(prelim, tag)
    title = (
        name=tag,
    )
    return merge(title, prelim)
end

function _store_transformed!(entries, key, value::AbstractVector)
    return append!(entries[key], value)
end

function _store_transformed!(entries, key, value)
    return push!(entries[key], value)
end

function _merge_results!(entries, other)
    merge!(entries["normalized"], other["normalized"])
    _store_transformed!(entries, "normal gdf", other["normal gdf"])
    _store_transformed!(entries, "nonnormal gdf", other["nonnormal gdf"])
    return entries
end

"""
    tabular_to_dataframe(path, sheet::AbstractString="") -> AbstractDataFrame

Load tabular data from a string `path`, and converts it to a data frame.

Acceptable file extensions are .csv, .dta, .ods, .sav, .xls, and .xlsx. If `path` ends in
.ods, .xls, or .xlsx, the worksheet that is named `sheet` is converted.

!!! compat "Julia 1.2"
    `tabular_to_dataframe` uses `Regex` in `endswith`, which requires at least Julia 1.2.
"""
function tabular_to_dataframe(path, sheet::AbstractString="")
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
    sheetcol_to_float!(df, colname; blank_to::Real) -> AbstractVector

Convert a column named `colname`  in a data frame `df` with element type `String` or `Any`
to `Float64`. Blank values (i.e., " ") will be replaced with `blank_to` before conversion.

If a value cannot be converted to `Float64`, an error is raised.

# Examples
```jldoctest
julia> df = DataFrame(a=[" ", "7"], b=[6.39, " "], c=[" ", " "])
2×3 DataFrame
 Row │ a       b     c
     │ String  Any   String
─────┼──────────────────────
   1 │         6.39
   2 │ 7

julia> sheetcol_to_float!(df, :a, blank_to=1.25)
2-element Vector{Float64}:
 1.25
 7.0

julia> sheetcol_to_float!(df, "b", blank_to=8)
2-element Vector{Float64}:
 6.39
 8.0

julia> sheetcol_to_float!(df, 3, blank_to=5)
2-element Vector{Float64}:
 5.0
 5.0

julia> df
2×3 DataFrame
 Row │ a        b        c
     │ Float64  Float64  Float64
─────┼───────────────────────────
   1 │    1.25     6.39      5.0
   2 │    7.0      8.0       5.0
```
"""
function sheetcol_to_float!(df, colname; blank_to::Real)
    col = df[!, colname]
    if _is_from_csv_excel_or_opendoc(col)
        if _is_from_csv(col)
            replace!(col, " " => "$(blank_to)")
            df[!, colname] = parse.(Float64, col)
        else
            replace!(col, " " => blank_to)
            df[!, colname] = convert(Vector{Float64}, col)
        end
    end

    return df[!, colname]
end

function _is_from_csv_excel_or_opendoc(col)
    return eltype(col) === String || eltype(col) === Any
end

function _is_from_csv(col)
    nonblanks = col[col .!== " "]
    return eltype(nonblanks) === String
end

"""
    replace_missing!(df, colname; new_value::Real) -> AbstractVector

Replace occurrences of `missing` with `new_value` in a column named `colname` in a data
frame `df`. The column will be converted to element type `Float64`.

If a value cannot be converted to `Float64`, an error is raised.

!!! compat "Julia 1.3"
    `replace_missing!` uses `nonmissingtype`, which is exported as of Julia 1.3.

# Examples
```jldoctest
julia> df = DataFrame(a=[missing, 10], b=[12.12, missing], c=[missing, missing])
2×3 DataFrame
 Row │ a        b           c
     │ Int64?   Float64?    Missing
─────┼──────────────────────────────
   1 │ missing       12.12  missing
   2 │      10  missing     missing

julia> replace_missing!(df, :a, new_value=2.5)
2-element Vector{Float64}:
  2.5
 10.0

julia> replace_missing!(df, "b", new_value=6)
2-element Vector{Float64}:
 12.12
  6.0

julia> replace_missing!(df, 3, new_value=9)
2-element Vector{Float64}:
 9.0
 9.0

julia> df
2×3 DataFrame
 Row │ a        b        c
     │ Float64  Float64  Float64
─────┼───────────────────────────
   1 │     2.5    12.12      9.0
   2 │    10.0     6.0       9.0
```
"""
function replace_missing!(df, colname; new_value::Real)
    col = df[!, colname]
    nonmissing = nonmissingtype(eltype(col))
    if nonmissing === String
        replace!(col, missing => "$(new_value)")
        df[!, colname] = parse.(Float64, col)
    else
        col = map(float, col)
        df[!, colname] = coalesce.(col, float(new_value))
    end

    return df[!, colname]
end

"""
    print_skewness_kurtosis(df::AbstractDataFrame; dependent::Bool=false)
    print_skewness_kurtosis(gd)

Print the skewness and kurtosis (statistic, standard error, ratio) of each column in `df`
or grouped data frame `gd`.

If `dependent` is `true`, then the skewness and kurtosis of the difference between the
columns of `df` will be printed.

# Examples
```jldoctest
julia> df = DataFrame(a=[1.072, 0.486, 0.133, 0.223, 1.139]);

julia> print_skewness_kurtosis(df)
--------
Variable: a
Skewness Statistic: 0.306
Skewness Std. Error: 0.913
Skewness Ratio: 0.335

Kurtosis Statistic: -2.952
Kurtosis Std. Error: 2.0
Kurtosis Ratio: -1.476


julia> df2 = DataFrame(b=[6.24, 6.34, 6.39, 2.6, 8.35], c=[5.5, 5.8, 5, 5, 7.5]);

julia> print_skewness_kurtosis(df2, dependent=true)
--------
Variable: c_minus_b
Skewness Statistic: 1.983
Skewness Std. Error: 0.913
Skewness Ratio: 2.172

Kurtosis Statistic: 4.212
Kurtosis Std. Error: 2.0
Kurtosis Ratio: 2.106


julia> df3 = DataFrame(
           group=[1,1,1,1,2,2,2,2],
           dd=[0.042, 0.849, 0.342, 0.771, 1.879, 1.289, 0.981, 1.073],
       );

julia> print_skewness_kurtosis(groupby(df3, :group))
--------
Variable: dd (group = 1)
Skewness Statistic: -0.478
Skewness Std. Error: 1.014
Skewness Ratio: -0.471

Kurtosis Statistic: -2.951
Kurtosis Std. Error: 2.619
Kurtosis Ratio: -1.127

--------
Variable: dd (group = 2)
Skewness Statistic: 1.439
Skewness Std. Error: 1.014
Skewness Ratio: 1.419

Kurtosis Statistic: 1.859
Kurtosis Std. Error: 2.619
Kurtosis Ratio: 0.71

```
"""
function print_skewness_kurtosis(df::AbstractDataFrame; dependent::Bool=false)
    try
        if dependent
            if ncol(df) === 2
                _print_dependent(df)
            else
                println("""
                    There can only be two dependent variables to measure
                    skewness and kurtosis.
                    """
                )
            end
        else
            _print_independent(df)
        end
    catch e
        println(e)
    end
end

function print_skewness_kurtosis(gd)
    try
        gd_new = combine(
            gd, valuecols(gd) .=> x -> [skewness(x), kurtosis(x),]; ungroup=false
        )
        for group in keys(gd_new)
            for var in valuecols(gd_new)
                tagged = _describe_var(gd_new, group, var)
                _print_summary(tagged)
            end
        end
    catch e
        println(e)
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
    grouping = _get_groupnames(group_column)
    tag = (
        name="$varname ($grouping)",
    )
    skews_and_kurts = gd[group_column][!, value_column]
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
        tagged = _describe_var(colname, df[!, colname])
        _print_summary(tagged)
    end
end

"""
    print_findings(findings)

Print the normal skewness and kurtosis in a dictionary `findings`.

`findings` has the following key-value pairs:
- `"normalized"` => a dictionary of a collection value of named tuples containing
    skewnesses and kurtoses for each normal transformation key
- `"normal gdf"` => a list of transformed (grouped) data frames whose data are normal
- `"nonnormal gdf"` => a list of transformed (grouped) data frames whose data are nonnormal

# Examples
```jldoctest
julia> df = DataFrame(a=[-0.5, 2.47, 2.54, 2.91, 3.13]);

julia> results = normalize(df);

julia> print_findings(results)
APPLIED: cube
--------
Variable: a
Skewness Statistic: -0.678
Skewness Std. Error: 0.913
Skewness Ratio: -0.743

Kurtosis Statistic: 0.669
Kurtosis Std. Error: 2.0
Kurtosis Ratio: 0.334


APPLIED: square
--------
Variable: a
Skewness Statistic: -1.315
Skewness Std. Error: 0.913
Skewness Ratio: -1.44

Kurtosis Statistic: 2.149
Kurtosis Std. Error: 2.0
Kurtosis Ratio: 1.074


APPLIED: antilog
--------
Variable: a
Skewness Statistic: 0.852
Skewness Std. Error: 0.913
Skewness Ratio: 0.933

Kurtosis Statistic: -0.029
Kurtosis Std. Error: 2.0
Kurtosis Ratio: -0.014


APPLIED: reflect and invert
--------
Variable: a
Skewness Statistic: -0.626
Skewness Std. Error: 0.913
Skewness Ratio: -0.686

Kurtosis Statistic: 0.843
Kurtosis Std. Error: 2.0
Kurtosis Ratio: 0.422


APPLIED: reflect and log base 10
--------
Variable: a
Skewness Statistic: 1.567
Skewness Std. Error: 0.913
Skewness Ratio: 1.716

Kurtosis Statistic: 2.896
Kurtosis Std. Error: 2.0
Kurtosis Ratio: 1.448



julia> results2
ERROR: UndefVarError: results2 not defined

julia> df2 = DataFrame(group=[1, 1, 1, 1, 2, 2, 2, 2], a=1:8);

julia> results2 = apply(Normalize.add_n_invert, groupby(df2, :group), 1);

julia> results2[:transformed_gdf]
GroupedDataFrame with 2 groups based on key: group
First Group (4 rows): group = 1
 Row │ group  a__add_n_invert
     │ Int64  Float64
─────┼────────────────────────
   1 │     1         1.0
   2 │     1         0.5
   3 │     1         0.333333
   4 │     1         0.25
⋮
Last Group (4 rows): group = 2
 Row │ group  a__add_n_invert
     │ Int64  Float64
─────┼────────────────────────
   1 │     2         0.2
   2 │     2         0.166667
   3 │     2         0.142857
   4 │     2         0.125

julia> df = DataFrame(a=[-0.5, 2.47, 2.54, 2.91, 3.13]);

julia> results = normalize(df);

julia> print_findings(results)
APPLIED: cube
--------
Variable: a
Skewness Statistic: -0.678
Skewness Std. Error: 0.913
Skewness Ratio: -0.743

Kurtosis Statistic: 0.669
Kurtosis Std. Error: 2.0
Kurtosis Ratio: 0.334


APPLIED: square
--------
Variable: a
Skewness Statistic: -1.315
Skewness Std. Error: 0.913
Skewness Ratio: -1.44

Kurtosis Statistic: 2.149
Kurtosis Std. Error: 2.0
Kurtosis Ratio: 1.074


APPLIED: antilog
--------
Variable: a
Skewness Statistic: 0.852
Skewness Std. Error: 0.913
Skewness Ratio: 0.933

Kurtosis Statistic: -0.029
Kurtosis Std. Error: 2.0
Kurtosis Ratio: -0.014


APPLIED: reflect and invert
--------
Variable: a
Skewness Statistic: -0.626
Skewness Std. Error: 0.913
Skewness Ratio: -0.686

Kurtosis Statistic: 0.843
Kurtosis Std. Error: 2.0
Kurtosis Ratio: 0.422


APPLIED: reflect and log base 10
--------
Variable: a
Skewness Statistic: 1.567
Skewness Std. Error: 0.913
Skewness Ratio: 1.716

Kurtosis Statistic: 2.896
Kurtosis Std. Error: 2.0
Kurtosis Ratio: 1.448


```
"""
function print_findings(findings)
    normal = findings["normalized"]
    for (transformation, altered) in normal
        println("APPLIED: $transformation")
        _print_summary.(altered)
        println()
    end

    if _has_no_changes(findings)
        println("No transformations applied, so unable to normalize.")
    elseif isempty(normal)
        println("Transformations applied, but no normal results.")
    end
end

function _has_no_changes(findings)
    return isempty(findings["nonnormal gdf"]) && isempty(findings["normal gdf"])
end

"""
    normal_to_csv(path, findings; dependent::Bool=false)

Export normal data from a dictionary `findings` to a CSV file in a string `path`.

`findings` has a key `"normal gdf"` with a collection value of (grouped) data frames.

If `path` already exists (i.e., a file has the same name), then the file will be
overwritten. Otherwise, a new file will be created.

If `dependent` is `true`, then a column of zeros will also be created in the CSV file for
dependent testing of data with only one group and two dependent variables.

If there are any `NaN`, they will be replaced with `missing` in the CSV file.
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

    if normal_data[1] isa AbstractDataFrame
        df_normal = _flatten_dataframes(normal_data)
        if dependent
            df_normal = _store_zeros(df_normal)
        end
    else
        df_normal = _flatten_grouped_dataframes(normal_data)
    end
    CSV.write(path, df_normal, transform=(col, val) -> _nan_to(missing, val))
    return nothing
end

function _flatten_dataframes(gdfs)
    df_new = DataFrame()
    for df in gdfs
        df_new = hcat(df_new, df)
    end

    return df_new
end

function _store_zeros(df)
    df_zero = DataFrame(["for_dependent_test" => zeros(nrow(df))])
    return hcat(df, df_zero)
end

function _flatten_grouped_dataframes(gdfs)
    df_new = DataFrame()
    df_new = _store_groupcols(df_new, gdfs)
    return _store_valuecols(df_new, gdfs)
end

function _store_groupcols(df, gdfs)
    gd_example = gdfs[1]
    df_example = DataFrame(gd_example)
    for group in groupcols(gd_example)
        df = hcat(df, df_example[!, [group]])
    end

    return df
end

function _store_valuecols(df, gdfs)
    for grouped in gdfs
        df_convert = DataFrame(grouped)
        for var in valuecols(grouped)
            df = hcat(df, df_convert[!, [var]])
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