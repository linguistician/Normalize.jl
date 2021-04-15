module Normalize

using CSV, DataFrames, SciPy, ExcelFiles

export  add_label,
        add_then_invert,
        add_then_log_base_10,
        add_then_logit,
        add_then_natural_log,
        add_then_square_root,
        add_then_square_root_then_invert,
        add_then_square_then_invert,
        antilog,
        apply_transformation,
        are_negative_skew,
        are_nonnormal,
        are_normal,
        are_positive_skew,
        blank_to_nan!,
        calculate_kurtosis,
        calculate_kurtosis_error,
        calculate_kurtosis_stat,
        calculate_kurtosis_variance,
        calculate_ratio,
        calculate_skewness,
        calculate_skewness_error,
        calculate_skewness_stat,
        calculate_skewness_variance,
        contains_invalid_for_add_then_logit,
        contains_invalid_for_logit,
        contains_predicated,
        contains_skew,
        cube,
        get_applied_transformations,
        get_error_negative_skew_transformations,
        get_error_positive_skew_transformations,
        get_extremum,
        get_filtered_cols,
        get_original_colnames,
        get_positive_skew_transformations,
        invert,
        is_invalid_for_add_then_logit,
        is_invalid_for_logit,
        is_negative_skew,
        is_normal,
        is_positive_skew,
        label_findings,
        locate_end_of_original_colname,
        log_base_10,
        logit,
        make_legible,
        make_list_phrase,
        make_phrase,
        merge_cols!,
        merge_results!,
        natural_log,
        normalize,
        print_findings,
        print_skewness_kurtosis,
        print_summary,
        record_all_transformations,
        record_transformations,
        reflect_then_invert,
        reflect_then_log_base_10,
        reflect_then_square_root,
        rename_with_function,
        square,
        square_root,
        square_root_then_add_then_invert,
        square_root_then_invert,
        square_then_invert,
        stretch_skew,
        string_to_float!,
        tabular_to_dataframe,
        transform_negative_skew,
        transform_positive_skew,
        update_normal!

"""
    tabular_to_dataframe(path::AbstractString,
                         sheet::AbstractString="") -> AbstractDataFrame


"""
function tabular_to_dataframe(path::AbstractString, sheet::AbstractString="")
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
        skews = calculate_skewness.(eachcol(df))
        kurts = calculate_kurtosis.(eachcol(df))
        normal = []
        if are_nonnormal(skews, kurts, normal_ratio)
            if are_positive_skew(skews)
                push!(normal, transform_positive_skew(df, normal_ratio))
            elseif are_negative_skew(skews)
                push!(normal, transform_negative_skew(df, normal_ratio))
            end
            push!(normal, stretch_skew(df, normal_ratio))
        end
        normal
    catch e
        []
    end
end

"""
    calculate_skewness(col::AbstractVector, round_to::Integer=3, biased::Bool=false,
                       nan_rule::AbstractString="omit") -> NamedTuple


"""
function calculate_skewness(col::AbstractVector, round_to::Integer=3, biased::Bool=false,
                            nan_rule::AbstractString="omit")
    stat = calculate_skewness_stat(col, round_to, biased, nan_rule)
    error = calculate_skewness_error(col, round_to)
    ratio = calculate_ratio(stat, error, round_to)
    (
        skewness_stat = stat,
        skewness_error = error,
        skewness_ratio = ratio,
    )
end

"""
    calculate_skewness_stat(col, round_to=3, biased=false, nan_rule="omit")


"""
function calculate_skewness_stat(col::AbstractVector, round_to::Integer=3,
                                 biased::Bool=false, nan_rule::AbstractString="omit")
    if any(isnan.(col))
        round(SciPy.stats.skew(col, bias=biased, nan_policy=nan_rule)[1], digits=round_to)
    else
        round(SciPy.stats.skew(col, bias=biased, nan_policy=nan_rule), digits=round_to)
    end
end

"""
    calculate_skewness_error(col::AbstractVector, round_to::Integer=3)

Compute skewness error of `col`, rounded to `round_to` digits.
"""
function calculate_skewness_error(col::AbstractVector, round_to::Integer=3)
    variance = calculate_skewness_variance(col)
    round(√variance, digits=round_to)
end

"""
    calculate_skewness_variance(col::AbstractVector)

Compute the skewness variance of `col`.

It is calculated from the formula:

``\\frac{6N(N-1)}{(N-2)(N+1)(N+3)}``
"""
function calculate_skewness_variance(col::AbstractVector)
    omission = count(isnan.(col))
    N = length(col) - omission
    6 * N * (N - 1) * invert((N - 2) * (N + 1) * (N + 3))
end

"""
    calculate_ratio(statistic::Real, std_error::Real, round_to::Integer=3)

Compute the ratio of `statistic` and `std_error`, rounded to `round_to` digits.
"""
function calculate_ratio(statistic::Real, std_error::Real, round_to::Integer=3)
   round(statistic * invert(std_error), digits=round_to)
end

"""
    calculate_kurtosis(col::AbstractVector, round_to::Integer=3, biased::Bool=false, 
                       nan_rule::AbstractString="omit") -> NamedTuple


"""
function calculate_kurtosis(col::AbstractVector, round_to::Integer=3, biased::Bool=false,
                            nan_rule::AbstractString="omit")
    stat = calculate_kurtosis_stat(col, round_to, biased, nan_rule)
    error = calculate_kurtosis_error(col, round_to)
    ratio = calculate_ratio(stat, error, round_to)
    (
        kurtosis_stat = stat,
        kurtosis_error = error,
        kurtosis_ratio = ratio,
    )
end

"""
    calculate_kurtosis_stat(col::AbstractVector, round_to::Integer=3, biased::Bool=false,
                            nan_rule::AbstractString="omit")


"""
function calculate_kurtosis_stat(col::AbstractVector, round_to::Integer=3,
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
    calculate_kurtosis_error(col::AbstractVector, round_to::Integer=3)

Compute the kurtosis error of `col`, rounded to `round_to` digits.
"""
function calculate_kurtosis_error(col::AbstractVector, round_to::Integer=3)
    variance = calculate_kurtosis_variance(col)
    round(√variance, digits=round_to)
end

"""
    calculate_kurtosis_variance(col::AbstractVector)

Compute the kurtosis variance of `col`.

It is calculated from the formula:

``\\frac{}{(N-3)(N+5)}``
"""
function calculate_kurtosis_variance(col::AbstractVector)
    omission = count(isnan.(col))
    N = length(col) - omission
    skewness_variance = calculate_skewness_variance(col)
    4 * (N^2 - 1) * skewness_variance * invert((N - 3) * (N + 5))
end

"""
    are_nonnormal(skewnesses::AbstractVector, kurtoses::AbstractVector,
                  normal_ratio::Real=2) -> Bool

Return `true` if ratios in `skewnesses` and `kurtoses` are not within the range of
±`normal_ratio`, and false otherwise.
"""
function are_nonnormal(skewnesses::AbstractVector, kurtoses::AbstractVector,
                       normal_ratio::Real=2)
    !are_normal(skewnesses, kurtoses, normal_ratio)
end

"""
    are_normal(skewnesses::AbstractVector, kurtoses::AbstractVector;
               normal_ratio::Real=2) -> Bool

Return `true` if items in `skewnesses` and `kurtoses` are normal, and `false` otherwise.
"""
function are_normal(skewnesses::AbstractVector, kurtoses::AbstractVector,
                    normal_ratio::Real=2)
    combined = merge.(skewnesses, kurtoses)
    normals = [is_normal(var.skewness_ratio, var.kurtosis_ratio, normal_ratio=normal_ratio)
               for var in combined]
    all(normals)
end

"""
    is_normal(skewness_ratio::Real, kurtosis_ratio::Real, normal_ratio::Real=2) -> Bool

Return `true` if `skewness_ratio` and `kurtosis_ratio` are within the range of
±`normal_ratio`, and false otherwise.
"""
function is_normal(skewness_ratio::Real, kurtosis_ratio::Real; normal_ratio::Real=2)
    (-normal_ratio ≤ skewness_ratio ≤ normal_ratio) &&
    (-normal_ratio ≤ kurtosis_ratio ≤ normal_ratio)
end

"""
    are_positive_skew(skewnesses::AbstractVector) -> Bool

Return `true` if `skewnesses` contains any positive ratios, and `false` otherwise.
"""
function are_positive_skew(skewnesses::AbstractVector)
    contains_skew(is_positive_skew, skewnesses)
end

"""
    contains_skew(f::Function, skewnesses::AbstractVector) -> Bool


"""
function contains_skew(f::Function, skewnesses::AbstractVector)
    ratios = [var.skewness_ratio
              for var in skewnesses]
    skews = f.(ratios)
    any(skews)
end

"""
    is_positive_skew(skewness_ratio::Real) -> Bool

Return `true` if `skewness_ratio` is positive, and `false` otherwise.
"""
function is_positive_skew(skewness_ratio::Real)
    skewness_ratio > 0
end

"""
    transform_positive_skew(df::AbstractDataFrame, normal_ratio::Real=2) -> AbstractDict

Apply positive skew transformations to `df`, and return
"""
function transform_positive_skew(df::AbstractDataFrame, normal_ratio::Real=2)
    try
        smallest = get_extremum(minimum, df)
        positive_transformations = get_positive_skew_transformations(smallest)
        record_all_transformations(df, 
                                   positive_transformations, 
                                   smallest, 
                                   normal_ratio=normal_ratio)
    catch e
        smallest = get_extremum(minimum, df)
        positive_transformations = get_error_positive_skew_transformations(e, smallest)
        record_all_transformations(df, 
                                   positive_transformations, 
                                   smallest, 
                                   normal_ratio=normal_ratio)
    end
end

"""
    get_extremum(f::Function, df::AbstractDataFrame)


"""
function get_extremum(f::Function, df::AbstractDataFrame)
    filtered_cols = get_filtered_cols(isfinite, df)
    col_with_extremum = f.(filtered_cols)
    f(col_with_extremum)
end

"""
    get_filtered_cols(f::Function,
                      df::AbstractDataFrame[, f_second_arg::Real]) -> AbstractVector


"""
function get_filtered_cols(f::Function, df::AbstractDataFrame)
    [df[(f.(df[!, name])), name]
     for name in names(df)]
end

function get_filtered_cols(f::Function, df::AbstractDataFrame, f_second_arg::Real)
    [df[(f.(df[!, name], f_second_arg)), name]
     for name in names(df)]
end

"""
    get_positive_skew_transformations(min::Real) -> AbstractDict

Return positive skew transformations for the data set based on `min`.
"""
function get_positive_skew_transformations(min::Real)
    if min < 0
        Dict(
            "one arg" => [],
            "two args" => [
                add_then_square_root,
                add_then_square_root_then_invert,
                add_then_invert,
                add_then_square_then_invert,
                add_then_log_base_10,
                add_then_natural_log,
            ],
        )
    elseif 0 ≤ min < 1
        Dict(
            "one arg" => [
                square_root,
            ],
            "two args" => [
                square_root_then_add_then_invert,
                add_then_invert,
                add_then_square_then_invert,
                add_then_log_base_10,
                add_then_natural_log,
            ],
        )
    else
        Dict(
            "one arg" => [
                square_root,
                square_root_then_invert,
                invert,
                square_then_invert,
                log_base_10,
                natural_log,
            ],
            "two args" => [],
        )
    end
end

# min < 0
"""
    add_then_square_root(x::Real, min::Real)

Add one to the difference of `x` and `min`, and compute its square root.

Throws error if `min > x`.
"""
function add_then_square_root(x::Real, min::Real)
    if min > x
        throw(error("min must be smaller."))
    end
    √(x + 1 - min)
end

"""
    add_then_square_root_then_invert(x::Real, min::Real)

Add one to the difference of `x` and `min`, and compute its reciprocal square root.

Throws error if `min > x`.
"""
function add_then_square_root_then_invert(x::Real, min::Real)
    invert(add_then_square_root(x, min))
end

"""
    add_then_invert(x::Real, min::Real)

Add one to the difference of `x` and `min`, and compute its reciprocal.

Throws error if `min > x`.
"""
function add_then_invert(x::Real, min::Real)
    if min > x
        throw(error("min must be smaller."))
    end
    invert(x + 1 - min)
end

"""
    add_then_square_then_invert(x::Real, min::Real)

Add one to the difference of squares of `x` and `min`, and compute its reciprocal.

Throws `DivideError` if x^2 + 1 - min^2 is 0.
"""
function add_then_square_then_invert(x::Real, min::Real)
    if min > x
        throw(error("The second parameter (min) must be smaller."))
    end
    invert(x^2 + 1 - min^2)
end

"""
    add_then_log_base_10(x::Real, min::Real)

Add one to the difference of `x` and `min`, and compute its logarithm to base 10.

Throws error if `min > x`.
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

Throws `DomainError` if `x ≤ 0`.
"""
function log_base_10(x::Real)
    if iszero(x)
        throw(DomainError(x))
    end
    log10(x)
end

"""
    add_then_natural_log(x::Real, min::Real)

Add one to the difference of `x` and `min`, and compute its natural logarithm.

Throws error if `min > x`.
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

Throws `DomainError` if `x ≤ 0`.
"""
function natural_log(x::Real)
    if iszero(x)
        throw(DomainError(x))
    end
    log(x)
end

# 0 ≤ min < 1 (with add_then_invert, add_then_square_then_invert, add_then_log_base_10, 
#              add_then_natural_log)

"""
    square_root(x::Real)

Compute the square root of `x`.

Throws `DomainError` if `x` is negative.
"""
function square_root(x::Real)
    √x
end

"""
    square_root_then_add_then_invert(x::Real, min::Real)

Add one to the difference of square roots of `x` and `min`, and compute its reciprocal.

Throws `DomainError` if `x` or `min` is negative, and ErrorException if `min > x`.
"""
function square_root_then_add_then_invert(x::Real, min::Real)
    if min > x
        throw(error("The second parameter (min) must be smaller."))
    end
    invert(√x + 1 - √min)
end

# min ≥ 1 (with square_root, log_base_10, natural_log)

"""
    square_root_then_invert(x::Real)

Compute the reciprocal square root of `x`.

Throws `DomainError` if `x` is negative, and `DivideError` if `x` is 0.
"""
function square_root_then_invert(x::Real)
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
    square_then_invert(x::Real)

Compute the reciprocal square of `x`.

Throws `DivideError` if `x` is 0.
"""
function square_then_invert(x::Real)
    invert(x^2)
end

"""
    record_all_transformations(df::AbstractDataFrame, transformations::AbstractDict,
                               extremum::Real; normal_ratio::Real=2) -> AbstractDict


"""
function record_all_transformations(df::AbstractDataFrame, transformations::AbstractDict,
                                    extremum::Real; normal_ratio::Real=2)
    record = record_transformations(df, 
                                    transformations["one arg"], 
                                    normal_ratio=normal_ratio)
    other_record = record_transformations(df, 
                                          transformations["two args"], 
                                          extremum,
                                          normal_ratio=normal_ratio)
    merge_results!(record, other_record)
end

"""
    record_transformations(df::AbstractDataFrame,
                           transformations::AbstractVector[, extremum::Real];
                           normal_ratio::Real=2) -> AbstractDict


"""
function record_transformations(df::AbstractDataFrame, transformations::AbstractVector;
                                normal_ratio::Real=2)
    record = Dict(
        "normal" => Dict(),
        "df_transformed" => DataFrame(),
    )
    for transformation in transformations
        results = apply_transformation(transformation, df)
        update_normal!(record, results, normal_ratio)
        merge_cols!(record, results)
    end
    record
end

function record_transformations(df::AbstractDataFrame, transformations::AbstractVector,
                                extremum::Real; normal_ratio::Real=2)
    record = Dict(
        "normal" => Dict(),
        "df_transformed" => DataFrame(),
    )
    for transformation in transformations
        results = apply_transformation(transformation, df, extremum)
        update_normal!(record, results, normal_ratio)
        merge_cols!(record, results)
    end
    record
end

"""
    apply_transformation(f::Function, df::AbstractDataFrame
                        [, f_second_arg::Real]) -> AbstractDict

Apply `f` to `df`, and return the skewness, kurtosis, and transformed data frame.
"""
function apply_transformation(f::Function, df::AbstractDataFrame)
    df_altered = rename(colname -> "$(colname)-+", df)
    select!(df_altered, 
            names(df_altered) .=> ByRow(f))
    rename!(colname -> rename_with_function(f, colname), df_altered)
    skew = calculate_skewness.(eachcol(df_altered))
    kurt = calculate_kurtosis.(eachcol(df_altered))
    Dict(
        "skewness" => skew,
        "kurtosis" => kurt,
        "df_transformed" => df_altered,
    )
end

function apply_transformation(f::Function, df::AbstractDataFrame, f_second_arg::Real)
    df_altered = rename(colname -> "$(colname)-+", df)
    select!(df_altered, 
            names(df_altered) .=> ByRow(x -> f(x, f_second_arg)))
    rename!(colname -> rename_with_function(f, colname), df_altered)
    skew = calculate_skewness.(eachcol(df_altered))
    kurt = calculate_kurtosis.(eachcol(df_altered))
    Dict(
        "skewness" => skew,
        "kurtosis" => kurt,
        "df_transformed" => df_altered,
    )
end

"""
    rename_with_function(f::Function, name::AbstractString) -> AbstractString

Create a string with `name` and `f`.
"""
function rename_with_function(f::Function, name::AbstractString)
    marker_before_function = findlast("-+_", name)[1]
    end_of_original_colname = marker_before_function - 1
    old_name = view(name, 1:end_of_original_colname)
    # change if double underscore is used in original colname
    marker = "__"
    function_name = string(f)
    old_name * marker * function_name
end

"""
    update_normal!(d::AbstractDict, other::AbstractDict, normal_ratio::Real=2)

Add the results from `other` to `d` whose ratios are within the range of ±`normal_ratio`.
"""
function update_normal!(d::AbstractDict, other::AbstractDict, normal_ratio::Real=2)
    skews = other["skewness"]
    kurts = other["kurtosis"]
    if are_normal(skews, kurts, normal_ratio)
        df_altered = other["df_transformed"]
        transformations = get_applied_transformations(df_altered)
        d["normal"][transformations] = label_findings(df_altered, skews, kurts)
    end
    nothing
end

"""
    get_applied_transformations(df::AbstractDataFrame) -> SubString

Return the transformations that were applied to `df`.
"""
function get_applied_transformations(df::AbstractDataFrame)
    new_colname = names(df)[1]
    before_marker = locate_end_of_original_colname(new_colname)
    after_marker = before_marker + 3
    function_names = SubString(new_colname, after_marker)
    make_legible(function_names)
end

"""
    make_legible(fragment::AbstractString) -> AbstractString


"""
function make_legible(fragment::AbstractString)
    words = make_phrase(fragment, "_", " ")
    listable = make_list_phrase(words)
    make_phrase(listable, "  ", "; ")
end

"""
    make_phrase(fragment::AbstractString, split_on::AbstractString,
                join_with::AbstractString) -> AbstractString


"""
function make_phrase(fragment::AbstractString, split_on::AbstractString, 
                     join_with::AbstractString)
    words = split(fragment, split_on)
    join(words, join_with)
end

"""
    make_list_phrase(phrase::AbstractString) -> AbstractString


"""
function make_list_phrase(phrase::AbstractString)
    words = split(phrase, " then ")
    if length(words) ≤ 2
       join(words, " and ")
    else
        join(words, ", ", ", and ")
    end
end

"""
    locate_end_of_original_colname(name::AbstractString) -> Integer

Return the index in `name` that is the last character of the original column name.
"""
function locate_end_of_original_colname(name::AbstractString)
    # change if double underscore is used in original colname
    marker_after_old_colname = "__"
    position = findfirst(marker_after_old_colname, name)[1]
    position - 1
end

"""
    label_findings(df::AbstractDataFrame, skewnesses::AbstractVector,
                   kurtoses::AbstractVector) -> AbstractVector


"""
function label_findings(df::AbstractDataFrame, skewnesses::AbstractVector,
                       kurtoses::AbstractVector)
    combined = merge.(skewnesses, kurtoses)
    original_colnames = get_original_colnames(df)
    add_label(combined, original_colnames)
end

"""
    get_original_colnames(df::AbstractDataFrame) -> AbstractVector

Return the original column names of `df`.
"""
function get_original_colnames(df::AbstractDataFrame)
    [view(colname, 1:locate_end_of_original_colname(colname))
     for colname in names(df)]
end

"""
    add_label(ratios::AbstractVector, names::AbstractVector) -> AbstractVector


"""
function add_label(ratios::AbstractVector, names::AbstractVector)
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
    merge_cols!(d::AbstractDict, other::AbstractDict) -> AbstractDataFrame

Concatenate `df` and a data frame from `results`.
"""
function merge_cols!(d::AbstractDict, other::AbstractDict)
    d["df_transformed"] = hcat(d["df_transformed"], other["df_transformed"])
end

"""
    merge_results!(d::AbstractDict, other::AbstractDict) -> AbstractDict


"""
function merge_results!(d::AbstractDict, other::AbstractDict)
    merge!(d["normal"], other["normal"])
    merge_cols!(d, other)
    d
end

"""
    get_error_positive_skew_transformations(error::Exception, min::Real) -> AbstractDict

Return positive skew transformations for the data set based on `error` and `min`.
"""
function get_error_positive_skew_transformations(error::Exception, min::Real)
    if isa(error, DivideError)
        if min < 1
            Dict(
                "one arg" => [],
                "two args" => [
                    add_then_square_root,
                    add_then_log_base_10,
                    add_then_natural_log,
                ],
            )
        else
            Dict(
                "one arg" => [
                    square_root,
                    log_base_10,
                    natural_log,
                ],
                "two args" => [],
            )
        end
    end
end

"""
    are_negative_skew(skewnesses::AbstractVector) -> Bool

Return `true` if `skewnesses` contains any negative ratios, and `false` otherwise.
"""
function are_negative_skew(skewnesses::AbstractVector)
    contains_skew(is_negative_skew, skewnesses)
end

"""
    is_negative_skew(skewness_ratio::Real) -> Bool

Return `true` if `skewness_ratio` is negative, and `false` otherwise.
"""
function is_negative_skew(skewness_ratio::Real)
    skewness_ratio < 0
end

"""
    transform_negative_skew(df::AbstractDataFrame, normal_ratio::Real=2) -> AbstractDict


"""
function transform_negative_skew(df::AbstractDataFrame, normal_ratio::Real=2)
    largest = get_extremum(maximum, df)
    negative_transformations = Dict(
        "one arg" => [
            square,
            cube,
            antilog,
        ],
        "two args" => [
            reflect_then_square_root,
            reflect_then_log_base_10,
            reflect_then_invert,
        ],
    )
    record_all_transformations(df, 
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
    reflect_then_square_root(x::Real, max::Real)

Add one to the difference of `max` and `x`, and return its square root.

Throws error if `max < x`.
"""
function reflect_then_square_root(x::Real, max::Real)
    if max < x
        throw(error("max must be greater."))
    end
    √(max + 1 - x)
end

"""
    reflect_then_log_base_10(x::Real, max::Real)

Add one to the difference of `max` and `x`, and compute its logarithm to base 10.

Throws error if `max < x`.
"""
function reflect_then_log_base_10(x::Real, max::Real)
    if max < x
        throw(error("max must be greater."))
    end
    log_base_10(max + 1 - x)
end

"""
    reflect_then_invert(x::Real, max::Real)

Add one to the difference of `max` and `x`, and compute its reciprocal.

Throws error if `max < x`.
"""
function reflect_then_invert(x::Real, max::Real)
    if max < x
        throw(error("max must be greater."))
    end
    invert(max + 1 - x)
end

"""
    get_error_negative_skew_transformations(error::Exception) -> AbstractDict

Return negative skew transformations for the data set based on `error` and `max`.
"""
function get_error_negative_skew_transformations(error::Exception)
    if isa(error, DomainError)
        Dict(
            "one arg" => [
                square,
                cube,
                antilog,
            ],
            "two args" => [
                reflect_then_invert,
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
                reflect_then_square_root,
                reflect_then_log_base_10,
            ],
        )
    end
end

"""
    stretch_skew(df::AbstractDataFrame, normal_ratio::Real=2) -> AbstractDict


"""
function stretch_skew(df::AbstractDataFrame, normal_ratio::Real=2)
    stretch_transformations = [
        add_then_logit,
        logit,
    ]
    if contains_invalid_for_add_then_logit(df)
        popfirst!(stretch_transformations)
    end
    if contains_invalid_for_logit(df)
        pop!(stretch_transformations)
    end
    record_transformations(df, stretch_transformations, normal_ratio=normal_ratio)
end

"""
    add_then_logit(x::Real)

Increase `x` by `0.25`, and compute the logit in base 10.

Throws `DomainError` if `x` = -0.25, and `DivideError` if `x` = 0.75.
"""
function add_then_logit(x::Real)
    logit(x + 0.25)
end

"""
    logit(x::Real)

Compute the logit of `x` to base 10.

Throws `DomainError` if `x` = 0, and `DivideError` if `x` = 1.
"""
function logit(x::Real)
    log_base_10(abs(x * invert(1 - x)))
end

"""
    contains_invalid_for_add_then_logit(df::AbstractDataFrame) -> Bool


"""
function contains_invalid_for_add_then_logit(df::AbstractDataFrame)
    contains_predicated(is_invalid_for_add_then_logit, df)
end

"""
    contains_predicated(f::Function, df::AbstractDataFrame
                       [, f_second_arg::Real]) -> Bool

Return `true` if `f` returns any values from `df`, and false otherwise.
"""
function contains_predicated(f::Function, df::AbstractDataFrame)
    filtered_cols = get_filtered_cols(f, df)
    sum(length.(filtered_cols)) > 0
end

function contains_predicated(f::Function, df::AbstractDataFrame, f_second_arg::Real)
    filtered_cols = get_filtered_cols(f, df, f_second_arg)
    sum(length.(filtered_cols)) > 0
end

"""
    is_invalid_for_add_then_logit(x::Real) -> Bool


"""
function is_invalid_for_add_then_logit(x::Real)
    x === -0.25 || x === 0.75
end

"""
    contains_invalid_for_logit(df::AbstractDataFrame) -> Bool

Return `true` if `0` or `1` exists in `df`, and `false` otherwise.
"""
function contains_invalid_for_logit(df::AbstractDataFrame)
    contains_predicated(is_invalid_for_logit, df)
end

"""
    is_invalid_for_logit(x::Real) -> Bool

Return `true` if `x` is 0 or 1.
"""
function is_invalid_for_logit(x::Real)
    iszero(x) || isone(x)
end

"""
    print_skewness_kurtosis(df::AbstractDataFrame)

Print the skewness and kurtosis (statistic, standard error, ratio) of each column in `df`.
"""
function print_skewness_kurtosis(df::AbstractDataFrame)
    for colname in names(df)
        skew = calculate_skewness(df[!, colname])
        kurt = calculate_kurtosis(df[!, colname])
        label = (
            name = colname,
        )
        combined = merge(skew, kurt, label)
        print_summary(combined)
    end
end

"""
    print_summary(sk::NamedTuple)


"""
function print_summary(sk::NamedTuple)
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
    string_to_float!(df::AbstractDataFrame) -> AbstractDataFrame

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

julia> string_to_float!(df)
2×2 DataFrame
 Row │ a      b
     │ Int64  Float64
─────┼────────────────
   1 │     1      4.0
   2 │     2      5.2
```
"""
function string_to_float!(df::AbstractDataFrame)
    for colname in names(df)
        col = df[!, colname]
        if eltype(col) === String
            blank_to_nan!(col)
            df[!, colname] = parse.(Float64, col)
        end
    end
    df
end

"""
    blank_to_nan!(col::AbstractVector) -> AbstractVector

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
function blank_to_nan!(col::AbstractVector)
    replace!(col, " " => "NaN")
end

"""
    print_findings(results::AbstractVector)


"""
function print_findings(results::AbstractVector)
    try
        normal = results[1]["normal"]
        for (transformation, altered) in normal
            println("APPLIED: $transformation")
            print_summary.(altered)
            println("\n")
        end
    catch e
        println("Unable to normalize.")
    end
end

end
