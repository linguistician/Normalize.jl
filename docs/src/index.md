# Functions

```@index

```

## Statistics

```@docs
skewness_stat(col, round_to::Integer=3)
skewness_variance(col)
skewness_error(col, round_to::Integer=3)
skewness(col, round_to::Integer=3)

kurtosis_stat(col, round_to::Integer=3)
kurtosis_variance(col)
kurtosis_error(col, round_to::Integer=3)
kurtosis(col, round_to::Integer=3)
```

## Comparisons

```@docs
is_normal(
  skewness_ratio::Real, kurtosis_ratio::Real;normal_ratio::Real=2
)
are_normal(skew_kurt_ratios; normal_ratio::Real=2)

is_positive_skew(skewness_ratio::Real)
are_positive_skews(skewnesses)

is_negative_skew(skewness_ratio::Real)
are_negative_skews(skewnesses)
```

## Formulae

### Positive Skew

```@docs
Normalize.square_root
Normalize.add_n_square_root
Normalize.invert
Normalize.add_n_invert
Normalize.square_n_invert
Normalize.square_n_add_n_invert
Normalize.square_root_n_invert
Normalize.add_n_square_root_n_invert
Normalize.square_root_n_add_n_invert
Normalize.log_base_10
Normalize.add_n_log_base_10
Normalize.natural_log
Normalize.add_n_natural_log
```

### Negative Skew

```@docs
Normalize.square
Normalize.cube
Normalize.antilog
Normalize.reflect_n_invert
Normalize.reflect_n_square_root
Normalize.reflect_n_log_base_10
```

### Stretch Skew

```@docs
Normalize.logit
Normalize.add_n_logit
```

## Applicable Transformations

```@docs
get_positive_skew_transformations(df)
get_negative_skew_transformations(df)
get_stretch_skew_transformations(df)
get_skew_transformations(gdf; normal_ratio::Real=2)
```

## Transforming Data

```@docs
apply
record
record_all(
  gdf, transform_series; normal_ratio::Real=2, marker::AbstractString="__"
)
normalize(
  gdf; normal_ratio::Real=2, dependent::Bool=false, marker::AbstractString="__"
)
```

## Displaying Info

```@docs
print_skewness_kurtosis
print_findings(findings)
```

## DataFrame and File Conversions

```@docs
tabular_to_dataframe(path, sheet::AbstractString="")
normal_to_csv(path, findings; dependent::Bool=false)
```

## Cleaning Data

```@docs
replace_blank!(df, colname; new_value::Real)
replace_missing!(col, new_value::Real)
```
