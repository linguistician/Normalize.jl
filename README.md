# Normalize

[![Code Style: Blue](https://img.shields.io/badge/code%20style-blue-4495d1.svg)](https://github.com/invenia/BlueStyle)
[![Build Status](https://travis-ci.com/quanteconuser/Normalize.jl.svg?branch=master)](https://travis-ci.com/quanteconuser/Normalize.jl)
[![Coverage](https://codecov.io/gh/quanteconuser/Normalize.jl/branch/master/graph/badge.svg)](https://codecov.io/gh/quanteconuser/Normalize.jl)

This tool provides the skewness and kurtosis details of a dataset, and if the dataset is nonnormal within a desired range (default ±2), separate transformations are applied to attempt once to normalize it.

## Info

Skewness and kurtosis details are listed as their statistic, standard error, and ratio. The available transformations for normalizations are those concerning positive skew, negative skew, and stretch skew, which are detailed in the notebook. Procedures of loading and transforming the dataset are also in the notebook.

## Setup

### Notebook

Download the project through Code, or through [Git](https://git-scm.com/downloads) in the command line:

```
$ git clone https://github.com/muhadamanhuri/Normalize.jl.git
```

To open Normalize.ipynb in the notebook folder, run [Julia](https://julialang.org/downloads/) in the [command line](https://julialang.org/downloads/platform/), add IJulia, and use the `notebook` function.

```
$ cd path/of/project/folder

$ cd ..

$ julia

# If downloaded through Code
julia> import Pkg; Pkg.activate("Normalize.jl-main")

# If downloaded through Git
julia> import Pkg; Pkg.activate("Normalize.jl")

julia> Pkg.add("IJulia")

julia> using IJulia; notebook(dir=pwd())
```

Navigate to the folder and then the file.

Click the code cells, and click Run.

### Module

If you want only the module, add the package in Julia. It is recommended to create a project environment beforehand.

```
julia> import Pkg; Pkg.activate("project name")

julia> Pkg.add(url="https://github.com/muhadamanhuri/Normalize.jl")

julia> using Normalize

# Excluding the first line, displays all function names in the module.
julia> for name in names(Normalize); println(name); end

# Gets info of a function.
julia> ?<function name>
```

## Example

```
julia> using Normalize, DataFrames

julia> df = DataFrame(a=[-0.5, 2.47, 2.54, 2.91, 3.13]);

julia> print_skewness_kurtosis(df)
--------
Variable: a
Skewness Statistic: -2.051
Skewness Std. Error: 0.913
Skewness Ratio: -2.246

Kurtosis Statistic: 4.364
Kurtosis Std. Error: 2.0
Kurtosis Ratio: 2.182

julia> results = normalize(df); print_findings(results)
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

## Todos

- Docstring examples
- Function to normalize multiple times
- Optimize performance
- Overflow warning
- Update tests

## Contact

For any questions, comments, or concerns:

- Email: mredamanhuri@gmail.com
