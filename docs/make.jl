push!(LOAD_PATH, "../src/")

using Documenter, Normalize

# Check docstring examples
DocMeta.setdocmeta!(Normalize, :DocTestSetup, :(using Normalize, DataFrames); recursive=true)

makedocs(modules=[Normalize], sitename="Normalize.jl")

deploydocs(repo="https://github.com/muhadamanhuri/Normalize.jl.git")