using PyCall

const PACKAGES = ["xlrd"]

try
    @pyimport pip
catch
    get_pip = joinpath(dirname(@__FILE__), "get-pip.py")
    download("https://bootstrap.pypa.io/pip/3.5/get-pip.py", get_pip)
    run(`$(PyCall.python) $get_pip --user`)
end

@pyimport pip
args = UTF8String[]
if haskey(ENV, "http_proxy")
    push!(args, "--proxy")
    push!(args, ENV["http_proxy"])
end
push!(args, "install")
push!(args, "--user")
append!(args, PACKAGES)

pip.main(args)