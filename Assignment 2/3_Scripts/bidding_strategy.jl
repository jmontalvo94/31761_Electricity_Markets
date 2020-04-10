## Import packages

using DataFrames
using Dates
using DelimitedFiles

## Define functions

function get_file_from(url::String, missing_str::String)
    file = HTTP.get(url)
    buffer = IOBuffer(file.body)
    df = CSV.read(buffer, missingstring=missing_str)
    return df
end

function replace_string!(df::DataFrame, weird::String, actual::Int64,
    cols::Tuple{Int64,Int64,Int64})
    for col in cols
        df[!,col] = replace(df[!,col], weird => actual)
    end
end

## Load raw data

# Fetch forecast data from URL
forecast_url = "http://pierrepinson.com/31761/Assignments/windpowerforecasts.dat"
df_forecast = get_file_from(forecast_url, "NA")

# Drop missing values and replace "1e+05" to 100000,
dropmissing!(df_forecast; disallowmissing=true)
replace_string!(df_forecast, "1e+05", 100000, (6,16,24))

# Define data types and convert columns
forecast_types = reduce(vcat,(fill(Date,2),fill(Int64,22)))
