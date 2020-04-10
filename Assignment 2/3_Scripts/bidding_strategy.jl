## Import packages

using CSV
using DataFrames
using Dates
using DelimitedFiles
using HTTP

## Define functions

function get_file_from(url::String, missing_str::String)
    file = HTTP.get(url)
    df = DataFrame(CSV.File(file.body; missingstring = missing_str))
    return df
end

function replace_string!(
    df::DataFrame,
    weird_str::String,
    normal_str::String,
    cols::Tuple{Integer,Integer,Integer}
)
    for col in cols
        for (i, str) in enumerate(df[!,col])
            if str == weird_str
                df[i,col] = normal_str
            end
        end
    end
    return df
end

function convert_str_to_int!(df::DataFrame, cols::Tuple{Integer,Integer,Integer})
    for col in cols
        df[!,col] = parse.(Int64, df_forecast[:,col])
    end
    return df
end

function convert_int_to_date!(df::DataFrame, cols::Tuple{Integer,Integer})
    format = DateFormat("yyyymmddHHMM")
    for col in cols
        df[!,col] = DateTime.(string.(df[!,col]), format)
    end
    return df
end


## Load data and clean

# Fetch forecast data from URL
forecast_url = "http://pierrepinson.com/31761/Assignments/windpowerforecasts.dat"
df_forecast = get_file_from(forecast_url, "NA")

# Drop rows with missing values
dropmissing!(df_forecast)

# Replace string "1e+05" to "100000"
replace_string!(df_forecast, "1e+05", "100000", (6,16,24))

# Convert types
convert_str_to_int!(df_forecast, (6,16,24))
convert_int_to_date!(df_forecast, (1,2))
