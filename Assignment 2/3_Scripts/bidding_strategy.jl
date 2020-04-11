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

function read_to_dfs(files::Array{String,1})
	dfs = Array{DataFrame,1}(undef,4)
	for (i, file) in enumerate(files)
	    if file[1:6] == "elspot"
			df = DataFrame(CSV.File(file, header=3, datarow=4, decimal=','))
		    df = rename!(df, :1 => "Date")
	        df = select!(df, [:Date, :Hours, :DK1])
			dfs[i] = df
		else
			df = DataFrame(CSV.File(file, header=4, datarow=5, decimal=','))
		    df = rename!(df, :1 => "Date")
	        df = select!(df, :Date, :Hours, :23, :24)
			dfs[i] = df
	    end
	end
	return dfs[1], dfs[2], dfs[3], dfs[4]
end

function adjust_date!(dfs::Array{DataFrame,1})
	for df in dfs
		newDate = fill(Date("1994-01-01"), size(df, 1))
	    for (i, oldDate) in enumerate(df.Date)
			try
				newDate[i] = Date(oldDate, "dd/mm/yyyy")
			catch
				newDate[i] = Date(oldDate, "dd-mm-yyyy")
			end
		end
		select!(df, Not(:Date)) # Remove old date
		insertcols!(df, 1, :Date => newDate) # Insert new date
		insertcols!(df, 2, :Year => year.(newDate))
		insertcols!(df, 3, :Month => month.(newDate))
		insertcols!(df, 4, :Day => day.(newDate))
	end
end

function adjust_hours!(dfs::Array{DataFrame,1})
	for df in dfs
		newHour = fill(0, size(df.Hours,1))
		for (i, oldHour) in enumerate(df.Hours)
			newHour[i] = parse(Int64, oldHour[1:2]) + 1
		end
		select!(df, Not(:Hours)) # Remove old hour
		insertcols!(df, 5, :Hour => newHour) # Insert new hour
	end
end


## Load data and clean

# Fetch forecast data from URL
forecast_url = "http://pierrepinson.com/31761/Assignments/windpowerforecasts.dat"
df_forecast = get_file_from(forecast_url, "NA")

# Cleaning
dropmissing!(df_forecast)
replace_string!(df_forecast, "1e+05", "100000", (6,16,24))
convert_str_to_int!(df_forecast, (6,16,24))
convert_int_to_date!(df_forecast, (1,2))

# File names to be fetched
files = ["elspot-prices_2016_hourly_eur.csv","elspot-prices_2017_hourly_eur.csv",
	"regulating-prices_2016_hourly_eur.csv","regulating-prices_2017_hourly_eur.csv"]

# Read files into separate DataFrames
df_elspot16, df_elspot17, df_reg16, df_reg17 = read_to_dfs(files)

# Check for missing
for df in [df_elspot16, df_elspot17]
	println(filter(row -> ismissing(row[:DK1]), df))
end
for df in [df_reg16, df_reg17]
	println(filter(row -> ismissing(row[:Up_10]), df))
	println(filter(row -> ismissing(row[:Down_10]), df))
end

# Cleaning
adjust_date!([df_elspot16, df_elspot17, df_reg16, df_reg17])
adjust_hours!([df_elspot16, df_elspot17, df_reg16, df_reg17])
for df in [df_elspot16, df_elspot17, df_reg16, df_reg17]
	dropmissing!(df)
end
