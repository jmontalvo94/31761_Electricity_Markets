## Import packages

using CSV
using DataFrames
using Dates
using DelimitedFiles
using HTTP
using Plots

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
        for (i, str) in enumerate(df[:, col])
            if str == weird_str
                df[i, col] = normal_str
            end
        end
    end
    return df
end

function convert_str_to_int!(df::DataFrame, cols::Tuple{Integer,Integer,Integer})
    for col in cols
        df[!, col] = parse.(Int64, df_forecast[:, col])
    end
    return df
end

function convert_int_to_date!(df::DataFrame, cols::Tuple{Integer,Integer})
    format = DateFormat("yyyymmddHHMM")
    for col in cols
        df[!, col] = DateTime.(string.(df[:, col]), format)
    end
    return df
end

function read_to_dfs(files::Array{String,1})
	dfs = Array{DataFrame,1}(undef,4)
	for (i, file) in enumerate(files)
	    if file[1:6] == "elspot"
			df = DataFrame(CSV.File(file; header=3, datarow=4))
		    df = rename!(df, :1 => "Date")
	        df = select!(df, [:Date, :Hours, :DK1])
			dfs[i] = df
		else
			df = DataFrame(CSV.File(file; header=4, datarow=5))
		    df = rename!(df, :1 => "Date")
	        df = select!(df, :Date, :Hours, :23, :24)
			dfs[i] = df
	    end
	end
	return dfs[1], dfs[2], dfs[3], dfs[4]
end

function adjust_DST_March!(dfs::Array{DataFrame,1})
	for df in dfs
		missing_idx = findfirst(ismissing, df[!, end])
		df[missing_idx, end] = df[missing_idx + 1, end]
		if length(names(df)) > 3
			df[missing_idx, end - 1] = df[missing_idx + 1, end - 1]
		end
		dropmissing!(df)
	end
	return dfs
end

function adjust_DST_October!(dfs::Array{DataFrame,1})
	for df in dfs
		unique!(df, [:Date, :Hours])
	end
	return dfs
end

function adjust_date!(dfs::Array{DataFrame,1})
	for df in dfs
		date_new = fill(Date("1994-01-01"), size(df, 1))
	    for (i, date_old) in enumerate(df.Date)
			try
				date_new[i] = Date(date_old, "dd/mm/yyyy")
			catch
				date_new[i] = Date(date_old, "dd-mm-yyyy")
			end
		end
		df.Date = date_new
	end
	return dfs
end

function adjust_hours!(dfs::Array{DataFrame,1})
	for df in dfs
		hours_new = fill(0, size(df.Hours, 1))
		for (i, hours_old) in enumerate(df.Hours)
			hours_new[i] = parse(Int64, hours_old[1:2]) + 1
		end
		df.Hours = hours_new
		insertcols!(df, 1, :Datetime => DateTime.(df.Date, Time.(df.Hours .- 1)))
	end
	return dfs
end

function convert_str_to_int!(dfs::Array{DataFrame,1})
    for df in dfs
		temp = replace.(df[:, end], "," => ".")
        df[!, end] = parse.(Float64, temp)
		if length(names(df)) > 4
			temp = replace.(df[:, end - 1], "," => ".")
			df[!, end - 1] = parse.(Float64, temp)
		end
    end
    return dfs
end

## Data loading and cleaning

# Fetch forecast data from URL and change column name for q10
forecast_url = "http://pierrepinson.com/31761/Assignments/windpowerforecasts.dat"
df_forecast = get_file_from(forecast_url, "NA")
rename!(df_forecast, Symbol("10") => :q10)

#=
Missing values:
There are 122 missing actual power measurements and 238 missing deterministic power
forecasts and quantiles; these are removed.
=#
dropmissing!(df_forecast)

#=
Dataset conversions:
There are four "1e+05" values in the dataset that are saved as Strings; these are changed
and the columns converted to Int. Then dato and dati are converted to DateTime.
=#
if (sum(String .== eltype.(eachcol(df_forecast))) > 0) # If at least 1 String column type
	replace_string!(df_forecast, "1e+05", "100000", (6,16,24))
	convert_str_to_int!(df_forecast, (6,16,24))
end
convert_int_to_date!(df_forecast, (1,2))

# Fetch market data from CSVs and load to dfs
files = ["elspot-prices_2016_hourly_eur.csv","elspot-prices_2017_hourly_eur.csv",
	"regulating-prices_2016_hourly_eur.csv","regulating-prices_2017_hourly_eur.csv"]
df_elspot16, df_elspot17, df_reg16, df_reg17 = read_to_dfs(files)

#=
DST timezone change:
There's a missing value when there's a DST change at hours 2 - 3 of March 26, 2016 and
March 27, 2017; the value of the next hour 3 - 4 is copied instead of the missing value.
Also, there's a repeted value with the reverse change at hours 2 - 3 of October 30, 2016
and October 29, 2017; one of the repeated dates was removed from the dataset.
=#
adjust_DST_March!([df_elspot16, df_elspot17, df_reg16, df_reg17])
adjust_DST_October!([df_elspot16, df_elspot17, df_reg16, df_reg17])

#=
Dataset conversions:
Column Date of each DataFrame is converted to Date type, and columns Hours, Up_10, and
Down_10 are converted to Integer.
=#
adjust_date!([df_elspot16, df_elspot17, df_reg16, df_reg17])
adjust_hours!([df_elspot16, df_elspot17, df_reg16, df_reg17])
convert_str_to_int!([df_elspot16, df_elspot17, df_reg16, df_reg17])

# Merge of market data DataFrames and clean variables
df_market16 = join(df_elspot16, df_reg16, on=[:Datetime, :Date, :Hours])
df_market17 = join(df_elspot17, df_reg17, on=[:Datetime, :Date, :Hours])
df_elspot16, df_elspot17, df_reg16, df_reg17 = [nothing for _ = 1:4]

# Create clean DateTime indexes to compare with datasets
dt_16 = collect(DateTime(2016,1,1,0,0,0):Hour(1):DateTime(2016,12,31,23,0,0))
dt_17 = collect(DateTime(2017,1,1,0,0,0):Hour(1):DateTime(2017,12,31,23,0,0))

# Sanity-check
dt_16 == df_market16.Datetime
dt_17 == df_market17.Datetime
# unique dates on dato when year is 2017
forecast_dato = unique(filter(row -> year(row[:dato]) == 2017, df_forecast).dato)
missing_dato = setdiff(dt_17, forecast_dato)


## Analysis

# Forecast
describe(df_forecast, :mean, :std, :min, :median, :max, cols=Not([:dato, :dati, :hors]))

# Market analysis of 2016
filter(row -> row[:DK1] == row[:Up_10] == row[:Down_10], df_market16) # Balance
filter(row -> row[:DK1] == row[:Up_10] != row[:Down_10], df_market16) # Down-regulation
filter(row -> row[:DK1] == row[:Down_10] != row[:Up_10], df_market16) # Up-regulation
filter(row -> row[:DK1] != row[:Down_10] && row[:DK1] != row[:Up_10], df_market16) # Both


## Bidding

# Base

# Deterministic
I=collect(1:length(dt_17))
revenue_dayahead = sum(df_forecast.fore[i]*df_market17.DK1[i] for i in I)
revenue_balancing = zeros(length(dt_17))
for i in I
	if df_market17.DK1[i]==df_market17.Up_10[i]==df_market17.Down_10[i]
		revenue_balancing[i]=(df_forecast.meas[i]-df_forecast.fore[i])*df_market17.DK1[i]
	elseif df_market17.DK1[i]==df_market17.Up_10[i]!=df_market17.Down_10[i]
		revenue_balancing[i]=(df_forecast.meas[i]-df_forecast.fore[i])*df_market17.Down_10[i]
	elseif df_market17.DK1[i]==df_market17.Down_10[i]!=df_market17.Up_10[i]
		revenue_balancing[i]=(df_forecast.fore[i]-df_forecast.meas[i])*df_market17.Up_10[i]
	elseif df_market17.DK1[i]!=df_market17.Down_10[i] && df_market17.DK1[i]!=df_market17.Up_10[i]
		revenue_balancing[i]=(df_forecast.meas[i]-df_forecast.fore[i])*df_market17.Down_10[i]-(df_forecast.fore[i]-df_forecast.meas[i])*df_market17.Up_10[i]
	end
end
revenue_det=revenue_dayahead+sum(revenue_balancing)

plot(1:24, df_forecast.fore[1:24],label = "forecast")
plot!(1:24, df_forecast.meas[1:24],label = "real")

# Probabilistic
