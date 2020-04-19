## Import packages

using CSV
using DataFrames
using DataFramesMeta
using Dates
using DelimitedFiles
using HTTP
using Plots
using Statistics

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

function count_hours(dfs::Array{DataFrame,1})
	df_counts = Array{DataFrame,1}(undef,length(dfs))
	for (i, df) in enumerate(dfs)
		df_counts[i] = @linq df |>
		transform(Month = month.(:Datetime)) |>
		by(
		[:Month, :Hours],
		N = length(:DK1)
		)
	end
	return df_counts
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


## Insights from 2016

# Hourly profile per month from 2016 market data
price_profile = @linq df_market16 |>
	transform(Month = month.(:Datetime)) |>
	by(
	[:Month, :Hours],
	DK1_mean = mean(:DK1), Up_10_mean = mean(:Up_10), Down_10_mean = mean(:Down_10)
)
# Plot monthly hourly price profile (monthly average per hour)
plot(
	#DateTime.(Date.(2016, mhourly_profile.Month), Time.(mhourly_profile.Hours.-1)),
	[price_profile.Up_10_mean, price_profile.DK1_mean, price_profile.Down_10_mean],
	label = ["Up-regulation" "Spot" "Down-regulation"],
	xlabel = "Hour",
	ylabel = "EUR/MWh",
)

#=
DataFrames that hold the hours of 2016 where the system was in balance, when there was a
need of up-regulation, down-regulation, or both. These DataFrames are then combined to only
represent the count of hours with these four states.
=#
df_balance16 = filter(row -> row[:DK1] == row[:Up_10] == row[:Down_10], df_market16)
df_down16 = filter(row -> row[:DK1] == row[:Up_10] != row[:Down_10], df_market16)
df_up16 = filter(row -> row[:DK1] == row[:Down_10] != row[:Up_10], df_market16)
df_updown16 = filter(row -> row[:DK1] != row[:Down_10] && row[:DK1] != row[:Up_10], df_market16)
# Count per DataFrame
df_counts16 = count_hours([df_balance16, df_down16, df_up16, df_updown16])
# Clean dataframe with all months and hours
df_cleancounts16 = DataFrame(Month = month.(dt_16), Hours = hour.(dt_16).+1)
# Join each state DataFrame into clean, outer join!
df_insight16 = join(
	df_cleancounts16,
	df_counts16[1], df_counts16[2], df_counts16[3], df_counts16[4],
	on = [:Month, :Hours],
	kind = :outer,
	makeunique = true
)
# Change missing values to 0
for col in 3:6
	df_insight16[!, col] = coalesce.(df_insight16[:, col], 0)
end
# Drop missing labels (no missing values in reality)
dropmissing!(df_insight16)
# Rename columns to represent states
rename!(df_insight16, Dict(:N => "Balance", :N_1 => "Down", :N_2 => "Up", :N_3 => "Both"))

# DataFrame with average occurences per month per hour of each state
df_insight16 = by(
	df_insight16,
	[:Month, :Hours],
	Down = :Down => sum,
	Balance = :Balance => sum,
	Up = :Up => sum,
	Both = :Both => sum
)
plot(
	#DateTime.(Date.(2016, df_insight16.Month), Time.(df_insight16.Hours.-1)),
	[
	df_insight16.Down,
	df_insight16.Balance,
	df_insight16.Up,
	df_insight16.Both
	],
	label = ["Down-regulation" "Balance" "Up-regulation" "Both"],
	xlabel = "Month/Hour",
	ylabel = "Occurences",
)

df_november16 = filter(row -> row[:Month] == 11, df_insight16)
plot(
	[
	df_november16.Down,
	df_november16.Balance,
	df_november16.Up,
	df_november16.Both
	],
	label = ["Down-regulation" "Balance" "Up-regulation" "Both"],
	xlabel = "Month/Hour",
	ylabel = "Occurences",
)

state = Array{Int64}(undef, size(df_insight16)[1])
for i in range(1, length = size(df_insight16)[1])
	if df_insight16.Down[i] > df_insight16.Balance[i]
		if df_insight16.Down[i] > df_insight16.Up[i]
			state[i] = 1 # Down-regulation
		else
			state[i] = 2 # Up-regulation
		end
	elseif df_insight16.Balance[i] > df_insight16.Up[i]
		state[i] = 3 # Balance
	else
		state[i] = 2 # Up-regulation
	end
end
insertcols!(df_insight16, 7, :State => state)

histogram(state, xticks=(1:3, ["Down-regulation" "Up-regulation" "Balance"]))


## Base



## Deterministic

## Probabilistic

# Forecast descriptive statistics
describe(df_forecast, :mean, :std, :min, :median, :max, cols=Not([:dato, :dati, :hors]))
