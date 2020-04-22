## Import packages

using CSV
using DataFrames
using DataFramesMeta
using Dates
using HTTP
using Interpolations
using LaTeXStrings
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

function create_bid(series::Array{Int64,1}, arr::Vector{Float64})
	bid = []
	j = 1
	for i in 1:24:size(series, 1)
		bid = vcat(bid, repeat(series[i:i+23], Int(arr[j])))
		j += 1
	end
	return bid
end

function create_bid(series::Array{Float64,1}, arr::Vector{Float64})
	bid = []
	j = 1
	for i in 1:24:size(series, 1)
		bid = vcat(bid, repeat(series[i:i+23], Int(arr[j])))
		j += 1
	end
	return bid
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
dt_16 == df_market16.Datetime # ok
dt_17 == df_market17.Datetime # ok
# unique dates on dato when year is 2017
forecast_dato = unique(filter(row -> year(row[:dato]) == 2017, df_forecast).dato)
missing_dato = setdiff(dt_17, forecast_dato) # missing forecast dates


## Insights from 2016

# Hourly profile per month from 2016 market data
price_profile = @linq df_market16 |>
	transform(Month = month.(:Datetime)) |>
	by(
	[:Month, :Hours],
	DK1_mean = mean(:DK1), Up_10_mean = mean(:Up_10), Down_10_mean = mean(:Down_10)
)
# Monthly hourly price profile (monthly average per hour)
plot(
	#DateTime.(Date.(2016, mhourly_profile.Month), Time.(mhourly_profile.Hours.-1)),
	[price_profile.Up_10_mean, price_profile.DK1_mean, price_profile.Down_10_mean],
	label = ["Up-regulation" "Spot" "Down-regulation"],
	xlabel = "Hour",
	ylabel = "EUR/MWh",
)

# Yearly-hourly marginal profile from 2016 data
marginal_profile_yearly = @linq df_market16 |>
	transform(π_plus = :DK1 - :Down_10) |>
	transform(π_minus = :Up_10 - :DK1) |>
	by(
	[:Hours],
	π_plus_mean = mean(:π_plus), π_minus_mean = mean(:π_minus)
)
# Plot monthly hourly marginal price profile (monthly average per hour)
plot(
	#DateTime.(Date.(2016, mhourly_profile.Month), Time.(mhourly_profile.Hours.-1)),
	[marginal_profile_yearly.π_plus_mean, marginal_profile_yearly.π_minus_mean],
	label = [L"\pi^{+}" L"\pi^{-}"],
	xlabel = "Hour",
	ylabel = "EUR/MWh",
	xticks = 1:24,
	ylims = (0, 5)
)

# Hourly profile per month from 2016 market data
marginal_profile = @linq df_market16 |>
	transform(Month = month.(:Datetime)) |>
	transform(π_plus = :DK1 - :Down_10) |>
	transform(π_minus = :Up_10 - :DK1) |>
	by(
	[:Month, :Hours],
	π_plus_mean = mean(:π_plus), π_minus_mean = mean(:π_minus)
)
# Plot monthly hourly marginal price profile (monthly average per hour)
plot(
	1:288,
	[marginal_profile.π_plus_mean, marginal_profile.π_minus_mean],
	label = [L"\pi^{+}" L"\pi^{-}"],
	xlabel = "Month",
	ylabel = "EUR/MWh",
	minorticks = 6,
	xticks = (
	1:24:288,
	["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]),
	size = (750, 300),
	dpi = 1000,
)
savefig("marginal_profile.png")

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
	1:288,
	[
	df_insight16.Down,
	df_insight16.Balance,
	df_insight16.Up,
	df_insight16.Both
	],
	ylims = (-20, 1000),
	legend = :topright,
	label = ["Down-regulation" "Balance" "Up-regulation" "Both"],
	xlabel = "Month",
	ylabel = "Occurences",
	xticks = (
	1:24:288,
	["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]),
	size = (1000, 300),
	dpi = 1000,
	linewidth = 2,
)
savefig("occurences.png")

# Example of January and November
df_january16 = filter(row -> row[:Month] == 1, df_insight16)
df_november16 = filter(row -> row[:Month] == 11, df_insight16)
plot(
	[
	df_january16.Down,
	df_january16.Balance,
	df_january16.Up,
	df_january16.Both
	],
	xaxis = 1:24,
	label = ["Down-regulation" "Balance" "Up-regulation" "Both"],
	xlabel = "Hour",
	ylabel = "Occurences",
)
plot(
	[
	df_november16.Down,
	df_november16.Balance,
	df_november16.Up,
	df_november16.Both
	],
	xaxis = 1:24,
	label = ["Down-regulation" "Balance" "Up-regulation" "Both"],
	xlabel = "Hour",
	ylabel = "Occurences"
)

# Divide the representative hours into three states
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
insertcols!(
	df_insight16,
	1,
	:Datetime => DateTime.(Date.(2016, df_insight16.Month), Time.(df_insight16.Hours.-1))
)
histogram(
	state,
	xticks = (1:5, ["Down-regulation" "Up-regulation" "Balance"])
)

# Obtain indexes of df_forecast with latest possible date (11am or at least 12 day before)
idx_match = zeros(Int64, length(dt_17))
for (i, date) in enumerate(dt_17)
	idxs = findall(x -> x == date, df_forecast.dato)
	for (j, idx) in enumerate(idxs)
		if (
			(hour(df_forecast.dati[idx]) == 11) ||
			(
			hour(df_forecast.dati[idx]) == 12 &&
			(date - df_forecast.dati[idx]) >= Millisecond(86400000) # 24 hours
			)
		)
			if idx_match[i] == 0
				idx_match[i] = idx
			elseif df_forecast.hors[idx] < df_forecast.hors[idxs[j-1]]
				idx_match[i] = idx
			end
		end
	end
end
missing_idx = findall(x-> x == 0, idx_match) #52 missing dates
missing_date = dt_17[missing_idx]


## Optimal

# Initialize empty arrays
revenue_optimal = zeros(length(dt_17))

# Calculate
for i in 1:length(dt_17)
	if idx_match[i] != 0
		production = df_forecast.meas[idx_match[i]] / 1000
		λ_s = df_market17.DK1[i]
		revenue_optimal[i] = production * λ_s
	end
end

# Total revenue
revenue_optimal_total = sum(revenue_optimal) # probabilistic_biding exactly as generated
γ_optimal = revenue_optimal_total / revenue_optimal_total * 100


## Base

# Initialize bid array
month_hours = [count(i-> i == j, month.(dt_17)) for j in 1:12]
month_days = month_hours/24
base_bid = create_bid(df_insight16.State, month_days)

# Initialize empty arrays
revenue_base_dayahead = zeros(length(dt_17))
revenue_base_balancing = zeros(length(dt_17))
full_capacity = 160 # [MW]
half_capacity = full_capacity * 0.5 # [MW]

# Calculate
for i in 1:length(dt_17)
	if idx_match[i] != 0
		if base_bid[i] == 1 || base_bid[i] == 3 # if in balance or in down-regulation state
			forecast = full_capacity
		else # if in up-regulation
			forecast = half_capacity
		end
		production = df_forecast.meas[idx_match[i]] / 1000
		λ_s = df_market17.DK1[i]
		λ_down = df_market17.Down_10[i]
		λ_up = df_market17.Up_10[i]
		revenue_base_dayahead[i] = forecast * λ_s
		if production > forecast
			revenue_base_balancing[i] = (production - forecast) * λ_down
		elseif production < forecast
			revenue_base_balancing[i] = -(forecast - production) * λ_up
		end
	end
end

# Total revenue and performance ratio
revenue_base = revenue_base_dayahead + revenue_base_balancing
revenue_base_dayahead_total = sum(revenue_base_dayahead)
revenue_base_balancing_total = sum(revenue_base_balancing)
revenue_base_total = revenue_base_dayahead_total + revenue_base_balancing_total
γ_base = revenue_base_total / revenue_optimal_total * 100


## Deterministic

# Baseline

# Initialize empty arrays
revenue_det_dayahead = zeros(length(dt_17))
revenue_det_balancing = zeros(length(dt_17))

# Calculate
for i in 1:length(dt_17)
	if idx_match[i] != 0
		production = df_forecast.meas[idx_match[i]] / 1000
		forecast = df_forecast.fore[idx_match[i]] / 1000
		λ_s = df_market17.DK1[i]
		λ_down = df_market17.Down_10[i]
		λ_up = df_market17.Up_10[i]
		revenue_det_dayahead[i] = forecast * λ_s
		if production > forecast
			revenue_det_balancing[i] = (production - forecast) * λ_down
		elseif production < forecast
			revenue_det_balancing[i] = -(forecast - production) * λ_up
		end
	end
end

# Total revenue and performance ratio
revenue_det = revenue_det_dayahead + revenue_det_balancing
revenue_det_dayahead_total = sum(revenue_det_dayahead)
revenue_det_balancing_total = sum(revenue_det_balancing)
revenue_det_total = revenue_det_dayahead_total + revenue_det_balancing_total
γ_det= revenue_det_total / revenue_optimal_total * 100


# Baseline plus 1% increase

# Initialize increase variable and empty arrays
increase = 1.01
revenue_det2_dayahead = zeros(length(dt_17))
revenue_det2_balancing = zeros(length(dt_17))

# Calculate
for i in 1:length(dt_17)
	if idx_match[i] != 0
		production = df_forecast.meas[idx_match[i]] / 1000
		forecast = (df_forecast.fore[idx_match[i]] / 1000) * increase
		λ_s = df_market17.DK1[i]
		λ_down = df_market17.Down_10[i]
		λ_up = df_market17.Up_10[i]
		revenue_det2_dayahead[i] = forecast * λ_s
		if production > forecast
			revenue_det2_balancing[i] = (production - forecast) * λ_down
		elseif production < forecast
			revenue_det2_balancing[i] = -(forecast - production) * λ_up
		end
	end
end

# Revenue total and performance ratio
revenue_det2 = revenue_det2_dayahead + revenue_det2_balancing
revenue_det2_dayahead_total = sum(revenue_det2_dayahead)
revenue_det2_balancing_total = sum(revenue_det2_balancing)
revenue_det2_total = revenue_det2_dayahead_total + revenue_det2_balancing_total
γ_det2 = revenue_det2_total / revenue_optimal_total * 100


## Probabilistic

# Initialize empty arrays
revenue_prob_balancing = zeros(length(dt_17))
revenue_prob_dayahead = zeros(length(dt_17))
prob_bid = zeros(length(dt_17))

# Create average marginal prices profiles per hour per month
π_plus = create_bid(marginal_profile.π_plus_mean, month_days)
π_minus = create_bid(marginal_profile.π_minus_mean, month_days)
α = π_plus./(π_plus + π_minus)

# Example plot for 4 am of 2/2/2017
plot(
	[df_forecast[2449, col]/1000 for col in 6:24],
	0.05:0.05:0.95,
	label="Forecast",
	legend = :topleft,
	xlabel = "Forecasted power [MW]",
	ylabel = "Probability",
	ylims = (0, 1),
	xlims = (df_forecast[2449, 6]/1000, (df_forecast[2449, 24]/1000)+1),
	formatter = :auto,
	markershape = :auto
)
plot!(
	[df_forecast[2449, 6]/1000, df_forecast[2449, 13]/1000, df_forecast[2449, 13]/1000],
	[0.4, 0.4, 0],
	label = LaTeXString("Optimal quantile\\alpha")
)
savefig("optimal_quantile.png")

# Find bid at optimal quantile
for i in 1:length(dt_17)
	if idx_match[i] != 0
		quants = [df_forecast[idx_match[i], col] for col in 6:24]
		itp = interpolate(quants, BSpline(Linear()))
		sitp = scale(itp, 0.05:0.05:0.95)
		if α[i] > 0.95
			prob_bid[i] = sitp(0.95)
		elseif α[i] < 0.05
			prob_bid[i] = sitp(0.05)
		else
			prob_bid[i] = sitp(α[i])
		end
	end
end

# Calculate
for i in 1:length(dt_17)
	if idx_match[i] != 0
		production = df_forecast.meas[idx_match[i]] / 1000
		forecast = prob_bid[i] / 1000
		λ_s = df_market17.DK1[i]
		λ_down = df_market17.Down_10[i]
		λ_up = df_market17.Up_10[i]
		revenue_prob_dayahead[i] = forecast * λ_s
		if production > forecast
			revenue_prob_balancing[i] = (production - forecast) * λ_down
		elseif production < forecast
			revenue_prob_balancing[i] = -(forecast - production) * λ_up
		end
	end
end

# Revenue total and performance ratio
revenue_prob = revenue_prob_dayahead + revenue_prob_balancing
revenue_prob_dayahead_total = sum(revenue_prob_dayahead)
revenue_prob_balancing_total = sum(revenue_prob_balancing)
revenue_prob_total = revenue_prob_dayahead_total + revenue_prob_balancing_total
γ_prob = revenue_prob_total / revenue_optimal_total * 100


## Final data viz

revenue_optimal_acc = accumulate(+, revenue_optimal)
revenue_base_acc = accumulate(+, revenue_base)
revenue_det_acc = accumulate(+, revenue_det)
revenue_det2_acc = accumulate(+, revenue_det2)
revenue_prob_acc = accumulate(+, revenue_prob)

γ = Dict("γ_base" => γ_base, "γ_det" => γ_det, "γ_det2" => γ_det2, "γ_prob" => γ_prob)
revenue = [revenue_optimal, revenue_base, revenue_det, revenue_det2, revenue_prob]
revenue_acc = [revenue_optimal_acc, revenue_base_acc, revenue_det_acc, revenue_det2_acc, revenue_prob_acc]

plot(
	dt_17,
	revenue_acc,
	labels = ["Optimal" "Base" "Deterministic" "Deterministic II" "Probabilistic"],
	legend = :topleft,
	xlabel = "Date",
	ylabel = "EUR",
	yformatter = :auto,
)
